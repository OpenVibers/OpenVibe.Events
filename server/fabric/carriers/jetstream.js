'use strict';
/**
 * nats-js-v1 — the JetStream STREAM carrier (ADR-042 decisions 1, 2, 6: low-latency, durable, cell-local), only when
 * NATS_URL is set and NATS_JETSTREAM is not `off`. Without it, or with EVENTS_CARRIERS_DISABLED=nats-js-v1, nothing
 * connects and STREAM stays on pg-v1.
 *
 * PostgreSQL stays the record. After a publish commits, signal(rows) publishes one JetStream message per new STREAM
 * delivery — only `{ event_id, subscription_id }`, never content — on `<prefix>fabric.stream.<key>` of the stream
 * NATS_STREAM (default OV_EVENTS_STREAM), and waits for the stream's ack. `<key>` is a subject-safe hash of the
 * ordering key (a key may hold dots, spaces or wildcards), so one key is one subject.
 *
 * Every process that runs the delivery worker has its own durable push consumer on that stream (deliver_policy new,
 * ack explicit, max_ack_pending 1) and handles its messages one at a time in stream order: claim exactly that row
 * (handlers.claim -> store.claimDelivery: the same key-head lease, max_inflight and due rules as the poll), send it
 * through the worker (handlers.deliver), then ack. The message is acked whatever the claim answered: a null claim
 * means another process or the poll has it, it is not its key's head yet, it is done, or it was never committed (a
 * forged message names nothing) — the row stays pending and the 500 ms poll finds it when it is due. A process that
 * dies before its ack gets the message again after ack_wait (same durable on restart): the claim finds the row
 * delivered or leased, so the redelivery sends nothing. Per-key order is the store's (a key's head first, one lease
 * per key); the stream order only makes the head usually arrive first. A durable left by a crashed process is removed
 * by the broker after inactive_threshold; a clean stop deletes it.
 *
 * The JetStream API ($JS.API.*) is spoken as JSON over ../nats-core.js request/reply, without a client library: Events
 * needs only stream info/create, consumer create/delete, publish-with-ack and ack. Replies come back under
 * `<prefix>_inbox.`, so the broker user needs only `<prefix>>` plus that stream's API and ack subjects
 * (deploy/nats/nats-server.conf). Needs nats-server 2.10+ started with -js.
 *
 * Health: connected, no permissions violation, the stream ensured on this connection (re-checked after every
 * reconnect, retried while it fails) and the breaker on consecutive publish errors (../signals.js). While any fails the
 * planner drops nats-js-v1 with the reason from unhealthyReason() and pg-v1 carries STREAM; nothing is lost because
 * nothing depends on the message.
 */
const crypto = require('crypto');
const { createNatsCore } = require('../nats-core');
const { createSignals } = require('../signals');

const STREAM = 'OV_EVENTS_STREAM';
const SUBJECT_ROOT = 'fabric.stream';
const NS = 1e6;   // JetStream durations are nanoseconds
const ID_MAX = 64;

/** The subject token of an ordering key: unambiguous, fixed length, always subject-safe. */
function keyToken(key) {
    return key == null || key === '' ? '_' : crypto.createHash('sha256').update(String(key)).digest('base64url').slice(0, 22);
}

function createJetStreamCarrier({ url, subjectPrefix = 'ov.events.', stream = STREAM, clock = { now: () => Date.now() }, log = console, instanceId,
    consumerName = null, ackWaitMs = 60000, maxAgeMs = 24 * 3600 * 1000, requestTimeoutMs = 2000, publishTimeoutMs = 1000, retryMs = 5000, deniedRetryMs, inactiveMs = 3600 * 1000 } = {}) {
    if (!/^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*\.$/.test(subjectPrefix)) throw new Error(`NATS_SUBJECT_PREFIX "${subjectPrefix}" must be dot-separated tokens ending in a dot`);
    if (!/^[A-Za-z0-9_-]{1,64}$/.test(stream)) throw new Error(`NATS_STREAM "${stream}" must be 1-64 letters, digits, _ or -`);
    const signals = createSignals({ now: () => clock.now() });
    const durable = (consumerName || `events_${String(instanceId || crypto.randomBytes(6).toString('hex'))}`).replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 64);
    const subjects = `${subjectPrefix}${SUBJECT_ROOT}.>`;
    const deliverSubject = `${subjectPrefix}fabric.jsdeliver.${durable}`;
    const ackPrefix = `$JS.ACK.${stream}.${durable}.`;
    let handlers = null;      // set by start() when this process consumes
    let streamReady = false;  // the stream (and our consumer, when consuming) exist on the current connection
    let lastError = null;
    let ensuring = null;
    let retryTimer = null;
    let retryDelay = retryMs;        // doubles up to a minute while the stream cannot be ensured (no -js, refused)
    let chain = Promise.resolve();   // consumer messages, one after another
    let unsubscribe = null;
    let stopped = false;

    const nats = createNatsCore({
        url, name: `openvibe-events js ${instanceId || durable}`, log, probe: [deliverSubject], deniedRetryMs,
        inboxPrefix: `${subjectPrefix}_inbox.${crypto.randomBytes(8).toString('hex')}.`,
        onConnect: () => { streamReady = false; ensure(); },
    });

    async function api(subject, body = {}) {
        const r = await nats.request(`$JS.API.${subject}`, body, { timeoutMs: requestTimeoutMs });
        if (r && r.error) throw Object.assign(new Error(`${subject}: ${r.error.description || 'error'} (${r.error.err_code || r.error.code})`), { jsCode: r.error.err_code, status: r.error.code });
        return r;
    }

    // Stream, then (when consuming) our durable push consumer. Idempotent: an existing stream is used as it is.
    function ensure() {
        if (ensuring || stopped) return ensuring;
        ensuring = (async () => {
            try {
                try { await api(`STREAM.INFO.${stream}`); }
                catch (err) {
                    if (err.jsCode !== 10059 && err.status !== 404) throw err;
                    await api(`STREAM.CREATE.${stream}`, {
                        name: stream, subjects: [subjects], retention: 'limits', storage: 'file', discard: 'old',
                        max_age: maxAgeMs * NS, max_msg_size: 1024, duplicate_window: 120000 * NS, num_replicas: 1,
                    }).catch((e) => { if (e.jsCode !== 10058) throw e; });   // another process created it meanwhile
                }
                if (handlers) {
                    await api(`CONSUMER.CREATE.${stream}.${durable}`, {
                        stream_name: stream,
                        config: {
                            durable_name: durable, deliver_subject: deliverSubject, filter_subject: subjects,
                            deliver_policy: 'new', ack_policy: 'explicit', ack_wait: ackWaitMs * NS, max_ack_pending: 1,
                            max_deliver: 5, replay_policy: 'instant', inactive_threshold: inactiveMs * NS,
                        },
                    });
                }
                streamReady = true; lastError = null; retryDelay = retryMs;
            } catch (err) {
                streamReady = false; lastError = err.message;
                log.warn(`[nats-js-v1] stream not ready: ${err.message}`);
                if (!retryTimer && !stopped) {
                    retryTimer = setTimeout(() => { retryTimer = null; if (nats.connected()) ensure(); }, retryDelay);
                    retryTimer.unref?.();
                    retryDelay = Math.min(retryDelay * 2, 60000);
                }
            } finally { ensuring = null; }
        })();
        return ensuring;
    }

    async function handle(msg, reply) {
        // Only JetStream's own deliveries of this consumer carry an ack subject; anything else on the subject is ignored.
        if (!reply || !reply.startsWith(ackPrefix)) return;
        try {
            const ok = msg && typeof msg.event_id === 'string' && typeof msg.subscription_id === 'string'
                && msg.event_id.length <= ID_MAX && msg.subscription_id.length <= ID_MAX;
            const row = ok ? await handlers.claim(msg.event_id, msg.subscription_id) : null;
            if (row) await handlers.deliver(row);
        } catch (err) {
            log.warn(`[nats-js-v1] delivery ${msg && msg.event_id}: ${err.message}`);   // acked anyway: the poll retries
        }
        try { nats.publishRaw(reply, ''); } catch { /* not acked: redelivered after ack_wait, and the claim dedupes */ }
    }

    async function signalStream(rows) {
        if (!streamReady) return;   // not ready: the poll carries these (the planner already prefers pg-v1)
        const t0 = clock.now();
        try {
            const acks = await Promise.all(rows.map((r) => nats.request(`${subjectPrefix}${SUBJECT_ROOT}.${keyToken(r.ordering_key)}`,
                { event_id: r.event_id, subscription_id: r.subscription_id }, { timeoutMs: publishTimeoutMs })));
            const bad = acks.find((a) => !a || a.error || a.stream !== stream);
            if (bad) throw new Error(`stream publish refused: ${(bad && bad.error && bad.error.description) || JSON.stringify(bad)}`);
            signals.record('stream', true, clock.now() - t0);
        } catch (err) {
            signals.record('stream', false);
            log.warn(`[nats-js-v1] stream publish: ${err.message}`);
        }
    }

    return {
        id: 'nats-js-v1',
        classes: new Set(['STREAM']),
        client: nats,
        signals,
        durable,

        async start(h = {}) {
            stopped = false;
            // The consumer is a delivery path: only where the delivery worker runs (EVENTS_WORKER), as valkey-v1's QUEUE.
            if (h.consumeQueue !== false && h.claim && h.deliver) {
                handlers = h;
                unsubscribe = nats.subscribe(deliverSubject, (msg, meta) => { chain = chain.then(() => handle(msg, meta.reply)); });
            }
            // Not fatal: the client keeps reconnecting, and nats-js-v1 is ineligible (not connected) until it is up.
            await nats.connect().catch((err) => log.warn(`[nats-js-v1] not connected yet: ${err.message}`));
            if (ensuring) await ensuring;
        },

        async stop() {
            stopped = true;
            if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
            if (unsubscribe) { await unsubscribe().catch(() => {}); unsubscribe = null; }
            await chain;
            if (handlers && nats.connected()) await api(`CONSUMER.DELETE.${stream}.${durable}`).catch(() => {});
            await nats.close();
        },

        signal(rows, kind) { return kind === 'STREAM' ? signalStream(rows) : undefined; },
        healthy: () => nats.connected() && !nats.denied() && streamReady && signals.healthy('stream'),
        unhealthyReason: () => (!nats.connected() ? 'not connected (nats-js-v1)' : nats.denied() ? 'permission denied (nats-js-v1)'
            : !streamReady ? `stream not ready (nats-js-v1)${lastError ? `: ${lastError}` : ''}` : !signals.healthy('stream') ? 'breaker open (nats-js-v1)' : null),

        offer() {
            const ms = signals.ewma('stream') || 1;
            return {
                offer_id: 'nats-js-v1',
                kind: 'provider',
                provider: 'nats',
                adapter: 'nats-js-v1',
                region: 'cell',
                trust: 'first-party',
                capabilities: ['events:gateway', 'events:durable', 'events:ordered'],
                latency_ms: { publish_p95: ms, ack_p95: ms },
                health: { status: 'up', checked_at: new Date(clock.now()).toISOString() },
                pricing: { model: 'per-operation', unit: 'stream-operation', rate_card: 'rc-nats-js-v1' },
                updated_at: new Date(clock.now()).toISOString(),
            };
        },
    };
}

module.exports = { createJetStreamCarrier, STREAM, SUBJECT_ROOT, keyToken };
