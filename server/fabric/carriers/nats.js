'use strict';
/**
 * nats-v1 — a NATS Core TOPIC carrier (ADR-042 decision 5), only when NATS_URL is set. Optional: without it, or with
 * EVENTS_CARRIERS_DISABLED=nats-v1, nothing connects and TOPIC stays on valkey-v1 or pg-v1.
 *
 * TOPIC: after a publish commits, signal(rows) PUBLISHes the event ids (never the envelopes) on the subject
 * `<NATS_SUBJECT_PREFIX>fabric.topic`, tagged with this process's id. Every process subscribes and, for ids from
 * OTHER processes, loads the committed rows from PostgreSQL (handlers.getEvent) and re-emits them to its own SSE
 * clients in arrival order, deduped by event id with a small LRU (the gateway's per-connection seq check is the second
 * guard). The broker is never trusted with content: an id that is not committed is ignored, so a message from anyone
 * else allowed on the subject can neither inject an event nor advance a connection's seq. Visibility and topic
 * matching are applied by the receiving gateway exactly as for local events. Core NATS is at most once and keeps
 * nothing: a lost message reaches a remote browser on its next resume (Last-Event-ID replays from PostgreSQL, the
 * record), a duplicate is dropped.
 *
 * Health: connected (the socket is up and the handshake done), no permissions violation from the broker (a refused
 * PUB or SUB keeps the connection open but drops the message; the handshake probes the subject) and the breaker on
 * consecutive publish errors (../signals.js). While any fails the planner drops nats-v1 with the reason from
 * unhealthyReason(); the client reconnects in the background and the adapter is eligible again once it is back.
 *
 * JetStream (STREAM) is ./jetstream.js (nats-js-v1), on the same broker and client.
 */
const { createNatsCore } = require('../nats-core');
const { createSignals } = require('../signals');

const TOPIC_SUBJECT = 'fabric.topic';
const SEEN_MAX = 5000;
const IDS_MAX = 1000;     // ids per message taken from the broker (a publish batch is smaller)

function createNatsCarrier({ url, subjectPrefix = 'ov.events.', clock = { now: () => Date.now() }, log = console, instanceId, deniedRetryMs } = {}) {
    if (!/^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*\.$/.test(subjectPrefix)) throw new Error(`NATS_SUBJECT_PREFIX "${subjectPrefix}" must be dot-separated tokens ending in a dot`);
    const signals = createSignals({ now: () => clock.now() });
    const subject = `${subjectPrefix}${TOPIC_SUBJECT}`;
    const nats = createNatsCore({ url, name: `openvibe-events ${instanceId}`, log, probe: [subject], deniedRetryMs });
    const seen = new Map();   // event id -> first-seen ms
    let unsubscribe = null;
    let remote = Promise.resolve();   // remote messages are handled one after another, in arrival order

    function markSeen(id) {
        seen.set(id, clock.now());
        if (seen.size > SEEN_MAX) {
            const cut = clock.now() - 300000;
            for (const [k, t] of seen) { if (t < cut) seen.delete(k); if (seen.size <= SEEN_MAX) break; }
            while (seen.size > SEEN_MAX) seen.delete(seen.keys().next().value);
        }
    }

    async function signalTopic(rows) {
        const t0 = clock.now();
        try {
            nats.publish(subject, { from: instanceId, ids: rows.map((r) => r.id) });
            signals.record('topic', true, clock.now() - t0);
        } catch (err) {
            signals.record('topic', false);
            log.warn(`[nats-v1] topic publish: ${err.message}`);
        }
    }

    return {
        id: 'nats-v1',
        classes: new Set(['TOPIC']),
        client: nats,
        signals,

        async start(handlers = {}) {
            const fanOut = async (ids) => {
                const rows = [];
                for (const id of ids) {
                    if (seen.has(id)) continue;
                    const row = await handlers.getEvent(id);
                    if (!row) continue;   // not committed (or pruned): nothing to re-emit
                    markSeen(id);
                    rows.push(row);
                }
                rows.sort((x, y) => x.seq - y.seq);
                if (rows.length) handlers.onRemote(rows);
            };
            unsubscribe = nats.subscribe(subject, (msg) => {
                if (!msg || msg.from === instanceId || !Array.isArray(msg.ids) || !handlers.onRemote || !handlers.getEvent) return;
                const ids = msg.ids.slice(0, IDS_MAX).filter((id) => typeof id === 'string' && id.length <= 64);
                if (!ids.length) return;
                remote = remote.then(() => fanOut(ids)).catch((err) => log.warn(`[nats-v1] remote fan-out: ${err.message}`));
            });
            // Not fatal: the client keeps reconnecting, and nats-v1 is ineligible (not connected) until it is up.
            await nats.connect().catch((err) => log.warn(`[nats-v1] not connected yet: ${err.message}`));
        },

        async stop() {
            if (unsubscribe) { await unsubscribe().catch(() => {}); unsubscribe = null; }
            await nats.close();
            await remote;
        },

        signal(rows, kind) { return kind === 'TOPIC' ? signalTopic(rows) : undefined; },
        healthy: () => nats.connected() && !nats.denied() && signals.healthy('topic'),
        unhealthyReason: () => (!nats.connected() ? 'not connected (nats-v1)' : nats.denied() ? 'permission denied (nats-v1)'
            : !signals.healthy('topic') ? 'breaker open (nats-v1)' : null),

        offer() {
            const topicMs = signals.ewma('topic') || 1;
            return {
                offer_id: 'nats-v1',
                kind: 'provider',
                provider: 'nats',
                adapter: 'nats-v1',
                region: 'cell',
                trust: 'first-party',
                capabilities: ['events:gateway'],
                latency_ms: { publish_p95: topicMs, ack_p95: topicMs },
                health: { status: 'up', checked_at: new Date(clock.now()).toISOString() },
                pricing: { model: 'per-operation', unit: 'signal-operation', rate_card: 'rc-nats-v1' },
                updated_at: new Date(clock.now()).toISOString(),
            };
        },
    };
}

module.exports = { createNatsCarrier, TOPIC_SUBJECT };
