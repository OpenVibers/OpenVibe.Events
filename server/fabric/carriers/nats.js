'use strict';
/**
 * nats-v1 — a NATS Core TOPIC carrier (ADR-042 decision 5), only when NATS_URL is set. Optional: without it, or with
 * EVENTS_CARRIERS_DISABLED=nats-v1, nothing connects and TOPIC stays on valkey-v1 or pg-v1.
 *
 * TOPIC: after a publish commits, signal(rows) PUBLISHes the stored envelopes on the subject
 * `<NATS_SUBJECT_PREFIX>fabric.topic`, tagged with this process's id. Every process subscribes and re-emits rows from
 * OTHER processes to its own SSE clients, deduped by event id with a small LRU (the gateway's per-connection seq check
 * is the second guard); visibility and topic matching are applied by the receiving gateway exactly as for local
 * events. Core NATS is at most once and keeps nothing: a lost message reaches a remote browser on its next resume
 * (Last-Event-ID replays from PostgreSQL, the record), a duplicate is dropped. A batch over the server's max_payload
 * is sent row by row; a single row over it is skipped (the resume carries it).
 *
 * Health: connected (the socket is up and the handshake done) and the breaker on consecutive publish errors
 * (../signals.js). While either fails the planner drops nats-v1 with the reason from unhealthyReason(); the client
 * reconnects in the background and the adapter is eligible again once it is back.
 *
 * JetStream (STREAM) is not built here: STREAM stays on pg-v1.
 */
const { createNatsCore } = require('../nats-core');
const { createSignals } = require('../signals');

const TOPIC_SUBJECT = 'fabric.topic';
const SEEN_MAX = 5000;

function createNatsCarrier({ url, subjectPrefix = 'ov.events.', clock = { now: () => Date.now() }, log = console, instanceId } = {}) {
    if (!/^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*\.$/.test(subjectPrefix)) throw new Error(`NATS_SUBJECT_PREFIX "${subjectPrefix}" must be dot-separated tokens ending in a dot`);
    const signals = createSignals({ now: () => clock.now() });
    const nats = createNatsCore({ url, name: `openvibe-events ${instanceId}`, log });
    const subject = `${subjectPrefix}${TOPIC_SUBJECT}`;
    const seen = new Map();   // event id -> first-seen ms
    let unsubscribe = null;

    function markSeen(id) {
        seen.set(id, clock.now());
        if (seen.size > SEEN_MAX) {
            const cut = clock.now() - 300000;
            for (const [k, t] of seen) { if (t < cut) seen.delete(k); if (seen.size <= SEEN_MAX) break; }
            while (seen.size > SEEN_MAX) seen.delete(seen.keys().next().value);
        }
    }

    function send(rows) {
        try {
            nats.publish(subject, { from: instanceId, rows });
        } catch (err) {
            if (err.code !== 'max_payload') throw err;
            if (rows.length === 1) { log.warn(`[nats-v1] event ${rows[0].id} over max_payload: left to the resume`); return; }
            for (const r of rows) send([r]);
        }
    }

    async function signalTopic(rows) {
        const t0 = clock.now();
        try {
            send(rows);
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
            unsubscribe = nats.subscribe(subject, (msg) => {
                if (!msg || msg.from === instanceId) return;
                const rows = (msg.rows || []).filter((r) => { if (!r || seen.has(r.id)) return false; markSeen(r.id); return true; });
                if (rows.length && handlers.onRemote) { try { handlers.onRemote(rows); } catch (err) { log.warn(`[nats-v1] remote fan-out: ${err.message}`); } }
            });
            // Not fatal: the client keeps reconnecting, and nats-v1 is ineligible (not connected) until it is up.
            await nats.connect().catch((err) => log.warn(`[nats-v1] not connected yet: ${err.message}`));
        },

        async stop() {
            if (unsubscribe) { await unsubscribe().catch(() => {}); unsubscribe = null; }
            await nats.close();
        },

        signal(rows, kind) { return kind === 'TOPIC' ? signalTopic(rows) : undefined; },
        healthy: () => nats.connected() && signals.healthy('topic'),
        unhealthyReason: () => (!nats.connected() ? 'not connected (nats-v1)' : !signals.healthy('topic') ? 'breaker open (nats-v1)' : null),

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
