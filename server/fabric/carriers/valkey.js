'use strict';
/**
 * valkey-v1 — push carriers for QUEUE and TOPIC (ADR-042 decisions 1 and 5), only when VALKEY_URL is set.
 *
 * QUEUE: after a publish commits, signal(rows) XADDs { event_id, subscription_id } — one entry per new delivery —
 * to one Valkey stream per carrier class through openvibe-sdk/queue (consumer group `workers`). Every Events process
 * consumes the group; for each item it asks the worker to claim exactly that one row (the same claimable + cap +
 * re-check rules as the poll) and sends it if it got it. The item is acked either way (a null claim means someone
 * else has it, it is not its key's head yet, or it is done: the 500 ms poll will find it when it is due). Nothing
 * depends on the stream — a lost or duplicated item changes latency, never the outcome.
 *
 * TOPIC: signal(rows) PUBLISHes the stored envelopes on a Valkey channel tagged with this process's id. Every
 * process subscribes and re-emits envelopes from OTHER processes to its own SSE clients, deduped by event id with a
 * small LRU; visibility and topic matching are applied by the receiving gateway exactly as for local events. So an
 * event published on one Events process reaches a browser connected to another.
 *
 * Health: an EWMA of signal/publish latency and a breaker on consecutive errors (../signals.js). healthy() is false
 * while any op's breaker is open, which drops valkey-v1 from the plan with a reason; the poll then carries delivery.
 */
const os = require('os');
const crypto = require('crypto');
const { createQueue } = require('openvibe-sdk/queue');
const { createPubSub } = require('openvibe-sdk/pubsub');
const { createSignals } = require('../signals');

const QUEUE_NAME = 'fabric.queue';     // one stream per carrier class (QUEUE)
const TOPIC_CHANNEL = 'fabric.topic';  // one channel per carrier class (TOPIC)
const SEEN_MAX = 5000;                 // event ids remembered for TOPIC dedupe

function createValkeyCarrier({ valkey, clock = { now: () => Date.now() }, log = console, instanceId } = {}) {
    const signals = createSignals({ now: () => clock.now() });
    const queue = createQueue({ valkey, name: QUEUE_NAME, group: 'workers', log });
    const pubsub = createPubSub({ valkey, log });
    const seen = new Map();   // event id -> first-seen ms (small LRU)
    let consumer = null;
    let unsubscribe = null;

    function markSeen(id) {
        seen.set(id, clock.now());
        if (seen.size > SEEN_MAX) {
            const cut = clock.now() - 300000;
            for (const [k, t] of seen) { if (t < cut) seen.delete(k); if (seen.size <= SEEN_MAX) break; }
        }
    }

    async function signalQueue(rows) {
        if (!consumer) return;   // nothing consumes the stream (the delivery worker is off): the poll carries delivery
        const t0 = clock.now();
        try {
            for (const r of rows) {
                await queue.add({ event_id: r.event_id, subscription_id: r.subscription_id }, { id: `${r.event_id}:${r.subscription_id}` });
            }
            signals.record('queue', true, clock.now() - t0);
        } catch (err) {
            signals.record('queue', false);
            log.warn(`[valkey-v1] queue signal: ${err.message}`);
        }
    }

    async function signalTopic(rows) {
        const t0 = clock.now();
        try {
            await pubsub.publish(TOPIC_CHANNEL, { from: instanceId, rows });
            signals.record('topic', true, clock.now() - t0);
        } catch (err) {
            signals.record('topic', false);
            log.warn(`[valkey-v1] topic publish: ${err.message}`);
        }
    }

    return {
        id: 'valkey-v1',
        classes: new Set(['QUEUE', 'TOPIC']),

        async start(handlers = {}) {
            // The QUEUE consumer is a delivery path: it runs only when the delivery worker does (EVENTS_WORKER). With
            // the worker off (a test driving its own worker, or an operator pausing delivery) there is no consumer and
            // signal() enqueues nothing. The TOPIC subscription always runs: it is the realtime fan-out.
            if (handlers.consumeQueue !== false) consumer = queue.process(async (data) => {
                try {
                    if (!handlers.claim || !handlers.deliver) return;
                    const row = await handlers.claim(data && data.event_id, data && data.subscription_id);
                    if (row) await handlers.deliver(row);
                } catch (err) {
                    log.warn(`[valkey-v1] delivery ${data && data.event_id}: ${err.message}`);   // acked anyway: the poll retries
                }
            }, { concurrency: 1 });   // in stream order: a key's head before its successors (a non-head claim is acked and falls to the poll)
            unsubscribe = await pubsub.subscribe(TOPIC_CHANNEL, (msg) => {
                if (!msg || msg.from === instanceId) return;
                const rows = (msg.rows || []).filter((r) => { if (seen.has(r.id)) return false; markSeen(r.id); return true; });
                if (rows.length && handlers.onRemote) { try { handlers.onRemote(rows); } catch (err) { log.warn(`[valkey-v1] remote fan-out: ${err.message}`); } }
            });
        },

        async stop() {
            if (unsubscribe) { await unsubscribe().catch(() => {}); unsubscribe = null; }
            await pubsub.close();
            if (consumer) { await consumer.stop(); consumer = null; }
        },

        signal(rows, kind) { return kind === 'TOPIC' ? signalTopic(rows) : signalQueue(rows); },
        healthy: () => signals.healthy('queue') && signals.healthy('topic'),
        signals,

        offer() {
            const queueMs = signals.ewma('queue') || 2;
            const topicMs = signals.ewma('topic') || 2;
            return {
                offer_id: 'valkey-v1',
                kind: 'provider',
                provider: 'valkey',
                adapter: 'valkey-v1',
                region: 'cell',
                trust: 'first-party',
                capabilities: ['events:gateway'],
                latency_ms: { publish_p95: topicMs, ack_p95: queueMs },
                health: { status: 'up', checked_at: new Date(clock.now()).toISOString() },
                pricing: { model: 'per-operation', unit: 'signal-operation', rate_card: 'rc-valkey-v1' },
                updated_at: new Date(clock.now()).toISOString(),
            };
        },
    };
}

function newInstanceId() { return `${os.hostname()}:${process.pid}:${crypto.randomBytes(4).toString('hex')}`; }

module.exports = { createValkeyCarrier, newInstanceId, QUEUE_NAME, TOPIC_CHANNEL };
