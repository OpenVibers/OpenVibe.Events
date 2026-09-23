'use strict';
/**
 * Delivery worker. Pending deliveries are POSTed to their subscription endpoints as
 *
 *   { "event": <envelope>, "seq": <n> }
 *
 * with X-OpenVibe-Event-Id, X-OpenVibe-Signature (sha256=<HMAC of the raw body with the
 * subscription secret>), traceparent (the event's trace). A 2xx is delivered; anything else
 * (including redirects, which are never followed) is retried with backoff 1s, 5s, 30s, 2m, 10m, 1h
 * up to maxAttempts, then the delivery is `dead` (the DLQ; replay requeues it).
 *
 * App subscriptions (events.app.subscribe) go through server/egress.js instead of fetch: https to a
 * public address only, re-resolved and checked on every attempt, the connection pinned to the checked
 * address, redirects refused. A refused address is permanent (dead at once; replay requeues it).
 *
 * Scheduling: a setTimeout chain (never overlapping ticks); priority classes critical > important >
 * low, then seq; one delivery in flight per subscription; at most maxInflight overall.
 */
const crypto = require('crypto');
const { sign } = require('../lib/client');
const { rowToEnvelope } = require('./store');
const { checkEndpoint } = require('./endpoints');
const { createGuardedPost } = require('./egress');

// observe (optional): { delivered(seconds, row), attempt(outcome) } — metrics hooks, never required.
function createWorker({ store, config, clock = { now: () => Date.now() }, fetchImpl = globalThis.fetch, appPost = createGuardedPost(), log = console, observe = null }) {
    const opts = config.worker;
    const busy = new Set();          // subscription ids with a delivery in flight
    const inflight = new Set();      // promises
    let timer = null;
    let running = false;
    let lastTickAt = null;

    function policy(sub) {
        let p = null;
        try { p = sub.retry_policy ? JSON.parse(sub.retry_policy) : null; } catch { p = null; }
        const maxAttempts = Number.isInteger(p && p.max_attempts) && p.max_attempts >= 1 && p.max_attempts <= 20 ? p.max_attempts : opts.maxAttempts;
        const backoffMs = Array.isArray(p && p.backoff_ms) && p.backoff_ms.length && p.backoff_ms.every(n => Number.isInteger(n) && n >= 0 && n <= 86400000)
            ? p.backoff_ms : opts.backoffMs;
        return { maxAttempts, backoffMs };
    }

    async function send(delivery) {
        const sub = store.getSubscription(delivery.subscription_id);
        const row = store.getEvent(delivery.event_id);
        const attempt = delivery.attempt + 1;
        if (!sub || !row) return; // pruned or removed while queued; cascade deletes the delivery
        const { maxAttempts, backoffMs } = policy(sub);
        let outcome;
        try {
            const body = JSON.stringify({ event: rowToEnvelope(row), seq: row.seq });
            const headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'OpenVibe.Events/0.1',
                'X-OpenVibe-Event-Id': row.id,
                'X-OpenVibe-Event-Type': row.event_type,
                'X-OpenVibe-Seq': String(row.seq),
                'X-OpenVibe-Subscription-Id': sub.id,
                'X-OpenVibe-Delivery-Attempt': String(attempt),
                'X-OpenVibe-Hops': String(row.hops),
                'X-OpenVibe-Signature': sign(body, sub.secret),
                traceparent: `00-${row.trace_id}-${crypto.randomBytes(8).toString('hex')}-01`,
            };
            let res;
            if (sub.project_id) {
                // Developer-app endpoint: public https only, checked again now (egress.js).
                res = await appPost(sub.endpoint, { headers, body, timeoutMs: opts.timeoutMs });
            } else {
                // Re-check the endpoint at send time too (the allow-list may have been tightened).
                const check = checkEndpoint(sub.endpoint, config.endpointHosts);
                if (!check.ok) throw Object.assign(new Error(`endpoint not allowed: ${check.reason}`), { permanent: true });
                res = await fetchImpl(sub.endpoint, {
                    method: 'POST',
                    redirect: 'manual',
                    signal: AbortSignal.timeout(opts.timeoutMs),
                    headers,
                    body,
                });
                try { await res.body?.cancel(); } catch { /* body is not needed */ }
            }
            if (res.status >= 200 && res.status < 300) outcome = { ok: true, attempt, status: res.status };
            else outcome = { ok: false, attempt, status: res.status, error: `HTTP ${res.status}` };
        } catch (err) {
            outcome = { ok: false, attempt, status: null, error: err.name === 'TimeoutError' ? 'timeout' : (err.message || String(err)), permanent: Boolean(err.permanent) };
        }
        if (!outcome.ok) {
            outcome.dead = outcome.permanent || attempt >= maxAttempts;
            if (!outcome.dead) outcome.nextAttemptAt = clock.now() + backoffMs[Math.min(attempt - 1, backoffMs.length - 1)];
            if (outcome.dead) log.warn(`[worker] ${row.id} -> ${sub.id} dead after ${attempt} attempts: ${outcome.error}`);
        }
        store.recordAttempt(delivery.event_id, delivery.subscription_id, outcome);
        if (observe) {
            try {
                observe.attempt(outcome.ok ? 'delivered' : outcome.dead ? 'dead' : 'retry');
                // Acceptance to successful delivery, retries included.
                if (outcome.ok) observe.delivered(Math.max(0, clock.now() - row.received_at) / 1000, row);
            } catch { /* metrics never break delivery */ }
        }
    }

    /** Start every due delivery there is room for. Returns how many were started. */
    function dispatch() {
        lastTickAt = Date.now();
        const room = opts.maxInflight - inflight.size;
        if (room <= 0) return 0;
        const due = store.dueDeliveries(clock.now(), room, [...busy]);
        for (const d of due) {
            busy.add(d.subscription_id);
            const p = send(d)
                .catch(err => log.error(`[worker] delivery ${d.event_id} -> ${d.subscription_id} crashed: ${err.stack || err}`))
                .finally(() => {
                    busy.delete(d.subscription_id);
                    inflight.delete(p);
                    if (running) schedule(0);
                });
            inflight.add(p);
        }
        return due.length;
    }

    function schedule(ms) {
        if (!running) return;
        if (timer && ms > 0) return; // a tick is already coming
        clearTimeout(timer);
        timer = setTimeout(() => {
            timer = null;
            try { dispatch(); } catch (err) { log.error(`[worker] tick failed: ${err.stack || err}`); }
            schedule(opts.intervalMs);
        }, ms);
        timer.unref?.();
    }

    /** Deliver everything due now (and whatever becomes due as a result), then resolve. Tests. */
    async function drain() {
        for (;;) {
            const started = dispatch();
            if (!started && !inflight.size) return;
            if (inflight.size) await Promise.race([...inflight]);
        }
    }

    return {
        start() { if (!running) { running = true; schedule(0); } },
        async stop() { running = false; clearTimeout(timer); timer = null; await Promise.allSettled([...inflight]); },
        kick() { schedule(0); },
        drain,
        dispatch,
        running: () => running,
        stats: () => ({ running, inflight: inflight.size, busy_subscriptions: busy.size, last_tick_at: lastTickAt ? new Date(lastTickAt).toISOString() : null }),
    };
}

module.exports = { createWorker };
