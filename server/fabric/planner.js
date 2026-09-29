'use strict';
/**
 * The planner (ADR-042 decision 5): which carrier carries a (carrier class, ordering key). It calls
 * openvibe-sdk/placement's plan() with requirements `{ kind: 'events.deliver', mobility: 'stateful-partition' }`
 * plus the policy's latency/durability/ordering, against one resource-offer@1 per carrier adapter and the rate cards
 * in ./rate-cards.json. plan() filters by hard constraints, then scores by cost and latency with hysteresis (a
 * candidate replaces the current one only when it is clearly better, the SDK's default minGain) — so a decision does
 * not flap.
 *
 * Decisions are cached per (class, key) in a bounded LRU (10k) and written to `delivery_placements` only when a
 * decision CHANGES, so a past delivery can be explained from history. The chosen adapter id is written on every
 * delivery at fan-out (store.js). `pg-v1` is the floor: whenever no candidate is eligible the plan falls back to it.
 *
 * Pinning (decision 4) is applied by the store, not here: while a (subscription, key) still has pending/failed
 * deliveries on carrier X, new deliveries of that key stay on X; the store passes the pinned carrier through.
 */
const fs = require('fs');
const path = require('path');
const { plan } = require('openvibe-sdk/placement');

const RATE_CARDS = JSON.parse(fs.readFileSync(path.join(__dirname, 'rate-cards.json'), 'utf8'));
const CACHE_MAX = 10000;

// The policy's routing objective vocabulary maps onto the platform objective enum.
const OBJECTIVES = { latency_first: 'lowest-latency', cheapest: 'cheapest', balanced: 'balanced', correctness: 'correctness' };
// A policy class maps to the placement latency class (a label the objective scoring can weigh).
const LATENCY_CLASS = {
    realtime_ephemeral: 'interactive', interactive: 'interactive', interactive_durable: 'interactive',
    domain: 'interactive', critical: 'critical', background: 'background', bulk: 'bulk', webhook: 'interactive',
};

function requirements(klass, key, policy) {
    const lat = (policy && policy.latency) || {};
    const objective = OBJECTIVES[(policy && policy.routing && policy.routing.objective) || 'balanced'] || 'balanced';
    return {
        kind: 'events.deliver',
        mobility: 'stateful-partition',
        latency_class: LATENCY_CLASS[klass] || 'interactive',
        objective,
        partition_key: key || undefined,
        capabilities: ['events:gateway'],
        latency_op: 'ack_p95',
        ...(Number.isFinite(lat.maximum_p95_ms) ? { max_latency_ms: lat.maximum_p95_ms } : {}),
        units: 1,
    };
}

function decisionRow(r) {
    return r ? { carrier: r.carrier, decided_at: Number(r.decided_at), result: typeof r.result === 'string' ? JSON.parse(r.result) : r.result } : null;
}

function createPlanner({ registry, db = null, clock = { now: () => Date.now() }, log = console, cacheMax = CACHE_MAX } = {}) {
    const cache = new Map();   // `${class}\0${key}` -> { carrier, result, at }

    function decide(klass, key, policy, { update }) {
        const ck = `${klass}\u0000${key || ''}`;
        const current = cache.get(ck) || null;
        const now = clock.now();
        const excluded = [];
        const offers = [];
        for (const a of registry.forClass(klass)) {
            if (a.disabledReason) { excluded.push({ id: a.id, eligible: false, excluded_because: a.disabledReason }); continue; }
            const o = a.offer(now);
            if (!a.healthy()) o.health = { ...o.health, status: 'down', checked_at: new Date(now).toISOString() };
            offers.push(o);
        }
        const r = plan(requirements(klass, key, policy), offers, { rateCards: RATE_CARDS, now, current: current ? current.carrier : null });
        // The offer's health already excludes an unhealthy adapter; name the breaker in the reason.
        for (const c of r.candidates) {
            const a = registry.get(c.id);
            if (a && !a.disabledReason && !a.healthy() && !c.eligible) c.excluded_because = `breaker open (${a.id})`;
        }
        for (const e of excluded) if (!r.candidates.some((c) => c.id === e.id)) r.candidates.push(e);
        if (!r.selected) {
            const floor = registry.eligible(klass).find((a) => a.id === 'pg-v1') || registry.eligible(klass)[0];
            r.selected = floor ? floor.id : 'pg-v1';
            r.reasons.push(`no eligible candidate: falling back to ${r.selected}`);
        }
        const carrier = r.selected;
        let record = null;
        if (update) {
            if (!current || current.carrier !== carrier) {
                record = { carrier_class: klass, ordering_key: key || null, carrier, result: r, decided_at: now };
            }
            cache.set(ck, { carrier, result: r, at: now });
            if (cache.size > cacheMax) { const oldest = cache.keys().next().value; cache.delete(oldest); }
        }
        return { carrier, result: r, record };
    }

    return {
        rateCards: RATE_CARDS,
        place: (klass, key, policy) => decide(klass, key, policy, { update: true }),
        explain: (klass, key, policy) => decide(klass, key, policy, { update: false }),
        clear: () => cache.clear(),
        size: () => cache.size,

        /** The most recent recorded decision for a (class, key), or null. */
        async lastDecision(klass, key) {
            if (!db) return null;
            const r = await db.prepare(`SELECT carrier, decided_at, result FROM delivery_placements
                WHERE carrier_class = ? AND ordering_key IS NOT DISTINCT FROM ? ORDER BY decided_at DESC, id DESC LIMIT 1`)
                .get(klass, key == null ? null : key);
            return decisionRow(r);
        },

        /** The decision in force when a delivery was created: the latest row at or before `ts`, or null. */
        async decisionBefore(klass, key, ts) {
            if (!db) return null;
            const r = await db.prepare(`SELECT carrier, decided_at, result FROM delivery_placements
                WHERE carrier_class = ? AND ordering_key IS NOT DISTINCT FROM ? AND decided_at <= ? ORDER BY decided_at DESC, id DESC LIMIT 1`)
                .get(klass, key == null ? null : key, ts);
            return decisionRow(r);
        },
    };
}

module.exports = { createPlanner, RATE_CARDS };
