'use strict';

/**
 * What the home page shows of the bus at work, read from this service's own tables and kept a few minutes:
 *
 *   createHomeStats({ store, now, ttlMs, log }) → { read() }
 *   read() → { day, week, types, services: [{ ns, n }], public: [{ type, n, last }],
 *             subscriptions, consumers, delivered, dead } | null
 *
 * Only counts and names leave this module: never a payload, an actor, a subject id or a publisher's principal. The
 * per-type list is the events whose visibility is `public` (the Contracts registry's own word); the
 * per-service counts are every event, by the first segment of its type. read() never throws: a failed read keeps the
 * last answer (null before the first), so the page leaves the sections out instead of failing.
 */
const DAY_MS = 24 * 60 * 60 * 1000;

function createHomeStats({ store, now = () => Date.now(), ttlMs = 5 * 60 * 1000, waitMs = 1500, log = console } = {}) {
    let cached = null;
    let at = 0;
    let pending = null;
    const db = store && store.db;

    async function load() {
        const t = now();
        const day = t - DAY_MS;
        const week = t - 7 * DAY_MS;
        // The week in one pass, by type and visibility (1.4 s on 1.1M events, 2026-11-02): the services, the number of
        // types and the public list all come from it. Three reads did the same, and COUNT(DISTINCT event_type) alone
        // took 7 s, past the query timeout, so the home waited 1.5 s on every request and showed no figures.
        const [dayRow, byType, subs, counts] = await Promise.all([
            db.prepare('SELECT COUNT(*) AS n FROM events WHERE received_at > ?').get(day),
            db.prepare('SELECT event_type, visibility, COUNT(*) AS n, MAX(received_at) AS last FROM events WHERE received_at > ? GROUP BY 1, 2').all(week),
            db.prepare('SELECT COUNT(*) AS n, COUNT(DISTINCT consumer) AS c FROM subscriptions WHERE enabled = 1').get(),
            store.deliveryCounts(),
        ]);
        const byNs = new Map();
        const types = new Set();
        const pub = new Map();
        for (const r of byType) {
            const type = String(r.event_type);
            const n = Number(r.n);
            types.add(type);
            const ns = type.split('.')[0];
            if (ns) byNs.set(ns, (byNs.get(ns) || 0) + n);
            if (r.visibility === 'public') {
                const p = pub.get(type) || { type, n: 0, last: 0 };
                p.n += n;
                p.last = Math.max(p.last, Number(r.last));
                pub.set(type, p);
            }
        }
        const byCount = (a, b) => b.n - a.n || (a.ns || a.type).localeCompare(b.ns || b.type);
        const services = [...byNs].map(([ns, n]) => ({ ns, n })).filter((r) => r.n > 0).sort(byCount);
        return {
            day: Number(dayRow.n),
            week: services.reduce((sum, r) => sum + r.n, 0),
            types: types.size,
            services,
            public: [...pub.values()].sort(byCount),
            subscriptions: Number(subs.n),
            consumers: Number(subs.c),
            delivered: Number(counts.delivered || 0),
            dead: Number(counts.dead || 0),
            measuredAt: t,
        };
    }

    function refresh() {
        if (!pending) {
            pending = load()
                .then((v) => { cached = v; at = now(); return v; })
                .catch((e) => { log.warn(`[events] home stats: ${e.message}`); at = now() - ttlMs + 60 * 1000; return cached; })
                .finally(() => { pending = null; });
        }
        return pending;
    }

    async function read() {
        if (!db) return null;
        // Fresh, or a failure less than a minute ago (at is moved back so the next try waits a minute, not a request).
        if (at && now() - at < ttlMs) return cached;
        const p = refresh();
        if (cached) return cached;
        return Promise.race([p, new Promise((resolve) => setTimeout(() => resolve(null), waitMs).unref())]);
    }

    return { read };
}

module.exports = { createHomeStats };
