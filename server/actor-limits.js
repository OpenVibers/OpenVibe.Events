'use strict';
/**
 * Per-actor rate limits at the capability routes (roadmap WS-R task 4; openvibe-sdk/limits).
 *
 * nginx limits by address; these limit by the verified principal that calls: a first-party service
 * (svc:live) or a developer app (app:app_…). Every limited route sits behind requireCap or
 * appOrService, so the principal is known before it is counted. Past a limit the route answers 429
 * problem+json `rate_limited` with Retry-After before it does any work; the refusal is logged once and
 * counted in events_rate_limited_total{limit,window}. Read routes get EVENTS_LIMITS_MINUTE /
 * EVENTS_LIMITS_HOUR (120 and 3000); publish, pull, subscriptions and replay set their own numbers
 * where they are mounted (server/api/*.js). Counters live in this process: a restart forgets them.
 * EVENTS_LIMITS=off turns every limit off (a rollback lever).
 *
 * Never limited: /api/health, /api/ready, /release.json, /limits.json, /metrics, and the realtime
 * stream (a connection, capped by REALTIME_MAX_CONNECTIONS and REALTIME_MAX_TOPICS).
 */
const { createActorLimiter, createValkeyLimitStore } = require('openvibe-sdk/limits');

/** limits(name, own) middleware for one app; `clock` is the service's (manual in tests). */
function createLimits({ config, clock = { now: () => Date.now() }, metrics = null, log = console, valkey = null }) {
    // Every named limit with its numbers, as the routes declare them (published in /limits.json as rate_limits).
    const registered = new Map();
    const record = (name, own = {}) => registered.set(name, { minute: own.minute != null ? own.minute : config.limits.minute, hour: own.hour != null ? own.hour : config.limits.hour });
    const listed = () => [...registered].map(([id, n]) => ({ id, ...n })).sort((x, y) => x.id.localeCompare(y.id));
    if (!config.limits.enabled) {                                                // EVENTS_LIMITS=off
        const off = (name, own) => { record(name, own); return (_req, _res, next) => next(); };
        off.registered = () => [];
        return off;
    }
    const refused = metrics && metrics.registry
        ? metrics.registry.counter({ name: 'events_rate_limited_total', help: 'Requests refused 429 by a per-actor limit, by limit name and window', labelNames: ['limit', 'window'] })
        : null;
    const limiter = createActorLimiter({
        limits: { minute: config.limits.minute, hour: config.limits.hour },
        now: () => clock.now(),
        // Shared across processes on Valkey (ADR-035) when VALKEY_URL is set; in-process otherwise.
        ...(valkey ? { store: createValkeyLimitStore(valkey) } : {}),
        onLimited(e) {
            // The actor is a principal id (svc:…, app:app_…), never a token.
            log.warn(`[limits] ${e.name}: ${e.actor} refused, over ${e.limit} per ${e.window}`);
            if (refused) refused.inc({ limit: e.name, window: e.window });
        },
    });
    const limits = (name, own) => { record(name, own); return limiter(name, own); };
    limits.stats = () => limiter.stats();
    limits.reset = () => limiter.reset();
    limits.registered = listed;
    return limits;
}

module.exports = { createLimits };
