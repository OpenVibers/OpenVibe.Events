'use strict';
/**
 * Billing readings (plan T5 step 14, the Tools recipe of step 7): what each developer project's queue use here costs,
 * sent to OpenVibe.Billing as platform.usage-sample@1 readings through billing.usage.record (POST /api/v1/usage).
 *
 *   metric   queue-operation-64kb (the platform.rate-card@1 metric: the reading's `resource` and `unit`)
 *   subject  the production project that owns the work: an app event's project (`events.project_id`) for a publish,
 *            the app subscription's project (`subscriptions.project_id`) for a delivery. First-party traffic
 *            (no project) and sandbox are never billed.
 *   period   one UTC hour [H, H+1h): publishes by `received_at`, webhook deliveries by `delivered_at` (status
 *            delivered; a failed or dead delivery is not billed). Closed once the clock passes H+1h+GRACE_MS.
 *   quantity each publish and each delivery counts ceil(size_bytes / 65536) operations, at least one.
 *
 * aggregate(H) runs ONE aggregate query over the closed hour and stores one reading per project in billing_readings
 * (migrations/0007_billing_readings.sql), in the transaction that marks the hour aggregated in billing_periods.
 * Nothing is written on the publish or delivery path. The reading id is its idempotency_key,
 * `events:queue-operation-64kb:<project>:<hour ISO>`: re-running an hour inserts nothing (ON CONFLICT DO NOTHING),
 * and a stored reading is never edited (a trigger refuses it), so every retry posts the same body.
 *
 * send() claims due pending readings (FOR UPDATE SKIP LOCKED plus a lease, so two processes never post one row at
 * once) and posts each with Events' Network service token (openvibe-sdk createServiceTokenClient, audience
 * openvibe.billing; the Events client needs the grant billing.usage.record):
 *   2xx (201 stored, 200 Billing's replay of the key)    sent
 *   any other 4xx (the reading itself: 400, 409, 422 …)  refused, last_error kept, never retried
 *   401, 403, 404, 408, 425, 429, 5xx, network, token    stays pending with backoff (credentials, grant, address or
 *                                                        pressure, not the reading); the rest of the batch waits
 *
 * Off by default (EVENTS_BILLING_INTERVAL_MS=0): no timer, nothing aggregated or sent. With an interval but without
 * OV_BILLING_INTERNAL_URL (or OV_OAUTH_CLIENT_SECRET) readings are aggregated and stay queued; nothing is sent.
 */
const { validate } = require('openvibe-contracts');
const { createServiceTokenClient } = require('openvibe-sdk/auth');

const HOUR_MS = 60 * 60 * 1000;
const GRACE_MS = 5 * 60 * 1000;
const METRIC = 'queue-operation-64kb';
const OP_BYTES = 65536;
const PATH = '/api/v1/usage';
const BATCH = 100;
const LEASE_MS = 2 * 60 * 1000;
const BACKOFF_MS = 30 * 1000;
const MAX_BACKOFF_MS = HOUR_MS;
const MAX_CATCHUP_HOURS = 7 * 24;
const PROJECT_RE = /^prj_[0-9A-HJKMNP-TV-Z]{26}$/;
// 4xx answers that are about the credentials, the grant, the address or load, not the reading: retried.
const RETRY_4XX = new Set([401, 403, 404, 408, 425, 429]);

const hourOf = (ms) => Math.floor(ms / HOUR_MS) * HOUR_MS;
const keyOf = (projectId, periodStart) => `events:${METRIC}:${projectId}:${new Date(periodStart).toISOString()}`;

/** The platform.usage-sample@1 reading of one project's closed hour. */
function readingOf({ projectId, periodStart, quantity }) {
    const key = keyOf(projectId, periodStart);
    return {
        id: key,
        idempotency_key: key,
        service: 'events',            // the Contracts manifest id: Billing keys budgets on it
        project: projectId,
        resource: METRIC,             // Billing matches the rate card's metric on `resource`
        provider: 'local',            // self-hosted queue (PostgreSQL), as Media's local storage
        operation: 'events.queue',
        quantity,
        unit: METRIC,
        at: new Date(periodStart).toISOString(),
        source: 'openvibe.events',
    };
}

/** POST one reading to Billing. { ok, status, final?, error? }; never throws. */
function createBillingClient({ config, fetchImpl = globalThis.fetch, now = () => Date.now(), networkUrl }) {
    if (!config.url || !config.clientSecret) return null;
    const tokens = createServiceTokenClient({ network: networkUrl, clientId: config.clientId, clientSecret: config.clientSecret,
        audience: config.audience, fetch: fetchImpl, timeoutMs: config.timeoutMs, now });
    return {
        async post(reading) {
            let headers;
            try { headers = await tokens.authHeaders({ audience: config.audience }); } catch (err) {
                return { ok: false, status: 0, error: `token: ${String(err && err.message || err).slice(0, 300)}` };
            }
            let r;
            try {
                r = await fetchImpl(`${config.url}${PATH}`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json', Accept: 'application/json', ...headers },
                    body: JSON.stringify(reading), signal: AbortSignal.timeout(config.timeoutMs),
                });
            } catch (err) {
                return { ok: false, status: 0, error: String(err && err.message || err).slice(0, 300) };
            }
            if (r.ok) return { ok: true, status: r.status };
            if (r.status === 401) tokens.invalidate({ audience: config.audience });
            const text = (await r.text().catch(() => '')).replace(/\s+/g, ' ').slice(0, 300);
            return { ok: false, status: r.status, error: `${r.status} ${text}`.trim(), final: r.status >= 400 && r.status < 500 && !RETRY_4XX.has(r.status) };
        },
    };
}

/**
 * @param {object} o
 * @param {object} o.db       the Events database (openvibe-sdk/db)
 * @param {object} o.config   config.billing (server/config.js)
 * @param {string} [o.networkUrl]  where the service token comes from (OV_NETWORK_INTERNAL_URL)
 * @param {{ now(): number }} [o.clock]
 * @param {Function} [o.fetchImpl]
 * @param {object} [o.client] a ready client ({ post(reading) }); default createBillingClient from the config
 */
function createBillingReadings({ db, config, networkUrl, clock = { now: () => Date.now() }, fetchImpl = globalThis.fetch, client, log = console }) {
    const sender = client !== undefined ? client : createBillingClient({ config, fetchImpl, now: () => clock.now(), networkUrl });
    const q = {
        // The one aggregate: every billable operation of the hour, per project.
        aggregate: db.prepare(`SELECT project_id, SUM(ops)::bigint AS quantity FROM (
                SELECT e.project_id, GREATEST(1, CEIL(e.size_bytes / ${OP_BYTES}.0))::bigint AS ops
                  FROM events e
                 WHERE e.project_id IS NOT NULL AND e.env = 'production' AND e.received_at >= ? AND e.received_at < ?
                UNION ALL
                SELECT s.project_id, GREATEST(1, CEIL(e.size_bytes / ${OP_BYTES}.0))::bigint
                  FROM deliveries d JOIN subscriptions s ON s.id = d.subscription_id JOIN events e ON e.id = d.event_id
                 WHERE s.project_id IS NOT NULL AND s.env = 'production' AND d.status = 'delivered'
                   AND d.delivered_at >= ? AND d.delivered_at < ?
            ) ops GROUP BY project_id ORDER BY project_id`),
        add: db.prepare(`INSERT INTO billing_readings (id, project_id, metric, period_start, reading, next_attempt_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING RETURNING id`),
        mark: db.prepare('INSERT INTO billing_periods (metric, period_start, readings, aggregated_at) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING'),
        last: db.prepare('SELECT MAX(period_start) AS p FROM billing_periods WHERE metric = ?'),
        claim: db.prepare(`UPDATE billing_readings SET lease_until = ? WHERE id IN (
                SELECT id FROM billing_readings WHERE state = 'pending' AND next_attempt_at <= ? AND (lease_until IS NULL OR lease_until <= ?)
                ORDER BY next_attempt_at, id LIMIT ? FOR UPDATE SKIP LOCKED)
            RETURNING id, reading, attempts, next_attempt_at`),
        sent: db.prepare(`UPDATE billing_readings SET state = 'sent', sent_at = ?, attempts = attempts + 1, last_error = NULL, lease_until = NULL
            WHERE id = ? AND state = 'pending'`),
        refused: db.prepare(`UPDATE billing_readings SET state = 'refused', attempts = attempts + 1, last_error = ?, lease_until = NULL
            WHERE id = ? AND state = 'pending'`),
        retry: db.prepare(`UPDATE billing_readings SET attempts = attempts + 1, last_error = ?, next_attempt_at = ?, lease_until = NULL
            WHERE id = ? AND state = 'pending'`),
        release: db.prepare('UPDATE billing_readings SET lease_until = NULL WHERE id = ? AND state = \'pending\''),
        counts: db.prepare('SELECT state, COUNT(*) AS n FROM billing_readings GROUP BY state'),
    };
    const stats = { invalid: 0, lastError: null };

    /** Store the readings of the closed hour starting at `periodStart`; { created, readings, invalid }. */
    async function aggregate(periodStart, { now = clock.now() } = {}) {
        if (periodStart !== hourOf(periodStart)) throw new RangeError(`billing: ${periodStart} is not the start of an hour`);
        if (periodStart + HOUR_MS + GRACE_MS > now) throw new RangeError(`billing: the hour ${new Date(periodStart).toISOString()} has not closed`);
        return await db.tx(async () => {
            const rows = await q.aggregate.all(periodStart, periodStart + HOUR_MS, periodStart, periodStart + HOUR_MS);
            let created = 0, invalid = 0;
            for (const row of rows) {
                const quantity = Number(row.quantity);
                if (!PROJECT_RE.test(String(row.project_id)) || !(quantity > 0)) continue;
                const reading = readingOf({ projectId: row.project_id, periodStart, quantity });
                const v = validate('platform.usage-sample@1', reading);
                if (!v.valid) {
                    invalid++;
                    stats.invalid++;
                    stats.lastError = `${reading.id}: ${v.errors.map(e => `${e.path || e.instancePath || ''} ${e.message}`).join('; ')}`;
                    log.error(`[billing] reading not stored (does not match platform.usage-sample@1): ${stats.lastError}`);
                    continue;
                }
                if (await q.add.get(reading.id, row.project_id, METRIC, periodStart, JSON.stringify(reading), now, now)) created++;
            }
            await q.mark.run(METRIC, periodStart, rows.length, now);
            return { period: periodStart, readings: rows.length, created, invalid };
        });
    }

    /**
     * Aggregate every closed hour not yet aggregated: from the hour after the last one marked (at most
     * MAX_CATCHUP_HOURS back) to the newest closed hour; the first run starts at the newest closed hour.
     */
    async function aggregateClosed({ now = clock.now() } = {}) {
        const newest = hourOf(now - GRACE_MS) - HOUR_MS;
        const last = (await q.last.get(METRIC)).p;
        let from = last == null ? newest : Math.max(Number(last) + HOUR_MS, newest - (MAX_CATCHUP_HOURS - 1) * HOUR_MS);
        if (last != null && Number(last) + HOUR_MS < from) log.warn(`[billing] hours from ${new Date(Number(last) + HOUR_MS).toISOString()} to ${new Date(from).toISOString()} were never aggregated and are past the catch-up window`);
        let created = 0;
        for (; from <= newest; from += HOUR_MS) created += (await aggregate(from, { now })).created;
        return { created };
    }

    /** Post due pending readings to Billing; { sent, refused, retried }. No client: nothing (readings stay queued). */
    async function send({ now = clock.now(), limit = BATCH } = {}) {
        const out = { sent: 0, refused: 0, retried: 0 };
        if (!sender) return out;
        const rows = (await q.claim.all(now + LEASE_MS, now, now, limit))
            .sort((a, b) => Number(a.next_attempt_at) - Number(b.next_attempt_at) || (a.id < b.id ? -1 : 1));
        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const reading = typeof row.reading === 'string' ? JSON.parse(row.reading) : row.reading;
            const res = await sender.post(reading);
            if (res.ok) { await q.sent.run(clock.now(), row.id); out.sent++; continue; }
            stats.lastError = `${row.id}: ${res.error || res.status}`;
            if (res.final) {
                await q.refused.run(String(res.error || res.status).slice(0, 500), row.id);
                out.refused++;
                log.error(`[billing] reading ${row.id} refused for good: ${res.error}`);
                continue;
            }
            const backoff = Math.min(MAX_BACKOFF_MS, BACKOFF_MS * 2 ** Math.min(Number(row.attempts), 10));
            await q.retry.run(String(res.error || res.status).slice(0, 500), clock.now() + backoff, row.id);
            out.retried++;
            log.warn(`[billing] reading ${row.id} not sent, retried in ${Math.round(backoff / 1000)} s: ${res.error}`);
            // Billing, the token or the grant is down: the rest of the batch waits for the next tick.
            for (const rest of rows.slice(i + 1)) await q.release.run(rest.id);
            break;
        }
        return out;
    }

    let timer = null, running = null;
    /** One pass: aggregate the closed hours, then send. Never overlaps itself; never throws. */
    function tick() {
        if (!running) running = (async () => {
            try {
                const a = await aggregateClosed();
                const s = await send();
                return { ...a, ...s };
            } catch (err) {
                stats.lastError = err.message;
                log.error(`[billing] tick failed: ${err.message}`);
                return null;
            }
        })().finally(() => { running = null; });
        return running;
    }

    return {
        enabled: config.intervalMs > 0,
        sending: !!sender,
        aggregate, aggregateClosed, send, tick,
        start() {
            if (timer || !(config.intervalMs > 0)) return false;
            if (!sender) log.warn('[billing] OV_BILLING_INTERNAL_URL or OV_OAUTH_CLIENT_SECRET is not set: readings are stored and stay queued');
            timer = setInterval(tick, config.intervalMs);
            timer.unref?.();
            return true;
        },
        async stop() {
            if (timer) clearInterval(timer);
            timer = null;
            if (running) await running;
        },
        get running() { return !!timer; },
        async status() {
            const by = Object.fromEntries((await q.counts.all()).map(r => [r.state, Number(r.n)]));
            return { enabled: config.intervalMs > 0, sending: !!sender, pending: by.pending || 0, sent: by.sent || 0, refused: by.refused || 0, invalid: stats.invalid, last_error: stats.lastError };
        },
    };
}

module.exports = { createBillingReadings, createBillingClient, readingOf, keyOf, METRIC, HOUR_MS, GRACE_MS };
