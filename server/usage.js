'use strict';
/**
 * Project usage rollups (roadmap WS-N task 4, ADR-014): what each developer project used here, per
 * environment and UTC hour, reported as `events.usage.recorded` (openvibe-contracts
 * common.usage-recorded@1) for the project's dashboard on OpenVibe.Codes (Network adds them up).
 *
 *   events.app.publish    unit events       quantity: app events stored (a repeated event_id is not counted)
 *                                            errors: publish requests refused with a problem (429 quota,
 *                                            403 type/source/actor, 413 size, 422 envelope)
 *   events.app.subscribe  unit deliveries   quantity: webhook delivery attempts to the project's subscriptions
 *                                            errors: attempts without a 2xx (events.delivery.http_<status>,
 *                                            events.delivery.timeout, events.delivery.failed)
 *
 * Counted in the transaction that does the accounting: insertBatch (the stored rows and their quota
 * check) and recordAttempt (the delivery row). A refused publish rolls its transaction back, so the
 * refusal is counted on its own as it is answered.
 *
 * Once an hour has closed (plus GRACE_MS), flush() stores each of its rollups as an event of source
 * `events` in this same database (Events is its own outbox) in the transaction that marks the row
 * sent: a rollup is stored once, and delivered like any first-party event (Network's usage consumer).
 * A count that lands in an hour already sent (a clock step back) reopens it with revision + 1, and the
 * next flush sends the corrected totals. Samples keep the time, code, status, trace id and event id of
 * the last ten failures; never a subject, an address or anything of a payload.
 */
const crypto = require('crypto');
const { validate, ids } = require('openvibe-contracts');

const HOUR_MS = 60 * 60 * 1000;
const DAY_MS = 24 * HOUR_MS;
const GRACE_MS = 60 * 1000;
const KEEP_SENT_MS = 7 * DAY_MS;
const MAX_CODES = 20;
const MAX_SAMPLES = 10;
const CODE_RE = /^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$/;
const TRACE_RE = /^[0-9a-f]{32}$/;
const REF_RE = /^evt_[0-9A-HJKMNP-TV-Z]{26}$/;
const PROJECT_RE = /^prj_[0-9A-HJKMNP-TV-Z]{26}$/;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS app_usage (
    project_id   TEXT NOT NULL,
    env          TEXT NOT NULL,
    capability   TEXT NOT NULL,
    unit         TEXT NOT NULL,
    window_start INTEGER NOT NULL,
    quantity     INTEGER NOT NULL DEFAULT 0,
    errors       INTEGER NOT NULL DEFAULT 0,
    error_codes  TEXT NOT NULL DEFAULT '{}',
    samples      TEXT NOT NULL DEFAULT '[]',
    revision     INTEGER NOT NULL DEFAULT 1,
    event_id     TEXT,
    emitted_at   INTEGER,
    PRIMARY KEY (project_id, env, capability, unit, window_start)
);
CREATE INDEX IF NOT EXISTS idx_app_usage_open ON app_usage(emitted_at, window_start);
`;

const hourOf = (ms) => Math.floor(ms / HOUR_MS) * HOUR_MS;
const parse = (s, d) => { try { return JSON.parse(s); } catch { return d; } };

/** The problem code of a failed delivery attempt (worker.js outcome). */
function deliveryCode(outcome) {
    if (outcome.status) return `events.delivery.http_${outcome.status}`;
    return outcome.error === 'timeout' ? 'events.delivery.timeout' : 'events.delivery.failed';
}

/**
 * @param {import('better-sqlite3').Database} db  the Events database
 * @param {object} [o]
 * @param {{ now(): number }} [o.clock]
 * @param {boolean} [o.enabled=true]  false (EVENTS_USAGE=off): nothing is counted
 */
function createUsage(db, { clock = { now: () => Date.now() }, enabled = true } = {}) {
    db.exec(SCHEMA);
    const q = {
        add: db.prepare(`INSERT INTO app_usage (project_id, env, capability, unit, window_start, quantity, errors)
            VALUES (@project_id, @env, @capability, @unit, @window_start, @quantity, @errors)
            ON CONFLICT(project_id, env, capability, unit, window_start) DO UPDATE SET
                quantity = quantity + excluded.quantity, errors = errors + excluded.errors,
                revision = revision + (CASE WHEN emitted_at IS NULL THEN 0 ELSE 1 END), emitted_at = NULL`),
        get: db.prepare('SELECT error_codes, samples FROM app_usage WHERE project_id = ? AND env = ? AND capability = ? AND unit = ? AND window_start = ?'),
        setErrors: db.prepare(`UPDATE app_usage SET error_codes = ?, samples = ?
            WHERE project_id = ? AND env = ? AND capability = ? AND unit = ? AND window_start = ?`),
        due: db.prepare('SELECT * FROM app_usage WHERE emitted_at IS NULL AND window_start <= ? ORDER BY window_start LIMIT ?'),
        sent: db.prepare('UPDATE app_usage SET emitted_at = ?, event_id = ? WHERE project_id = ? AND env = ? AND capability = ? AND unit = ? AND window_start = ? AND emitted_at IS NULL'),
        prune: db.prepare('DELETE FROM app_usage WHERE emitted_at IS NOT NULL AND window_start < ?'),
        pending: db.prepare('SELECT COUNT(*) AS n FROM app_usage WHERE emitted_at IS NULL'),
    };

    /**
     * Count usage (and at most one failure) for a project, inside the caller's transaction when there
     * is one. `error` = { code, status?, traceId?, ref? } counts one error with a sample.
     */
    const record = db.transaction(({ projectId, env, capability, unit, quantity = 0, error = null, at = clock.now() }) => {
        if (!enabled || !PROJECT_RE.test(String(projectId || '')) || (env !== 'sandbox' && env !== 'production')) return;
        const key = { project_id: projectId, env, capability, unit, window_start: hourOf(at) };
        q.add.run({ ...key, quantity: Math.max(0, Math.trunc(Number(quantity) || 0)), errors: error ? 1 : 0 });
        if (!error) return;
        const code = CODE_RE.test(String(error.code || '')) ? String(error.code) : 'events.error';
        const row = q.get.get(projectId, env, capability, unit, key.window_start);
        const codes = parse(row.error_codes, {});
        if (codes[code] || Object.keys(codes).length < MAX_CODES) codes[code] = (codes[code] || 0) + 1;
        const sample = { at: new Date(at).toISOString(), code };
        if (Number.isInteger(error.status) && error.status >= 100 && error.status <= 599) sample.status = error.status;
        if (TRACE_RE.test(String(error.traceId || ''))) sample.trace_id = error.traceId;
        if (REF_RE.test(String(error.ref || ''))) sample.ref = error.ref;
        const samples = [sample, ...parse(row.samples, [])].slice(0, MAX_SAMPLES);
        q.setErrors.run(JSON.stringify(codes), JSON.stringify(samples), projectId, env, capability, unit, key.window_start);
    });

    function payloadOf(row) {
        const p = {
            project_id: row.project_id, env: row.env, capability: row.capability, unit: row.unit, window: 'hour',
            window_start: new Date(row.window_start).toISOString(), window_end: new Date(row.window_start + HOUR_MS).toISOString(),
            quantity: row.quantity, errors: row.errors,
        };
        const codes = parse(row.error_codes, {});
        if (Object.keys(codes).length) p.error_codes = codes;
        const samples = parse(row.samples, []);
        if (samples.length) p.samples = samples;
        if (row.revision > 1) p.revision = row.revision;
        return p;
    }

    /**
     * Store every closed hour's rollups as events.usage.recorded (at most `limit`). `insertBatch` is
     * the store's; the returned rows are what it stored, for the realtime fan-out and the worker kick.
     */
    function flush(insertBatch, { now = clock.now(), limit = 500 } = {}) {
        const stored = [];
        let invalid = 0;
        const rows = q.due.all(hourOf(now - GRACE_MS) - HOUR_MS, limit);
        for (const row of rows) {
            const payload = payloadOf(row);
            const envelope = {
                event_id: ids.newId('event'), event_type: 'events.usage.recorded', version: 1, source: 'events',
                actor: { type: 'service', id: 'events' }, timestamp: new Date(now).toISOString(),
                subject: { type: 'project', id: row.project_id }, visibility: 'internal', priority: 'low',
                trace_id: crypto.randomBytes(16).toString('hex'), payload,
            };
            const v = validate('events.event-envelope@1', envelope);
            const p = v.valid ? validate('events.usage.recorded@1', payload) : v;
            if (!p.valid) {
                // Never stop the others; the row stays unsent and is reported by status().
                invalid++;
                continue;
            }
            db.transaction(() => {
                const out = insertBatch([envelope], { publisher: 'svc:events', requestId: null });
                q.sent.run(now, envelope.event_id, row.project_id, row.env, row.capability, row.unit, row.window_start);
                stored.push(...out.inserted);
            })();
        }
        q.prune.run(now - KEEP_SENT_MS);
        return { stored, invalid };
    }

    return {
        record, flush, payloadOf, deliveryCode,
        pending: () => q.pending.get().n,
    };
}

module.exports = { createUsage, deliveryCode, HOUR_MS, GRACE_MS };
