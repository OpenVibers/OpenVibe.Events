'use strict';
/**
 * openvibe-events client: what producing and consuming services use.
 *
 *   const events = require('openvibe-events');
 *
 *   // Producer: transactional outbox (better-sqlite3), relayed to OpenVibe.Events
 *   const publisher = events.createPublisher({ eventsUrl: 'http://127.0.0.1:4300', tokenClient });
 *   const outbox = events.createOutbox(db, { publisher });
 *   outbox.ensureSchema();
 *   db.transaction(() => {
 *       db.prepare('UPDATE vods SET status = ? WHERE id = ?').run('ready', id);
 *       outbox.enqueue({ event_type: 'media.vod.ready', source: 'media', actor, subject, payload });
 *   })();
 *   outbox.start();
 *
 *   // Consumer: signed webhook (v2: signature + 5-minute replay window) + exactly-once effects
 *   app.post('/internal/events', express.json({ verify: (req, _res, buf) => { req.rawBody = buf; } }), (req, res) => {
 *       if (!events.verifyDeliveryV2(req.rawBody, req.headers, SECRET)) return res.sendStatus(401);
 *       inbox.once('live', req.body.event.event_id, () => { ...db writes... });
 *       res.sendStatus(204);
 *   });
 *
 * `tokenClient` is anything with `authHeaders()` (openvibe-contracts serviceAuth.createTokenClient
 * with audience 'openvibe.events').
 */
const crypto = require('crypto');
const { ids } = require('openvibe-contracts');

const TRACE_RE = /^[0-9a-f]{32}$/;
const TRACEPARENT_RE = /^00-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$/;

// ── Signatures ─────────────────────────────────────────────

/** `sha256=<hex HMAC of the raw body>`, the value of X-OpenVibe-Signature. */
function sign(rawBody, secret) {
    return 'sha256=' + crypto.createHmac('sha256', String(secret)).update(rawBody).digest('hex');
}

/** Constant-time check of a delivery's X-OpenVibe-Signature against the raw request body. */
function verifyDelivery(rawBody, signature, secret) {
    if (rawBody == null || typeof signature !== 'string' || !secret) return false;
    const expected = Buffer.from(sign(Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(String(rawBody)), secret));
    const given = Buffer.from(signature.trim());
    return given.length === expected.length && crypto.timingSafeEqual(given, expected);
}

// v2 (replay window). Each attempt also carries
//   X-OpenVibe-Timestamp:    <unix seconds>
//   X-OpenVibe-Signature-V2: t=<unix seconds>,v2=<hex HMAC-SHA256 of "<t>.<raw body>">
// so a captured delivery stops verifying once it is older than the consumer's tolerance. v1
// (X-OpenVibe-Signature) is still sent, unchanged, until every consumer checks v2.

const V2_TOLERANCE_SEC = 300;

function v2Hex(rawBody, secret, timestamp) {
    return crypto.createHmac('sha256', String(secret)).update(`${timestamp}.`).update(rawBody).digest('hex');
}

/** `t=<ts>,v2=<hex HMAC of "<ts>.<raw body>">`, the value of X-OpenVibe-Signature-V2. */
function signV2(rawBody, secret, timestamp = Math.floor(Date.now() / 1000)) {
    if (!Number.isSafeInteger(timestamp) || timestamp < 0) throw new TypeError('timestamp must be unix seconds');
    return `t=${timestamp},v2=${v2Hex(Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(String(rawBody)), secret, timestamp)}`;
}

function headerValue(headers, name) {
    if (!headers) return undefined;
    if (typeof headers.get === 'function') return headers.get(name) ?? undefined;
    let v = headers[name];
    if (v === undefined) {
        const key = Object.keys(headers).find(k => k.toLowerCase() === name);
        v = key === undefined ? undefined : headers[key];
    }
    return Array.isArray(v) ? v.join(',') : v;
}

/**
 * Constant-time check of a delivery's X-OpenVibe-Signature-V2 against the raw request body, and of
 * its timestamp: false when it is more than `toleranceSec` away from `now` (ms) either way, or when
 * X-OpenVibe-Timestamp is present and names another time. `headers` is req.headers or a Fetch Headers.
 */
function verifyDeliveryV2(rawBody, headers, secret, { toleranceSec = V2_TOLERANCE_SEC, now = Date.now() } = {}) {
    if (!Number.isFinite(toleranceSec) || toleranceSec < 0) throw new TypeError('toleranceSec must be a number of seconds >= 0');
    const header = headerValue(headers, 'x-openvibe-signature-v2');
    if (rawBody == null || typeof header !== 'string' || !secret) return false;
    let t = null;
    const given = [];
    for (const part of header.split(',')) {
        const i = part.indexOf('=');
        if (i < 0) return false;
        const k = part.slice(0, i).trim();
        const v = part.slice(i + 1).trim();
        if (k === 't') { if (t !== null || !/^\d{1,12}$/.test(v)) return false; t = Number(v); }
        else if (k === 'v2') given.push(v);
    }
    if (t === null || !given.length) return false;
    const stated = headerValue(headers, 'x-openvibe-timestamp');
    if (stated !== undefined && String(stated).trim() !== String(t)) return false;
    if (Math.abs(Number(now) / 1000 - t) > toleranceSec) return false;
    const expected = Buffer.from(v2Hex(Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(String(rawBody)), secret, t));
    let ok = false;
    for (const v of given) {
        const g = Buffer.from(v);
        if (g.length === expected.length && crypto.timingSafeEqual(g, expected)) ok = true;
    }
    return ok;
}

// ── Envelopes ──────────────────────────────────────────────

function traceIdFrom(traceparent) {
    const m = typeof traceparent === 'string' && TRACEPARENT_RE.exec(traceparent.trim().toLowerCase());
    return m ? m[1] : null;
}

/**
 * Fill the fields a producer should not have to think about: event_id (evt_<ULID>), timestamp,
 * trace_id (from `traceparent` when given, so a consumer that republishes stays in the trace),
 * version 1, priority 'important', visibility 'internal'. Returns a new object.
 */
function prepareEnvelope(envelope, { traceparent, now = Date.now() } = {}) {
    if (!envelope || typeof envelope !== 'object') throw new TypeError('envelope must be an object');
    const out = { ...envelope };
    if (!out.event_id) out.event_id = ids.newId('event', now);
    if (!out.timestamp) out.timestamp = new Date(now).toISOString();
    if (!out.trace_id) out.trace_id = traceIdFrom(traceparent) || crypto.randomBytes(16).toString('hex');
    if (!TRACE_RE.test(out.trace_id)) throw new TypeError('trace_id must be 32 lowercase hex characters');
    if (out.version == null) out.version = 1;
    if (!out.priority) out.priority = 'important';
    if (!out.visibility) out.visibility = 'internal';
    if (out.payload == null) out.payload = {};
    return out;
}

// ── Publisher ──────────────────────────────────────────────

class PublishError extends Error {
    constructor(status, problem) {
        super(`events publish failed: ${status} ${(problem && (problem.code || problem.detail || problem.error)) || ''}`.trim());
        this.status = status;
        this.problem = problem || null;
        this.code = problem && problem.code;
        // 4xx other than 401/408/429 will fail the same way again.
        this.permanent = status >= 400 && status < 500 && ![401, 408, 429].includes(status);
    }
}

/**
 * createPublisher({ eventsUrl, tokenClient, fetchImpl?, timeoutMs? })
 *   publish(envelope | [envelopes], { traceparent }) -> { event_id, seq, duplicate } | { results: [...] }
 * A 401 invalidates the cached token and retries once.
 */
function createPublisher({ eventsUrl, tokenClient, fetchImpl = globalThis.fetch, timeoutMs = 10000 }) {
    if (!eventsUrl) throw new TypeError('eventsUrl is required');
    if (!tokenClient || typeof tokenClient.authHeaders !== 'function') throw new TypeError('tokenClient with authHeaders() is required');
    const url = `${String(eventsUrl).replace(/\/$/, '')}/api/v1/events`;

    async function post(body, traceparent) {
        for (let attempt = 0; ; attempt++) {
            const headers = { 'Content-Type': 'application/json', Accept: 'application/json', ...(await tokenClient.authHeaders()) };
            if (traceparent) headers.traceparent = traceparent;
            const res = await fetchImpl(url, { method: 'POST', headers, body: JSON.stringify(body), signal: AbortSignal.timeout(timeoutMs) });
            const json = await res.json().catch(() => null);
            if (res.ok) return json;
            if (res.status === 401 && attempt === 0 && typeof tokenClient.invalidate === 'function') {
                tokenClient.invalidate();
                continue;
            }
            throw new PublishError(res.status, json);
        }
    }

    async function publish(input, { traceparent } = {}) {
        if (Array.isArray(input)) {
            const events = input.map(e => prepareEnvelope(e, { traceparent }));
            return post({ events }, traceparent);
        }
        return post(prepareEnvelope(input, { traceparent }), traceparent);
    }

    return { publish, prepare: prepareEnvelope };
}

// ── Transactional outbox ───────────────────────────────────

/**
 * createOutbox(db, { publisher, table = 'event_outbox', batchSize = 50, intervalMs = 1000, now })
 *
 *   ensureSchema()     create the outbox table (idempotent)
 *   enqueue(envelope)  INSIDE the caller's db transaction: the event exists iff the change commits
 *   flush()            publish due rows once (batch; a rejected batch is retried row by row so one
 *                      bad row cannot block the rest); resolves { sent, failed, rejected }
 *   start() / stop()   relay loop (setTimeout chain, never overlapping)
 *   pending()          count of rows not yet accepted by Events
 *
 * At-least-once: a crash between publish and mark-sent republishes the same event_id, which
 * OpenVibe.Events answers as a duplicate.
 */
function createOutbox(db, { publisher, table = 'event_outbox', batchSize = 50, intervalMs = 1000, backoffMs = [1000, 5000, 30000, 120000, 600000], now = () => Date.now(), onError = null, allowOutsideTransaction = false } = {}) {
    if (!db || typeof db.prepare !== 'function') throw new TypeError('a better-sqlite3 database is required');
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)) throw new TypeError('bad outbox table name');
    let stmts = null;
    let timer = null;
    let running = false;
    let flushing = null;

    function ensureSchema() {
        db.exec(`CREATE TABLE IF NOT EXISTS ${table} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        TEXT NOT NULL UNIQUE,
            envelope        TEXT NOT NULL,
            traceparent     TEXT,
            created_at      INTEGER NOT NULL,
            attempts        INTEGER NOT NULL DEFAULT 0,
            next_attempt_at INTEGER NOT NULL DEFAULT 0,
            sent_at         INTEGER,
            seq             INTEGER,
            rejected_at     INTEGER,
            last_error      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_${table}_due ON ${table}(sent_at, rejected_at, next_attempt_at);`);
        stmts = null;
    }

    function q() {
        if (!stmts) {
            stmts = {
                insert: db.prepare(`INSERT INTO ${table} (event_id, envelope, traceparent, created_at, next_attempt_at) VALUES (?, ?, ?, ?, 0)`),
                due: db.prepare(`SELECT * FROM ${table} WHERE sent_at IS NULL AND rejected_at IS NULL AND next_attempt_at <= ? ORDER BY id LIMIT ?`),
                sent: db.prepare(`UPDATE ${table} SET sent_at = ?, seq = ?, attempts = attempts + 1, last_error = NULL WHERE id = ?`),
                failed: db.prepare(`UPDATE ${table} SET attempts = attempts + 1, next_attempt_at = ?, last_error = ? WHERE id = ?`),
                rejected: db.prepare(`UPDATE ${table} SET rejected_at = ?, attempts = attempts + 1, last_error = ? WHERE id = ?`),
                pending: db.prepare(`SELECT COUNT(*) AS n FROM ${table} WHERE sent_at IS NULL AND rejected_at IS NULL`),
                prune: db.prepare(`DELETE FROM ${table} WHERE sent_at IS NOT NULL AND sent_at < ?`),
            };
        }
        return stmts;
    }

    function enqueue(envelope, { traceparent } = {}) {
        if (!allowOutsideTransaction && !db.inTransaction) {
            throw new Error('outbox.enqueue() must run inside the transaction that makes the change');
        }
        const env = prepareEnvelope(envelope, { traceparent, now: now() });
        q().insert.run(env.event_id, JSON.stringify(env), traceparent || null, now());
        return env;
    }

    function backoff(attempts) {
        return backoffMs[Math.min(attempts, backoffMs.length - 1)];
    }

    function markFailure(row, err) {
        const message = String((err && err.message) || err).slice(0, 500);
        if (err && err.permanent) q().rejected.run(now(), message, row.id);
        else q().failed.run(now() + backoff(row.attempts), message, row.id);
        if (onError) { try { onError(err, row); } catch { /* reporting must not break the relay */ } }
    }

    async function publishRows(rows) {
        const stats = { sent: 0, failed: 0, rejected: 0 };
        const envelopes = rows.map(r => JSON.parse(r.envelope));
        try {
            const out = rows.length === 1
                ? { results: [await publisher.publish(envelopes[0], { traceparent: rows[0].traceparent || undefined })] }
                : await publisher.publish(envelopes, { traceparent: rows[0].traceparent || undefined });
            const byId = new Map((out.results || []).map(r => [r.event_id, r]));
            db.transaction(() => { for (const row of rows) q().sent.run(now(), byId.get(row.event_id)?.seq ?? null, row.id); })();
            stats.sent += rows.length;
        } catch (err) {
            if (rows.length > 1 && err.permanent) {
                // One bad envelope rejects the whole batch: isolate it.
                for (const row of rows) {
                    const s = await publishRows([row]);
                    stats.sent += s.sent; stats.failed += s.failed; stats.rejected += s.rejected;
                }
            } else {
                for (const row of rows) markFailure(row, err);
                if (err.permanent) stats.rejected += rows.length; else stats.failed += rows.length;
            }
        }
        return stats;
    }

    async function doFlush() {
        const total = { sent: 0, failed: 0, rejected: 0 };
        for (;;) {
            const rows = q().due.all(now(), batchSize);
            if (!rows.length) break;
            const s = await publishRows(rows);
            total.sent += s.sent; total.failed += s.failed; total.rejected += s.rejected;
            if (s.failed || rows.length < batchSize) break;
        }
        return total;
    }

    /** Publish due rows. Concurrent callers share the flush in progress. */
    function flush() {
        if (!flushing) flushing = doFlush().finally(() => { flushing = null; });
        return flushing;
    }

    function schedule(ms) {
        if (!running) return;
        clearTimeout(timer);
        timer = setTimeout(async () => {
            timer = null;
            try { await flush(); } catch (err) { if (onError) { try { onError(err); } catch { /* ignore */ } } }
            schedule(intervalMs);
        }, ms);
        timer.unref?.();
    }

    return {
        ensureSchema,
        enqueue,
        flush,
        start() { if (!running) { running = true; schedule(0); } },
        stop() { running = false; clearTimeout(timer); timer = null; return flushing || Promise.resolve(); },
        /** Wake the relay now (e.g. right after the transaction commits). */
        kick() { schedule(0); },
        pending: () => q().pending.get().n,
        /** Delete sent rows older than `olderThanMs`. */
        prune(olderThanMs = 7 * 24 * 60 * 60 * 1000) { return q().prune.run(now() - olderThanMs).changes; },
    };
}

// ── Inbox (exactly-once effects) ───────────────────────────

/**
 * createInbox(db, { table = 'idempotency_receipts' })
 *   ensureSchema()
 *   once(consumer, eventId, fn) -> { duplicate: true } | { duplicate: false, result }
 *
 * fn runs inside a better-sqlite3 transaction together with the receipt insert: either both commit
 * (the effect happened exactly once) or neither does (fn threw; the redelivery will try again).
 * fn must be synchronous and make its effects through this same database.
 */
function createInbox(db, { table = 'idempotency_receipts', now = () => Date.now() } = {}) {
    if (!db || typeof db.prepare !== 'function') throw new TypeError('a better-sqlite3 database is required');
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(table)) throw new TypeError('bad inbox table name');
    let claim = null;

    function ensureSchema() {
        db.exec(`CREATE TABLE IF NOT EXISTS ${table} (
            consumer     TEXT NOT NULL,
            event_id     TEXT NOT NULL,
            processed_at INTEGER NOT NULL,
            PRIMARY KEY (consumer, event_id)
        )`);
        claim = null;
    }

    function once(consumer, eventId, fn) {
        if (!consumer || !eventId) throw new TypeError('consumer and eventId are required');
        if (!claim) claim = db.prepare(`INSERT OR IGNORE INTO ${table} (consumer, event_id, processed_at) VALUES (?, ?, ?)`);
        return db.transaction(() => {
            if (claim.run(String(consumer), String(eventId), now()).changes === 0) return { duplicate: true };
            const result = fn();
            if (result && typeof result.then === 'function') {
                throw new TypeError('inbox.once(): fn must be synchronous (it runs inside a SQLite transaction)');
            }
            return { duplicate: false, result };
        })();
    }

    function seen(consumer, eventId) {
        return Boolean(db.prepare(`SELECT 1 FROM ${table} WHERE consumer = ? AND event_id = ?`).get(String(consumer), String(eventId)));
    }

    return { ensureSchema, once, seen };
}

module.exports = { sign, verifyDelivery, signV2, verifyDeliveryV2, prepareEnvelope, createPublisher, createOutbox, createInbox, PublishError };
