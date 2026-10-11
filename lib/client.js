'use strict';
/**
 * openvibe-events client: what producing and consuming services use.
 *
 *   const events = require('openvibe-events');
 *
 *   // Producer: transactional outbox in the service's own PostgreSQL (openvibe-sdk/db), relayed to OpenVibe.Events
 *   const publisher = events.createPublisher({ eventsUrl: 'http://127.0.0.1:4300', tokenClient });
 *   const outbox = events.createPgOutbox(db, { events: publisher });      // table from events.outboxSchema() in a migration
 *   await db.tx(async (t) => {
 *       await t.query('UPDATE vods SET status = $1 WHERE id = $2', ['ready', id]);
 *       await outbox.enqueue(t, { event_type: 'media.vod.ready', source: 'media', actor, subject, payload });
 *   });
 *   outbox.start();
 *
 *   // Consumer: signed webhook (v2: signature + 5-minute replay window) + exactly-once effects
 *   const inbox = events.createPgInbox(db);                                // table from events.inboxSchema()
 *   app.post('/internal/events', express.json({ verify: (req, _res, buf) => { req.rawBody = buf; } }), async (req, res) => {
 *       if (!events.verifyDeliveryV2(req.rawBody, req.headers, SECRET)) return res.sendStatus(401);
 *       await inbox.once('live', req.body.event.event_id, async (t) => { ...writes on t... });
 *       res.sendStatus(204);
 *   });
 *
 *   // Consumer that must not apply an older revision of a subject after a newer one (replays, dead predecessors)
 *   const ordered = events.createPgOrderedInbox(db);                       // tables from inboxSchema() + inboxHeadsSchema()
 *   const r = await ordered.apply('live', req.body.event, async (t) => { ...writes on t... });   // r.stale: superseded
 *
 * The outbox and inbox are openvibe-sdk's PostgreSQL kits (ADR-042 decision 9): this package no longer carries its
 * own SQLite copies.
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
 *   publish(envelope | [envelopes], { traceparent }) -> { event_id, cursor, duplicate } | { results: [...] }
 *   (`cursor` is the event's opaque position; ADR-042 decision 7. Hand it back as `after=` or `Last-Event-ID`.)
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

// ── Outbox and inbox: openvibe-sdk's PostgreSQL kits ─────────

const { createPgOutbox, createPgInbox, outboxSchema, inboxSchema } = require('openvibe-sdk/events');

// ── Ordered inbox: the inbox plus a per-key head ───────────
//
// Events delivers at least once and, outside a policy's ordering key, not in order; a replay or a dead
// delivery's successor can also arrive after a later revision. The ordered inbox keeps, per (consumer,
// key), the highest subject.revision applied, in the same transaction as the receipt and the effects.

const TABLE_RE = /^[a-z_][a-z0-9_]{0,62}$/;
const MAX_KEY_LEN = 200;

/** The key-head table's DDL, for a service migration beside events.inboxSchema(). */
function inboxHeadsSchema(table = 'inbox_key_heads') {
    if (!TABLE_RE.test(table)) throw new TypeError('bad inbox heads table name');
    return `CREATE TABLE IF NOT EXISTS ${table} (
    consumer   text NOT NULL,
    key        text NOT NULL,
    revision   bigint NOT NULL,
    event_id   text NOT NULL,
    applied_at bigint NOT NULL,
    PRIMARY KEY (consumer, key)
);`;
}

/**
 * The subject's inbox key, `["<subject.type>","<subject.id>"]`; null without one. Unlike a delivery policy's ordering
 * key it is never truncated and a `:` cannot join two subjects: one longer than 200 characters is `sha256:<hex>` of that
 * form, so distinct subjects never share a head.
 */
function subjectKey(event) {
    const s = event && event.subject;
    if (!s || typeof s.type !== 'string' || !s.type || typeof s.id !== 'string' || !s.id) return null;
    const k = JSON.stringify([s.type, s.id]);
    return k.length > MAX_KEY_LEN ? `sha256:${crypto.createHash('sha256').update(k).digest('hex')}` : k;
}

/**
 * createPgOrderedInbox(db, { table?, headsTable?, now? }) on an openvibe-sdk/db handle:
 *   once(consumer, eventId, fn) and seen(consumer, eventId), as createPgInbox;
 *   apply(consumer, event, fn, { key?, revision? }) -> { duplicate: true }
 *                                                    | { duplicate: false, stale: true, head: { revision, event_id } }
 *                                                    | { duplicate: false, stale: false, result }
 * apply records the receipt, then moves the key's head to `revision` only if it is higher than the head, then runs
 * fn(t) — all in one transaction. An event at or below the head is stale: its receipt is kept (a redelivery is a
 * duplicate) and fn does not run, so a consumer answers 2xx either way. `key` defaults to subjectKey(event) and
 * `revision` to event.subject.revision; without both the event is only deduplicated. Concurrent applies on one key
 * serialise on its head row.
 */
function createPgOrderedInbox(db, { table = 'idempotency_receipts', headsTable = 'inbox_key_heads', now = () => Date.now() } = {}) {
    if (!TABLE_RE.test(headsTable)) throw new TypeError('bad inbox heads table name');
    const inbox = createPgInbox(db, { table, now });

    async function apply(consumer, event, fn, { key = subjectKey(event), revision = event && event.subject && event.subject.revision } = {}) {
        if (!event || typeof event !== 'object' || !event.event_id) throw new TypeError('event with an event_id is required');
        if (typeof fn !== 'function') throw new TypeError('fn is required');
        const ordered = key != null && revision != null;
        if (ordered) {
            if (typeof key !== 'string' || !key || key.length > MAX_KEY_LEN) throw new TypeError(`key must be a string of 1..${MAX_KEY_LEN} characters`);
            if (!Number.isSafeInteger(revision) || revision < 0) throw new TypeError('revision must be an integer >= 0');
        }
        const r = await inbox.once(consumer, event.event_id, async (t) => {
            if (ordered) {
                const moved = await t.exec(
                    `INSERT INTO ${headsTable} (consumer, key, revision, event_id, applied_at) VALUES ($1, $2, $3, $4, $5)
                     ON CONFLICT (consumer, key) DO UPDATE SET revision = EXCLUDED.revision, event_id = EXCLUDED.event_id, applied_at = EXCLUDED.applied_at
                     WHERE ${headsTable}.revision < EXCLUDED.revision`,
                    [String(consumer), key, revision, String(event.event_id), now()]);
                if (moved === 0) {
                    const head = await t.one(`SELECT revision, event_id FROM ${headsTable} WHERE consumer = $1 AND key = $2`, [String(consumer), key]);
                    return { stale: true, head: { revision: Number(head.revision), event_id: head.event_id } };
                }
            }
            return { stale: false, result: await fn(t) };
        });
        return r.duplicate ? r : { duplicate: false, ...r.result };
    }

    return {
        schema: () => `${inboxSchema(table)}\n${inboxHeadsSchema(headsTable)}`,
        ensureSchema: () => db.query(`${inboxSchema(table)}\n${inboxHeadsSchema(headsTable)}`),
        once: inbox.once,
        seen: inbox.seen,
        apply,
        head: async (consumer, key) => {
            const row = await db.maybe(`SELECT revision, event_id FROM ${headsTable} WHERE consumer = $1 AND key = $2`, [String(consumer), String(key)]);
            return row ? { revision: Number(row.revision), event_id: row.event_id } : null;
        },
    };
}

module.exports = {
    sign, verifyDelivery, signV2, verifyDeliveryV2, prepareEnvelope, createPublisher, createPgOutbox, createPgInbox, outboxSchema, inboxSchema,
    createPgOrderedInbox, inboxHeadsSchema, subjectKey, PublishError,
};
