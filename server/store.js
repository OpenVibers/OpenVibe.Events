'use strict';
/**
 * The durable store (SQLite, better-sqlite3). An event is committed, together with one delivery row
 * per matching subscription, before anything is sent anywhere: "event persists before consumer
 * delivery". Global order is `seq`, handed out from a counter that never goes backwards, even after
 * retention has pruned the newest rows.
 */
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const topics = require('./topics');

const PRIORITY_RANK = { critical: 0, important: 1, low: 2 };
const RANK_PRIORITY = ['critical', 'important', 'low'];
const DAY_MS = 24 * 60 * 60 * 1000;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS sequences (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id               TEXT PRIMARY KEY,
    seq              INTEGER NOT NULL UNIQUE,
    event_type       TEXT NOT NULL,
    version          INTEGER NOT NULL,
    source           TEXT NOT NULL,
    actor            TEXT NOT NULL,
    subject_type     TEXT NOT NULL,
    subject_id       TEXT NOT NULL,
    subject_revision INTEGER,
    trace_id         TEXT NOT NULL,
    priority         TEXT NOT NULL,
    visibility       TEXT NOT NULL,
    occurred_at      TEXT NOT NULL,
    received_at      INTEGER NOT NULL,
    payload          TEXT NOT NULL,
    hops             INTEGER NOT NULL DEFAULT 1,
    publisher        TEXT NOT NULL,
    request_id       TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_trace ON events(trace_id);
CREATE INDEX IF NOT EXISTS idx_events_received ON events(received_at);
CREATE INDEX IF NOT EXISTS idx_events_type_seq ON events(event_type, seq);

CREATE TABLE IF NOT EXISTS subscriptions (
    id            TEXT PRIMARY KEY,
    consumer      TEXT NOT NULL,
    topic_pattern TEXT NOT NULL,
    endpoint      TEXT NOT NULL,
    secret        TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 1,
    retry_policy  TEXT,
    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subscriptions_consumer ON subscriptions(consumer);

CREATE TABLE IF NOT EXISTS deliveries (
    event_id        TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id TEXT NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    priority        INTEGER NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 0,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'delivered', 'failed', 'dead')),
    next_attempt_at INTEGER,
    last_error      TEXT,
    last_status     INTEGER,
    delivered_at    INTEGER,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL,
    PRIMARY KEY (event_id, subscription_id)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due ON deliveries(status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_sub ON deliveries(subscription_id, status, seq);

CREATE TABLE IF NOT EXISTS consumer_checkpoints (
    consumer      TEXT NOT NULL,
    topic_pattern TEXT NOT NULL,
    cursor        INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL,
    PRIMARY KEY (consumer, topic_pattern)
);

-- Publish receipts: remember accepted event ids for longer than the events themselves, so a
-- producer that re-publishes an old id after retention is still answered "duplicate".
CREATE TABLE IF NOT EXISTS idempotency_receipts (
    consumer     TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    processed_at INTEGER NOT NULL,
    PRIMARY KEY (consumer, event_id)
);
`;

const PUBLISH_RECEIPT = 'events:publish';  // receipts' consumer column for accepted publishes

function openDb(dbPath) {
    if (dbPath !== ':memory:') fs.mkdirSync(path.dirname(path.resolve(dbPath)), { recursive: true });
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');
    db.pragma('busy_timeout = 5000');
    db.exec(SCHEMA);
    db.prepare("INSERT OR IGNORE INTO sequences (name, value) VALUES ('events', 0)").run();
    return db;
}

class StoreError extends Error {
    constructor(status, code, detail, extra) {
        super(detail);
        this.status = status;
        this.code = code;
        this.extra = extra;
    }
}

/** Stored row -> the envelope exactly as consumers see it. */
function rowToEnvelope(row) {
    const subject = { type: row.subject_type, id: row.subject_id };
    if (row.subject_revision != null) subject.revision = row.subject_revision;
    return {
        event_id: row.id,
        trace_id: row.trace_id,
        event_type: row.event_type,
        version: row.version,
        source: row.source,
        actor: JSON.parse(row.actor),
        timestamp: row.occurred_at,
        priority: row.priority,
        visibility: row.visibility,
        subject,
        payload: JSON.parse(row.payload),
    };
}

function subscriptionView(row, { withSecret = false } = {}) {
    const out = {
        id: row.id,
        consumer: row.consumer,
        topic_pattern: row.topic_pattern,
        endpoint: row.endpoint,
        enabled: Boolean(row.enabled),
        retry_policy: row.retry_policy ? JSON.parse(row.retry_policy) : null,
        created_at: new Date(row.created_at).toISOString(),
        updated_at: new Date(row.updated_at).toISOString(),
    };
    if (withSecret) out.secret = row.secret;
    return out;
}

function createStore(db, { clock = { now: () => Date.now() }, maxHops = 8 } = {}) {
    const q = {
        getEvent: db.prepare('SELECT * FROM events WHERE id = ?'),
        getReceipt: db.prepare('SELECT processed_at FROM idempotency_receipts WHERE consumer = ? AND event_id = ?'),
        addReceipt: db.prepare('INSERT OR IGNORE INTO idempotency_receipts (consumer, event_id, processed_at) VALUES (?, ?, ?)'),
        nextSeq: db.prepare("UPDATE sequences SET value = value + 1 WHERE name = 'events' RETURNING value"),
        lastSeq: db.prepare("SELECT value FROM sequences WHERE name = 'events'"),
        minSeq: db.prepare('SELECT MIN(seq) AS s FROM events'),
        chainHops: db.prepare('SELECT MAX(hops) AS h FROM events WHERE trace_id = ? AND source != ?'),
        selfRepeats: db.prepare(`SELECT COUNT(*) AS n FROM events WHERE trace_id = ? AND source = ? AND event_type = ?
            AND subject_type = ? AND subject_id = ? AND (request_id IS NULL OR request_id != ?)`),
        insertEvent: db.prepare(`INSERT INTO events (id, seq, event_type, version, source, actor, subject_type, subject_id,
            subject_revision, trace_id, priority, visibility, occurred_at, received_at, payload, hops, publisher, request_id)
            VALUES (@id, @seq, @event_type, @version, @source, @actor, @subject_type, @subject_id, @subject_revision,
            @trace_id, @priority, @visibility, @occurred_at, @received_at, @payload, @hops, @publisher, @request_id)`),
        enabledSubs: db.prepare('SELECT id, topic_pattern FROM subscriptions WHERE enabled = 1'),
        insertDelivery: db.prepare(`INSERT OR IGNORE INTO deliveries (event_id, subscription_id, seq, priority, attempt, status,
            next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, 0, 'pending', ?, ?, ?)`),
        afterSeq: db.prepare('SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT ?'),
        afterSeqGlob: db.prepare('SELECT * FROM events WHERE seq > ? AND event_type GLOB ? ORDER BY seq LIMIT ?'),
    };

    // ── Events ───────────────────────────────────────────────

    /**
     * Store a batch atomically. `items` are validated envelopes with defaults filled. Returns
     * { results: [{ event_id, seq, duplicate }], inserted: [row] }. Throws StoreError for an id
     * conflict or a detected loop; nothing of the batch is stored then.
     */
    const insertBatch = db.transaction((items, { publisher, requestId }) => {
        const now = clock.now();
        const results = [];
        const inserted = [];
        const subs = q.enabledSubs.all();
        for (const env of items) {
            const existing = q.getEvent.get(env.event_id);
            if (existing) {
                if (existing.source !== env.source || existing.event_type !== env.event_type) {
                    throw new StoreError(409, 'events.id_conflict', `event ${env.event_id} already exists with different content`);
                }
                results.push({ event_id: env.event_id, seq: existing.seq, duplicate: true });
                continue;
            }
            if (q.getReceipt.get(PUBLISH_RECEIPT, env.event_id)) {
                results.push({ event_id: env.event_id, seq: null, duplicate: true, pruned: true });
                continue;
            }
            // Loop guard: depth of the cross-service chain in this trace, and how often this exact
            // (source, type, subject) already happened in the trace from other publish calls.
            const chain = (q.chainHops.get(env.trace_id, env.source).h || 0) + 1;
            const repeats = q.selfRepeats.get(env.trace_id, env.source, env.event_type, env.subject.type, env.subject.id, requestId || '').n + 1;
            const hops = Math.max(chain, repeats);
            if (hops > maxHops) {
                throw new StoreError(409, 'events.loop_detected',
                    `trace ${env.trace_id} already carries ${hops - 1} hops for ${env.event_type}; refusing to extend a probable loop`,
                    { event_id: env.event_id, hops: hops - 1, max_hops: maxHops });
            }
            const seq = q.nextSeq.get().value;
            const row = {
                id: env.event_id,
                seq,
                event_type: env.event_type,
                version: env.version,
                source: env.source,
                actor: JSON.stringify(env.actor),
                subject_type: env.subject.type,
                subject_id: env.subject.id,
                subject_revision: env.subject.revision ?? null,
                trace_id: env.trace_id,
                priority: env.priority,
                visibility: env.visibility,
                occurred_at: env.timestamp,
                received_at: now,
                payload: JSON.stringify(env.payload),
                hops,
                publisher,
                request_id: requestId || null,
            };
            q.insertEvent.run(row);
            q.addReceipt.run(PUBLISH_RECEIPT, env.event_id, now);
            for (const s of subs) {
                if (topics.matches(s.topic_pattern, env.event_type)) {
                    q.insertDelivery.run(env.event_id, s.id, seq, PRIORITY_RANK[env.priority], now, now, now);
                }
            }
            results.push({ event_id: env.event_id, seq, duplicate: false });
            inserted.push(row);
        }
        return { results, inserted };
    });

    function getEvent(id) {
        return q.getEvent.get(id) || null;
    }

    function lastSeq() {
        return q.lastSeq.get().value;
    }

    /** Oldest seq still stored; when the table is empty, the next seq to be handed out. */
    function oldestSeq() {
        const m = q.minSeq.get().s;
        return m == null ? lastSeq() + 1 : m;
    }

    /**
     * Events with seq > afterSeq that match any of `patterns`, in order, filtered by `accept(row)`.
     * Scans at most `scanMax` rows; returns { rows, cursor } where cursor is the last seq examined
     * (so a pull consumer's cursor moves past rows that did not match).
     */
    function scan(afterSeq, { patterns = ['*'], limit = 100, scanMax = 5000, accept = () => true } = {}) {
        const rows = [];
        let cursor = afterSeq;
        let scanned = 0;
        const single = patterns.length === 1 && patterns[0] !== '*' ? topics.toGlob(patterns[0]) : null;
        for (;;) {
            const page = single ? q.afterSeqGlob.all(cursor, single, 500) : q.afterSeq.all(cursor, 500);
            for (const row of page) {
                scanned++;
                cursor = row.seq;
                if (patterns.some(p => topics.matches(p, row.event_type)) && accept(row)) rows.push(row);
                if (rows.length >= limit || scanned >= scanMax) break;
            }
            if (rows.length >= limit || scanned >= scanMax) break;
            if (page.length < 500) {
                // Reached the end. With the GLOB narrowing, rows that were never fetched cannot match,
                // so the cursor may move up to the latest seq handed out.
                if (single) cursor = Math.max(cursor, lastSeq());
                break;
            }
        }
        return { rows, cursor };
    }

    // ── Subscriptions ────────────────────────────────────────

    function createSubscription({ id, consumer, topicPattern, endpoint, secret, retryPolicy }) {
        const now = clock.now();
        db.prepare(`INSERT INTO subscriptions (id, consumer, topic_pattern, endpoint, secret, enabled, retry_policy, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?)`).run(id, consumer, topicPattern, endpoint, secret, retryPolicy ? JSON.stringify(retryPolicy) : null, now, now);
        return getSubscription(id);
    }
    function getSubscription(id) {
        return db.prepare('SELECT * FROM subscriptions WHERE id = ?').get(id) || null;
    }
    function listSubscriptions(consumer) {
        return consumer
            ? db.prepare('SELECT * FROM subscriptions WHERE consumer = ? ORDER BY created_at, id').all(consumer)
            : db.prepare('SELECT * FROM subscriptions ORDER BY created_at, id').all();
    }
    function countSubscriptions(consumer) {
        return db.prepare('SELECT COUNT(*) AS n FROM subscriptions WHERE consumer = ?').get(consumer).n;
    }
    function setSubscriptionEnabled(id, enabled) {
        db.prepare('UPDATE subscriptions SET enabled = ?, updated_at = ? WHERE id = ?').run(enabled ? 1 : 0, clock.now(), id);
        return getSubscription(id);
    }

    // ── Deliveries ───────────────────────────────────────────

    /**
     * Next due delivery per subscription (at most one each, so per-subscription concurrency stays 1),
     * ordered by priority class, then seq. `busy` are subscription ids with a delivery in flight.
     */
    function dueDeliveries(now, limit, busy = []) {
        const rows = db.prepare(`
            SELECT * FROM (
                SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.subscription_id ORDER BY d.priority, d.seq) AS rn
                FROM deliveries d JOIN subscriptions s ON s.id = d.subscription_id
                WHERE d.status IN ('pending', 'failed') AND d.next_attempt_at <= ? AND s.enabled = 1
            ) WHERE rn = 1 ORDER BY priority, seq LIMIT ?`).all(now, limit + busy.length);
        const skip = new Set(busy);
        return rows.filter(r => !skip.has(r.subscription_id)).slice(0, limit);
    }

    function recordAttempt(eventId, subscriptionId, outcome) {
        const now = clock.now();
        if (outcome.ok) {
            db.prepare(`UPDATE deliveries SET status = 'delivered', attempt = ?, delivered_at = ?, last_status = ?, last_error = NULL,
                next_attempt_at = NULL, updated_at = ? WHERE event_id = ? AND subscription_id = ?`)
                .run(outcome.attempt, now, outcome.status ?? null, now, eventId, subscriptionId);
        } else {
            db.prepare(`UPDATE deliveries SET status = ?, attempt = ?, last_status = ?, last_error = ?, next_attempt_at = ?, updated_at = ?
                WHERE event_id = ? AND subscription_id = ?`)
                .run(outcome.dead ? 'dead' : 'failed', outcome.attempt, outcome.status ?? null, String(outcome.error || '').slice(0, 500),
                    outcome.dead ? null : outcome.nextAttemptAt, now, eventId, subscriptionId);
        }
    }

    function getDelivery(eventId, subscriptionId) {
        return db.prepare('SELECT * FROM deliveries WHERE event_id = ? AND subscription_id = ?').get(eventId, subscriptionId) || null;
    }

    function listDeliveries({ status, subscriptionId, limit = 100, afterSeq = 0 } = {}) {
        const where = ['seq > ?'];
        const args = [afterSeq];
        if (status) { where.push('status = ?'); args.push(status); }
        if (subscriptionId) { where.push('subscription_id = ?'); args.push(subscriptionId); }
        args.push(limit);
        return db.prepare(`SELECT * FROM deliveries WHERE ${where.join(' AND ')} ORDER BY seq, subscription_id LIMIT ?`).all(...args);
    }

    /**
     * Queue (again) the given events for one subscription: reset to pending, attempt 0, due now.
     * Only retained events that match the subscription's topic are queued. Returns the count.
     */
    const requeue = db.transaction((sub, { fromSeq, eventIds, max = 10000 }) => {
        const now = clock.now();
        let rows;
        if (Array.isArray(eventIds)) {
            const get = db.prepare('SELECT id, seq, priority, event_type FROM events WHERE id = ?');
            rows = eventIds.map(id => get.get(id)).filter(Boolean);
        } else {
            rows = db.prepare('SELECT id, seq, priority, event_type FROM events WHERE seq >= ? AND event_type GLOB ? ORDER BY seq LIMIT ?')
                .all(fromSeq, topics.toGlob(sub.topic_pattern), max * 2);
        }
        rows = rows.filter(r => topics.matches(sub.topic_pattern, r.event_type)).slice(0, max);
        const up = db.prepare(`INSERT INTO deliveries (event_id, subscription_id, seq, priority, attempt, status, next_attempt_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, 'pending', ?, ?, ?)
            ON CONFLICT(event_id, subscription_id) DO UPDATE SET status = 'pending', attempt = 0, next_attempt_at = excluded.next_attempt_at,
                last_error = NULL, last_status = NULL, delivered_at = NULL, updated_at = excluded.updated_at`);
        for (const r of rows) up.run(r.id, sub.id, r.seq, PRIORITY_RANK[r.priority], now, now, now);
        return rows.length;
    });

    function deliveryCounts() {
        const out = { pending: 0, delivered: 0, failed: 0, dead: 0 };
        for (const r of db.prepare('SELECT status, COUNT(*) AS n FROM deliveries GROUP BY status').all()) out[r.status] = r.n;
        return out;
    }

    // ── Checkpoints ──────────────────────────────────────────

    function getCheckpoint(consumer, topicPattern) {
        const r = db.prepare('SELECT cursor, updated_at FROM consumer_checkpoints WHERE consumer = ? AND topic_pattern = ?').get(consumer, topicPattern);
        return r ? { cursor: r.cursor, updated_at: new Date(r.updated_at).toISOString() } : null;
    }
    function setCheckpoint(consumer, topicPattern, cursor) {
        db.prepare(`INSERT INTO consumer_checkpoints (consumer, topic_pattern, cursor, updated_at) VALUES (?, ?, ?, ?)
            ON CONFLICT(consumer, topic_pattern) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at`)
            .run(consumer, topicPattern, cursor, clock.now());
        return getCheckpoint(consumer, topicPattern);
    }

    // ── Retention ────────────────────────────────────────────

    /** Drop events (and their deliveries) older than retentionDays, and old publish receipts. */
    function prune({ retentionDays = 30, receiptRetentionDays = 90, now = clock.now() } = {}) {
        const events = db.prepare('DELETE FROM events WHERE received_at < ?').run(now - retentionDays * DAY_MS).changes;
        const receipts = db.prepare('DELETE FROM idempotency_receipts WHERE processed_at < ?')
            .run(now - Math.max(receiptRetentionDays, retentionDays) * DAY_MS).changes;
        return { events, receipts };
    }

    function ping() {
        return db.prepare('SELECT 1 AS ok').get().ok === 1;
    }

    return {
        db, insertBatch, getEvent, lastSeq, oldestSeq, scan,
        createSubscription, getSubscription, listSubscriptions, countSubscriptions, setSubscriptionEnabled,
        dueDeliveries, recordAttempt, getDelivery, listDeliveries, requeue, deliveryCounts,
        getCheckpoint, setCheckpoint, prune, ping,
    };
}

module.exports = { openDb, createStore, rowToEnvelope, subscriptionView, StoreError, PRIORITY_RANK, RANK_PRIORITY };
