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
const apps = require('./apps');
const redaction = require('./redaction');
const { createUsage, deliveryCode } = require('./usage');

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

-- Developer apps whose access Network revoked (network.app.revoked -> scope 'app') or whose
-- events.app.subscribe grant it withdrew (scope 'events.app.subscribe'). An app token issued at or
-- before revoked_at (ms, the revocation's own timestamp) is refused for that scope.
CREATE TABLE IF NOT EXISTS app_revocations (
    consumer   TEXT NOT NULL,
    scope      TEXT NOT NULL,
    revoked_at INTEGER NOT NULL,
    PRIMARY KEY (consumer, scope)
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

/**
 * Columns added after the first release (developer apps, ADR-014). Existing databases get them
 * with ALTER TABLE; every row stored before is a first-party production row.
 *   events.project_id / env / size_bytes    who owns an app event, its environment, stored bytes
 *   subscriptions.project_id / env          an app subscription's scope (NULL project = first-party)
 * and redaction (server/redaction.js):
 *   events.redacted_at / redacted_by        set when the row became a tombstone (ms, the redacting event_id)
 *   events.redacts                          1 when the row carries a payload.redacts directive
 */
const ADDED_COLUMNS = {
    events: [['project_id', 'TEXT'], ['env', "TEXT NOT NULL DEFAULT 'production'"], ['size_bytes', 'INTEGER NOT NULL DEFAULT 0'],
        ['redacted_at', 'INTEGER'], ['redacted_by', 'TEXT'], ['redacts', 'INTEGER NOT NULL DEFAULT 0']],
    subscriptions: [['project_id', 'TEXT'], ['env', "TEXT NOT NULL DEFAULT 'production'"], ['previous_secret', 'TEXT'], ['previous_secret_until', 'INTEGER']],
};

function migrate(db) {
    for (const [table, cols] of Object.entries(ADDED_COLUMNS)) {
        const have = new Set(db.prepare(`PRAGMA table_info(${table})`).all().map(c => c.name));
        for (const [name, type] of cols) if (!have.has(name)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${type}`);
    }
    db.exec(`CREATE INDEX IF NOT EXISTS idx_events_project ON events(project_id, env, received_at) WHERE project_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_subscriptions_project ON subscriptions(project_id, env) WHERE project_id IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_events_subject ON events(source, subject_type, subject_id);`);
}

function openDb(dbPath) {
    if (dbPath !== ':memory:') fs.mkdirSync(path.dirname(path.resolve(dbPath)), { recursive: true });
    const db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = NORMAL');
    db.pragma('foreign_keys = ON');
    db.pragma('busy_timeout = 5000');
    // A redacted payload (and a pruned row) is overwritten with zeros, not left in free pages.
    db.pragma('secure_delete = ON');
    db.exec(SCHEMA);
    migrate(db);
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
    if (row.project_id) {
        out.project_id = row.project_id;
        out.env = row.env || 'production';
    }
    if (withSecret) out.secret = row.secret;
    return out;
}

function createStore(db, { clock = { now: () => Date.now() }, maxHops = 8, usage: usageConfig = { enabled: true } } = {}) {
    // Project usage rollups (./usage.js): counted in the transactions below, sent as events.usage.recorded.
    const usage = createUsage(db, { clock, enabled: usageConfig.enabled !== false });
    const q = {
        getEvent: db.prepare('SELECT * FROM events WHERE id = ?'),
        getReceipt: db.prepare('SELECT processed_at FROM idempotency_receipts WHERE consumer = ? AND event_id = ?'),
        addReceipt: db.prepare('INSERT OR IGNORE INTO idempotency_receipts (consumer, event_id, processed_at) VALUES (?, ?, ?)'),
        nextSeq: db.prepare("UPDATE sequences SET value = value + 1 WHERE name = 'events' RETURNING value"),
        lastSeq: db.prepare("SELECT value FROM sequences WHERE name = 'events'"),
        minSeq: db.prepare('SELECT MIN(seq) AS s FROM events'),
        // Hop depth counts only rows the publisher's own tenancy can have caused: a first-party
        // publish ignores app events (an app that saw a public trace_id cannot poison that trace), an
        // app publish counts first-party rows plus its own project and environment.
        chainHops: db.prepare('SELECT MAX(hops) AS h FROM events WHERE trace_id = ? AND source != ? AND project_id IS NULL'),
        chainHopsApp: db.prepare(`SELECT MAX(hops) AS h FROM events WHERE trace_id = ? AND source != ?
            AND (project_id IS NULL OR (project_id = ? AND env = ?))`),
        selfRepeats: db.prepare(`SELECT COUNT(*) AS n FROM events WHERE trace_id = ? AND source = ? AND event_type = ?
            AND subject_type = ? AND subject_id = ? AND (request_id IS NULL OR request_id != ?)`),
        insertEvent: db.prepare(`INSERT INTO events (id, seq, event_type, version, source, actor, subject_type, subject_id,
            subject_revision, trace_id, priority, visibility, occurred_at, received_at, payload, hops, publisher, request_id,
            project_id, env, size_bytes, redacts)
            VALUES (@id, @seq, @event_type, @version, @source, @actor, @subject_type, @subject_id, @subject_revision,
            @trace_id, @priority, @visibility, @occurred_at, @received_at, @payload, @hops, @publisher, @request_id,
            @project_id, @env, @size_bytes, @redacts)`),
        redactBySubject: db.prepare(`SELECT * FROM events WHERE source = ? AND subject_type = ? AND subject_id = ?
            AND redacted_at IS NULL AND redacts = 0`),
        applyTombstone: db.prepare(`UPDATE events SET payload = @payload, actor = @actor, size_bytes = @size_bytes,
            redacted_at = @redacted_at, redacted_by = @redacted_by WHERE id = @id AND redacted_at IS NULL`),
        firstSeqSince: db.prepare('SELECT MIN(seq) AS s FROM events WHERE received_at >= ?'),
        enabledSubs: db.prepare('SELECT id, topic_pattern, project_id, env FROM subscriptions WHERE enabled = 1'),
        projectRecent: db.prepare('SELECT COUNT(*) AS n FROM events WHERE project_id = ? AND env = ? AND received_at > ?'),
        projectBytes: db.prepare('SELECT COALESCE(SUM(size_bytes), 0) AS b FROM events WHERE project_id = ? AND env = ?'),
        disableAppSubs: db.prepare('UPDATE subscriptions SET enabled = 0, updated_at = ? WHERE consumer = ? AND enabled = 1'),
        revoke: db.prepare(`INSERT INTO app_revocations (consumer, scope, revoked_at) VALUES (?, ?, ?)
            ON CONFLICT(consumer, scope) DO UPDATE SET revoked_at = MAX(revoked_at, excluded.revoked_at)`),
        revokedAt: db.prepare('SELECT revoked_at FROM app_revocations WHERE consumer = ? AND scope = ?'),
        insertDelivery: db.prepare(`INSERT OR IGNORE INTO deliveries (event_id, subscription_id, seq, priority, attempt, status,
            next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, 0, 'pending', ?, ?, ?)`),
        afterSeq: db.prepare('SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT ?'),
        afterSeqGlob: db.prepare('SELECT * FROM events WHERE seq > ? AND event_type GLOB ? ORDER BY seq LIMIT ?'),
    };

    // ── Events ───────────────────────────────────────────────

    /**
     * Store a batch atomically. `items` are validated envelopes with defaults filled. Returns
     * { results: [{ event_id, seq, duplicate }], inserted: [row] }. Throws StoreError for an id
     * conflict, a detected loop or an exceeded project quota; nothing of the batch is stored then.
     *
     * `project` ({ projectId, env, publishPerMinute, retainedBytes }) marks an app publish: the rows
     * carry project_id and env, and the project's rate and retained-bytes quotas are checked here,
     * inside the transaction, against what is already stored.
     */
    const insertBatch = db.transaction((items, { publisher, requestId, project = null }) => {
        const now = clock.now();
        const results = [];
        const inserted = [];
        const subs = q.enabledSubs.all();
        let recent = null;
        let bytes = null;
        if (project) {
            recent = q.projectRecent.get(project.projectId, project.env, now - 60 * 1000).n;
            bytes = q.projectBytes.get(project.projectId, project.env).b;
        }
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
            const chainRow = project
                ? q.chainHopsApp.get(env.trace_id, env.source, project.projectId, project.env)
                : q.chainHops.get(env.trace_id, env.source);
            const chain = (chainRow.h || 0) + 1;
            const repeats = q.selfRepeats.get(env.trace_id, env.source, env.event_type, env.subject.type, env.subject.id, requestId || '').n + 1;
            const hops = Math.max(chain, repeats);
            if (hops > maxHops) {
                throw new StoreError(409, 'events.loop_detected',
                    `trace ${env.trace_id} already carries ${hops - 1} hops for ${env.event_type}; refusing to extend a probable loop`,
                    { event_id: env.event_id, hops: hops - 1, max_hops: maxHops });
            }
            const payloadJson = JSON.stringify(env.payload);
            const actorJson = JSON.stringify(env.actor);
            const sizeBytes = Buffer.byteLength(payloadJson) + Buffer.byteLength(actorJson) + env.event_type.length + env.subject.id.length;
            if (project) {
                recent += 1;
                bytes += sizeBytes;
                if (project.publishPerMinute && recent > project.publishPerMinute) {
                    throw new StoreError(429, 'events.quota_exceeded', `project ${project.projectId} (${project.env}) may publish at most ${project.publishPerMinute} events per minute`,
                        { quota: 'publish_rate', limit: project.publishPerMinute, window: 'minute', retry_after: 60 });
                }
                if (project.retainedBytes && bytes > project.retainedBytes) {
                    throw new StoreError(429, 'events.quota_exceeded', `project ${project.projectId} (${project.env}) may keep at most ${project.retainedBytes} bytes of events`,
                        { quota: 'retained_bytes', limit: project.retainedBytes, used: bytes - sizeBytes });
                }
            }
            const seq = q.nextSeq.get().value;
            const row = {
                id: env.event_id,
                seq,
                event_type: env.event_type,
                version: env.version,
                source: env.source,
                actor: actorJson,
                subject_type: env.subject.type,
                subject_id: env.subject.id,
                subject_revision: env.subject.revision ?? null,
                trace_id: env.trace_id,
                priority: env.priority,
                visibility: env.visibility,
                occurred_at: env.timestamp,
                received_at: now,
                payload: payloadJson,
                hops,
                publisher,
                request_id: requestId || null,
                project_id: project ? project.projectId : null,
                env: project ? project.env : 'production',
                size_bytes: sizeBytes,
                redacts: 0,
            };
            const directive = redaction.parseDirective(env.payload);
            if (directive && directive.error) throw new StoreError(422, 'events.invalid_redaction', directive.error, { event_id: env.event_id });
            if (directive) row.redacts = 1;
            q.insertEvent.run(row);
            q.addReceipt.run(PUBLISH_RECEIPT, env.event_id, now);
            for (const s of subs) {
                if (apps.deliverable(s, row)) {
                    q.insertDelivery.run(env.event_id, s.id, seq, PRIORITY_RANK[env.priority], now, now, now);
                }
            }
            revocationHook(row, now);
            results.push({ event_id: env.event_id, seq, duplicate: false });
            inserted.push(row);
            if (directive) {
                const redacted = applyRedaction(row, directive, now);
                // Rows stored earlier in this batch are fanned out after the commit from memory: they
                // must go out as the tombstones they now are.
                for (const t of redacted) {
                    const mine = inserted.find(r => r.id === t.id);
                    if (mine) Object.assign(mine, t.columns);
                }
            }
        }
        // An app publish counts toward its project's usage in the same transaction as the rows.
        if (project && inserted.length) {
            usage.record({ projectId: project.projectId, env: project.env, capability: 'events.app.publish', unit: 'events', quantity: inserted.length, at: now });
        }
        return { results, inserted };
    });

    /**
     * Turn the targets of `directive` (server/redaction.js) into tombstones on behalf of `by` (the
     * stored row of the redacting event). Throws StoreError 403 when an event id names an event of
     * another owner. Returns [{ id, seq, columns }] for the rows it rewrote.
     */
    function applyRedaction(by, directive, now) {
        const targets = new Map();
        for (const id of directive.eventIds) {
            const r = q.getEvent.get(id);
            if (!r) continue;   // never stored, or pruned already
            if (!redaction.sameOwner(by, r)) {
                throw new StoreError(403, 'events.redaction_not_allowed', `event ${id} belongs to ${r.source}; ${by.source} may redact only its own events`,
                    { event_id: by.id, target: id });
            }
            if (r.redacted_at == null && !r.redacts) targets.set(r.id, r);
        }
        if (directive.subjectType) {
            for (const subjectId of directive.subjectIds) {
                for (const r of q.redactBySubject.all(by.source, directive.subjectType, subjectId)) {
                    if (redaction.sameOwner(by, r)) targets.set(r.id, r);
                }
            }
        }
        const out = [];
        for (const r of targets.values()) {
            if (r.id === by.id) continue;
            const columns = redaction.tombstone(r, { by: by.id, at: now });
            if (q.applyTombstone.run({ id: r.id, ...columns }).changes) out.push({ id: r.id, seq: r.seq, columns });
        }
        return out;
    }

    /**
     * Operator redaction (scripts/redact-backfill.js): what an event of first-party `source` carrying
     * `directive` (the payload.redacts object) would have redacted. The tombstones name `by` as
     * redacted_by. Same owner rule as a publish. Returns the rewritten [{ id, seq }].
     */
    const redact = db.transaction((source, directive, { by = 'operator', now = clock.now() } = {}) => {
        const parsed = redaction.parseDirective({ redacts: directive });
        if (!parsed || parsed.error) throw new StoreError(422, 'events.invalid_redaction', parsed ? parsed.error : 'nothing to redact');
        return applyRedaction({ id: by, source, project_id: null, env: 'production' }, parsed, now).map(({ id, seq }) => ({ id, seq }));
    });

    /**
     * Network tells Events when an app loses its access (network.app.revoked, also sent for every
     * app of an archived project) or its events.app.subscribe grant (network.grant.changed). The
     * app's subscriptions stop at once, in the same transaction that stores the event, so a
     * revocation reaches deliveries without waiting for anything else.
     */
    function revocationHook(row, now) {
        if (row.source !== 'network' || row.project_id || row.subject_type !== 'app') return;
        let payload = {};
        try { payload = JSON.parse(row.payload) || {}; } catch { payload = {}; }
        let scope = null;
        if (row.event_type === 'network.app.revoked') scope = 'app';
        else if (row.event_type === 'network.grant.changed' && ['events.app.subscribe', 'events.app.*', 'events.*'].includes(payload.capability)
            && !['approved', 'requested'].includes(payload.to)) scope = 'events.app.subscribe';
        if (!scope || !/^app_[0-9A-HJKMNP-TV-Z]{26}$/.test(row.subject_id)) return;
        const consumer = `app:${row.subject_id}`;
        const at = Date.parse(row.occurred_at);
        q.revoke.run(consumer, scope, Number.isFinite(at) ? at : now);
        q.disableAppSubs.run(now, consumer);
    }

    /** When Network revoked `consumer` (app:app_…) for `scope` ('app' | 'events.app.subscribe'), in ms; or null. */
    function revokedAt(consumer, scope) {
        const r = q.revokedAt.get(consumer, scope);
        return r ? r.revoked_at : null;
    }

    function getEvent(id) {
        return q.getEvent.get(id) || null;
    }

    function lastSeq() {
        return q.lastSeq.get().value;
    }

    /** First seq received at or after `ms`; the next seq to be handed out when there is none. */
    function firstSeqSince(ms) {
        const m = q.firstSeqSince.get(ms).s;
        return m == null ? lastSeq() + 1 : m;
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

    function createSubscription({ id, consumer, topicPattern, endpoint, secret, retryPolicy, projectId = null, env = 'production' }) {
        const now = clock.now();
        db.prepare(`INSERT INTO subscriptions (id, consumer, topic_pattern, endpoint, secret, enabled, retry_policy, created_at, updated_at, project_id, env)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`).run(id, consumer, topicPattern, endpoint, secret, retryPolicy ? JSON.stringify(retryPolicy) : null, now, now, projectId, env);
        return getSubscription(id);
    }
    function countProjectSubscriptions(projectId, env) {
        return db.prepare('SELECT COUNT(*) AS n FROM subscriptions WHERE project_id = ? AND env = ?').get(projectId, env).n;
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
    /** Rotate a subscription's secret: the old one keeps signing (next to the new) until overlapMs from now. */
    function rotateSubscriptionSecret(id, secret, overlapMs) {
        const now = clock.now();
        db.prepare('UPDATE subscriptions SET previous_secret = secret, previous_secret_until = ?, secret = ?, updated_at = ? WHERE id = ?')
            .run(now + Math.max(0, overlapMs), secret, now, id);
        return getSubscription(id);
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
    function dueDeliveries(now, limit, busy = [], { firstPartyOnly = false } = {}) {
        // app_project_id: set for developer-app subscriptions (the worker caps their share of slots).
        const rows = db.prepare(`
            SELECT * FROM (
                SELECT d.*, s.project_id AS app_project_id, ROW_NUMBER() OVER (PARTITION BY d.subscription_id ORDER BY d.priority, d.seq) AS rn
                FROM deliveries d JOIN subscriptions s ON s.id = d.subscription_id
                WHERE d.status IN ('pending', 'failed') AND d.next_attempt_at <= ? AND s.enabled = 1
                ${firstPartyOnly ? 'AND s.project_id IS NULL' : ''}
            ) WHERE rn = 1 ORDER BY priority, seq LIMIT ?`).all(now, limit + busy.length);
        const skip = new Set(busy);
        return rows.filter(r => !skip.has(r.subscription_id)).slice(0, limit);
    }

    /**
     * Record one delivery attempt. `app` ({ projectId, env, traceId }) marks a developer-app
     * subscription: the attempt counts toward the project's events.app.subscribe usage in the same
     * transaction.
     */
    const recordAttempt = db.transaction((eventId, subscriptionId, outcome, app = null) => {
        const now = clock.now();
        if (app) {
            usage.record({
                projectId: app.projectId, env: app.env, capability: 'events.app.subscribe', unit: 'deliveries', quantity: 1, at: now,
                error: outcome.ok ? null : { code: deliveryCode(outcome), status: outcome.status || undefined, traceId: app.traceId, ref: eventId },
            });
        }
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
    });

    /** Send the closed hours' usage rollups (./usage.js); returns the stored rows. */
    function flushUsage(opts) {
        return usage.flush(insertBatch, opts);
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
        const cols = 'id, seq, priority, event_type, visibility, project_id, env';
        if (Array.isArray(eventIds)) {
            const get = db.prepare(`SELECT ${cols} FROM events WHERE id = ?`);
            rows = eventIds.map(id => get.get(id)).filter(Boolean);
        } else {
            rows = db.prepare(`SELECT ${cols} FROM events WHERE seq >= ? AND event_type GLOB ? ORDER BY seq LIMIT ?`)
                .all(fromSeq, topics.toGlob(sub.topic_pattern), max * 2);
        }
        // Replay never widens a subscription's scope (app scope, sandbox separation).
        rows = rows.filter(r => apps.deliverable(sub, r)).slice(0, max);
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

    /**
     * Drop events (and their deliveries) older than retentionDays (sandbox events older than
     * sandboxRetentionDays, when shorter), and old publish receipts.
     */
    function prune({ retentionDays = 30, receiptRetentionDays = 90, sandboxRetentionDays = retentionDays, now = clock.now() } = {}) {
        let events = db.prepare('DELETE FROM events WHERE received_at < ?').run(now - retentionDays * DAY_MS).changes;
        if (sandboxRetentionDays < retentionDays) {
            events += db.prepare("DELETE FROM events WHERE env = 'sandbox' AND received_at < ?").run(now - sandboxRetentionDays * DAY_MS).changes;
        }
        const receipts = db.prepare('DELETE FROM idempotency_receipts WHERE processed_at < ?')
            .run(now - Math.max(receiptRetentionDays, retentionDays) * DAY_MS).changes;
        return { events, receipts };
    }

    function ping() {
        return db.prepare('SELECT 1 AS ok').get().ok === 1;
    }

    return {
        db, insertBatch, redact, getEvent, revokedAt, lastSeq, oldestSeq, firstSeqSince, scan,
        createSubscription, getSubscription, listSubscriptions, countSubscriptions, countProjectSubscriptions, setSubscriptionEnabled, rotateSubscriptionSecret,
        dueDeliveries, recordAttempt, getDelivery, listDeliveries, requeue, deliveryCounts,
        getCheckpoint, setCheckpoint, prune, ping, usage, flushUsage,
    };
}

module.exports = { openDb, createStore, rowToEnvelope, subscriptionView, StoreError, PRIORITY_RANK, RANK_PRIORITY };
