'use strict';
/**
 * The durable store (PostgreSQL through openvibe-sdk/db). An event is committed, together with one
 * delivery row per matching subscription, before anything is sent anywhere: "event persists before
 * consumer delivery". Global order is `seq`, handed out from a counter that never goes backwards,
 * even after retention has pruned the newest rows. Beside it a cursor (./cursor.js) names a seq plus
 * the retention epoch — ADR-042 decision 7; seq stays on the wire for one release.
 */
const fs = require('fs');
const path = require('path');
const { createDb } = require('openvibe-sdk/db');
const topics = require('./topics');
const apps = require('./apps');
const redaction = require('./redaction');
const cursor = require('./cursor');
const { createUsage, deliveryCode } = require('./usage');

const PRIORITY_RANK = { critical: 0, important: 1, low: 2 };
const RANK_PRIORITY = ['critical', 'important', 'low'];
const DAY_MS = 24 * 60 * 60 * 1000;

const MIGRATIONS = path.join(__dirname, '..', 'migrations');
const DEV_PGLITE = path.join(__dirname, '..', 'data', 'pglite');

const PUBLISH_RECEIPT = 'events:publish';  // receipts' consumer column for accepted publishes

/**
 * The serving handle (ADR-035): DATABASE_URL through PgBouncer; in development without it, an embedded PGlite database
 * in data/pglite. Migrations run first, as the owner (DATABASE_DIRECT_URL), or on the embedded handle. The columns added
 * after the first release (developer apps, ADR-014; redaction) and the sequence row are in migrations/0001_initial.sql.
 */
async function openDb(config, { log = console, registry } = {}) {
    if (!config.db.url) {
        if (config.isProduction) throw new Error('DATABASE_URL is not set: production serves from PostgreSQL (OpenVibe.Host roles/data add-service.sh events)');
        log.warn(`[events] DATABASE_URL unset: embedded PGlite database in ${DEV_PGLITE} (development only, one process)`);
        fs.mkdirSync(DEV_PGLITE, { recursive: true });
        const db = createDb({ pglite: DEV_PGLITE, service: 'events', registry, log });
        await db.migrate({ dir: MIGRATIONS, log });
        return db;
    }
    if (!config.db.directUrl) throw new Error('DATABASE_DIRECT_URL is not set: migrations run with the owner role on a direct connection');
    const owner = createDb({ url: config.db.directUrl, service: 'events-migrate', max: 1, log });
    try { await owner.migrate({ dir: MIGRATIONS, log }); } finally { await owner.close(); }
    return createDb({ url: config.db.url, service: 'events', registry, log });
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
        addReceipt: db.prepare('INSERT INTO idempotency_receipts (consumer, event_id, processed_at) VALUES (?, ?, ?) ON CONFLICT DO NOTHING'),
        nextSeq: db.prepare("UPDATE sequences SET value = value + 1 WHERE name = 'events' RETURNING value"),
        lastSeq: db.prepare("SELECT value FROM sequences WHERE name = 'events'"),
        minSeq: db.prepare('SELECT MIN(seq) AS s FROM events'),
        epoch: db.prepare("SELECT value FROM store_epoch WHERE name = 'events'"),
        bumpEpoch: db.prepare("UPDATE store_epoch SET value = value + 1 WHERE name = 'events' RETURNING value"),
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
        projectBytes: db.prepare('SELECT COALESCE(SUM(size_bytes), 0)::bigint AS b FROM events WHERE project_id = ? AND env = ?'),
        disableAppSubs: db.prepare('UPDATE subscriptions SET enabled = 0, updated_at = ? WHERE consumer = ? AND enabled = 1'),
        revoke: db.prepare(`INSERT INTO app_revocations (consumer, scope, revoked_at) VALUES (?, ?, ?)
            ON CONFLICT(consumer, scope) DO UPDATE SET revoked_at = GREATEST(app_revocations.revoked_at, excluded.revoked_at)`),
        revokedAt: db.prepare('SELECT revoked_at FROM app_revocations WHERE consumer = ? AND scope = ?'),
        insertDelivery: db.prepare(`INSERT INTO deliveries (event_id, subscription_id, seq, priority, attempt, status,
            next_attempt_at, created_at, updated_at) VALUES (?, ?, ?, ?, 0, 'pending', ?, ?, ?) ON CONFLICT DO NOTHING`),
        afterSeq: db.prepare('SELECT * FROM events WHERE seq > ? ORDER BY seq LIMIT ?'),
        afterSeqLike: db.prepare("SELECT * FROM events WHERE seq > ? AND event_type ILIKE ? ESCAPE '\\' ORDER BY seq LIMIT ?"),
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
    const insertBatch = async (items, { publisher, requestId, project = null }) => await db.tx(async () => {
        const now = clock.now();
        const epoch = (await q.epoch.get()).value;   // the retention epoch the stored positions belong to (ADR-042)
        const results = [];
        const inserted = [];
        const subs = await q.enabledSubs.all();
        let recent = null;
        let bytes = null;
        if (project) {
            recent = (await q.projectRecent.get(project.projectId, project.env, now - 60 * 1000)).n;
            bytes = (await q.projectBytes.get(project.projectId, project.env)).b;
        }
        for (const env of items) {
            const existing = await q.getEvent.get(env.event_id);
            if (existing) {
                if (existing.source !== env.source || existing.event_type !== env.event_type) {
                    throw new StoreError(409, 'events.id_conflict', `event ${env.event_id} already exists with different content`);
                }
                results.push({ event_id: env.event_id, seq: existing.seq, duplicate: true, cursor: cursor.encode(existing.seq, epoch) });
                continue;
            }
            if (await q.getReceipt.get(PUBLISH_RECEIPT, env.event_id)) {
                results.push({ event_id: env.event_id, seq: null, duplicate: true, pruned: true });
                continue;
            }
            // Loop guard: depth of the cross-service chain in this trace, and how often this exact
            // (source, type, subject) already happened in the trace from other publish calls.
            const chainRow = project
                ? await q.chainHopsApp.get(env.trace_id, env.source, project.projectId, project.env)
                : await q.chainHops.get(env.trace_id, env.source);
            const chain = (chainRow.h || 0) + 1;
            const repeats = (await q.selfRepeats.get(env.trace_id, env.source, env.event_type, env.subject.type, env.subject.id, requestId || '')).n + 1;
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
            const seq = (await q.nextSeq.get()).value;
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
            await q.insertEvent.run(row);
            await q.addReceipt.run(PUBLISH_RECEIPT, env.event_id, now);
            for (const s of subs) {
                if (apps.deliverable(s, row)) {
                    await q.insertDelivery.run(env.event_id, s.id, seq, PRIORITY_RANK[env.priority], now, now, now);
                }
            }
            await revocationHook(row, now);
            results.push({ event_id: env.event_id, seq, duplicate: false, cursor: cursor.encode(seq, epoch) });
            inserted.push(row);
            if (directive) {
                const redacted = await applyRedaction(row, directive, now);
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
            await usage.record({ projectId: project.projectId, env: project.env, capability: 'events.app.publish', unit: 'events', quantity: inserted.length, at: now });
        }
        return { results, inserted };
    });

    /**
     * Turn the targets of `directive` (server/redaction.js) into tombstones on behalf of `by` (the
     * stored row of the redacting event). Throws StoreError 403 when an event id names an event of
     * another owner. Returns [{ id, seq, columns }] for the rows it rewrote.
     */
    async function applyRedaction(by, directive, now) {
        const targets = new Map();
        for (const id of directive.eventIds) {
            const r = await q.getEvent.get(id);
            if (!r) continue;   // never stored, or pruned already
            if (!redaction.sameOwner(by, r)) {
                throw new StoreError(403, 'events.redaction_not_allowed', `event ${id} belongs to ${r.source}; ${by.source} may redact only its own events`,
                    { event_id: by.id, target: id });
            }
            if (r.redacted_at == null && !r.redacts) targets.set(r.id, r);
        }
        if (directive.subjectType) {
            for (const subjectId of directive.subjectIds) {
                for (const r of await q.redactBySubject.all(by.source, directive.subjectType, subjectId)) {
                    if (redaction.sameOwner(by, r)) targets.set(r.id, r);
                }
            }
        }
        const out = [];
        for (const r of targets.values()) {
            if (r.id === by.id) continue;
            const columns = redaction.tombstone(r, { by: by.id, at: now });
            if ((await q.applyTombstone.run({ id: r.id, ...columns })).changes) out.push({ id: r.id, seq: r.seq, columns });
        }
        return out;
    }

    /**
     * Operator redaction (scripts/redact-backfill.js): what an event of first-party `source` carrying
     * `directive` (the payload.redacts object) would have redacted. The tombstones name `by` as
     * redacted_by. Same owner rule as a publish. Returns the rewritten [{ id, seq }].
     */
    const redact = async (source, directive, { by = 'operator', now = clock.now() } = {}) => await db.tx(async () => {
        const parsed = redaction.parseDirective({ redacts: directive });
        if (!parsed || parsed.error) throw new StoreError(422, 'events.invalid_redaction', parsed ? parsed.error : 'nothing to redact');
        return (await applyRedaction({ id: by, source, project_id: null, env: 'production' }, parsed, now)).map(({ id, seq }) => ({ id, seq }));
    });

    /**
     * Network tells Events when an app loses its access (network.app.revoked, also sent for every
     * app of an archived project) or its events.app.subscribe grant (network.grant.changed). The
     * app's subscriptions stop at once, in the same transaction that stores the event, so a
     * revocation reaches deliveries without waiting for anything else.
     */
    async function revocationHook(row, now) {
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
        await q.revoke.run(consumer, scope, Number.isFinite(at) ? at : now);
        await q.disableAppSubs.run(now, consumer);
    }

    /** When Network revoked `consumer` (app:app_…) for `scope` ('app' | 'events.app.subscribe'), in ms; or null. */
    async function revokedAt(consumer, scope) {
        const r = await q.revokedAt.get(consumer, scope);
        return r ? r.revoked_at : null;
    }

    async function getEvent(id) {
        return await q.getEvent.get(id) || null;
    }

    async function lastSeq() {
        return (await q.lastSeq.get()).value;
    }

    /** First seq received at or after `ms`; the next seq to be handed out when there is none. */
    async function firstSeqSince(ms) {
        const m = (await q.firstSeqSince.get(ms)).s;
        return m == null ? await lastSeq() + 1 : m;
    }

    /** Oldest seq still stored; when the table is empty, the next seq to be handed out. */
    async function oldestSeq() {
        const m = (await q.minSeq.get()).s;
        return m == null ? await lastSeq() + 1 : m;
    }

    /** The retention epoch of the hot store (ADR-042 decision 7): a cursor carries it so a position from a previous
     *  epoch — after a truncate or a re-import — is answered with a gap, never used as if it were current. */
    async function epoch() {
        return (await q.epoch.get()).value;
    }

    /** Advance the retention epoch. Called only when the hot store is truncated or re-imported. Returns the new epoch. */
    async function bumpEpoch() {
        return (await q.bumpEpoch.get()).value;
    }

    /**
     * Events with seq > afterSeq that match any of `patterns`, in order, filtered by `accept(row)`.
     * Scans at most `scanMax` rows; returns { rows, cursor } where cursor is the last seq examined
     * (so a pull consumer's cursor moves past rows that did not match).
     */
    async function scan(afterSeq, { patterns = ['*'], limit = 100, scanMax = 5000, accept = () => true } = {}) {
        const rows = [];
        let cursor = afterSeq;
        let scanned = 0;
        const single = patterns.length === 1 && patterns[0] !== '*' ? topics.toLike(patterns[0]) : null;
        for (;;) {
            const page = single ? await q.afterSeqLike.all(cursor, single, 500) : await q.afterSeq.all(cursor, 500);
            for (const row of page) {
                scanned++;
                cursor = row.seq;
                if (patterns.some(p => topics.matches(p, row.event_type)) && accept(row)) rows.push(row);
                if (rows.length >= limit || scanned >= scanMax) break;
            }
            if (rows.length >= limit || scanned >= scanMax) break;
            if (page.length < 500) {
                // Reached the end. With the ILIKE narrowing, rows that were never fetched cannot match,
                // so the cursor may move up to the latest seq handed out.
                if (single) cursor = Math.max(cursor, await lastSeq());
                break;
            }
        }
        return { rows, cursor };
    }

    // ── Subscriptions ────────────────────────────────────────

    async function createSubscription({ id, consumer, topicPattern, endpoint, secret, retryPolicy, projectId = null, env = 'production' }) {
        const now = clock.now();
        await db.prepare(`INSERT INTO subscriptions (id, consumer, topic_pattern, endpoint, secret, enabled, retry_policy, created_at, updated_at, project_id, env)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)`).run(id, consumer, topicPattern, endpoint, secret, retryPolicy ? JSON.stringify(retryPolicy) : null, now, now, projectId, env);
        return await getSubscription(id);
    }
    async function countProjectSubscriptions(projectId, env) {
        return (await db.prepare('SELECT COUNT(*) AS n FROM subscriptions WHERE project_id = ? AND env = ?').get(projectId, env)).n;
    }
    async function getSubscription(id) {
        return await db.prepare('SELECT * FROM subscriptions WHERE id = ?').get(id) || null;
    }
    async function listSubscriptions(consumer) {
        return consumer
            ? await db.prepare('SELECT * FROM subscriptions WHERE consumer = ? ORDER BY created_at, id').all(consumer)
            : await db.prepare('SELECT * FROM subscriptions ORDER BY created_at, id').all();
    }
    async function countSubscriptions(consumer) {
        return (await db.prepare('SELECT COUNT(*) AS n FROM subscriptions WHERE consumer = ?').get(consumer)).n;
    }
    /** Rotate a subscription's secret: the old one keeps signing (next to the new) until overlapMs from now. */
    async function rotateSubscriptionSecret(id, secret, overlapMs) {
        const now = clock.now();
        await db.prepare('UPDATE subscriptions SET previous_secret = secret, previous_secret_until = ?, secret = ?, updated_at = ? WHERE id = ?')
            .run(now + Math.max(0, overlapMs), secret, now, id);
        return await getSubscription(id);
    }

    async function setSubscriptionEnabled(id, enabled) {
        await db.prepare('UPDATE subscriptions SET enabled = ?, updated_at = ? WHERE id = ?').run(enabled ? 1 : 0, clock.now(), id);
        return await getSubscription(id);
    }

    // ── Deliveries ───────────────────────────────────────────

    /**
     * Claim up to `limit` due deliveries for `owner` until `leaseUntil` (ms): the next due delivery (by priority class,
     * then seq) of each subscription that has none in flight. The claim locks the subscription rows
     * (FOR UPDATE SKIP LOCKED), so two workers — in one process or many — never claim the same subscription at once, and
     * the lease it writes keeps every other worker off that subscription until recordAttempt clears it or it expires
     * (an expired lease is claimable again: delivery is at least once). `maxApp` caps developer-app subscriptions.
     */
    const claimDeliveries = async (now, limit, { owner, leaseUntil, maxApp = limit } = {}) => {
        if (!owner || !(leaseUntil > now) || limit <= 0) return [];
        return await db.tx(async () => {
            const due = `d.status IN ('pending', 'failed') AND d.next_attempt_at <= ?`;
            // First-party and developer-app subscriptions are fetched apart, each up to its own share, then merged by
            // priority: app subscriptions that outrank first-party ones can never crowd them out of a claim.
            const candidates = (apps, n) => (n <= 0 ? [] : db.prepare(`
                SELECT s.id, s.project_id,
                    (SELECT d.priority FROM deliveries d WHERE d.subscription_id = s.id AND ${due} ORDER BY d.priority, d.seq LIMIT 1) AS p,
                    (SELECT d.seq FROM deliveries d WHERE d.subscription_id = s.id AND ${due} ORDER BY d.priority, d.seq LIMIT 1) AS q
                FROM subscriptions s
                WHERE s.enabled = 1 AND s.project_id IS ${apps ? 'NOT ' : ''}NULL
                  AND EXISTS (SELECT 1 FROM deliveries d WHERE d.subscription_id = s.id AND ${due})
                  AND NOT EXISTS (SELECT 1 FROM deliveries x WHERE x.subscription_id = s.id AND x.lease_until > ?)
                ORDER BY p, q LIMIT ?
                FOR UPDATE OF s SKIP LOCKED`).all(now, now, now, now, n));
            const subs = [...await candidates(false, limit), ...await candidates(true, Math.min(limit, Math.max(0, maxApp)))]
                .sort((x, y) => Number(x.p) - Number(y.p) || Number(x.q) - Number(y.q));
            const picked = [];
            let apps = 0;
            const next = db.prepare(`SELECT d.*, s.project_id AS app_project_id FROM deliveries d JOIN subscriptions s ON s.id = d.subscription_id
                WHERE d.subscription_id = ? AND d.status IN ('pending', 'failed') AND d.next_attempt_at <= ? ORDER BY d.priority, d.seq LIMIT 1`);
            // The lease is taken only if nothing of this subscription is leased as of NOW: this statement starts after the
            // subscription row was locked, so it sees a lease another worker committed after the candidate query's
            // snapshot (READ COMMITTED does not re-check that query's NOT EXISTS when the locked row itself did not change).
            const lease = db.prepare(`UPDATE deliveries d SET lease_owner = ?, lease_until = ? WHERE d.event_id = ? AND d.subscription_id = ?
                AND d.status IN ('pending', 'failed') AND NOT EXISTS (SELECT 1 FROM deliveries x WHERE x.subscription_id = d.subscription_id AND x.lease_until > ?)`);
            for (const sub of subs) {
                if (picked.length >= limit) break;
                if (sub.project_id && apps >= maxApp) continue;
                const d = await next.get(sub.id, now);
                if (!d) continue;
                if ((await lease.run(owner, leaseUntil, d.event_id, d.subscription_id, now)).changes !== 1) continue;   // another worker got there first
                picked.push({ ...d, lease_owner: owner, lease_until: leaseUntil });
                if (sub.project_id) apps++;
            }
            return picked;
        });
    };

    /**
     * Record one delivery attempt. `app` ({ projectId, env, traceId }) marks a developer-app
     * subscription: the attempt counts toward the project's events.app.subscribe usage in the same
     * transaction.
     */
    const recordAttempt = async (eventId, subscriptionId, outcome, app = null, owner = null) => await db.tx(async () => {
        const now = clock.now();
        // The attempt is this worker's only while it still holds the lease: after an expiry another worker may have
        // claimed the delivery again, and its result is the one to keep (this one returns false and records nothing).
        if (owner) {
            const held = await db.prepare('SELECT 1 FROM deliveries WHERE event_id = ? AND subscription_id = ? AND lease_owner = ? FOR UPDATE').get(eventId, subscriptionId, owner);
            if (!held) return false;
        }
        if (app) {
            await usage.record({
                projectId: app.projectId, env: app.env, capability: 'events.app.subscribe', unit: 'deliveries', quantity: 1, at: now,
                error: outcome.ok ? null : { code: deliveryCode(outcome), status: outcome.status || undefined, traceId: app.traceId, ref: eventId },
            });
        }
        if (outcome.ok) {
            await db.prepare(`UPDATE deliveries SET status = 'delivered', attempt = ?, delivered_at = ?, last_status = ?, last_error = NULL,
                next_attempt_at = NULL, lease_owner = NULL, lease_until = NULL, updated_at = ? WHERE event_id = ? AND subscription_id = ?`)
                .run(outcome.attempt, now, outcome.status ?? null, now, eventId, subscriptionId);
        } else {
            await db.prepare(`UPDATE deliveries SET status = ?, attempt = ?, last_status = ?, last_error = ?, next_attempt_at = ?,
                lease_owner = NULL, lease_until = NULL, updated_at = ? WHERE event_id = ? AND subscription_id = ?`)
                .run(outcome.dead ? 'dead' : 'failed', outcome.attempt, outcome.status ?? null, String(outcome.error || '').slice(0, 500),
                    outcome.dead ? null : outcome.nextAttemptAt, now, eventId, subscriptionId);
        }
        return true;
    });

    /** Send the closed hours' usage rollups (./usage.js); returns the stored rows. */
    async function flushUsage(opts) {
        return await usage.flush(insertBatch, opts);
    }

    async function getDelivery(eventId, subscriptionId) {
        return await db.prepare('SELECT * FROM deliveries WHERE event_id = ? AND subscription_id = ?').get(eventId, subscriptionId) || null;
    }

    async function listDeliveries({ status, subscriptionId, limit = 100, afterSeq = 0 } = {}) {
        const where = ['seq > ?'];
        const args = [afterSeq];
        if (status) { where.push('status = ?'); args.push(status); }
        if (subscriptionId) { where.push('subscription_id = ?'); args.push(subscriptionId); }
        args.push(limit);
        return await db.prepare(`SELECT * FROM deliveries WHERE ${where.join(' AND ')} ORDER BY seq, subscription_id LIMIT ?`).all(...args);
    }

    /**
     * Queue (again) the given events for one subscription: reset to pending, attempt 0, due now.
     * Only retained events that match the subscription's topic are queued. Returns the count.
     */
    const requeue = async (sub, { fromSeq, eventIds, max = 10000 }) => await db.tx(async () => {
        const now = clock.now();
        let rows;
        const cols = 'id, seq, priority, event_type, visibility, project_id, env';
        if (Array.isArray(eventIds)) {
            const get = db.prepare(`SELECT ${cols} FROM events WHERE id = ?`);
            rows = (await Promise.all(eventIds.map(async id => await get.get(id)))).filter(Boolean);
        } else {
            rows = await db.prepare(`SELECT ${cols} FROM events WHERE seq >= ? AND event_type ILIKE ? ESCAPE '\\' ORDER BY seq LIMIT ?`)
                .all(fromSeq, topics.toLike(sub.topic_pattern), max * 2);
        }
        // Replay never widens a subscription's scope (app scope, sandbox separation).
        rows = rows.filter(r => apps.deliverable(sub, r)).slice(0, max);
        const up = db.prepare(`INSERT INTO deliveries (event_id, subscription_id, seq, priority, attempt, status, next_attempt_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, 0, 'pending', ?, ?, ?)
            ON CONFLICT(event_id, subscription_id) DO UPDATE SET status = 'pending', attempt = 0, next_attempt_at = excluded.next_attempt_at,
                last_error = NULL, last_status = NULL, delivered_at = NULL, updated_at = excluded.updated_at`);
        for (const r of rows) await up.run(r.id, sub.id, r.seq, PRIORITY_RANK[r.priority], now, now, now);
        return rows.length;
    });

    async function deliveryCounts() {
        const out = { pending: 0, delivered: 0, failed: 0, dead: 0 };
        for (const r of await db.prepare('SELECT status, COUNT(*) AS n FROM deliveries GROUP BY status').all()) out[r.status] = r.n;
        return out;
    }

    // ── Checkpoints ──────────────────────────────────────────

    /**
     * A checkpoint is a cursor in parts: `cursor` is the numeric position, `epoch` the retention epoch it belongs to
     * and `carrier` the carrier that position is on (ADR-042 decision 7). `cursor` stays numeric on the wire.
     */
    async function getCheckpoint(consumer, topicPattern) {
        const r = await db.prepare('SELECT cursor, epoch, carrier, updated_at FROM consumer_checkpoints WHERE consumer = ? AND topic_pattern = ?').get(consumer, topicPattern);
        return r ? { cursor: r.cursor, epoch: r.epoch, carrier: r.carrier || null, updated_at: new Date(r.updated_at).toISOString() } : null;
    }
    /** Store a position. `epoch` defaults to the current retention epoch (a legacy numeric cursor). */
    async function setCheckpoint(consumer, topicPattern, position, { epoch: atEpoch = null, carrier = null } = {}) {
        const at = atEpoch == null ? await epoch() : atEpoch;
        await db.prepare(`INSERT INTO consumer_checkpoints (consumer, topic_pattern, cursor, epoch, carrier, updated_at) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(consumer, topic_pattern) DO UPDATE SET cursor = excluded.cursor, epoch = excluded.epoch, carrier = excluded.carrier, updated_at = excluded.updated_at`)
            .run(consumer, topicPattern, position, at, carrier, clock.now());
        return await getCheckpoint(consumer, topicPattern);
    }

    // ── Retention ────────────────────────────────────────────

    /**
     * Drop events (and their deliveries) older than retentionDays (sandbox events older than
     * sandboxRetentionDays, when shorter), and old publish receipts.
     */
    async function prune({ retentionDays = 30, receiptRetentionDays = 90, sandboxRetentionDays = retentionDays, now = clock.now() } = {}) {
        let events = (await db.prepare('DELETE FROM events WHERE received_at < ?').run(now - retentionDays * DAY_MS)).changes;
        if (sandboxRetentionDays < retentionDays) {
            events += (await db.prepare("DELETE FROM events WHERE env = 'sandbox' AND received_at < ?").run(now - sandboxRetentionDays * DAY_MS)).changes;
        }
        const receipts = (await db.prepare('DELETE FROM idempotency_receipts WHERE processed_at < ?')
            .run(now - Math.max(receiptRetentionDays, retentionDays) * DAY_MS)).changes;
        return { events, receipts };
    }

    async function ping() {
        return (await db.prepare('SELECT 1 AS ok').get()).ok === 1;
    }

    return {
        db, insertBatch, redact, getEvent, revokedAt, lastSeq, oldestSeq, epoch, bumpEpoch, firstSeqSince, scan,
        createSubscription, getSubscription, listSubscriptions, countSubscriptions, countProjectSubscriptions, setSubscriptionEnabled, rotateSubscriptionSecret,
        claimDeliveries, recordAttempt, getDelivery, listDeliveries, requeue, deliveryCounts,
        getCheckpoint, setCheckpoint, prune, ping, usage, flushUsage,
    };
}

module.exports = { openDb, MIGRATIONS, createStore, rowToEnvelope, subscriptionView, StoreError, PRIORITY_RANK, RANK_PRIORITY };
