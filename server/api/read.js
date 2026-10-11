'use strict';
/**
 * Pull consumers (service token with events.event.read):
 *
 *   GET /api/v1/events?topic=media.vod.*[,…][&after=<cursor>]&limit=100
 *       -> { events: [{ cursor, event }], next_cursor, latest_cursor, gap? }
 *       A position is only ever an opaque cursor (ADR-042 decision 7): `after=` takes one, and without it the
 *       page starts at the oldest retained event. `after_seq` is refused (400), never read as "from the start".
 *       `latest_cursor` is the head: a consumer that starts at "now" (skipping history) stores it. There is no
 *       per-event sequence number (dedupe on event.event_id). A cursor from another retention epoch answers `gap`
 *       — never a silent restart.
 *       `gap` ({ from_seq, to_seq }) means
 *       events after the position were already pruned by retention (or the epoch changed). The pull
 *       spans the hot store and the replay tier (decision 8) with the one cursor: `gap` only outside both.
 *       Keep next_cursor as the cursor; it moves past events that did not match.
 *   GET /api/v1/events/:event_id -> { cursor, event }   (hot or replay)
 *   GET /api/v1/checkpoints?topic=…  /  PUT /api/v1/checkpoints { topic, cursor, carrier? }
 *       a consumer's own stored cursor per topic pattern (consumer = calling principal): `cursor` is an
 *       opaque cursor string (a number is refused), and both answers return it as stored, with its carrier
 *       ({ consumer, topic, cursor, carrier, updated_at }; cursor null when none is stored).
 *
 * Developer apps (events.app.read) use the same routes with an app token: only their project's
 * events in the token's environment plus public first-party events (server/apps.js); every topic
 * pattern must start with a literal segment, and app.* patterns must name the app's project_key.
 * Checkpoints are per app. First-party readers never see sandbox events, and see app events only
 * through patterns that start with `app.`.
 *
 * Operators (events.delivery.admin):
 *
 *   GET  /api/v1/deliveries?status=dead&subscription_id=&after_seq=&limit=
 *   POST /api/v1/deliveries/replay { subscription_id, from_seq | event_ids: [...] }
 */
const express = require('express');
const { http } = require('openvibe-contracts');
const topics = require('../topics');
const cursor = require('../cursor');
const { rowToEnvelope } = require('../store');
const { CAPS } = require('../auth');
const apps = require('../apps');

/** Why an app may not use these patterns (null for services and for allowed patterns). */
function scopeError(principal, patterns) {
    if (principal.kind !== 'app') return null;
    for (const p of patterns) {
        const err = apps.patternScopeError(p, principal);
        if (err) return `${p}: ${err}`;
    }
    return null;
}

/** Row filter for a reader: an app's scope, or a first-party reader's (no sandbox; app events via app.*). */
function acceptFor(principal, patterns) {
    if (principal.kind === 'app') return (row) => apps.visibleToApp(row, principal);
    return (row) => patterns.some(p => apps.serviceMatches(p, row));
}

const intParam = (v, d, min, max) => {
    if (v === undefined || v === '') return d;
    const n = Number(v);
    return Number.isInteger(n) && n >= min && n <= max ? n : NaN;
};

function readRouter({ store, auth, worker, limits }) {
    const router = express.Router();
    const canRead = auth.appOrService(CAPS.read, CAPS.appRead);
    const isAdmin = auth.requireCap(CAPS.admin);
    // Per-actor limits (server/actor-limits.js); single reads take the defaults. A pull consumer polls
    // every few seconds and pages through a backlog (Community's relay: up to 20 pages a tick), and
    // may store its checkpoint after each page: 600 a minute each, so a catch-up is never throttled.
    const pull = limits('events.event.pull', { minute: 600, hour: 20000 });

    router.get('/api/v1/events', canRead, pull, async (req, res) => {
        const ctx = req.ov;
        const patterns = String(req.query.topic || '*').split(',').map(s => s.trim()).filter(Boolean);
        if (!patterns.length || patterns.length > 20 || !patterns.every(topics.isValidPattern)) {
            return http.sendProblem(res, 400, 'events.bad_topic', { detail: 'topic must be 1..20 comma-separated patterns', ctx });
        }
        const scopeErr = scopeError(req.principal, patterns);
        if (scopeErr) return http.sendProblem(res, 403, 'events.topic_not_allowed', { detail: scopeErr, ctx });
        const limit = intParam(req.query.limit, 100, 1, 1000);
        if (Number.isNaN(limit)) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'limit must be 1..1000', ctx });
        }
        // A position is an opaque cursor and nothing else: a number would silently restart a consumer that still sends one.
        if (req.query.after_seq !== undefined) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: "after_seq is retired: pass after=<cursor> (a page's next_cursor, or latest_cursor for the head)", ctx });
        }
        const epoch = await store.epoch();
        // A cursor from another epoch cannot be mapped onto this store, so it is answered with a gap.
        let after = 0;
        let epochMismatch = false;
        if (req.query.after !== undefined && String(req.query.after) !== '') {
            const decoded = cursor.decode(String(req.query.after));
            if (!decoded) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'after must be an opaque cursor', ctx });
            after = decoded.seq;
            epochMismatch = decoded.epoch !== epoch;
        }
        const oldest = await store.oldestSeq();
        const out = {};
        let from = after;
        if (epochMismatch) {
            out.gap = { from_seq: 1, to_seq: Math.max(oldest - 1, 0) };
            from = Math.max(oldest - 1, 0);
        } else if (after < oldest - 1) {
            out.gap = { from_seq: after + 1, to_seq: oldest - 1 };
            from = oldest - 1;
        }
        const { rows, cursor: scanned } = await store.scan(from, { patterns, limit, accept: acceptFor(req.principal, patterns) });
        out.events = rows.map(r => ({ cursor: cursor.encode(r.seq, epoch), event: rowToEnvelope(r) }));
        out.next_cursor = cursor.encode(scanned, epoch);
        out.latest_cursor = cursor.encode(await store.lastSeq(), epoch);
        res.json(out);
    });

    router.get('/api/v1/events/:id', canRead, limits('events.event.read'), async (req, res) => {
        const row = await store.getEvent(String(req.params.id));
        const visible = row && (req.principal.kind === 'app' ? apps.visibleToApp(row, req.principal) : (row.env || 'production') === 'production');
        if (!visible) return http.sendProblem(res, 404, 'events.not_found', { detail: 'no such event (or pruned by retention)', ctx: req.ov });
        // A replay-tier row names the epoch its position belongs to; a hot row is in the current one.
        return res.json({ cursor: cursor.encode(row.seq, row.epoch ?? await store.epoch()), event: rowToEnvelope(row) });
    });

    const consumerOf = (req) => req.principal.service || req.principal.sub;
    // The stored position and epoch, handed back as the opaque cursor they make (null when nothing is stored).
    const checkpointView = (consumer, topic, cp) => ({
        consumer, topic, cursor: cp ? cursor.encode(Number(cp.cursor), Number(cp.epoch)) : null, carrier: cp ? cp.carrier || null : null, updated_at: cp ? cp.updated_at : null,
    });

    router.get('/api/v1/checkpoints', canRead, limits('events.checkpoint.read'), async (req, res) => {
        const topic = String(req.query.topic || '');
        if (!topics.isValidPattern(topic)) return http.sendProblem(res, 400, 'events.bad_topic', { detail: 'topic is required', ctx: req.ov });
        const scopeErr = scopeError(req.principal, [topic]);
        if (scopeErr) return http.sendProblem(res, 403, 'events.topic_not_allowed', { detail: scopeErr, ctx: req.ov });
        const cp = await store.getCheckpoint(consumerOf(req), topic);
        res.json(checkpointView(consumerOf(req), topic, cp));
    });

    router.put('/api/v1/checkpoints', canRead, limits('events.checkpoint.write', { minute: 600, hour: 20000 }), async (req, res) => {
        const b = req.body || {};
        // `cursor` is an opaque cursor, decoded to the position and epoch it names (ADR-042 decision 7); a number is
        // refused. `carrier` records the carrier the position is on.
        const decoded = typeof b.cursor === 'string' ? cursor.decode(b.cursor) : null;
        if (!topics.isValidPattern(b.topic) || !decoded) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: "topic (pattern) and cursor (an opaque cursor: a page's next_cursor) are required", ctx: req.ov });
        }
        const scopeErr = scopeError(req.principal, [b.topic]);
        if (scopeErr) return http.sendProblem(res, 403, 'events.topic_not_allowed', { detail: scopeErr, ctx: req.ov });
        const cp = await store.setCheckpoint(consumerOf(req), b.topic, decoded.seq, { epoch: decoded.epoch, carrier: typeof b.carrier === 'string' && b.carrier ? b.carrier : null });
        res.json(checkpointView(consumerOf(req), b.topic, cp));
    });

    router.get('/api/v1/deliveries', isAdmin, limits('events.delivery.list'), async (req, res) => {
        const status = req.query.status ? String(req.query.status) : undefined;
        if (status && !['pending', 'delivered', 'failed', 'dead'].includes(status)) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'status is pending|delivered|failed|dead', ctx: req.ov });
        }
        const limit = intParam(req.query.limit, 100, 1, 1000);
        const afterSeq = intParam(req.query.after_seq, 0, 0, Number.MAX_SAFE_INTEGER);
        if (Number.isNaN(limit) || Number.isNaN(afterSeq)) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'bad limit or after_seq', ctx: req.ov });
        const rows = await store.listDeliveries({ status, subscriptionId: req.query.subscription_id ? String(req.query.subscription_id) : undefined, limit, afterSeq });
        return res.json({
            deliveries: rows.map(d => ({
                event_id: d.event_id,
                subscription_id: d.subscription_id,
                seq: d.seq,
                status: d.status,
                attempt: d.attempt,
                next_attempt_at: d.next_attempt_at ? new Date(d.next_attempt_at).toISOString() : null,
                last_status: d.last_status,
                last_error: d.last_error,
                delivered_at: d.delivered_at ? new Date(d.delivered_at).toISOString() : null,
            })),
            counts: await store.deliveryCounts(),
        });
    });

    // A replay requeues up to a whole subscription's retained history: an operator's action, a few
    // times at most.
    router.post('/api/v1/deliveries/replay', isAdmin, limits('events.delivery.replay', { minute: 6, hour: 60 }), async (req, res) => {
        const ctx = req.ov;
        const b = req.body || {};
        const sub = typeof b.subscription_id === 'string' ? await store.getSubscription(b.subscription_id) : null;
        if (!sub) return http.sendProblem(res, 404, 'events.not_found', { detail: 'no such subscription', ctx });
        const hasIds = Array.isArray(b.event_ids);
        const hasFrom = Number.isInteger(b.from_seq) && b.from_seq >= 0;
        if (hasIds === hasFrom) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'pass exactly one of from_seq or event_ids', ctx });
        if (hasIds && (b.event_ids.length > 1000 || !b.event_ids.every(id => typeof id === 'string'))) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'event_ids: at most 1000 ids', ctx });
        }
        const queued = await store.requeue(sub, hasIds ? { eventIds: b.event_ids } : { fromSeq: b.from_seq });
        worker.kick();
        return res.json({ subscription_id: sub.id, queued });
    });

    return router;
}

module.exports = { readRouter };
