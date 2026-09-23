'use strict';
/**
 * Pull consumers (service token with events.event.read):
 *
 *   GET /api/v1/events?topic=media.vod.*[,…]&after_seq=0&limit=100
 *       -> { events: [{ seq, event }], next_after_seq, latest_seq, gap? }
 *       `gap` ({ from_seq, to_seq }) means events after after_seq were already pruned by retention.
 *       Keep next_after_seq as the cursor; it moves past events that did not match.
 *   GET /api/v1/events/:event_id -> { seq, event }
 *   GET /api/v1/checkpoints?topic=…  /  PUT /api/v1/checkpoints { topic, cursor }
 *       a consumer's own stored cursor per topic pattern (consumer = calling principal)
 *
 * Operators (events.delivery.admin):
 *
 *   GET  /api/v1/deliveries?status=dead&subscription_id=&after_seq=&limit=
 *   POST /api/v1/deliveries/replay { subscription_id, from_seq | event_ids: [...] }
 */
const express = require('express');
const { http } = require('openvibe-contracts');
const topics = require('../topics');
const { rowToEnvelope } = require('../store');
const { CAPS } = require('../auth');

const intParam = (v, d, min, max) => {
    if (v === undefined || v === '') return d;
    const n = Number(v);
    return Number.isInteger(n) && n >= min && n <= max ? n : NaN;
};

function readRouter({ store, auth, worker }) {
    const router = express.Router();
    const canRead = auth.requireCap(CAPS.read);
    const isAdmin = auth.requireCap(CAPS.admin);

    router.get('/api/v1/events', canRead, (req, res) => {
        const ctx = req.ov;
        const patterns = String(req.query.topic || '*').split(',').map(s => s.trim()).filter(Boolean);
        if (!patterns.length || patterns.length > 20 || !patterns.every(topics.isValidPattern)) {
            return http.sendProblem(res, 400, 'events.bad_topic', { detail: 'topic must be 1..20 comma-separated patterns', ctx });
        }
        const after = intParam(req.query.after_seq, 0, 0, Number.MAX_SAFE_INTEGER);
        const limit = intParam(req.query.limit, 100, 1, 1000);
        if (Number.isNaN(after) || Number.isNaN(limit)) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'after_seq must be >= 0 and limit 1..1000', ctx });
        }
        const oldest = store.oldestSeq();
        const out = {};
        let from = after;
        if (after < oldest - 1) {
            out.gap = { from_seq: after + 1, to_seq: oldest - 1 };
            from = oldest - 1;
        }
        const { rows, cursor } = store.scan(from, { patterns, limit });
        out.events = rows.map(r => ({ seq: r.seq, event: rowToEnvelope(r) }));
        out.next_after_seq = cursor;
        out.latest_seq = store.lastSeq();
        res.json(out);
    });

    router.get('/api/v1/events/:id', canRead, (req, res) => {
        const row = store.getEvent(String(req.params.id));
        if (!row) return http.sendProblem(res, 404, 'events.not_found', { detail: 'no such event (or pruned by retention)', ctx: req.ov });
        return res.json({ seq: row.seq, event: rowToEnvelope(row) });
    });

    const consumerOf = (req) => req.principal.service || req.principal.sub;

    router.get('/api/v1/checkpoints', canRead, (req, res) => {
        const topic = String(req.query.topic || '');
        if (!topics.isValidPattern(topic)) return http.sendProblem(res, 400, 'events.bad_topic', { detail: 'topic is required', ctx: req.ov });
        const cp = store.getCheckpoint(consumerOf(req), topic);
        res.json({ consumer: consumerOf(req), topic, cursor: cp ? cp.cursor : 0, updated_at: cp ? cp.updated_at : null });
    });

    router.put('/api/v1/checkpoints', canRead, (req, res) => {
        const b = req.body || {};
        if (!topics.isValidPattern(b.topic) || !Number.isInteger(b.cursor) || b.cursor < 0) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'topic (pattern) and cursor (integer >= 0) are required', ctx: req.ov });
        }
        const cp = store.setCheckpoint(consumerOf(req), b.topic, b.cursor);
        res.json({ consumer: consumerOf(req), topic: b.topic, ...cp });
    });

    router.get('/api/v1/deliveries', isAdmin, (req, res) => {
        const status = req.query.status ? String(req.query.status) : undefined;
        if (status && !['pending', 'delivered', 'failed', 'dead'].includes(status)) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'status is pending|delivered|failed|dead', ctx: req.ov });
        }
        const limit = intParam(req.query.limit, 100, 1, 1000);
        const afterSeq = intParam(req.query.after_seq, 0, 0, Number.MAX_SAFE_INTEGER);
        if (Number.isNaN(limit) || Number.isNaN(afterSeq)) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'bad limit or after_seq', ctx: req.ov });
        const rows = store.listDeliveries({ status, subscriptionId: req.query.subscription_id ? String(req.query.subscription_id) : undefined, limit, afterSeq });
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
            counts: store.deliveryCounts(),
        });
    });

    router.post('/api/v1/deliveries/replay', isAdmin, (req, res) => {
        const ctx = req.ov;
        const b = req.body || {};
        const sub = typeof b.subscription_id === 'string' ? store.getSubscription(b.subscription_id) : null;
        if (!sub) return http.sendProblem(res, 404, 'events.not_found', { detail: 'no such subscription', ctx });
        const hasIds = Array.isArray(b.event_ids);
        const hasFrom = Number.isInteger(b.from_seq) && b.from_seq >= 0;
        if (hasIds === hasFrom) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'pass exactly one of from_seq or event_ids', ctx });
        if (hasIds && (b.event_ids.length > 1000 || !b.event_ids.every(id => typeof id === 'string'))) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'event_ids: at most 1000 ids', ctx });
        }
        const queued = store.requeue(sub, hasIds ? { eventIds: b.event_ids } : { fromSeq: b.from_seq });
        worker.kick();
        return res.json({ subscription_id: sub.id, queued });
    });

    return router;
}

module.exports = { readRouter };
