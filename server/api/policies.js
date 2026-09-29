'use strict';
/**
 * Delivery policies (ADR-042 decisions 1 and 4; plan T7) — operators only (events.delivery.admin), behind the same
 * isAdmin guard and per-actor limits as /api/v1/deliveries.
 *
 *   GET    /api/v1/delivery-policies                          -> { policies: [{ pattern, policy, revision, updated_at, updated_by, carrier_class }] }
 *   PUT    /api/v1/delivery-policies/:pattern                 body = events.delivery-policy@1 -> the stored row (revision +1)
 *   DELETE /api/v1/delivery-policies/:pattern                 -> 204 (404 when absent)
 *   GET    /api/v1/delivery-policies/resolve?event_type=<t>   -> { event_type, pattern, revision, policy, carrier_class }
 *
 * The pattern is validated with the subscriptions' topic-pattern rule; the body with the contracts validator (an
 * unknown class or an extra field is a 400 carrying the validator's message). carrier_class is the derived label
 * (./../fabric/policy.js); it is answered here and nowhere else.
 */
const express = require('express');
const { http, validate } = require('openvibe-contracts');
const topics = require('../topics');
const { CAPS } = require('../auth');
const { carrierClass } = require('../fabric/policy');

function policiesRouter({ policies, auth, limits }) {
    const router = express.Router();
    const isAdmin = auth.requireCap(CAPS.admin);
    // Writes are rare operator actions: 30 a minute (like subscription changes); reads take the defaults.
    const change = { minute: 30, hour: 300 };

    const view = (row) => ({
        pattern: row.pattern,
        policy: row.policy,
        revision: row.revision,
        updated_at: new Date(row.updated_at).toISOString(),
        updated_by: row.updated_by || null,
        carrier_class: carrierClass(row.policy),
    });

    router.get('/api/v1/delivery-policies', isAdmin, limits('events.delivery-policy.list'), async (_req, res) => {
        res.json({ policies: (await policies.all()).map(view) });
    });

    router.get('/api/v1/delivery-policies/resolve', isAdmin, limits('events.delivery-policy.read'), async (req, res) => {
        const eventType = String(req.query.event_type || '');
        if (!eventType || eventType.length > 200) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'event_type is required and at most 200 characters', ctx: req.ov });
        }
        const { policy, pattern, revision } = await policies.resolve(eventType);
        res.json({ event_type: eventType, pattern, revision, policy, carrier_class: carrierClass(policy) });
    });

    router.put('/api/v1/delivery-policies/:pattern', isAdmin, limits('events.delivery-policy.write', change), async (req, res) => {
        const ctx = req.ov;
        const pattern = String(req.params.pattern);
        if (!topics.isValidPattern(pattern)) {
            return http.sendProblem(res, 422, 'events.bad_topic', { detail: 'pattern must be dot-separated segments of [a-z0-9_] or *', ctx });
        }
        const body = req.body && typeof req.body === 'object' && !Array.isArray(req.body) ? req.body : null;
        if (!body) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'body must be an events.delivery-policy@1 object', ctx });
        const v = validate('events.delivery-policy@1', body);
        if (!v.valid) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: v.errors.map((e) => `${e.path} ${e.message}`).join('; '), ctx });
        }
        const row = await policies.put(pattern, body, req.principal.service || req.principal.sub);
        res.json(view(row));
    });

    router.delete('/api/v1/delivery-policies/:pattern', isAdmin, limits('events.delivery-policy.write', change), async (req, res) => {
        const pattern = String(req.params.pattern);
        if (!topics.isValidPattern(pattern)) {
            return http.sendProblem(res, 422, 'events.bad_topic', { detail: 'pattern must be dot-separated segments of [a-z0-9_] or *', ctx: req.ov });
        }
        if (!await policies.remove(pattern)) return http.sendProblem(res, 404, 'events.not_found', { detail: 'no such delivery policy', ctx: req.ov });
        res.status(204).end();
    });

    return router;
}

module.exports = { policiesRouter };
