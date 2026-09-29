'use strict';
/**
 * Explain the fabric (ADR-042 decisions 1 and 5; plan T7) — operators only (events.delivery.admin), behind the same
 * isAdmin guard and per-actor limits as /api/v1/deliveries.
 *
 *   GET /api/v1/placement?event_type=<t>[&key=<k>]
 *     -> { event_type, policy, pattern, carrier_class, ordering_key, placement, last_decision }
 *        placement is platform.placement-result@1 exactly as the planner produced it (validated against the contract):
 *        non-empty reasons, and per candidate its eligibility / excluded_because / cost / latency.
 *        last_decision is { carrier, decided_at } from delivery_placements, or null before any decision changed.
 *   GET /api/v1/placement/deliveries/:event_id/:subscription_id
 *     -> { carrier, decision }  (the delivery_placements row in force when the delivery was created, or null)
 *        404 when the delivery is unknown.
 */
const express = require('express');
const { http, validate } = require('openvibe-contracts');
const { CAPS } = require('../auth');
const { carrierClass, orderingKey } = require('../fabric/policy');

function placementRouter({ planner, policies, auth, limits, store }) {
    const router = express.Router();
    const isAdmin = auth.requireCap(CAPS.admin);

    router.get('/api/v1/placement', isAdmin, limits('events.placement.read'), async (req, res) => {
        const ctx = req.ov;
        const eventType = String(req.query.event_type || '');
        if (!eventType || eventType.length > 200) {
            return http.sendProblem(res, 400, 'events.bad_request', { detail: 'event_type is required and at most 200 characters', ctx });
        }
        const { policy, pattern } = await policies.resolve(eventType);
        const klass = carrierClass(policy);
        const key = req.query.key != null ? String(req.query.key).slice(0, 200) : orderingKey(policy, { event_type: eventType });
        const { result } = planner.explain(klass, key, policy);
        const v = validate('platform.placement-result@1', result);
        if (!v.valid) {
            return http.sendProblem(res, 500, 'events.internal', { detail: `planner result does not match platform.placement-result@1: ${v.errors.map((e) => `${e.path} ${e.message}`).join('; ')}`, ctx });
        }
        const last = await planner.lastDecision(klass, key);
        res.json({
            event_type: eventType,
            policy,
            pattern,
            carrier_class: klass,
            ordering_key: key,
            placement: result,
            last_decision: last ? { carrier: last.carrier, decided_at: new Date(last.decided_at).toISOString() } : null,
        });
    });

    router.get('/api/v1/placement/deliveries/:event_id/:subscription_id', isAdmin, limits('events.placement.read'), async (req, res) => {
        const ctx = req.ov;
        const d = await store.getDelivery(String(req.params.event_id), String(req.params.subscription_id));
        if (!d) return http.sendProblem(res, 404, 'events.not_found', { detail: 'no such delivery', ctx });
        const event = await store.getEvent(d.event_id);
        const { policy } = event ? await policies.resolve(event.event_type) : { policy: null };
        const klass = policy ? carrierClass(policy) : null;
        const decision = klass ? await planner.decisionBefore(klass, d.ordering_key, d.created_at) : null;
        res.json({ carrier: d.carrier || null, decision: decision || null });
    });

    return router;
}

module.exports = { placementRouter };
