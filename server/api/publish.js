'use strict';
/**
 * POST /api/v1/events  (service token, audience openvibe.events, capability events.publish)
 *
 *   body: <envelope>            -> 201 { event_id, seq, duplicate: false } | 200 on a repeat
 *   body: { events: [...] }     -> 201 { results: [{ event_id, seq, duplicate }] } (<= 100, atomic)
 *
 * Every envelope must validate against events.event-envelope@1; `source` must be the calling
 * service (svc:live -> 'live'); `event_type` must start with a prefix that source owns. A
 * re-published event_id is answered with the stored seq and never stored twice.
 */
const express = require('express');
const { validate, http } = require('openvibe-contracts');
const { StoreError } = require('../store');
const { CAPS } = require('../auth');

function normalize(input, ctx) {
    const env = { ...input };
    if (env.trace_id === undefined) env.trace_id = ctx.traceId;
    if (env.priority === undefined) env.priority = 'important';
    if (env.visibility === undefined) env.visibility = 'internal';
    return env;
}

function publishRouter({ config, store, auth, worker, realtime }) {
    const router = express.Router();

    function checkOne(input, principal, ctx) {
        if (!input || typeof input !== 'object' || Array.isArray(input)) {
            return { status: 422, code: 'events.invalid_envelope', detail: 'an event envelope must be a JSON object' };
        }
        const env = normalize(input, ctx);
        const v = validate('events.event-envelope@1', env);
        if (!v.valid) {
            return { status: 422, code: 'events.invalid_envelope', detail: 'envelope does not match events.event-envelope@1', errors: v.errors };
        }
        if (Buffer.byteLength(JSON.stringify(env.payload)) > config.maxPayloadBytes) {
            return { status: 413, code: 'events.payload_too_large', detail: `payload is larger than ${config.maxPayloadBytes} bytes` };
        }
        if (env.source !== principal.service) {
            return { status: 403, code: 'events.source_mismatch', detail: `source "${env.source}" is not the calling service "${principal.service}"` };
        }
        const prefixes = config.sourcePrefixes[env.source];
        if (!prefixes) return { status: 403, code: 'events.unknown_source', detail: `source "${env.source}" may not publish events` };
        if (!prefixes.some(p => env.event_type.startsWith(p))) {
            return { status: 403, code: 'events.type_not_allowed', detail: `${env.source} may publish ${prefixes.join(', ')}* only, not ${env.event_type}` };
        }
        return { env };
    }

    router.post('/api/v1/events', auth.requireCap(CAPS.publish, { requireService: true }), (req, res) => {
        const ctx = req.ov;
        const body = req.body;
        const isBatch = body && typeof body === 'object' && !Array.isArray(body) && Array.isArray(body.events) && body.event_id === undefined;
        const items = isBatch ? body.events : [body];
        if (isBatch && !items.length) return http.sendProblem(res, 400, 'events.bad_request', { detail: 'events must not be empty', ctx });
        if (items.length > config.maxBatch) {
            return http.sendProblem(res, 413, 'events.batch_too_large', { detail: `at most ${config.maxBatch} events per request`, ctx });
        }

        const envelopes = [];
        const seen = new Set();
        for (let i = 0; i < items.length; i++) {
            const r = checkOne(items[i], req.principal, ctx);
            if (r.env && seen.has(r.env.event_id)) {
                return http.sendProblem(res, 422, 'events.invalid_envelope', { detail: `events[${i}]: event_id repeated within the batch`, ctx, extra: { index: i } });
            }
            if (!r.env) {
                return http.sendProblem(res, r.status, r.code, {
                    detail: isBatch ? `events[${i}]: ${r.detail}` : r.detail, errors: r.errors, ctx, extra: isBatch ? { index: i } : undefined,
                });
            }
            seen.add(r.env.event_id);
            envelopes.push(r.env);
        }

        let out;
        try {
            out = store.insertBatch(envelopes, { publisher: req.principal.sub, requestId: ctx.requestId });
        } catch (err) {
            if (err instanceof StoreError) return http.sendProblem(res, err.status, err.code, { detail: err.message, ctx, extra: err.extra });
            throw err;
        }
        if (out.inserted.length) {
            realtime.publish(out.inserted);
            worker.kick();
        }
        const status = out.inserted.length ? 201 : 200;
        return res.status(status).json(isBatch ? { results: out.results } : out.results[0]);
    });

    return router;
}

module.exports = { publishRouter };
