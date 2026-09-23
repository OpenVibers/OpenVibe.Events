'use strict';
/**
 * Subscriptions (service token with events.subscription.manage; the consumer is always the calling service).
 *
 *   POST /api/v1/subscriptions               { topic_pattern, endpoint, secret?, retry_policy? }
 *                                            -> 201 subscription + secret (shown once)
 *   GET  /api/v1/subscriptions               -> { subscriptions: [...] } (own only, no secrets)
 *   GET  /api/v1/subscriptions/:id
 *   POST /api/v1/subscriptions/:id/disable | /enable
 *
 * The endpoint must be http(s) on the allow-list (127.0.0.1, *.openvibe.*): server/endpoints.js.
 */
const crypto = require('crypto');
const express = require('express');
const { ids, http } = require('openvibe-contracts');
const topics = require('../topics');
const { checkEndpoint } = require('../endpoints');
const { subscriptionView } = require('../store');
const { CAPS } = require('../auth');

function checkRetryPolicy(p) {
    if (p == null) return { ok: true, value: null };
    if (typeof p !== 'object' || Array.isArray(p)) return { ok: false, reason: 'retry_policy must be an object' };
    const out = {};
    for (const k of Object.keys(p)) if (!['max_attempts', 'backoff_ms'].includes(k)) return { ok: false, reason: `unknown retry_policy field ${k}` };
    if (p.max_attempts !== undefined) {
        if (!Number.isInteger(p.max_attempts) || p.max_attempts < 1 || p.max_attempts > 20) return { ok: false, reason: 'max_attempts must be 1..20' };
        out.max_attempts = p.max_attempts;
    }
    if (p.backoff_ms !== undefined) {
        if (!Array.isArray(p.backoff_ms) || !p.backoff_ms.length || p.backoff_ms.length > 20
            || !p.backoff_ms.every(n => Number.isInteger(n) && n >= 0 && n <= 86400000)) {
            return { ok: false, reason: 'backoff_ms must be 1..20 integers between 0 and 86400000' };
        }
        out.backoff_ms = p.backoff_ms;
    }
    return { ok: true, value: out };
}

function subscriptionsRouter({ config, store, auth }) {
    const router = express.Router();
    const guard = auth.requireCap(CAPS.subscribe, { requireService: true });

    function own(req, res) {
        const sub = store.getSubscription(String(req.params.id));
        if (!sub || sub.consumer !== req.principal.service) {
            http.sendProblem(res, 404, 'events.not_found', { detail: 'no such subscription', ctx: req.ov });
            return null;
        }
        return sub;
    }

    router.post('/api/v1/subscriptions', guard, (req, res) => {
        const ctx = req.ov;
        const b = req.body && typeof req.body === 'object' ? req.body : {};
        const pattern = b.topic_pattern ?? b.topic;
        if (!topics.isValidPattern(pattern)) {
            return http.sendProblem(res, 422, 'events.bad_topic', { detail: 'topic_pattern must be dot-separated segments of [a-z0-9_] or *', ctx });
        }
        const ep = checkEndpoint(b.endpoint, config.endpointHosts);
        if (!ep.ok) return http.sendProblem(res, 422, 'events.endpoint_not_allowed', { detail: ep.reason, ctx });
        if (b.secret !== undefined && (typeof b.secret !== 'string' || b.secret.length < 32 || b.secret.length > 256)) {
            return http.sendProblem(res, 422, 'events.bad_request', { detail: 'secret must be a string of 32..256 characters', ctx });
        }
        const rp = checkRetryPolicy(b.retry_policy);
        if (!rp.ok) return http.sendProblem(res, 422, 'events.bad_request', { detail: rp.reason, ctx });

        const consumer = req.principal.service;
        const endpoint = ep.url.toString();
        const dup = store.listSubscriptions(consumer).find(s => s.topic_pattern === pattern && s.endpoint === endpoint);
        if (dup) {
            return http.sendProblem(res, 409, 'events.subscription_exists', { detail: 'this consumer already subscribes that endpoint to that topic', ctx, extra: { subscription_id: dup.id } });
        }
        if (store.countSubscriptions(consumer) >= config.maxSubscriptionsPerConsumer) {
            return http.sendProblem(res, 409, 'events.subscription_limit', { detail: `at most ${config.maxSubscriptionsPerConsumer} subscriptions per consumer`, ctx });
        }
        const row = store.createSubscription({
            id: `sub_${ids.ulid()}`,
            consumer,
            topicPattern: pattern,
            endpoint,
            secret: b.secret || `whsec_${crypto.randomBytes(32).toString('hex')}`,
            retryPolicy: rp.value && Object.keys(rp.value).length ? rp.value : null,
        });
        return res.status(201).json(subscriptionView(row, { withSecret: true }));
    });

    router.get('/api/v1/subscriptions', guard, (req, res) => {
        res.json({ subscriptions: store.listSubscriptions(req.principal.service).map(s => subscriptionView(s)) });
    });

    router.get('/api/v1/subscriptions/:id', guard, (req, res) => {
        const sub = own(req, res);
        if (sub) res.json(subscriptionView(sub));
    });

    for (const [action, enabled] of [['disable', false], ['enable', true]]) {
        router.post(`/api/v1/subscriptions/:id/${action}`, guard, (req, res) => {
            const sub = own(req, res);
            if (sub) res.json(subscriptionView(store.setSubscriptionEnabled(sub.id, enabled)));
        });
    }

    return router;
}

module.exports = { subscriptionsRouter, checkRetryPolicy };
