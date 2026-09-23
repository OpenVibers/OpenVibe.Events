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
 *
 * Developer apps (events.app.subscribe) use the same routes with an app token. The consumer is the
 * app (app:app_<ULID>); the subscription records the token's project_id and env, and delivers only
 * what events.app.read would show that app (server/apps.js). Topic patterns follow the same scope
 * rule. The endpoint must be https and resolve only to public addresses (server/egress.js), checked
 * here and again on every delivery. At most EVENTS_APP_[SANDBOX_]MAX_SUBSCRIPTIONS per project and env.
 */
const crypto = require('crypto');
const express = require('express');
const { ids, http } = require('openvibe-contracts');
const topics = require('../topics');
const { checkEndpoint } = require('../endpoints');
const { subscriptionView } = require('../store');
const { CAPS } = require('../auth');
const apps = require('../apps');
const { checkAppEndpoint } = require('../egress');

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

function subscriptionsRouter({ config, store, auth, dnsLookup }) {
    const router = express.Router();
    const guard = auth.appOrService(CAPS.subscribe, CAPS.appSubscribe, { requireService: true });
    const consumerOf = (req) => (req.principal.kind === 'app' ? req.principal.sub : req.principal.service);

    /** The app's events.app.subscribe grant was withdrawn after this token was issued. */
    function subscribeRevoked(req, res) {
        const p = req.principal;
        if (p.kind !== 'app') return false;
        const at = store.revokedAt(p.sub, CAPS.appSubscribe);
        if (at && p.iat * 1000 <= at) {
            http.sendProblem(res, 403, 'capability.denied', { detail: `${CAPS.appSubscribe} was revoked after this token was issued`, ctx: req.ov });
            return true;
        }
        return false;
    }

    function own(req, res) {
        const sub = store.getSubscription(String(req.params.id));
        if (!sub || sub.consumer !== consumerOf(req)) {
            http.sendProblem(res, 404, 'events.not_found', { detail: 'no such subscription', ctx: req.ov });
            return null;
        }
        return sub;
    }

    router.post('/api/v1/subscriptions', guard, async (req, res, next) => {
        try {
            await create(req, res);
        } catch (err) {
            next(err);
        }
    });

    async function create(req, res) {
        const ctx = req.ov;
        const b = req.body && typeof req.body === 'object' ? req.body : {};
        const pattern = b.topic_pattern ?? b.topic;
        if (!topics.isValidPattern(pattern)) {
            return http.sendProblem(res, 422, 'events.bad_topic', { detail: 'topic_pattern must be dot-separated segments of [a-z0-9_] or *', ctx });
        }
        const app = req.principal.kind === 'app' ? req.principal : null;
        if (subscribeRevoked(req, res)) return undefined;
        if (app) {
            const err = apps.patternScopeError(pattern, app);
            if (err) return http.sendProblem(res, 403, 'events.topic_not_allowed', { detail: err, ctx });
        }
        const ep = app ? await checkAppEndpoint(b.endpoint, { lookup: dnsLookup }) : checkEndpoint(b.endpoint, config.endpointHosts);
        if (!ep.ok) return http.sendProblem(res, 422, 'events.endpoint_not_allowed', { detail: ep.reason, ctx });
        if (b.secret !== undefined && (typeof b.secret !== 'string' || b.secret.length < 32 || b.secret.length > 256)) {
            return http.sendProblem(res, 422, 'events.bad_request', { detail: 'secret must be a string of 32..256 characters', ctx });
        }
        const rp = checkRetryPolicy(b.retry_policy);
        if (!rp.ok) return http.sendProblem(res, 422, 'events.bad_request', { detail: rp.reason, ctx });

        const consumer = consumerOf(req);
        const endpoint = ep.url.toString();
        const dup = store.listSubscriptions(consumer).find(s => s.topic_pattern === pattern && s.endpoint === endpoint);
        if (dup) {
            return http.sendProblem(res, 409, 'events.subscription_exists', { detail: 'this consumer already subscribes that endpoint to that topic', ctx, extra: { subscription_id: dup.id } });
        }
        if (store.countSubscriptions(consumer) >= config.maxSubscriptionsPerConsumer) {
            return http.sendProblem(res, 409, 'events.subscription_limit', { detail: `at most ${config.maxSubscriptionsPerConsumer} subscriptions per consumer`, ctx });
        }
        if (app) {
            const max = config.apps.quotas[app.env].maxSubscriptions;
            if (max && store.countProjectSubscriptions(app.projectId, app.env) >= max) {
                return http.sendProblem(res, 429, 'events.quota_exceeded', {
                    detail: `at most ${max} subscriptions per project in ${app.env}`, ctx, extra: { quota: 'subscriptions', limit: max },
                });
            }
        }
        const row = store.createSubscription({
            id: `sub_${ids.ulid()}`,
            consumer,
            topicPattern: pattern,
            endpoint,
            secret: b.secret || `whsec_${crypto.randomBytes(32).toString('hex')}`,
            retryPolicy: rp.value && Object.keys(rp.value).length ? rp.value : null,
            projectId: app ? app.projectId : null,
            env: app ? app.env : 'production',
        });
        return res.status(201).json(subscriptionView(row, { withSecret: true }));
    }

    router.get('/api/v1/subscriptions', guard, (req, res) => {
        res.json({ subscriptions: store.listSubscriptions(consumerOf(req)).map(s => subscriptionView(s)) });
    });

    router.get('/api/v1/subscriptions/:id', guard, (req, res) => {
        const sub = own(req, res);
        if (sub) res.json(subscriptionView(sub));
    });

    for (const [action, enabled] of [['disable', false], ['enable', true]]) {
        router.post(`/api/v1/subscriptions/:id/${action}`, guard, (req, res) => {
            if (enabled && subscribeRevoked(req, res)) return;
            const sub = own(req, res);
            if (sub) res.json(subscriptionView(store.setSubscriptionEnabled(sub.id, enabled)));
        });
    }

    return router;
}

module.exports = { subscriptionsRouter, checkRetryPolicy };
