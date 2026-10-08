'use strict';
/** Express app: request context, the v1 API, the realtime gateway, health/readiness, metrics. */
const path = require('path');
const express = require('express');
const { instrument } = require('openvibe-shared/metrics');
const { createReadiness } = require('openvibe-shared/ready');
const { createRelease } = require('openvibe-shared/release');
const { http } = require('openvibe-contracts');
const { publishRouter } = require('./api/publish');
const { subscriptionsRouter } = require('./api/subscriptions');
const { readRouter } = require('./api/read');
const { policiesRouter } = require('./api/policies');
const { placementRouter } = require('./api/placement');
const { resourcesRouter } = require('./api/resources');
const { mountLimits } = require('./limits');
const { createLimits } = require('./actor-limits');
const { renderHome, renderNotFound, HOME_CSP } = require('./home');
const appIcon = require('openvibe-shared/app-icon');
const { createDiscoveryRoutes } = require('./discovery');
const ovServe = require('openvibe-shared/serve');
const pkg = require('../package.json');

function createApp({ config, store, auth, keys, worker, realtime, metrics, dnsLookup, clock, log = console, valkey = null, policies = null, planner = null, carriers = null }) {
    const app = express();
    app.disable('x-powered-by');
    app.set('trust proxy', 'loopback');
    const release = createRelease({ service: 'events', root: path.join(__dirname, '..') });
    // HTTP golden signals by route template + GET /metrics (direct loopback callers only). An SSE
    // connection is a session, not a request, so it is counted by events_realtime_connections instead.
    const instrumented = instrument(app, { service: 'events', release: release.release, registry: metrics && metrics.registry, skip: (req) => req.path === '/realtime/stream' });
    app.use(http.middleware());
    // GET /release.json (ADR-016) and POST /release-metrics (open tabs' update reports into /metrics).
    release.mount(app, { registry: instrumented.registry });
    // GET /limits.json: the developer limits enforced here, from config (WS-N task 7; Codes renders them).
    mountLimits(app, config, { rateLimits: () => (limits && limits.registered ? limits.registered() : []) });
    app.use((req, res, next) => {
        res.setHeader('X-Content-Type-Options', 'nosniff');
        next();
    });

    // The product home's own browser files (the OpenVibe Frame, openvibe-shared/serve) under
    // content-addressed /shared/* URLs, from this repo's pinned openvibe-shared. Mounted before the
    // routers and outside the per-actor limiter: it is static and unauthenticated.
    app.use('/shared', ovServe.handler());

    // Realtime first: it answers CORS preflights and must not go through the JSON body parser.
    app.options('/realtime/stream', realtime.cors);
    app.get('/realtime/stream', realtime.cors, realtime.handler);

    app.use('/api', express.json({ limit: '2mb', type: ['application/json', 'application/*+json'] }));

    app.get('/api/health', (_req, res) => {
        res.json({ status: 'ok', service: 'openvibe-events', version: pkg.version });
    });

    // Readiness (openvibe-shared/ready): 503 only when a required check fails. A DLQ past
    // EVENTS_DLQ_DEGRADED_AT degrades the service (still ready: new events are accepted and delivered).
    const checks = [
        // A real round trip that names the store (postgresql / pglite).
        { name: 'db', required: true, check: async () => { const r = await store.db.ready(); return r.ok ? { ok: true, detail: r.detail } : r.error; } },
        { name: 'valkey', required: false, check: async () => (valkey ? valkey.ready() : { skipped: 'VALKEY_URL not set: per-actor limits count in this process only' }) },
        { name: 'network_jwks', required: true, check: () => keys.loaded() || 'Network signing key not loaded yet' },
    ];
    if (config.worker.enabled) checks.push({ name: 'delivery_worker', required: true, check: () => worker.running() || 'delivery worker is not running' });
    checks.push({
        name: 'dlq', required: false, check: async () => {
            const depth = (await store.deliveryCounts()).dead;
            return depth > config.dlqDegradedAt
                ? { ok: false, error: `${depth} dead deliveries (threshold ${config.dlqDegradedAt})`, detail: { depth, threshold: config.dlqDegradedAt } }
                : { ok: true, detail: { depth, threshold: config.dlqDegradedAt } };
        },
    });
    const readiness = createReadiness({
        service: 'events', release: release.release, checks,
        details: async (body) => {
            const dbOk = body.checks.db.status === 'ok';
            return {
                latest_seq: dbOk ? await store.lastSeq() : null,
                deliveries: dbOk ? await store.deliveryCounts() : null,
                worker: { enabled: config.worker.enabled, ...worker.stats() },
                realtime_connections: realtime.count(),
            };
        },
    });
    app.get('/api/ready', readiness.handler);

    // One per-actor limiter for the capability routes below (server/actor-limits.js); health, ready,
    // release.json, limits.json, metrics and the realtime stream above are never limited.
    const limits = createLimits({ config, clock, metrics, log, valkey });
    app.use(publishRouter({ config, store, auth, worker, realtime, limits, fabric: carriers }));
    app.use(subscriptionsRouter({ config, store, auth, dnsLookup, limits }));
    app.use(readRouter({ store, auth, worker, limits }));
    if (policies) app.use(policiesRouter({ policies, auth, limits }));
    if (planner) app.use(placementRouter({ planner, policies, auth, limits, store }));
    // The authority resource index (ADR-048, capability events.resource.read): the subscriptions Events owns,
    // for OpenVibe.Services' fan-out. First-party like the routes above; no per-actor limit of its own.
    app.use(resourcesRouter({ store, auth }));

    // Discovery for the product domain (openvibe.events): robots.txt, sitemap.xml, llms.txt.
    app.use(createDiscoveryRoutes({ config }));

    // GET / on the product domain: the home page for a browser (server/home.js, openvibe.events), the
    // API index for curl and API clients (`*/*`, no Accept, or any non-HTML Accept). Vary: Accept so a
    // shared cache never serves one to the other. The home carries its own CSP (HOME_CSP); the only
    // global security header is X-Content-Type-Options above, kept on every route.
    app.get('/', (req, res) => {
        res.vary('Accept');
        if (req.accepts(['text/plain', 'text/html']) === 'text/html') {
            return res.type('html')
                .set('Content-Security-Policy', HOME_CSP)
                // private: Cloudflare caches by URL and ignores Vary: Accept, so a shared copy could reach an API client.
                .set('Cache-Control', 'private, max-age=300')
                .send(renderHome({ siteUrl: config.siteUrl, apiUrl: config.baseUrl }));
        }
        res.type('text/plain').send([
            'OpenVibe.Events: durable events, subscriptions, signed delivery, dead letters and replay.',
            '',
            'POST /api/v1/events              publish (service token, events.event.publish; app token, events.app.publish)',
            'GET  /api/v1/events              pull with a cursor (events.event.read; app token, events.app.read)',
            '     /api/v1/subscriptions       webhook subscriptions (events.subscription.manage; app token, events.app.subscribe)',
            '     /api/v1/deliveries          DLQ inspect and replay (events.delivery.admin)',
            'GET  /realtime/stream?topics=... server-sent events for browsers',
            'GET  /api/health, /api/ready, /release.json, /limits.json',
            '',
            'Source: https://github.com/OpenVibers/OpenVibe.Events',
            '',
        ].join('\n'));
    });

    // Browsers ask for /favicon.ico on their own: the app icon, as SVG.
    app.get('/favicon.ico', (req, res) => res.type('image/svg+xml').set('Cache-Control', 'public, max-age=86400').send(appIcon.favicon({ site: 'events' })));

    // A browser asking for a page that is not here gets a page; an API client the problem document.
    app.use((req, res) => {
        if (req.method === 'GET' && !req.path.startsWith('/api/') && req.accepts(['application/json', 'text/html']) === 'text/html') {
            return res.status(404).type('html').set('Content-Security-Policy', HOME_CSP).set('Cache-Control', 'no-store').send(renderNotFound());
        }
        return http.sendProblem(res, 404, 'events.not_found', { detail: `no route ${req.method} ${req.path}`, ctx: req.ov });
    });

    // eslint-disable-next-line no-unused-vars
    app.use((err, req, res, _next) => {
        if (err.type === 'entity.parse.failed') return http.sendProblem(res, 400, 'events.bad_json', { detail: 'request body is not valid JSON', ctx: req.ov });
        if (err.type === 'entity.too.large') return http.sendProblem(res, 413, 'events.batch_too_large', { detail: 'request body too large', ctx: req.ov });
        log.error(`[app] ${req.method} ${req.path}: ${err.stack || err}`);
        if (res.headersSent) return res.end();
        return http.sendProblem(res, 500, 'events.internal', { detail: 'internal error', ctx: req.ov });
    });

    return app;
}

module.exports = { createApp };
