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
const pkg = require('../package.json');

function createApp({ config, store, auth, keys, worker, realtime, metrics, dnsLookup, log = console }) {
    const app = express();
    app.disable('x-powered-by');
    app.set('trust proxy', 'loopback');
    const release = createRelease({ service: 'events', root: path.join(__dirname, '..') });
    // HTTP golden signals by route template + GET /metrics (direct loopback callers only). An SSE
    // connection is a session, not a request, so it is counted by events_realtime_connections instead.
    instrument(app, { service: 'events', release: release.release, registry: metrics && metrics.registry, skip: (req) => req.path === '/realtime/stream' });
    app.use(http.middleware());
    app.get('/release.json', release.handler);
    app.use((req, res, next) => {
        res.setHeader('X-Content-Type-Options', 'nosniff');
        next();
    });

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
        { name: 'db', required: true, check: () => store.ping() },
        { name: 'network_jwks', required: true, check: () => keys.loaded() || 'Network signing key not loaded yet' },
    ];
    if (config.worker.enabled) checks.push({ name: 'delivery_worker', required: true, check: () => worker.running() || 'delivery worker is not running' });
    checks.push({
        name: 'dlq', required: false, check: () => {
            const depth = store.deliveryCounts().dead;
            return depth > config.dlqDegradedAt
                ? { ok: false, error: `${depth} dead deliveries (threshold ${config.dlqDegradedAt})`, detail: { depth, threshold: config.dlqDegradedAt } }
                : { ok: true, detail: { depth, threshold: config.dlqDegradedAt } };
        },
    });
    const readiness = createReadiness({
        service: 'events', release: release.release, checks,
        details: (body) => {
            const dbOk = body.checks.db.status === 'ok';
            return {
                latest_seq: dbOk ? store.lastSeq() : null,
                deliveries: dbOk ? store.deliveryCounts() : null,
                worker: { enabled: config.worker.enabled, ...worker.stats() },
                realtime_connections: realtime.count(),
            };
        },
    });
    app.get('/api/ready', readiness.handler);

    app.use(publishRouter({ config, store, auth, worker, realtime }));
    app.use(subscriptionsRouter({ config, store, auth, dnsLookup }));
    app.use(readRouter({ store, auth, worker }));

    app.get('/', (_req, res) => {
        res.type('text/plain').send([
            'OpenVibe.Events: durable events, subscriptions, signed delivery, dead letters and replay.',
            '',
            'POST /api/v1/events              publish (service token, events.event.publish; app token, events.app.publish)',
            'GET  /api/v1/events              pull with a cursor (events.event.read; app token, events.app.read)',
            '     /api/v1/subscriptions       webhook subscriptions (events.subscription.manage; app token, events.app.subscribe)',
            '     /api/v1/deliveries          DLQ inspect and replay (events.delivery.admin)',
            'GET  /realtime/stream?topics=... server-sent events for browsers',
            'GET  /api/health, /api/ready, /release.json',
            '',
            'Source: https://github.com/OpenVibers/OpenVibe.Events',
            '',
        ].join('\n'));
    });

    app.use((req, res) => http.sendProblem(res, 404, 'events.not_found', { detail: `no route ${req.method} ${req.path}`, ctx: req.ov }));

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
