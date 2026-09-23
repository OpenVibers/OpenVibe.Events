'use strict';
/** Express app: request context, the v1 API, the realtime gateway, health/readiness. */
const express = require('express');
const { http } = require('openvibe-contracts');
const { publishRouter } = require('./api/publish');
const { subscriptionsRouter } = require('./api/subscriptions');
const { readRouter } = require('./api/read');
const pkg = require('../package.json');

function createApp({ config, store, auth, keys, worker, realtime, log = console }) {
    const app = express();
    app.disable('x-powered-by');
    app.set('trust proxy', 'loopback');
    app.use(http.middleware());
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

    app.get('/api/ready', (_req, res) => {
        let dbOk = false;
        try { dbOk = store.ping(); } catch { dbOk = false; }
        const checks = { db: dbOk, worker: !config.worker.enabled || worker.running(), key: keys.loaded() };
        const ready = Object.values(checks).every(Boolean);
        res.status(ready ? 200 : 503).json({
            status: ready ? 'ready' : 'not_ready',
            checks,
            latest_seq: dbOk ? store.lastSeq() : null,
            deliveries: dbOk ? store.deliveryCounts() : null,
            worker: worker.stats(),
            realtime_connections: realtime.count(),
        });
    });

    app.use(publishRouter({ config, store, auth, worker, realtime }));
    app.use(subscriptionsRouter({ config, store, auth }));
    app.use(readRouter({ store, auth, worker }));

    app.get('/', (_req, res) => {
        res.type('text/plain').send([
            'OpenVibe.Events: durable events, subscriptions, signed delivery, dead letters and replay.',
            '',
            'POST /api/v1/events              publish (service token, events.publish)',
            'GET  /api/v1/events              pull with a cursor (events.read)',
            '     /api/v1/subscriptions       webhook subscriptions (events.subscribe)',
            '     /api/v1/deliveries          DLQ inspect and replay (events.admin)',
            'GET  /realtime/stream?topics=... server-sent events for browsers',
            'GET  /api/health, /api/ready',
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
