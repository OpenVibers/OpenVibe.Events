'use strict';
/**
 * OpenVibe.Events entry point.
 *
 *   node server/index.js            (systemd: openvibe-events.service)
 *
 * start() is also what the tests use: it takes a config (server/config.js load()) plus injectable
 * clock/fetch/log, and returns handles to every part so they can be driven directly.
 */
const { load } = require('./config');
const { openDb, createStore } = require('./store');
const { createKeyStore, createAuth } = require('./auth');
const { createWorker } = require('./worker');
const { createRealtime } = require('./realtime');
const { createApp } = require('./app');
const { createMetrics } = require('./metrics');
const { createGuardedPost } = require('./egress');

async function start({
    config, clock = { now: () => Date.now() }, fetchImpl = globalThis.fetch, deliveryFetch = fetchImpl,
    appPost = undefined, dnsLookup = undefined, log = console, listen = true,
} = {}) {
    config = config || load();
    const db = openDb(config.dbPath);
    const store = createStore(db, { clock, maxHops: config.maxHops });
    const keys = createKeyStore({ urls: [config.networkInternalUrl, config.networkUrl], pem: config.networkPublicKey, fetchImpl, log });
    const auth = createAuth({ config, keys, store });
    const metrics = createMetrics({ store });
    // appPost / dnsLookup: developer-app delivery and the subscribe-time DNS check (server/egress.js);
    // injectable for tests only.
    const worker = createWorker({ store, config, clock, fetchImpl: deliveryFetch, appPost: appPost || createGuardedPost({ lookup: dnsLookup }), log, observe: metrics.observe });
    const realtime = createRealtime({ store, auth, config, clock, log });
    metrics.bind({ realtime });
    const app = createApp({ config, store, auth, keys, worker, realtime, metrics, dnsLookup, log });

    // The key loads in the background (retrying while Network boots); /api/ready says when it has.
    const keyLoaded = keys.start().catch(() => null);
    if (config.worker.enabled) worker.start();
    realtime.start();

    const prune = () => {
        try {
            const r = store.prune({ retentionDays: config.retentionDays, receiptRetentionDays: config.receiptRetentionDays, sandboxRetentionDays: config.apps.sandboxRetentionDays });
            if (r.events || r.receipts) log.log(`[retention] pruned ${r.events} events, ${r.receipts} receipts`);
        } catch (err) {
            log.error(`[retention] prune failed: ${err.message}`);
        }
    };
    const pruneTimer = setInterval(prune, config.pruneIntervalMs);
    pruneTimer.unref?.();
    if (config.worker.enabled) prune();

    let server = null;
    if (listen) {
        server = await new Promise((resolve, reject) => {
            const s = app.listen(config.port, config.host, () => resolve(s));
            s.on('error', reject);
        });
        // SSE connections stay open; Node's default keep-alive/header timers only affect idle sockets.
        server.keepAliveTimeout = 65000;
        server.headersTimeout = 66000;
        log.log(`[events] listening on http://${config.host}:${server.address().port}`);
    }

    async function close() {
        clearInterval(pruneTimer);
        realtime.stop();
        keys.stop();
        await worker.stop();
        if (server) {
            server.closeAllConnections?.();
            await new Promise(resolve => server.close(() => resolve()));
        }
        db.close();
    }

    return { config, db, store, keys, keyLoaded, auth, worker, realtime, metrics, app, server, close, prune };
}

if (require.main === module) {
    require('dotenv').config();
    start().then((handles) => {
        const shutdown = (sig) => {
            console.log(`[events] ${sig}: shutting down`);
            handles.close().then(() => process.exit(0), () => process.exit(1));
            setTimeout(() => process.exit(1), 10000).unref();
        };
        process.on('SIGTERM', () => shutdown('SIGTERM'));
        process.on('SIGINT', () => shutdown('SIGINT'));
    }).catch((err) => {
        console.error(`[events] failed to start: ${err.stack || err}`);
        process.exit(1);
    });
}

module.exports = { start };
