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
const { createPolicyLibrary } = require('./fabric/policy');
const { createCarriers } = require('./fabric/carriers');
const { createPlanner } = require('./fabric/planner');
const { createKeyStore, createAuth } = require('./auth');
const { createWorker } = require('./worker');
const { createRealtime } = require('./realtime');
const { createApp } = require('./app');
const { createMetrics } = require('./metrics');
const { createGuardedPost } = require('./egress');
const { gracefulStop } = require('openvibe-sdk/service');

async function start({
    config, db: givenDb = null, clock = { now: () => Date.now() }, fetchImpl = globalThis.fetch, deliveryFetch = fetchImpl,
    appPost = undefined, dnsLookup = undefined, log = console, listen = true,
} = {}) {
    config = config || load();
    // PostgreSQL (ADR-035): opened and migrated here unless the caller (a test) hands in a migrated handle.
    const db = givenDb || await openDb(config, { log });
    // Delivery policies (ADR-042 decisions 1 and 4): the store reads the resolved ordering key on fan-out, the admin
    // API writes rows and drops the compiled cache. One library per process, shared by both.
    const policies = createPolicyLibrary(db, { clock, log });
    // Valkey (before the carriers: valkey-v1 needs it). Without VALKEY_URL it is null and no valkey carrier registers.
    const valkey = config.valkey.url ? require('openvibe-sdk/valkey').createValkey({ url: config.valkey.url, prefix: config.valkey.prefix, log }) : null;
    // The carrier registry (ADR-042 decision 5) and the planner that chooses among them; the store writes the chosen
    // carrier on every delivery at fan-out and records a (class, key) decision in delivery_placements when it changes.
    const carriers = createCarriers({ config, clock, log, valkey });
    const planner = createPlanner({ registry: carriers, db, clock, log });
    const store = createStore(db, { clock, maxHops: config.maxHops, usage: config.usage, policies, planner });
    const keys = createKeyStore({ urls: [config.networkInternalUrl, config.networkUrl], pem: config.networkPublicKey, fetchImpl, log });
    const auth = createAuth({ config, keys, store });
    const metrics = createMetrics({ store });
    // appPost / dnsLookup: developer-app delivery and the subscribe-time DNS check (server/egress.js);
    // injectable for tests only.
    const worker = createWorker({ store, config, clock, fetchImpl: deliveryFetch, appPost: appPost || createGuardedPost({ lookup: dnsLookup }), log, observe: metrics.observe });
    const realtime = createRealtime({ store, auth, config, clock, log });
    metrics.bind({ realtime });
    const app = createApp({ config, store, auth, keys, worker, realtime, metrics, dnsLookup, clock, log, valkey, policies, planner, carriers });

    // The key loads in the background (retrying while Network boots); /api/ready says when it has.
    const keyLoaded = keys.start().catch(() => null);
    if (config.worker.enabled) worker.start();
    realtime.start();
    // The push carriers consume their streams and re-emit remote TOPIC events to this process's SSE clients
    // (ADR-042 decision 5). A failure to start a carrier is not fatal: the poll (pg-v1) still carries delivery.
    try {
        await carriers.start({
            consumeQueue: config.worker.enabled,
            claim: (eventId, subscriptionId) => store.claimDelivery(eventId, subscriptionId, worker.lease()),
            deliver: (delivery) => worker.deliver(delivery),
            onRemote: (rows) => realtime.publish(rows),
        });
    } catch (err) {
        log.warn(`[events] carriers did not start: ${err.message}`);
    }

    const prune = async () => {
        try {
            const r = await store.prune({ retentionDays: config.retentionDays, receiptRetentionDays: config.receiptRetentionDays, sandboxRetentionDays: config.apps.sandboxRetentionDays });
            if (r.events || r.receipts) log.log(`[retention] pruned ${r.events} events, ${r.receipts} receipts`);
        } catch (err) {
            log.error(`[retention] prune failed: ${err.message}`);
        }
    };
    const pruneTimer = setInterval(prune, config.pruneIntervalMs);
    pruneTimer.unref?.();
    if (config.worker.enabled) await prune();

    // Project usage rollups (server/usage.js): each closed hour is stored as events.usage.recorded and
    // fanned out like a published event.
    const flushUsage = async (opts) => {
        try {
            const r = await store.flushUsage(opts);
            if (r.stored.length) {
                realtime.publish(r.stored);
                worker.kick();
            }
            if (r.invalid) log.error(`[usage] ${r.invalid} rollup(s) do not match events.usage.recorded@1 and stay unsent`);
            return r;
        } catch (err) {
            log.error(`[usage] flush failed: ${err.message}`);
            return { stored: [], invalid: 0 };
        }
    };
    const usageTimer = config.usage && config.usage.enabled ? setInterval(flushUsage, config.usage.flushIntervalMs) : null;
    usageTimer?.unref?.();

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
        if (usageTimer) clearInterval(usageTimer);
        policies.stop();
        await carriers.stop().catch(() => {});
        realtime.stop();
        keys.stop();
        await worker.stop();
        if (server) {
            server.closeAllConnections?.();
            await new Promise(resolve => server.close(() => resolve()));
        }
        if (valkey) await valkey.close();
        if (!givenDb) await db.close();
    }

    return { config, db, store, keys, keyLoaded, auth, worker, realtime, metrics, app, server, policies, carriers, planner, valkey, close, prune, flushUsage };
}

if (require.main === module) {
    require('dotenv').config();
    start().then((handles) => {
        // SIGTERM/SIGINT (openvibe-sdk/service, docs/service.md's handles family): requests in flight get 8 s,
        // then handles.close() (the timers stopped, the carriers/realtime/worker stopped, the server closed, the
        // database closed; a rejection exits 1); past 10 s the process exits 1.
        gracefulStop({ name: 'events', server: handles.server, handles, drainMs: 8000, deadlineMs: 10000 });
    }).catch((err) => {
        console.error(`[events] failed to start: ${err.stack || err}`);
        process.exit(1);
    });
}

module.exports = { start };
