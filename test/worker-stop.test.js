'use strict';
// The worker stops cleanly: a pass that is reading the due rows when the service stops still starts what it claimed
// (those rows are leased to this worker), and stop() resolves only after every one of those deliveries has recorded its
// attempt, because the database pool is closed right after. No new pass claims anything once stopping. On 2026-11-02
// both deploys logged "delivery … crashed: Cannot use a pool after calling end on the pool": the deliveries started by
// the pass outlived the pool, and the next worker sent them again.
const assert = require('assert');
const { boot, request, serviceToken, envelope, subscriber, suite, sleep } = require('./helpers');
const { createWorker } = require('../server/worker');

const t = suite('worker-stop');
let h;
const silent = { log() {}, warn() {}, error() {} };

t('boot', async () => { h = await boot(); });

t('stop() waits for the pass that is claiming and for what it started; nothing is claimed after', async () => {
    const stub = await subscriber(() => 204);
    const sub = await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken('stopper', ['events.subscription.manage']), body: { topic_pattern: 'live.stopper.*', endpoint: stub.url } });
    assert.strictEqual(sub.status, 201, sub.text);
    for (let i = 0; i < 3; i++) {
        const r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.event.publish']), body: envelope('live', { event_type: 'live.stopper.x' }) });
        assert.ok(r.status === 201 || r.status === 200, r.text);
    }
    // The store, with the claim held until the test lets it go, and every recorded attempt counted.
    let release;
    const gate = new Promise((r) => { release = r; });
    let claims = 0;
    let claimedRows = 0;
    let recorded = 0;
    let closed = false;
    const store = new Proxy(h.store, {
        get(target, key) {
            if (key === 'claimDeliveries') {
                return async (...args) => {
                    claims += 1;
                    if (claims === 1) await gate;
                    const rows = await target.claimDeliveries(...args);
                    claimedRows += rows.length;
                    return rows;
                };
            }
            if (key === 'recordAttempt') {
                return async (...args) => {
                    assert.ok(!closed, 'an attempt is recorded only while the pool is open');
                    await sleep(5);
                    const out = await target.recordAttempt(...args);
                    recorded += 1;
                    return out;
                };
            }
            const v = target[key];
            return typeof v === 'function' ? v.bind(target) : v;
        },
    });
    const fetchImpl = async () => { await sleep(10); return new Response(null, { status: 204 }); };
    const w = createWorker({ store, config: { ...h.config, worker: { ...(h.config.worker || {}), intervalMs: 5 } }, clock: h.clock, fetchImpl, log: silent });
    w.start();
    for (let i = 0; i < 100 && claims === 0; i++) await sleep(5);
    assert.strictEqual(claims, 1, 'the first pass is claiming');
    const stopping = w.stop();
    release();
    await stopping;
    closed = true;   // what index.js does next: close the pool
    assert.ok(claimedRows >= 1, 'the pass claimed the due rows');
    assert.strictEqual(recorded, claimedRows, 'every delivery the pass started recorded its attempt before stop() returned');
    const before = claims;
    await sleep(60);
    assert.strictEqual(claims, before, 'no pass claims anything after stop()');
    await stub.close();
});

t('close', async () => { await h.close(); });

t.run();
