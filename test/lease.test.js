'use strict';
// ADR-042 decision 3: deliveries are claimed with a database lease, so two workers never send one delivery twice and
// keep at most one delivery in flight per subscription; an expired lease is claimed again (at least once), and the
// late result of the worker that lost it is not recorded over the newer claim.
const assert = require('assert');
const { boot, request, serviceToken, envelope, subscriber, suite, sleep } = require('./helpers');
const { createWorker } = require('../server/worker');

const t = suite('lease');
let h;
const live = serviceToken('live', ['events.event.publish']);
const silent = { log() {}, warn() {}, error() {} };

async function subscribe(slug, topic, url) {
    const r = await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken(slug, ['events.subscription.manage']), body: { topic_pattern: topic, endpoint: url } });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
}
async function publish(env) {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.ok(r.status === 201 || r.status === 200, r.text);
}

t('boot', async () => { h = await boot(); });

t('two workers share the queue: every delivery exactly once, one in flight per subscription', async () => {
    const sent = new Map();
    const inFlight = new Map();
    let perSubPeak = 0;
    const stubs = [];
    for (let i = 0; i < 4; i++) {
        stubs.push(await subscriber(async (c) => 204));
    }
    const fetchImpl = async (url, init) => {
        const key = `${init.headers['X-OpenVibe-Event-Id']}|${init.headers['X-OpenVibe-Subscription-Id']}`;
        sent.set(key, (sent.get(key) || 0) + 1);
        const sub = init.headers['X-OpenVibe-Subscription-Id'];
        inFlight.set(sub, (inFlight.get(sub) || 0) + 1); perSubPeak = Math.max(perSubPeak, inFlight.get(sub));
        await sleep(3);
        inFlight.set(sub, inFlight.get(sub) - 1);
        return new Response(null, { status: 204 });
    };
    for (let i = 0; i < 4; i++) await subscribe(`lease${i}`, 'live.lease.*', stubs[i].url);
    for (let i = 0; i < 10; i++) await publish(envelope('live', { event_type: 'live.lease.x' }));
    const a = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    const b = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    await Promise.all([a.drain(), b.drain()]);
    assert.strictEqual(sent.size, 40, 'every (event, subscription) pair was delivered');
    assert.ok([...sent.values()].every((n) => n === 1), `nothing was sent twice: ${JSON.stringify([...sent.entries()].filter(([, n]) => n > 1))}`);
    assert.strictEqual(perSubPeak, 1, 'at most one delivery in flight per subscription across both workers');
    for (const s of stubs) await s.close();
    for (const sub of await h.store.listSubscriptions()) await h.store.setSubscriptionEnabled(sub.id, false);
});

t('an expired lease is claimed again; the late result of the lost claim is not recorded', async () => {
    const stub = await subscriber(() => 204);
    const sub = await subscribe('leaseexp', 'live.leaseexp.*', stub.url);
    await publish(envelope('live', { event_type: 'live.leaseexp.x' }));
    const now = h.clock.now();
    const [first] = await h.store.claimDeliveries(now, 10, { owner: 'worker-a', leaseUntil: now + 1000 });
    assert.ok(first, 'worker A claims it');
    assert.deepStrictEqual(await h.store.claimDeliveries(now, 10, { owner: 'worker-b', leaseUntil: now + 1000 }), [], 'while A holds the lease, B gets nothing');
    h.clock.advance(1001);
    const [again] = await h.store.claimDeliveries(h.clock.now(), 10, { owner: 'worker-b', leaseUntil: h.clock.now() + 1000 });
    assert.strictEqual(again && again.event_id, first.event_id, 'after the lease expires B claims the same delivery');
    const late = await h.store.recordAttempt(first.event_id, first.subscription_id, { ok: false, attempt: 1, status: 500, error: 'late', nextAttemptAt: h.clock.now() }, null, 'worker-a');
    assert.strictEqual(late, false, "A's late result is not recorded");
    assert.strictEqual(await h.store.recordAttempt(again.event_id, again.subscription_id, { ok: true, attempt: 1, status: 204 }, null, 'worker-b'), true);
    const d = await h.store.getDelivery(first.event_id, first.subscription_id);
    assert.deepStrictEqual([d.status, d.lease_owner, d.lease_until], ['delivered', null, null], "B's result stands and the lease is released");
    await stub.close();
    await h.store.setSubscriptionEnabled(sub.id, false);
});

t('a claim with no room, or a lease that ends now, claims nothing', async () => {
    const now = h.clock.now();
    assert.deepStrictEqual(await h.store.claimDeliveries(now, 5, { owner: 'x', leaseUntil: now }), [], 'a lease that ends now claims nothing');
    assert.deepStrictEqual(await h.store.claimDeliveries(now, 0, { owner: 'x', leaseUntil: now + 10 }), []);
});

t('stop', async () => { await h.stop(); });
t.run();
