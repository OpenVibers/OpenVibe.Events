'use strict';
// A subscription with max_inflight above 1 runs that many DIFFERENT deliveries at once, never one delivery several
// times. A delivery with no ordering key was claimable while leased, so one pass claimed the same lowest row up to
// max_inflight times: Search's deliveries went out six times each on 2026-11-02.
const assert = require('assert');
const { boot, request, serviceToken, envelope, subscriber, suite } = require('./helpers');

const t = suite('inflight-claim');
let h;

t('boot', async () => { h = await boot(); });

t('max_inflight 3, no ordering keys: one pass claims three different deliveries, then nothing while they are leased', async () => {
    const stub = await subscriber(() => 204);
    const sub = await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken('wide', ['events.subscription.manage']), body: { topic_pattern: 'live.wide.*', endpoint: stub.url } });
    assert.strictEqual(sub.status, 201, sub.text);
    await h.db.prepare('UPDATE subscriptions SET max_inflight = 3 WHERE id = ?').run(sub.body.id);
    for (let i = 0; i < 5; i++) {
        const r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.event.publish']), body: envelope('live', { event_type: 'live.wide.x' }) });
        assert.ok(r.status === 201 || r.status === 200, r.text);
    }
    const now = h.clock.now();
    const first = (await h.store.claimDeliveries(now, 10, { owner: 'worker-a', leaseUntil: now + 60_000 })).filter((d) => d.subscription_id === sub.body.id);
    assert.strictEqual(first.length, 3, 'up to max_inflight');
    assert.strictEqual(new Set(first.map((d) => d.event_id)).size, 3, 'three different deliveries, not one three times');
    const again = (await h.store.claimDeliveries(now + 1000, 10, { owner: 'worker-a', leaseUntil: now + 61_000 })).filter((d) => d.subscription_id === sub.body.id);
    assert.strictEqual(again.length, 0, 'at its cap: nothing more while those are leased');
    // One of them finishes: one more, and never one already leased.
    await h.store.recordAttempt(first[0].event_id, sub.body.id, { ok: true, attempt: 1, status: 204 }, null, 'worker-a');
    const more = (await h.store.claimDeliveries(now + 2000, 10, { owner: 'worker-a', leaseUntil: now + 62_000 })).filter((d) => d.subscription_id === sub.body.id);
    assert.strictEqual(more.length, 1, 'one slot freed, one claimed');
    assert.ok(!first.some((d) => d.event_id === more[0].event_id), 'a delivery that was not leased');
    // The single-delivery claim (the carrier path) refuses one that is leased.
    const leased = first[1];
    assert.strictEqual(await h.store.claimDelivery(leased.event_id, sub.body.id, { owner: 'worker-b', leaseUntil: h.clock.now() + 60_000 }), null, 'a leased delivery is not claimed again');
    await stub.close();
});

t('close', async () => { await h.close(); });

t.run();
