'use strict';
const assert = require('assert');
const { verifyDelivery } = require('../lib/client');
const { boot, request, serviceToken, envelope, subscriber, suite, sleep } = require('./helpers');

const t = suite('delivery');
let h;
const live = serviceToken('live', ['events.publish']);
const media = serviceToken('media', ['events.subscribe']);
const admin = serviceToken('ops', ['events.admin']);

async function subscribe(token, body) {
    return request(h.base, 'POST', '/api/v1/subscriptions', { token, body });
}
async function publish(env) {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.ok(r.status === 201 || r.status === 200, r.text);
    return r.body;
}

t('boot', async () => { h = await boot(); });

t('subscription API: create, list own, secret shown once', async () => {
    const stub = await subscriber();
    const r = await subscribe(media, { topic_pattern: 'live.demo.*', endpoint: stub.url });
    assert.strictEqual(r.status, 201, r.text);
    assert.match(r.body.id, /^sub_/);
    assert.strictEqual(r.body.consumer, 'media');
    assert.match(r.body.secret, /^whsec_[0-9a-f]{64}$/);
    const list = await request(h.base, 'GET', '/api/v1/subscriptions', { token: media });
    assert.strictEqual(list.body.subscriptions.length, 1);
    assert.strictEqual(list.body.subscriptions[0].secret, undefined);
    const other = await request(h.base, 'GET', '/api/v1/subscriptions', { token: serviceToken('games', ['events.subscribe']) });
    assert.strictEqual(other.body.subscriptions.length, 0, 'consumers only see their own');
    const dup = await subscribe(media, { topic_pattern: 'live.demo.*', endpoint: stub.url });
    assert.strictEqual(dup.status, 409);
    const foreign = await request(h.base, 'POST', `/api/v1/subscriptions/${r.body.id}/disable`, { token: serviceToken('games', ['events.subscribe']) });
    assert.strictEqual(foreign.status, 404, 'cannot disable another consumer\'s subscription');
    const noCap = await subscribe(serviceToken('media', ['events.read']), { topic_pattern: 'x.y.z', endpoint: stub.url });
    assert.strictEqual(noCap.status, 403);
    await request(h.base, 'POST', `/api/v1/subscriptions/${r.body.id}/disable`, { token: media });
    await stub.close();
});

t('SSRF: only http(s) endpoints on 127.0.0.1 or *.openvibe.* are accepted', async () => {
    const bad = [
        'http://10.0.0.5/hook', 'http://169.254.169.254/latest/meta-data', 'http://localhost:4000/x', 'http://[::1]:4000/x',
        'https://evil.example/hook', 'https://openvibe.live.evil.com/x', 'https://xopenvibe.live/x', 'file:///etc/passwd',
        'gopher://127.0.0.1:25/', 'http://user:pw@127.0.0.1/x', 'http://0.0.0.0:4000/x', 'https://openvibe.network./x', 'not a url',
    ];
    for (const endpoint of bad) {
        const r = await subscribe(media, { topic_pattern: 'live.*', endpoint });
        assert.strictEqual(r.status, 422, endpoint);
        assert.strictEqual(r.body.code, 'events.endpoint_not_allowed', endpoint);
    }
    for (const endpoint of ['http://127.0.0.1:3000/internal/events', 'https://openvibe.live/internal/events', 'https://live.openvibe.network/hook']) {
        const r = await subscribe(serviceToken('ssrfok', ['events.subscribe']), { topic_pattern: 'nothing.matches.this', endpoint });
        assert.strictEqual(r.status, 201, endpoint);
    }
    // http://2130706433/ is 127.0.0.1 once parsed; it is stored normalised.
    const r = await subscribe(serviceToken('ssrfok', ['events.subscribe']), { topic_pattern: 'nothing.matches.that', endpoint: 'http://2130706433:9/x' });
    assert.strictEqual(r.status, 201);
    assert.strictEqual(r.body.endpoint, 'http://127.0.0.1:9/x');
});

t('success: signed POST, verified by verifyDelivery; trace propagated', async () => {
    const stub = await subscriber();
    const sub = (await subscribe(media, { topic_pattern: 'live.stream.*', endpoint: stub.url })).body;
    const env = envelope('live', { trace_id: '4bf92f3577b34da6a3ce929d0e0e4736' });
    const { seq } = await publish(env);
    assert.strictEqual(h.store.getDelivery(env.event_id, sub.id).status, 'pending', 'persisted before delivery');
    await h.worker.drain();
    assert.strictEqual(stub.calls.length, 1);
    const call = stub.calls[0];
    assert.ok(verifyDelivery(call.rawBody, call.headers['x-openvibe-signature'], sub.secret));
    assert.ok(!verifyDelivery(call.rawBody, call.headers['x-openvibe-signature'], 'whsec_wrong_secret_wrong_secret_wrong'));
    assert.ok(!verifyDelivery(Buffer.concat([call.rawBody, Buffer.from(' ')]), call.headers['x-openvibe-signature'], sub.secret));
    assert.strictEqual(call.headers['x-openvibe-event-id'], env.event_id);
    assert.match(call.headers.traceparent, /^00-4bf92f3577b34da6a3ce929d0e0e4736-[0-9a-f]{16}-01$/);
    assert.deepStrictEqual(call.body.event.payload, env.payload);
    assert.strictEqual(call.body.seq, seq);
    const d = h.store.getDelivery(env.event_id, sub.id);
    assert.strictEqual(d.status, 'delivered');
    assert.strictEqual(d.attempt, 1);
    await request(h.base, 'POST', `/api/v1/subscriptions/${sub.id}/disable`, { token: media });
    await stub.close();
});

t('retries with backoff (injectable clock), then DLQ after 8 attempts, then replay', async () => {
    let healthy = false;
    const stub = await subscriber(() => (healthy ? 200 : 503));
    const sub = (await subscribe(media, { topic_pattern: 'live.retry.*', endpoint: stub.url })).body;
    const env = envelope('live', { event_type: 'live.retry.me' });
    await publish(env);
    const expectedWaits = [1000, 5000, 30000, 120000, 600000, 3600000, 3600000];
    for (let attempt = 1; attempt <= 8; attempt++) {
        await h.worker.drain();
        assert.strictEqual(stub.calls.length, attempt, `attempt ${attempt} made`);
        const d = h.store.getDelivery(env.event_id, sub.id);
        assert.strictEqual(d.attempt, attempt);
        if (attempt < 8) {
            assert.strictEqual(d.status, 'failed');
            assert.strictEqual(d.next_attempt_at - h.clock.now(), expectedWaits[attempt - 1]);
            h.clock.advance(expectedWaits[attempt - 1] - 1);
            await h.worker.drain();
            assert.strictEqual(stub.calls.length, attempt, 'not retried before the backoff elapses');
            h.clock.advance(1);
        } else {
            assert.strictEqual(d.status, 'dead');
            assert.strictEqual(d.last_error, 'HTTP 503');
        }
    }
    h.clock.advance(86400000);
    await h.worker.drain();
    assert.strictEqual(stub.calls.length, 8, 'a dead delivery is never retried on its own');

    const dlq = await request(h.base, 'GET', `/api/v1/deliveries?status=dead&subscription_id=${sub.id}`, { token: admin });
    assert.strictEqual(dlq.status, 200);
    assert.strictEqual(dlq.body.deliveries.length, 1);
    assert.strictEqual(dlq.body.deliveries[0].event_id, env.event_id);
    const denied = await request(h.base, 'GET', '/api/v1/deliveries?status=dead', { token: media });
    assert.strictEqual(denied.status, 403);

    healthy = true;
    const rp = await request(h.base, 'POST', '/api/v1/deliveries/replay', { token: admin, body: { subscription_id: sub.id, event_ids: [env.event_id] } });
    assert.strictEqual(rp.status, 200, rp.text);
    assert.strictEqual(rp.body.queued, 1);
    await h.worker.drain();
    assert.strictEqual(stub.calls.length, 9);
    assert.strictEqual(h.store.getDelivery(env.event_id, sub.id).status, 'delivered');
    await request(h.base, 'POST', `/api/v1/subscriptions/${sub.id}/disable`, { token: media });
    await stub.close();
});

t('replay from_seq re-sends retained matching events (new subscription catches up)', async () => {
    const e1 = envelope('live', { event_type: 'live.history.a' });
    const e2 = envelope('live', { event_type: 'live.history.b' });
    const e3 = envelope('live', { event_type: 'live.other.c' });
    const { seq } = await publish(e1);
    await publish(e2);
    await publish(e3);
    const stub = await subscriber();
    const sub = (await subscribe(media, { topic_pattern: 'live.history.*', endpoint: stub.url })).body;
    await h.worker.drain();
    assert.strictEqual(stub.calls.length, 0, 'subscriptions only get events published after they exist');
    const rp = await request(h.base, 'POST', '/api/v1/deliveries/replay', { token: admin, body: { subscription_id: sub.id, from_seq: seq } });
    assert.strictEqual(rp.body.queued, 2);
    await h.worker.drain();
    assert.deepStrictEqual(stub.calls.map(c => c.body.event.event_id), [e1.event_id, e2.event_id]);
    await request(h.base, 'POST', `/api/v1/subscriptions/${sub.id}/disable`, { token: media });
    await stub.close();
});

t('priority classes: critical before important before low', async () => {
    const orders = [[], [], []];
    const stubs = [];
    for (let i = 0; i < 3; i++) stubs.push(await subscriber((c) => { orders[i].push(c.body.event.priority); return 204; }));
    for (let i = 0; i < 3; i++) await subscribe(serviceToken(`prio${i}`, ['events.subscribe']), { topic_pattern: 'live.prio.*', endpoint: stubs[i].url });
    // Low first, critical last: the queue must still send critical first.
    await publish(envelope('live', { event_type: 'live.prio.x', priority: 'low' }));
    await publish(envelope('live', { event_type: 'live.prio.x', priority: 'important' }));
    await publish(envelope('live', { event_type: 'live.prio.x', priority: 'critical' }));
    const due = h.store.dueDeliveries(h.clock.now(), 100);
    assert.strictEqual(due.length, 3, 'one per subscription');
    assert.ok(due.every(d => d.priority === 0), 'each subscription\'s head is its critical event');
    await h.worker.drain();
    for (const o of orders) assert.deepStrictEqual(o, ['critical', 'important', 'low']);
    for (const s of stubs) await s.close();
    for (const sub of h.store.listSubscriptions()) h.store.setSubscriptionEnabled(sub.id, false);
});

t('backpressure: one in flight per subscription, <= maxInflight (20) overall', async () => {
    let concurrent = 0;
    let peak = 0;
    const perSub = new Map();
    let perSubPeak = 0;
    const fetchImpl = async (url, init) => {
        const sub = init.headers['X-OpenVibe-Subscription-Id'];
        concurrent++; peak = Math.max(peak, concurrent);
        perSub.set(sub, (perSub.get(sub) || 0) + 1); perSubPeak = Math.max(perSubPeak, perSub.get(sub));
        await sleep(15);
        concurrent--; perSub.set(sub, perSub.get(sub) - 1);
        return new Response(null, { status: 204 });
    };
    const bp = await boot({ deliveryFetch: fetchImpl });
    for (let i = 0; i < 25; i++) {
        const r = await request(bp.base, 'POST', '/api/v1/subscriptions', { token: serviceToken(`bp${i}`, ['events.subscribe']), body: { topic_pattern: 'live.bp.*', endpoint: 'http://127.0.0.1:9/hook' } });
        assert.strictEqual(r.status, 201);
    }
    for (let i = 0; i < 3; i++) await request(bp.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.bp.x' }) });
    await bp.worker.drain();
    assert.strictEqual(peak, 20, `peak in flight ${peak}`);
    assert.strictEqual(perSubPeak, 1);
    assert.strictEqual(bp.store.deliveryCounts().delivered, 75);
    await bp.stop();
});

t('the real loop delivers without drain()', async () => {
    const stub = await subscriber();
    const loop = await boot({ worker: 'on', env: { EVENTS_WORKER_INTERVAL_MS: '50' } });
    await request(loop.base, 'POST', '/api/v1/subscriptions', { token: media, body: { topic_pattern: 'live.loop.*', endpoint: stub.url } });
    await request(loop.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.loop.go' }) });
    for (let i = 0; i < 50 && !stub.calls.length; i++) await sleep(20);
    assert.strictEqual(stub.calls.length, 1);
    const ready = await request(loop.base, 'GET', '/api/ready');
    assert.strictEqual(ready.status, 200);
    assert.strictEqual(ready.body.checks.worker, true);
    await loop.stop();
    await stub.close();
});

t('stop', async () => { await h.stop(); });

t.run();
