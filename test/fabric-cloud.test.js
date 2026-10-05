'use strict';
// cloud-v1 (ADR-042 decision 5, optional): a QUEUE-only push carrier over an SQS-compatible endpoint, SigV4-signed.
// The registry builds it only with CLOUD_QUEUE_URL, refuses an unknown id, and honours the disable switch. Against a
// fake HTTP SQS endpoint: a due QUEUE delivery pinned to cloud-v1 is POSTed as {event_id, subscription_id}; a broker
// that errors opens the breaker, so the planner excludes cloud-v1 with a reason and pg-v1 carries QUEUE; disabled,
// QUEUE stays on pg-v1. The rate card is not written (no owner price yet), so the offer names rc-cloud-queue-v1 and
// placement prices the paid provider at Infinity — pg-v1/valkey-v1 keep carrying QUEUE.
const assert = require('assert');
const nodeHttp = require('http');
const { validate } = require('openvibe-contracts');
const { boot, request, serviceToken, envelope, subscriber, suite } = require('./helpers');
const { load } = require('../server/config');
const { createCarriers } = require('../server/fabric/carriers');
const { createPlanner, RATE_CARDS } = require('../server/fabric/planner');
const { createCloudCarrier } = require('../server/fabric/carriers/cloud');

const t = suite('fabric-cloud');
const silent = { log() {}, warn() {}, error() {} };
const admin = serviceToken('ops', ['events.delivery.admin']);
const live = serviceToken('live', ['events.event.publish']);
const policy = { class: 'domain', durability: 'optional', delivery_semantics: 'at_least_once' };

// A fake SQS endpoint: it records every form body and answers 200, or `fail()` to answer 500 (a broker outage).
async function fakeSqs() {
    const calls = [];
    let status = 200;
    const server = nodeHttp.createServer((req, res) => {
        const chunks = [];
        req.on('data', (c) => chunks.push(c));
        req.on('end', () => {
            const raw = Buffer.concat(chunks).toString('utf8');
            calls.push({ url: req.url, headers: req.headers, raw, body: Object.fromEntries(new URLSearchParams(raw)) });
            res.statusCode = status;
            res.end(status === 200 ? '<SendMessageResponse/>' : 'error');
        });
    });
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    return {
        calls,
        url: `http://127.0.0.1:${server.address().port}/queue/ov-events`,
        fail: () => { status = 500; },
        close: () => new Promise((r) => { server.closeAllConnections?.(); server.close(() => r()); }),
    };
}
async function subscribe(base, slug, topic, url) {
    const r = await request(base, 'POST', '/api/v1/subscriptions', { token: serviceToken(slug, ['events.subscription.manage']), body: { topic_pattern: topic, endpoint: url } });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
}
async function publish(base, env) {
    const r = await request(base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.ok(r.status === 201 || r.status === 200, r.text);
    return r.body;
}
async function putPolicy(base, pattern, body) {
    const r = await request(base, 'PUT', `/api/v1/delivery-policies/${pattern}`, { token: admin, body });
    assert.strictEqual(r.status, 200, r.text);
    return r.body;
}

// ── Registry, offer, unpriced rate card, unknown id ─────────────────────────

t('registry: cloud-v1 only with CLOUD_QUEUE_URL; QUEUE only; unknown id refused; the offer names the unpriced card', async () => {
    const without = createCarriers({ config: load({ NODE_ENV: 'test' }), log: silent });
    assert.strictEqual(without.get('cloud-v1'), null, 'no CLOUD_QUEUE_URL: no cloud-v1');

    const reg = createCarriers({ config: load({ NODE_ENV: 'test', CLOUD_QUEUE_URL: 'http://127.0.0.1:1/queue/ov-events' }), log: silent });
    const c = reg.get('cloud-v1');
    assert.deepStrictEqual([...c.classes], ['QUEUE'], 'QUEUE only');
    assert.ok(!c.disabledReason, 'enabled by default when the URL is set');
    assert.ok(reg.forClass('QUEUE').some((a) => a.id === 'cloud-v1'));
    assert.ok(reg.forClass('TOPIC').every((a) => a.id !== 'cloud-v1') && reg.forClass('STREAM').every((a) => a.id !== 'cloud-v1'));
    const offer = c.offer();
    assert.ok(validate('platform.resource-offer@1', offer).valid, JSON.stringify(offer));
    assert.strictEqual(offer.capabilities.includes('events:gateway'), true);
    assert.strictEqual(offer.pricing.rate_card, 'rc-cloud-queue-v1');
    // The card is absent on purpose: no owner price yet, so the paid provider is never assumed free by placement.
    assert.strictEqual(RATE_CARDS.find((x) => x.id === 'rc-cloud-queue-v1'), undefined, 'no AI-written price');

    assert.throws(() => createCarriers({ config: load({ NODE_ENV: 'test', EVENTS_CARRIERS: 'pg-v1,kafka-v1' }) }), /unknown carrier adapter "kafka-v1"/);
});

// ── Signalling (fake SQS endpoint) ──────────────────────────────────────────

t('cloud-v1 QUEUE: a signal POSTs {event_id, subscription_id} with SigV4 and never logs the secret', async () => {
    const sqs = await fakeSqs();
    const warnings = [];
    const log = { log() {}, warn: (m) => warnings.push(String(m)), error() {} };
    const carrier = createCloudCarrier({ url: sqs.url, region: 'eu-west-3', accessKey: 'AKIA_TEST', secretKey: 'very-secret-key', log });
    try {
        await carrier.signal([{ event_id: 'evt_1', subscription_id: 'sub_1' }], 'QUEUE');
        assert.strictEqual(sqs.calls.length, 1);
        const c = sqs.calls[0];
        assert.strictEqual(c.body.Action, 'SendMessage');
        assert.strictEqual(c.body.Version, '2012-11-05');
        assert.deepStrictEqual(JSON.parse(c.body.MessageBody), { event_id: 'evt_1', subscription_id: 'sub_1' });
        assert.match(c.headers.authorization, /^AWS4-HMAC-SHA256 Credential=AKIA_TEST\/\d{8}\/eu-west-3\/sqs\/aws4_request,/);
        assert.ok(c.headers['x-amz-date'] && c.headers['x-amz-content-sha256'].length === 64);
        await carrier.signal([{ event_id: 'evt_2', subscription_id: 'sub_2' }], 'TOPIC');   // not this carrier's class
        assert.strictEqual(sqs.calls.length, 1);
        assert.ok(warnings.every((w) => !w.includes('very-secret-key')), 'the secret is never logged');
    } finally { await sqs.close(); }
});

t('cloud-v1 QUEUE: a due delivery pinned to cloud-v1 is posted to the endpoint after the commit', async () => {
    const sqs = await fakeSqs();
    const h = await boot({ env: {
        EVENTS_CARRIERS: 'pg-v1,cloud-v1',
        CLOUD_QUEUE_URL: sqs.url, CLOUD_QUEUE_REGION: 'eu-west-3',
        CLOUD_QUEUE_ACCESS_KEY: 'AKIA_TEST', CLOUD_QUEUE_SECRET_KEY: 'very-secret-key',
    }, worker: 'off' });
    const stub = await subscriber(() => 204);
    try {
        const sub = await subscribe(h.base, 'cloud', 'live.cl.*', stub.url);
        await putPolicy(h.base, 'live.cl.*', { ...policy, ordering: { scope: 'key', key: 'subject' } });
        // The first delivery is planned on pg-v1 (cloud-v1 is unpriced); rewriting its carrier models a backlog already
        // placed on cloud-v1, so the next same-key event is pinned there and its signal goes to the endpoint.
        const E0 = envelope('live', { event_type: 'live.cl.x', subject: { type: 'stream', id: 'K' } });
        await publish(h.base, E0);
        await h.store.db.prepare("UPDATE deliveries SET carrier = 'cloud-v1' WHERE event_id = ? AND subscription_id = ?").run(E0.event_id, sub.id);
        sqs.calls.length = 0;
        const E1 = envelope('live', { event_type: 'live.cl.x', subject: { type: 'stream', id: 'K' } });
        await publish(h.base, E1);
        assert.strictEqual(sqs.calls.length, 1, 'the pinned QUEUE delivery was posted');
        assert.deepStrictEqual(JSON.parse(sqs.calls[0].body.MessageBody), { event_id: E1.event_id, subscription_id: sub.id });
    } finally { await stub.close(); await h.stop(); await sqs.close(); }
});

// ── The planner ─────────────────────────────────────────────────────────────

t('planner: cloud-v1 is unpriced while up; a down broker (breaker open) excludes it with a reason and pg-v1 delivers', async () => {
    const sqs = await fakeSqs();
    const reg = createCarriers({ config: load({ NODE_ENV: 'test', CLOUD_QUEUE_URL: sqs.url, CLOUD_QUEUE_REGION: 'eu-west-3' }), log: silent });
    const planner = createPlanner({ registry: reg });
    const c = reg.get('cloud-v1');
    try {
        // Up: eligible, but no rate card means Infinity cost, so pg-v1 is planned (the paid provider is not assumed free).
        let ex = planner.explain('QUEUE', 'stream:K', policy);
        assert.strictEqual(ex.carrier, 'pg-v1');
        const up = ex.result.candidates.find((x) => x.id === 'cloud-v1');
        assert.strictEqual(up.eligible, true);

        // Broker down: five failed signals open the breaker; the planner excludes cloud-v1 with the reason and pg-v1 carries.
        sqs.fail();
        for (let i = 0; i < 5; i++) await c.signal([{ event_id: 'evt_x', subscription_id: 'sub_x' }], 'QUEUE');
        assert.strictEqual(c.healthy(), false, 'the breaker is open after consecutive errors');
        ex = planner.explain('QUEUE', 'stream:K', policy);
        assert.strictEqual(ex.carrier, 'pg-v1', 'pg-v1 (the poll) delivers');
        const down = ex.result.candidates.find((x) => x.id === 'cloud-v1');
        assert.strictEqual(down.eligible, false);
        assert.strictEqual(down.excluded_because, 'breaker open (cloud-v1)');
    } finally { await sqs.close(); }
});

t('a hung queue cannot hold the publish: each request is bounded by timeoutMs and counts as a failure', async () => {
    const silentLog = { warn() {}, info() {}, error() {} };
    // A fetch that never answers unless its abort signal fires.
    const hung = (_url, opts) => new Promise((_resolve, reject) => { opts.signal.addEventListener('abort', () => reject(opts.signal.reason)); });
    const c = createCloudCarrier({ url: 'http://queue.test/q', timeoutMs: 50, concurrency: 4, log: silentLog, fetchImpl: hung });
    const t0 = Date.now();
    await c.signal(Array.from({ length: 8 }, (_, i) => ({ event_id: `evt_${i}`, subscription_id: 'sub' })), 'QUEUE');
    const took = Date.now() - t0;
    assert.ok(took < 1000, `eight hung requests finish in about two timeouts (took ${took} ms)`);
});

t('EVENTS_CARRIERS_DISABLED=cloud-v1: QUEUE stays on pg-v1 and the reason is configuration', async () => {
    const reg = createCarriers({ config: load({ NODE_ENV: 'test', CLOUD_QUEUE_URL: 'http://127.0.0.1:1/queue/ov-events', EVENTS_CARRIERS_DISABLED: 'cloud-v1' }), log: silent });
    assert.strictEqual(reg.get('cloud-v1').disabledReason, 'disabled by configuration');
    const ex = createPlanner({ registry: reg }).explain('QUEUE', 'k', policy);
    assert.strictEqual(ex.carrier, 'pg-v1');
    const cand = ex.result.candidates.find((x) => x.id === 'cloud-v1');
    assert.strictEqual(cand.eligible, false);
    assert.strictEqual(cand.excluded_because, 'disabled by configuration');
});

t.run();
