'use strict';
// ADR-042 decisions 1 and 4 (plan T7): the delivery policy is the semantic vocabulary (class -> derived carrier
// class, ordering key resolved against the envelope) and per-key ordering is enforced in the claim: a key's head is
// claimable, its successors wait for it, and a subscription keeps at most max_inflight deliveries leased (one key in
// flight by default). Concurrency bugs show only on real PostgreSQL — npm run test:pg runs this file there.
const assert = require('assert');
const { boot, request, serviceToken, envelope, subscriber, suite, sleep } = require('./helpers');
const { createWorker } = require('../server/worker');
const { DEFAULT_POLICY, carrierClass, orderingKey } = require('../server/fabric/policy');

const t = suite('ordering');
let h;
const admin = serviceToken('ops', ['events.delivery.admin']);
const reader = serviceToken('media', ['events.event.read']);
const live = serviceToken('live', ['events.event.publish']);
const silent = { log() {}, warn() {}, error() {} };

const CLASSES = ['realtime_ephemeral', 'interactive', 'interactive_durable', 'domain', 'critical', 'background', 'bulk', 'webhook'];
const policy = (over = {}) => ({ class: 'domain', durability: 'required', delivery_semantics: 'at_least_once', ...over });
const keyed = (key, scope = 'key') => policy({ ordering: { scope, key } });

async function subscribe(slug, topic, url) {
    const r = await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken(slug, ['events.subscription.manage']), body: { topic_pattern: topic, endpoint: url } });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
}
async function publish(env) {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.ok(r.status === 201 || r.status === 200, r.text);
}
async function putPolicy(pattern, body) {
    const r = await request(h.base, 'PUT', `/api/v1/delivery-policies/${pattern}`, { token: admin, body });
    assert.strictEqual(r.status, 200, r.text);
    return r.body;
}
const deletePolicy = (pattern) => request(h.base, 'DELETE', `/api/v1/delivery-policies/${pattern}`, { token: admin });

t('boot', async () => { h = await boot(); });

t('policy: default, carrier classes, key resolution and the admin API', async () => {
    // Every class maps to exactly one carrier class; durability required + an ordering key is STREAM for all of them.
    for (const c of CLASSES) {
        assert.strictEqual(carrierClass(policy({ class: c })), c === 'realtime_ephemeral' ? 'TOPIC' : 'QUEUE');
        assert.strictEqual(carrierClass(keyed('subject', 'key', c)), 'STREAM', `${c}: required + ordering is STREAM`);
        assert.strictEqual(carrierClass({ class: c, durability: 'optional', delivery_semantics: 'at_least_once', ordering: { scope: 'key', key: 'subject' } }), c === 'realtime_ephemeral' ? 'TOPIC' : 'QUEUE', `${c}: ordering without required durability is not STREAM`);
    }
    // Key resolution: 'subject', dotted paths, fallbacks, the 200-char cap, never the seq; no ordering -> null.
    const env = envelope('live', { subject: { type: 'stream', id: 'str_9' }, actor: { type: 'service', id: 'live' }, payload: { room: 42, empty: '', deep: { x: 'y' } } });
    assert.strictEqual(orderingKey(keyed('subject'), env), 'stream:str_9');
    assert.strictEqual(orderingKey(keyed('payload.room'), env), '42');
    assert.strictEqual(orderingKey(keyed('actor.id'), env), 'live');
    assert.strictEqual(orderingKey(keyed('payload.deep.x'), env), 'y');
    assert.strictEqual(orderingKey(keyed('payload.empty'), env), 'stream:str_9', 'an empty/unresolvable path falls back to the subject');
    assert.strictEqual(orderingKey(keyed('payload.missing'), env), 'stream:str_9');
    assert.strictEqual(orderingKey(keyed('payload.missing'), { event_type: 'live.x.y' }), 'live.x.y', 'no subject: falls back to the event type');
    assert.strictEqual(orderingKey(keyed('payload.big'), { event_type: 'live.x', payload: { big: 'a'.repeat(500) } }).length, 200, 'the key is capped at 200 characters');
    assert.strictEqual(orderingKey(DEFAULT_POLICY, env), null, 'the default policy has no ordering');
    assert.strictEqual(orderingKey({ ordering: { scope: 'key', key: 'paylaod.typo' } }, env), 'stream:str_9', 'a path that is not subject./actor./payload. falls back to the subject');

    // No rows: the default, and an empty list. Resolve answers the default shape.
    assert.deepStrictEqual(await h.policies.resolve('live.anything'), { policy: DEFAULT_POLICY, pattern: null, revision: 0 });
    const empty = await request(h.base, 'GET', '/api/v1/delivery-policies', { token: admin });
    assert.strictEqual(empty.status, 200);
    assert.deepStrictEqual(empty.body.policies, []);

    // PUT: revision starts at 1, +1 on a rewrite; updated_by is the admin principal; carrier_class is derived.
    const exact = await putPolicy('live.order.exact', keyed('subject'));
    assert.strictEqual(exact.revision, 1);
    assert.strictEqual(exact.carrier_class, 'STREAM');
    assert.strictEqual(exact.updated_by, 'ops');
    await putPolicy('live.order.*', keyed('subject'));
    await putPolicy('live.*', keyed('subject'));
    const rewrite = await putPolicy('live.order.exact', { class: 'critical', durability: 'none', delivery_semantics: 'at_most_once' });
    assert.strictEqual(rewrite.revision, 2);
    assert.strictEqual(rewrite.carrier_class, 'QUEUE');

    // Specificity: the exact type beats any wildcard; the longer literal wildcard beats the shorter one.
    const rExact = await request(h.base, 'GET', '/api/v1/delivery-policies/resolve?event_type=live.order.exact', { token: admin });
    assert.deepStrictEqual([rExact.status, rExact.body.pattern, rExact.body.revision, rExact.body.carrier_class], [200, 'live.order.exact', 2, 'QUEUE']);
    const rWild = await request(h.base, 'GET', '/api/v1/delivery-policies/resolve?event_type=live.order.other', { token: admin });
    assert.deepStrictEqual([rWild.body.pattern, rWild.body.carrier_class], ['live.order.*', 'STREAM']);
    const rShort = await request(h.base, 'GET', '/api/v1/delivery-policies/resolve?event_type=live.elsewhere.x', { token: admin });
    assert.strictEqual(rShort.body.pattern, 'live.*', 'the shorter wildcard is the only match');

    // Ties (equal literal length) break by pattern string order: 'a.*.c.d' beats 'a.b.*.d'.
    await putPolicy('a.b.*.d', keyed('subject'));
    await putPolicy('a.*.c.d', keyed('subject'));
    const tie = await request(h.base, 'GET', '/api/v1/delivery-policies/resolve?event_type=a.b.c.d', { token: admin });
    assert.strictEqual(tie.body.pattern, 'a.*.c.d', 'ties break deterministically by pattern string order');

    // The resolve endpoint answers the default for an unknown type.
    const none = await request(h.base, 'GET', '/api/v1/delivery-policies/resolve?event_type=nobody.home', { token: admin });
    assert.deepStrictEqual([none.body.pattern, none.body.revision, none.body.carrier_class, none.body.policy], [null, 0, 'QUEUE', DEFAULT_POLICY]);

    // The validator refuses an unknown class and an extra field with its own message.
    const badClass = await request(h.base, 'PUT', '/api/v1/delivery-policies/live.bad', { token: admin, body: { class: 'warp_speed', durability: 'required', delivery_semantics: 'at_least_once' } });
    assert.strictEqual(badClass.status, 400, badClass.text);
    assert.ok(/class/i.test(badClass.body.detail), badClass.body.detail);
    const badField = await request(h.base, 'PUT', '/api/v1/delivery-policies/live.bad', { token: admin, body: { ...policy(), mode: 'fast' } });
    assert.strictEqual(badField.status, 400, badField.text);

    // Non-admin: the same answers as the other admin routes (403 for a service without the capability, 401 without a token).
    assert.strictEqual((await request(h.base, 'GET', '/api/v1/delivery-policies', { token: reader })).status, 403);
    assert.strictEqual((await request(h.base, 'GET', '/api/v1/delivery-policies')).status, 401);
    assert.strictEqual((await request(h.base, 'PUT', '/api/v1/delivery-policies/live.bad', { token: reader, body: policy() })).status, 403);

    // DELETE: 404 when absent, 204 then again 404.
    assert.strictEqual((await deletePolicy('live.bad')).status, 404);
    assert.strictEqual((await deletePolicy('live.order.exact')).status, 204);
    assert.strictEqual((await deletePolicy('live.order.exact')).status, 404);

    for (const p of ['live.order.*', 'live.*', 'a.b.*.d', 'a.*.c.d']) assert.strictEqual((await deletePolicy(p)).status, 204);
});

t('per-key order under retries: a successor waits for its head, another key goes meanwhile', async () => {
    const stub = await subscriber(() => 204);
    const sub = await subscribe('ord2', 'live.ord2.*', stub.url);
    await putPolicy('live.ord2.*', keyed('subject'));
    const A1 = envelope('live', { event_type: 'live.ord2.x', subject: { type: 'stream', id: 'A' } });
    const A2 = envelope('live', { event_type: 'live.ord2.x', subject: { type: 'stream', id: 'A' } });
    const B1 = envelope('live', { event_type: 'live.ord2.x', subject: { type: 'stream', id: 'B' } });
    await publish(A1); await publish(A2); await publish(B1);

    let failA1 = true;
    const attempts = [];
    const fetchImpl = async (url, init) => {
        const ev = JSON.parse(init.body).event;
        let status = 204;
        if (ev.event_id === A1.event_id && failA1) { failA1 = false; status = 500; }
        attempts.push({ id: ev.event_id, status });
        return new Response(null, { status });
    };
    const w = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    await w.drain();
    const ids = () => attempts.map((a) => a.id);
    assert.deepStrictEqual(ids(), [A1.event_id, B1.event_id], 'A1 failed and B1 went; A2 (a successor of A1) did not');
    assert.strictEqual(attempts[0].status, 500);

    h.clock.advance(1001);   // A1's backoff elapses
    await w.drain();
    assert.deepStrictEqual(ids(), [A1.event_id, B1.event_id, A1.event_id, A2.event_id], 'A1 is retried and only then A2');
    assert.strictEqual(attempts.filter((a) => a.id === A1.event_id).length, 2);
    assert.strictEqual(attempts.filter((a) => a.id === A2.event_id).length, 1);

    await deletePolicy('live.ord2.*');
    await stub.close();
    await h.store.setSubscriptionEnabled(sub.id, false);
});

t('per-key order across two workers with a lease expiry, and never two distinct events of one key in flight', async () => {
    const stub = await subscriber(() => 204);
    const sub = await subscribe('ord3', 'live.ord3.*', stub.url);
    await putPolicy('live.ord3.*', keyed('subject'));

    const keys = ['K0', 'K1', 'K2'];
    const byId = new Map();               // event_id -> deliveries
    const firstIds = new Map(keys.map((k) => [k, []]));   // key -> event ids in first-delivery order
    const inflight = new Map();           // key -> Set(event_id)
    let distinctPeak = 0;                 // the most distinct events of one key ever in flight at once
    const fetchImpl = async (url, init) => {
        const { event } = JSON.parse(init.body);
        const k = event.subject.id;
        byId.set(event.event_id, (byId.get(event.event_id) || 0) + 1);
        const set = inflight.get(k) || new Set();
        inflight.set(k, set);
        set.add(event.event_id);
        distinctPeak = Math.max(distinctPeak, set.size);
        await sleep(2);
        set.delete(event.event_id);
        const list = firstIds.get(k);
        if (!list.includes(event.event_id)) list.push(event.event_id);
        return new Response(null, { status: 204 });
    };

    // 30 events over 3 keys, interleaved across keys.
    const expectedIds = new Map(keys.map((k) => [k, []]));
    for (let i = 0; i < 10; i++) {
        for (const k of keys) {
            const env = envelope('live', { event_type: 'live.ord3.x', subject: { type: 'stream', id: k } });
            expectedIds.get(k).push(env.event_id);
            await publish(env);
        }
    }

    const a = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    const b = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    const pa = a.drain();
    await sleep(4);
    h.clock.advance(40001);   // whatever worker A holds now has an expired lease: worker B may re-claim it (at least once)
    const pb = b.drain();
    await Promise.all([pa, pb]);
    await a.drain(); await b.drain();

    assert.strictEqual(byId.size, 30, 'every event of every key was delivered');
    assert.ok([...byId.values()].every((n) => n >= 1), 'every event delivered at least once');
    for (const k of keys) {
        const list = firstIds.get(k);
        assert.deepStrictEqual(list, expectedIds.get(k), `${k}: first deliveries follow publish order`);
    }
    assert.strictEqual(distinctPeak, 1, 'never two distinct events of one key in flight at once');

    await deletePolicy('live.ord3.*');
    await stub.close();
    await h.store.setSubscriptionEnabled(sub.id, false);
});

t('max_inflight = 2: two keys run together, never two of one key, never three at once', async () => {
    const stub = await subscriber(() => 204);
    const sub = await subscribe('ord4', 'live.ord4.*', stub.url);
    await putPolicy('live.ord4.*', keyed('subject'));
    await h.store.db.prepare('UPDATE subscriptions SET max_inflight = 2 WHERE id = ?').run(sub.id);

    const inflight = [];   // { key, event id } currently being sent
    let totalPeak = 0;
    let keyPeak = 0;
    const fetchImpl = async (url, init) => {
        const { event } = JSON.parse(init.body);
        const k = event.subject.id;
        inflight.push({ k, id: event.event_id });
        totalPeak = Math.max(totalPeak, inflight.length);
        keyPeak = Math.max(keyPeak, inflight.filter((x) => x.k === k).length);
        await sleep(4);
        inflight.splice(inflight.findIndex((x) => x.k === k && x.id === event.event_id), 1);
        return new Response(null, { status: 204 });
    };

    for (let i = 0; i < 2; i++) {
        for (const k of ['K0', 'K1']) await publish(envelope('live', { event_type: 'live.ord4.x', subject: { type: 'stream', id: k } }));
    }
    const w = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    await w.drain();

    assert.strictEqual(totalPeak, 2, 'two keys of the subscription went in flight together');
    assert.strictEqual(keyPeak, 1, 'never two of one key at once');
    assert.strictEqual((await h.store.deliveryCounts()).pending, 0, 'every delivery was sent');

    await h.store.db.prepare('UPDATE subscriptions SET max_inflight = 1 WHERE id = ?').run(sub.id);
    await deletePolicy('live.ord4.*');
    await stub.close();
    await h.store.setSubscriptionEnabled(sub.id, false);
});

t('stop', async () => { await h.stop(); });
t.run();
