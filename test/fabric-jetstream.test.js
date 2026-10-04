'use strict';
// nats-js-v1 (ADR-042 decisions 1, 2, 6, optional): the JetStream STREAM carrier. The registry builds it only with
// NATS_URL and NATS_JETSTREAM not off, and honours the disable switch; the planner asks events:durable +
// events:ordered for STREAM, prefers nats-js-v1 while it is healthy and explains why it is excluded when it is not.
// Against a real broker: (a) a durability-required + ordering-key policy is STREAM and the explain API selects
// nats-js-v1; (b) a broker outage drops it to pg-v1 with the reason in excluded_because and nothing is lost (the poll
// delivers what the carrier signalled and what it did not), and it comes back; (c) two consuming workers and one due
// delivery send it exactly once; (d) per-key order holds across two keys and across a crash before the ack (the
// redelivery claims nothing); (e) a forged (event_id, subscription_id) that was never committed claims nothing;
// (f) EVENTS_CARRIERS_DISABLED=nats-js-v1 never connects and leaves STREAM on pg-v1. The broker parts need
// OV_TEST_NATS_URL (scripts/test-nats.sh up, which starts the broker with -js); without it they print one
// `skipped (…)` line.
const assert = require('assert');
const net = require('net');
const { validate, ids } = require('openvibe-contracts');
const { boot, request, serviceToken, envelope, subscriber, suite, sleep, manualClock } = require('./helpers');
const { testDb } = require('./db');
const { load } = require('../server/config');
const { createCarriers } = require('../server/fabric/carriers');
const { createPlanner, RATE_CARDS } = require('../server/fabric/planner');
const { createJetStreamCarrier, keyToken } = require('../server/fabric/carriers/jetstream');
const { createNatsCore } = require('../server/fabric/nats-core');

const t = suite('fabric-jetstream');
const NATS = process.env.OV_TEST_NATS_URL || '';
const admin = serviceToken('ops', ['events.delivery.admin']);
const live = serviceToken('live', ['events.event.publish']);
const streamPolicy = { class: 'domain', durability: 'required', delivery_semantics: 'at_least_once', ordering: { scope: 'key', key: 'subject' } };
const silent = { log() {}, warn() {}, error() {} };

const fakeValkey = {
    client: { ovQueuePromote() {}, defineCommand() {}, on() {} },
    key: (...p) => `k:${p.join(':')}`,
    duplicate() { return { on() {}, subscribe() {}, unsubscribe: async () => {}, disconnect() {} }; },
};

async function waitFor(pred, ms = 8000) {
    const until = Date.now() + ms;
    for (;;) { if (await pred()) return; if (Date.now() > until) throw new Error('waitFor timed out'); await sleep(25); }
}
async function publish(base, env) {
    const r = await request(base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
}
async function putPolicy(base, pattern, body) {
    const r = await request(base, 'PUT', `/api/v1/delivery-policies/${pattern}`, { token: admin, body });
    assert.strictEqual(r.status, 200, r.text);
}
async function subscribe(base, slug, topic, url) {
    const r = await request(base, 'POST', '/api/v1/subscriptions', { token: serviceToken(slug, ['events.subscription.manage']), body: { topic_pattern: topic, endpoint: url } });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
}
async function explain(base, eventType) {
    const r = await request(base, 'GET', `/api/v1/placement?event_type=${eventType}`, { token: admin });
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(validate('platform.placement-result@1', r.body.placement).valid, JSON.stringify(r.body.placement));
    return r.body;
}
const cand = (ex, id) => ex.placement.candidates.find((c) => c.id === id);
const keyed = (k, over = {}) => envelope('live', { event_type: 'live.js.x', subject: { type: 'stream', id: k }, ...over });
const sent = (stub, id) => stub.calls.filter((c) => c.body.event.event_id === id).length;
const carrierOf = (db, eventId) => db.prepare('SELECT carrier, status FROM deliveries WHERE event_id = ?').get(eventId);

// A TCP proxy in front of the broker: down() drops every connection and stops listening, up() listens again.
function natsProxy(target) {
    const u = new URL(target);
    const socks = new Set();
    let server = null;
    let port = 0;
    return {
        get url() { return `nats://127.0.0.1:${port}`; },
        up() {
            server = net.createServer((c) => {
                const b = net.connect(Number(u.port) || 4222, u.hostname);
                socks.add(c); socks.add(b);
                const end = () => { c.destroy(); b.destroy(); socks.delete(c); socks.delete(b); };
                c.on('error', end); b.on('error', end); c.on('close', end); b.on('close', end);
                c.pipe(b); b.pipe(c);
            });
            return new Promise((r) => server.listen(port, '127.0.0.1', () => { port = server.address().port; r(); }));
        },
        down() {
            for (const s of socks) s.destroy();
            socks.clear();
            return new Promise((r) => server.close(() => r()));
        },
    };
}

// ── Registry, configuration gate, rate card, planner (no broker) ───────────

t('registry: nats-js-v1 only with NATS_URL and JetStream on; STREAM only; the rate card is cheaper than pg-v1 and validates', async () => {
    assert.strictEqual(createCarriers({ config: load({ NODE_ENV: 'test' }), log: silent }).get('nats-js-v1'), null, 'no NATS_URL: no nats-js-v1');
    assert.strictEqual(createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1', NATS_JETSTREAM: 'off' }), log: silent }).get('nats-js-v1'), null, 'NATS_JETSTREAM=off');
    assert.throws(() => createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1', NATS_STREAM: 'a.b' }), log: silent }), /NATS_STREAM/);

    const cfg = load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1' });
    assert.deepStrictEqual([cfg.nats.jetstream, cfg.nats.stream], [true, 'OV_EVENTS_STREAM']);
    assert.ok(cfg.carriers.enabled.includes('nats-js-v1'), 'on by default once NATS_URL is set');
    const reg = createCarriers({ config: cfg, log: silent });
    const js = reg.get('nats-js-v1');
    assert.deepStrictEqual([...js.classes], ['STREAM']);
    assert.ok(reg.forClass('QUEUE').every((a) => a.id !== 'nats-js-v1') && reg.forClass('TOPIC').every((a) => a.id !== 'nats-js-v1'));
    const card = RATE_CARDS.find((c) => c.id === 'rc-nats-js-v1');
    assert.ok(card && validate('platform.rate-card@1', card).valid, JSON.stringify(card));
    assert.ok(card.unit_price_usd < RATE_CARDS.find((c) => c.id === 'rc-pg-v1').unit_price_usd, 'preferred over pg-v1 while healthy');
    const offer = js.offer();
    assert.ok(validate('platform.resource-offer@1', offer).valid, JSON.stringify(offer));
    assert.deepStrictEqual(offer.capabilities, ['events:gateway', 'events:durable', 'events:ordered']);
    // The ordering key becomes one subject token: fixed length, subject-safe, distinct per key.
    assert.match(keyToken('stream:a.b *>'), /^[A-Za-z0-9_-]{22}$/);
    assert.notStrictEqual(keyToken('a'), keyToken('b'));
    assert.strictEqual(keyToken(null), '_');
});

t('planner: STREAM needs durable + ordered; prefers a healthy nats-js-v1; (f) disabled or unhealthy, pg-v1 with a reason', async () => {
    const reg = createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1' }), valkey: fakeValkey, log: silent });
    const planner = createPlanner({ registry: reg });
    // Never started: not connected.
    let ex = planner.explain('STREAM', 'stream:K', streamPolicy);
    assert.strictEqual(ex.carrier, 'pg-v1');
    assert.strictEqual(ex.result.candidates.find((c) => c.id === 'nats-js-v1').excluded_because, 'not connected (nats-js-v1)');

    const js = reg.get('nats-js-v1');
    js.healthy = () => true;   // as if connected with the stream ensured
    ex = planner.explain('STREAM', 'stream:K', streamPolicy);
    assert.strictEqual(ex.carrier, 'nats-js-v1', JSON.stringify(ex.result));
    assert.ok(ex.result.candidates.some((c) => c.id === 'pg-v1' && c.eligible), 'pg-v1 stays a candidate underneath');
    assert.strictEqual(planner.explain('QUEUE', 'k', streamPolicy).carrier, 'valkey-v1', 'QUEUE never goes to JetStream');
    assert.notStrictEqual(planner.explain('TOPIC', 'k', streamPolicy).carrier, 'nats-js-v1', 'TOPIC never goes to JetStream');

    // valkey-v1 and nats-v1 only signal: never selected for STREAM even when they are the only push carriers.
    const reg2 = createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1', NATS_JETSTREAM: 'off' }), valkey: fakeValkey, log: silent });
    assert.deepStrictEqual(reg2.forClass('STREAM').map((a) => a.id), ['pg-v1']);

    // (f) EVENTS_CARRIERS_DISABLED=nats-js-v1 (the rollback): excluded at once, STREAM on pg-v1.
    const off = createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1', EVENTS_CARRIERS_DISABLED: 'nats-js-v1' }), log: silent });
    off.get('nats-js-v1').healthy = () => true;
    const ex2 = createPlanner({ registry: off }).explain('STREAM', 'stream:K', streamPolicy);
    assert.strictEqual(ex2.carrier, 'pg-v1');
    assert.strictEqual(ex2.result.candidates.find((c) => c.id === 'nats-js-v1').excluded_because, 'disabled by configuration');

    // signalPublish groups STREAM deliveries by their carrier.
    const got = [];
    js.signal = async (rows, kind) => { got.push([kind, rows.map((r) => r.event_id)]); };
    await reg.signalPublish([], [
        { event_id: 'e1', subscription_id: 's', carrier: 'nats-js-v1', carrier_class: 'STREAM', ordering_key: 'k' },
        { event_id: 'e2', subscription_id: 's', carrier: 'pg-v1', carrier_class: 'STREAM', ordering_key: 'k' },
        { event_id: 'e3', subscription_id: 's', carrier: 'nats-js-v1', carrier_class: 'STREAM', ordering_key: 'j' },
    ]);
    assert.deepStrictEqual(got, [['STREAM', ['e1', 'e3']]]);
});

// ── Against a NATS broker with JetStream (OV_TEST_NATS_URL) ────────────────

t('nats-js-v1: explain, outage to pg-v1 and back, exactly once, per-key order across a crash, forged ids, disable', async () => {
    if (!NATS) return;   // the skip line is printed once, below
    const run = `${process.pid}_${Date.now()}`;
    const prefix = `ov.events.test.js${run}.`;
    const stream = `OV_EVENTS_TEST_${run}`;
    const env = { NATS_URL: NATS, NATS_SUBJECT_PREFIX: prefix, NATS_STREAM: stream, EVENTS_CARRIERS: 'pg-v1,nats-js-v1' };
    const shared = await testDb();
    const clock = manualClock();
    const proxy = natsProxy(NATS);
    await proxy.up();
    // a publishes (its worker is manual: no poll, no consumer); b and c are the consuming workers' stores.
    const a = await boot({ db: shared.db, clock, env });
    const b = await boot({ db: shared.db, clock, env: { EVENTS_CARRIERS: 'pg-v1' } });
    const c = await boot({ db: shared.db, clock, env: { EVENTS_CARRIERS: 'pg-v1' } });
    const p = await boot({ db: shared.db, clock, env: { ...env, NATS_URL: proxy.url } });
    const d = await boot({ db: shared.db, clock, env: { ...env, EVENTS_CARRIERS_DISABLED: 'nats-js-v1' } });
    const stub = await subscriber(() => 204);
    const raw = createNatsCore({ url: NATS, log: silent, inboxPrefix: `${prefix}_inbox.raw.` });
    const consumers = [];
    // A consuming carrier for instance `h`: claims through h's store with h's worker lease and sends with h's worker.
    const consumer = async (h, opts = {}, wrap = {}) => {
        const claims = [];
        const k = createJetStreamCarrier({ url: NATS, subjectPrefix: prefix, stream, log: silent, instanceId: `${opts.consumerName || ids.newId('event').slice(-8)}`, ...opts });
        await k.start({
            claim: async (e, s) => { const row = await h.store.claimDelivery(e, s, h.worker.lease()); claims.push([e, Boolean(row)]); return row; },
            deliver: async (row) => { await h.worker.deliver(row); if (wrap.afterDeliver) await wrap.afterDeliver(row, k); },
        });
        consumers.push(k);
        k.claims = claims;
        return k;
    };
    try {
        await raw.connect();
        await waitFor(() => a.carriers.get('nats-js-v1').healthy() && p.carriers.get('nats-js-v1').healthy());
        const sub = await subscribe(a.base, 'fabjs', 'live.js.*', stub.url);
        await putPolicy(a.base, 'live.js.*', streamPolicy);
        for (const i of [b, c, p, d]) await i.policies.invalidate();

        // (a) STREAM, and the explain API selects nats-js-v1; the delivery is written on it.
        const ex = await explain(a.base, 'live.js.x');
        assert.strictEqual(ex.carrier_class, 'STREAM');
        assert.strictEqual(ex.placement.selected, 'nats-js-v1', JSON.stringify(ex.placement));
        assert.ok(cand(ex, 'pg-v1').eligible);

        // (c) Two consuming workers, one due delivery: both get the message, exactly one claims and sends it.
        const cb = await consumer(b);
        const cc = await consumer(c);
        const E1 = keyed('K1');
        await publish(a.base, E1);
        assert.strictEqual((await carrierOf(shared.db, E1.event_id)).carrier, 'nats-js-v1');
        await waitFor(() => sent(stub, E1.event_id) === 1);
        await waitFor(() => [cb, cc].every((k) => k.claims.some(([e]) => e === E1.event_id)));
        assert.strictEqual([cb, cc].flatMap((k) => k.claims).filter(([e, won]) => e === E1.event_id && won).length, 1, 'one claim won');
        await sleep(200);
        assert.strictEqual(sent(stub, E1.event_id), 1, 'sent exactly once');
        assert.strictEqual((await carrierOf(shared.db, E1.event_id)).status, 'delivered');

        // (d) Per-key order across two keys, interleaved: each key's sends in publish order, each once. The poll runs
        // underneath as in production: a message whose claim yields (not its key's head yet, or the subscription row
        // locked by the other consumer's claim on PostgreSQL) is acked and the poll sends that row; the store keeps the order.
        const order = { K1: [], K2: [] };
        const poll = setInterval(() => { c.worker.dispatch().catch(() => {}); }, 500);
        try {
            for (let i = 0; i < 4; i++) for (const k of ['K1', 'K2']) { const e = keyed(k); order[k].push(e.event_id); await publish(a.base, e); }
            await waitFor(() => [...order.K1, ...order.K2].every((id) => sent(stub, id) === 1));
        } finally { clearInterval(poll); }
        const all = [...order.K1, ...order.K2];
        assert.ok([cb, cc].flatMap((k) => k.claims).some(([e, won]) => won && all.includes(e)), 'the carrier claimed and sent');
        for (const k of ['K1', 'K2']) {
            const seen = stub.calls.map((x) => x.body.event.event_id).filter((id) => order[k].includes(id));
            assert.deepStrictEqual(seen, order[k], `${k}: sent in publish order`);
        }

        // (e) A forged message on the stream naming an uncommitted event (and one naming a committed event for a
        // subscription that has no delivery of it) claims nothing; a real event behind them is still delivered.
        const forged = ids.newId('event');
        const before = stub.calls.length;
        const subj = `${prefix}fabric.stream.${keyToken('stream:K1')}`;
        assert.strictEqual((await raw.request(subj, { event_id: forged, subscription_id: sub.id })).stream, stream);
        await raw.request(subj, { event_id: E1.event_id, subscription_id: 'sub_forged0000000000' });
        await raw.request(subj, { nonsense: true });
        const E2 = keyed('K1');
        await publish(a.base, E2);
        await waitFor(() => sent(stub, E2.event_id) === 1);
        assert.strictEqual(stub.calls.length, before + 1, 'the forged messages sent nothing');
        assert.ok(cb.claims.some(([e, won]) => e === forged && !won), 'the forged id was looked up and claimed nothing');
        assert.strictEqual(await shared.db.prepare('SELECT 1 FROM deliveries WHERE event_id = ?').get(forged), undefined);
        await cc.stop();

        // (d, crash) A worker that sends and dies before its ack: the restarted worker (same durable) gets the message
        // again after ack_wait, the claim finds it delivered and sends nothing; the key's next events follow in order.
        await cb.stop();
        let crashed = false;
        const durable = `crash_${run}`;
        await consumer(b, { consumerName: durable, ackWaitMs: 1000 }, {
            afterDeliver: async (row, k) => { if (!crashed) { crashed = true; await k.client.close(); } },
        });
        const C1 = keyed('K3');
        await publish(a.base, C1);
        await waitFor(() => crashed && sent(stub, C1.event_id) === 1);
        const C2 = keyed('K3');
        const C3 = keyed('K3');
        await publish(a.base, C2);
        await publish(a.base, C3);
        const again = await consumer(b, { consumerName: durable, ackWaitMs: 1000 });
        await waitFor(() => sent(stub, C3.event_id) === 1, 10000);
        assert.ok(again.claims.some(([e, won]) => e === C1.event_id && !won), 'the unacked message was redelivered and claimed nothing');
        assert.strictEqual(sent(stub, C1.event_id), 1, 'the redelivery was deduped by the claim');
        const k3 = stub.calls.map((x) => x.body.event.event_id).filter((id) => [C1, C2, C3].some((e) => e.event_id === id));
        assert.deepStrictEqual(k3, [C1.event_id, C2.event_id, C3.event_id], 'K3 in order across the crash');
        await again.stop();

        // (b) Broker down for `p`: excluded with the reason, STREAM falls to pg-v1, nothing is lost; back up, preferred again.
        const pj = p.carriers.get('nats-js-v1');
        await proxy.down();
        await waitFor(() => !pj.healthy());
        const exDown = await explain(p.base, 'live.js.x');
        assert.strictEqual(exDown.placement.selected, 'pg-v1');
        assert.strictEqual(cand(exDown, 'nats-js-v1').excluded_because, 'not connected (nats-js-v1)');
        const L1 = keyed('K4');
        await publish(p.base, L1);
        assert.strictEqual((await carrierOf(shared.db, L1.event_id)).carrier, 'pg-v1');
        // A delivery signalled on nats-js-v1 that no worker consumed (no consumer runs now) is the poll's too.
        const L2 = keyed('K5');
        await publish(a.base, L2);
        assert.strictEqual((await carrierOf(shared.db, L2.event_id)).carrier, 'nats-js-v1');
        await p.worker.drain();
        assert.strictEqual(sent(stub, L1.event_id) + sent(stub, L2.event_id), 2, 'the poll delivered both: nothing lost');
        await proxy.up();
        await waitFor(() => pj.healthy(), 10000);
        assert.strictEqual((await explain(p.base, 'live.js.x')).placement.selected, 'nats-js-v1', 'preferred again once back');

        // (f) Disabled: never connects, STREAM stays on pg-v1.
        const dj = d.carriers.get('nats-js-v1');
        assert.strictEqual(dj.client.connected(), false, 'a disabled nats-js-v1 never connects');
        const exOff = await explain(d.base, 'live.js.x');
        assert.strictEqual(exOff.placement.selected, 'pg-v1');
        assert.strictEqual(cand(exOff, 'nats-js-v1').excluded_because, 'disabled by configuration');
        const F = keyed('K6');
        await publish(d.base, F);
        assert.strictEqual((await carrierOf(shared.db, F.event_id)).carrier, 'pg-v1');
    } finally {
        for (const k of consumers) await k.stop().catch(() => {});
        for (const i of [a, b, c, p, d]) await i.stop();
        await raw.request(`$JS.API.STREAM.DELETE.${stream}`, {}).catch(() => {});
        await raw.close();
        await proxy.down().catch(() => {});
        await stub.close();
        await shared.close();
    }
});

if (!NATS) console.log('fabric jetstream: skipped (OV_TEST_NATS_URL not set: eval "$(scripts/test-nats.sh up)")');

t.run();
