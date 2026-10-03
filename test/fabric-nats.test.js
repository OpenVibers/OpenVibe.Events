'use strict';
// nats-v1 (ADR-042 decision 5, optional): a NATS Core TOPIC carrier for cross-process SSE fan-out. The registry builds
// it only with NATS_URL, refuses a bad URL or subject prefix, and honours the disable switch; the planner prefers it for
// TOPIC while it is connected and explains why it is excluded when it is not. Against a real broker: two instances
// see one remote public event once, visibility holds on the remote gateway, a duplicate signal is dropped, a lost one
// is replayed from PostgreSQL on resume, a broker outage falls back to pg-v1 and recovers, and a disabled nats-v1
// never connects. The broker parts need OV_TEST_NATS_URL (scripts/test-nats.sh up); without it they print one
// `skipped (…)` line.
const assert = require('assert');
const net = require('net');
const { validate, ids } = require('openvibe-contracts');
const { boot, request, serviceToken, userToken, envelope, sse, suite, sleep, manualClock } = require('./helpers');
const { testDb } = require('./db');
const { load } = require('../server/config');
const { createCarriers } = require('../server/fabric/carriers');
const { createPlanner, RATE_CARDS } = require('../server/fabric/planner');

const t = suite('fabric-nats');
const NATS = process.env.OV_TEST_NATS_URL || '';
const admin = serviceToken('ops', ['events.delivery.admin']);
const live = serviceToken('live', ['events.event.publish']);
const reader = serviceToken('media', ['events.event.read']);
const topicPolicy = { class: 'realtime_ephemeral', durability: 'none', delivery_semantics: 'at_least_once' };
const silent = { log() {}, warn() {}, error() {} };

// A valkey-shaped stub (as in fabric.test.js): the registry constructs valkey-v1 from it, nothing connects.
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
async function explain(base, eventType) {
    const r = await request(base, 'GET', `/api/v1/placement?event_type=${eventType}`, { token: admin });
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(validate('platform.placement-result@1', r.body.placement).valid, JSON.stringify(r.body.placement));
    return r.body;
}
const cand = (ex, id) => ex.placement.candidates.find((c) => c.id === id);
const count = (client, id) => client.events().filter((m) => m.event.event_id === id).length;
// envelope() defaults to internal visibility: a public event says so.
const pub = (over = {}) => envelope('live', { event_type: 'live.nt.x', visibility: 'public', ...over });
const has = (id) => (c) => c.events().some((m) => m.event.event_id === id);

// A TCP proxy in front of the broker: down() drops every connection and stops listening (a broker outage as one
// instance sees it), up() listens again on the same port.
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

t('registry: nats-v1 only with NATS_URL; a bad URL or prefix refuses to start; the rate card and offer validate', async () => {
    const without = createCarriers({ config: load({ NODE_ENV: 'test' }), log: silent });
    assert.strictEqual(without.get('nats-v1'), null, 'no NATS_URL: no nats-v1');
    assert.throws(() => createCarriers({ config: load({ NODE_ENV: 'test', EVENTS_CARRIERS: 'pg-v1,kafka-v1' }) }), /unknown carrier adapter "kafka-v1"/);
    assert.throws(() => createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'http://127.0.0.1:4222' }), log: silent }), /nats:\/\//);
    assert.throws(() => createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1', NATS_SUBJECT_PREFIX: 'a b.' }), log: silent }), /NATS_SUBJECT_PREFIX/);

    const reg = createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1' }), log: silent });
    const n = reg.get('nats-v1');
    assert.deepStrictEqual([...n.classes], ['TOPIC'], 'TOPIC only: JetStream (STREAM) is not built');
    assert.ok(!n.disabledReason);
    assert.ok(reg.forClass('QUEUE').every((a) => a.id !== 'nats-v1') && reg.forClass('STREAM').every((a) => a.id !== 'nats-v1'));
    const card = RATE_CARDS.find((c) => c.id === 'rc-nats-v1');
    assert.ok(card && validate('platform.rate-card@1', card).valid, JSON.stringify(card));
    const offer = n.offer();
    assert.ok(validate('platform.resource-offer@1', offer).valid, JSON.stringify(offer));
    assert.strictEqual(offer.pricing.rate_card, 'rc-nats-v1');
});

t('planner: TOPIC prefers a connected nats-v1; not connected or disabled, it is excluded with a reason', async () => {
    const reg = createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1' }), valkey: fakeValkey, log: silent });
    const planner = createPlanner({ registry: reg });
    // Never started: not connected, so valkey-v1 carries TOPIC and the reason names the connection.
    let ex = planner.explain('TOPIC', 'live.x', topicPolicy);
    assert.strictEqual(ex.carrier, 'valkey-v1');
    assert.strictEqual(ex.result.candidates.find((c) => c.id === 'nats-v1').excluded_because, 'not connected (nats-v1)');

    reg.get('nats-v1').client.connected = () => true;   // as if the handshake were done
    ex = planner.explain('TOPIC', 'live.x', topicPolicy);
    assert.strictEqual(ex.carrier, 'nats-v1', JSON.stringify(ex.result));
    assert.ok(ex.result.candidates.some((c) => c.id === 'pg-v1' && c.eligible), 'pg-v1 stays a candidate underneath');
    assert.strictEqual(planner.explain('QUEUE', 'k', topicPolicy).carrier, 'valkey-v1', 'QUEUE never goes to NATS Core');
    assert.strictEqual(planner.explain('STREAM', 'k', topicPolicy).carrier, 'pg-v1', 'STREAM stays on pg-v1');

    // The breaker on consecutive publish errors also drops it.
    const n = reg.get('nats-v1');
    for (let i = 0; i < 5; i++) n.signals.record('topic', false);
    assert.strictEqual(planner.explain('TOPIC', 'live.x', topicPolicy).result.candidates.find((c) => c.id === 'nats-v1').excluded_because, 'breaker open (nats-v1)');

    // EVENTS_CARRIERS_DISABLED=nats-v1 (the rollback): excluded at once; with no valkey, pg-v1 carries TOPIC.
    const off = createCarriers({ config: load({ NODE_ENV: 'test', NATS_URL: 'nats://127.0.0.1:1', EVENTS_CARRIERS_DISABLED: 'nats-v1' }), log: silent });
    off.get('nats-v1').client.connected = () => true;
    const ex2 = createPlanner({ registry: off }).explain('TOPIC', 'live.x', topicPolicy);
    assert.strictEqual(ex2.carrier, 'pg-v1');
    assert.strictEqual(ex2.result.candidates.find((c) => c.id === 'nats-v1').excluded_because, 'disabled by configuration');
});

// ── Against a NATS broker (OV_TEST_NATS_URL) ────────────────────────────────

t('nats-v1: two instances, remote fan-out once, visibility, duplicate and lost signals, outage and disable fall back to pg-v1', async () => {
    if (!NATS) return;   // the skip line is printed once, below
    const shared = await testDb();
    const clock = manualClock();
    const env = { NATS_URL: NATS, NATS_SUBJECT_PREFIX: `ov.events.test.${process.pid}.${Date.now()}.`, EVENTS_CARRIERS: 'pg-v1,nats-v1' };
    const proxy = natsProxy(NATS);
    await proxy.up();
    const a = await boot({ db: shared.db, clock, env, worker: 'off' });
    const b = await boot({ db: shared.db, clock, env, worker: 'off' });
    const c = await boot({ db: shared.db, clock, env: { ...env, NATS_URL: proxy.url }, worker: 'off' });
    const d = await boot({ db: shared.db, clock, env: { ...env, EVENTS_CARRIERS_DISABLED: 'nats-v1' }, worker: 'off' });
    const clients = [];
    const open = async (inst, headers = {}) => { const x = await sse(inst.base, '/realtime/stream?topics=live.nt.*', { headers }); clients.push(x); return x; };
    try {
        await waitFor(() => [a, b, c].every((i) => i.carriers.get('nats-v1').healthy()));
        const put = await request(a.base, 'PUT', '/api/v1/delivery-policies/live.nt.*', { token: admin, body: topicPolicy });
        assert.strictEqual(put.status, 200, put.text);
        for (const i of [b, c, d]) await i.policies.invalidate();

        const U1 = ids.newId('user');
        const anon = await open(b);
        const u1 = await open(b, { Authorization: `Bearer ${userToken({ subjectId: U1 })}` });
        const u2 = await open(b, { Authorization: `Bearer ${userToken({ subjectId: ids.newId('user') })}` });
        const svc = await open(b, { Authorization: `Bearer ${reader}` });
        // A sentinel published on `a` over nats-v1: once `anon` on `b` has it, everything before it has arrived too.
        const sentinel = async () => { const s = pub({ event_type: 'live.nt.sentinel' }); await publish(a.base, s); await anon.waitFor(has(s.event_id), 5000); return s; };

        // Explain on `a`: nats-v1 is selected for TOPIC, pg-v1 eligible underneath.
        const ex = await explain(a.base, 'live.nt.x');
        assert.strictEqual(ex.carrier_class, 'TOPIC');
        assert.strictEqual(ex.placement.selected, 'nats-v1', JSON.stringify(ex.placement));
        assert.ok(cand(ex, 'pg-v1').eligible);

        // One public event published on `a` reaches the browser on `b` once, and is already committed when it does.
        let capturedRows = null;
        const origPublish = a.realtime.publish;
        a.realtime.publish = (rows) => { capturedRows = rows; return origPublish(rows); };
        const P = pub();
        await publish(a.base, P);
        await anon.waitFor(has(P.event_id), 5000);
        assert.strictEqual((await request(b.base, 'GET', `/api/v1/events/${P.event_id}`, { token: reader })).status, 200, 'signalled after the commit');
        a.realtime.publish = origPublish;

        // Visibility on the remote gateway: a subject event only to its person, an internal one only to a service.
        const S = envelope('live', { event_type: 'live.nt.x', visibility: 'subject', subject: { type: 'user', id: U1 }, actor: { type: 'user', id: U1 } });
        const I = envelope('live', { event_type: 'live.nt.x', visibility: 'internal' });
        await publish(a.base, S);
        await publish(a.base, I);
        await sentinel();
        await u1.waitFor(has(S.event_id), 2000);
        await svc.waitFor(has(I.event_id), 2000);
        assert.strictEqual(count(u2, S.event_id), 0, 'another person never sees a subject event');
        assert.strictEqual(count(anon, S.event_id) + count(anon, I.event_id) + count(u1, I.event_id), 0, 'internal never reaches a browser');

        // Duplicate signals (the same rows re-published twice): still exactly once on `b`.
        await a.carriers.signal('nats-v1', capturedRows, 'TOPIC');
        await a.carriers.signal('nats-v1', capturedRows, 'TOPIC');
        await sentinel();
        assert.strictEqual(count(anon, P.event_id), 1, 'a duplicate signal is dropped');

        // A lost signal: the live stream on `b` misses it, a resume (Last-Event-ID) replays it from PostgreSQL.
        const before = anon.messages[anon.messages.length - 1].id;
        const client = a.carriers.get('nats-v1').client;
        const realPub = client.publish;
        client.publish = () => { client.publish = realPub; };   // the next message vanishes
        const L = pub();
        await publish(a.base, L);
        await sentinel();
        assert.strictEqual(count(anon, L.event_id), 0, 'the lost signal never arrived live');
        const resumed = await open(b, { 'Last-Event-ID': before });
        await resumed.waitFor(has(L.event_id), 3000);

        // Broker outage as `c` sees it: nats-v1 drops out with a reason, pg-v1 is selected, the publish still commits
        // and reaches `c`'s own browsers; `b` does not get it live. Back up, `c` reconnects and nats-v1 is chosen again.
        const local = await open(c);
        await proxy.down();
        await waitFor(() => !c.carriers.get('nats-v1').healthy(), 3000);
        const exDown = await explain(c.base, 'live.nt.x');
        assert.strictEqual(exDown.placement.selected, 'pg-v1', JSON.stringify(exDown.placement));
        assert.strictEqual(cand(exDown, 'nats-v1').excluded_because, 'not connected (nats-v1)');
        const O = pub();
        await publish(c.base, O);
        await local.waitFor(has(O.event_id), 2000);
        await sentinel();
        assert.strictEqual(count(anon, O.event_id), 0, 'nothing crossed the broker while it was down');
        await proxy.up();
        await waitFor(() => c.carriers.get('nats-v1').healthy(), 8000);
        assert.strictEqual((await explain(c.base, 'live.nt.x')).placement.selected, 'nats-v1', 'chosen again once reconnected');
        const R = pub();
        await publish(c.base, R);
        await anon.waitFor(has(R.event_id), 5000);

        // Disabled (the rollback): `d` never connects, explains why, and its TOPIC events stay on pg-v1.
        assert.strictEqual(d.carriers.get('nats-v1').client.connected(), false, 'a disabled nats-v1 never connects');
        const exOff = await explain(d.base, 'live.nt.x');
        assert.strictEqual(exOff.placement.selected, 'pg-v1');
        assert.strictEqual(cand(exOff, 'nats-v1').excluded_because, 'disabled by configuration');
        const X = pub();
        await publish(d.base, X);
        await sentinel();
        assert.strictEqual(count(anon, X.event_id), 0, 'a disabled carrier is never signalled');
    } finally {
        for (const x of clients) x.close();
        for (const i of [a, b, c, d]) await i.stop();
        await proxy.down().catch(() => {});
        await shared.close();
    }
});

if (!NATS) console.log('fabric nats: skipped (OV_TEST_NATS_URL not set: eval "$(scripts/test-nats.sh up)")');

t.run();
