'use strict';
// ADR-042 decisions 1-5 (plan T7): carriers, the planner and the explain API, beside the poll. The registry maps
// every policy class to exactly one carrier class, refuses an unknown adapter id, and honours the disable switch;
// the planner picks a carrier with hysteresis and records a (class, key) decision that explains a past delivery;
// valkey-v1 pushes QUEUE deliveries and TOPIC fan-out across processes while pg-v1 (the poll) stays underneath.
// The Valkey parts need a running Valkey (npm run test:pg); on PGlite they print one `skipped (…)` line.
const assert = require('assert');
const { validate, ids } = require('openvibe-contracts');
const { boot, request, serviceToken, userToken, envelope, subscriber, suite, sleep, manualClock, publicKey } = require('./helpers');
const { testDb } = require('./db');
const { load } = require('../server/config');
const { createWorker } = require('../server/worker');
const { createCarriers } = require('../server/fabric/carriers');
const { createPlanner, RATE_CARDS } = require('../server/fabric/planner');
const { carrierClass } = require('../server/fabric/policy');

const t = suite('fabric');
const silent = { log() {}, warn() {}, error() {} };
const HAS_VALKEY = process.env.EVENTS_TEST_STORE === 'pg' && !!process.env.OV_TEST_VALKEY_URL;

const admin = serviceToken('ops', ['events.delivery.admin']);
const reader = serviceToken('media', ['events.event.read']);
const live = serviceToken('live', ['events.event.publish']);
const CLASSES = ['realtime_ephemeral', 'interactive', 'interactive_durable', 'domain', 'critical', 'background', 'bulk', 'webhook'];
const policy = (over = {}) => ({ class: 'domain', durability: 'required', delivery_semantics: 'at_least_once', ...over });
const keyed = (key) => policy({ ordering: { scope: 'key', key } });

// A valkey-shaped stub: the registry only constructs the queue/pubsub adapters from it, never connects in these tests.
const fakeValkey = {
    client: { ovQueuePromote() {}, defineCommand() {}, on() {} },
    key: (...p) => `k:${p.join(':')}`,
    duplicate() { return { on() {}, subscribe() {}, unsubscribe: async () => {}, disconnect() {} }; },
};

// A minimal registry for the planner's own tests: two tunable adapters over one class.
function fakeAdapter(id, ms) {
    return {
        id, healthy: () => true, disabledReason: null,
        offer: () => ({
            offer_id: id, kind: 'provider', provider: id, region: 'cell', trust: 'first-party',
            capabilities: ['events:gateway'], latency_ms: { ack_p95: ms }, health: { status: 'up' },
            pricing: { model: 'per-operation', marginal_usd_per_unit: 0 }, updated_at: new Date(0).toISOString(),
        }),
    };
}
const fakeRegistry = (adapters) => ({
    get: (id) => adapters.find((a) => a.id === id) || null,
    forClass: () => adapters, eligible: () => adapters,
});

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
async function waitFor(pred, ms = 8000) {
    const until = Date.now() + ms;
    for (;;) { if (pred()) return; if (Date.now() > until) throw new Error('waitFor timed out'); await sleep(25); }
}

// ── Registry and policy classes ─────────────────────────────────────────────

t('registry: every class -> one carrier class; unknown id refused; disabled valkey excluded with a reason', async () => {
    // Every policy class maps to exactly one carrier class (the ADR table; also asserted in ordering.test.js).
    for (const c of CLASSES) {
        assert.strictEqual(carrierClass(policy({ class: c })), c === 'realtime_ephemeral' ? 'TOPIC' : 'QUEUE');
        assert.strictEqual(carrierClass(keyed('subject', c)), 'STREAM');
    }
    // An unknown adapter id in config refuses to start.
    assert.throws(() => createCarriers({ config: load({ NODE_ENV: 'test', EVENTS_CARRIERS: 'pg-v1,kafka-v1' }) }), /unknown carrier adapter "kafka-v1"/);

    // The rate cards are valid platform.rate-card@1 (the model is exercised with a small per-op figure).
    for (const card of RATE_CARDS) {
        const v = validate('platform.rate-card@1', card);
        assert.ok(v.valid, `${card.id}: ${JSON.stringify(v.errors)}`);
    }

    // EVENTS_CARRIERS_DISABLED=valkey-v1: excluded at once with reason `disabled by configuration`; pg-v1 chosen.
    const reg = createCarriers({ config: load({ NODE_ENV: 'test', EVENTS_CARRIERS_DISABLED: 'valkey-v1' }), valkey: fakeValkey });
    assert.strictEqual(reg.get('valkey-v1').disabledReason, 'disabled by configuration');
    const planner = createPlanner({ registry: reg });
    const ex = planner.explain('QUEUE', 'stream:K', policy({ class: 'critical', durability: 'none' }));
    assert.strictEqual(ex.carrier, 'pg-v1');
    const vk = ex.result.candidates.find((c) => c.id === 'valkey-v1');
    assert.strictEqual(vk.eligible, false);
    assert.strictEqual(vk.excluded_because, 'disabled by configuration');
    assert.ok(ex.result.reasons.length > 0 && validate('platform.placement-result@1', ex.result).valid);
});

// ── The planner ─────────────────────────────────────────────────────────────

t('planner: hysteresis holds a placement inside minGain; a much better candidate moves it', async () => {
    const a = fakeAdapter('a', 10);
    const b = fakeAdapter('b', 20);
    const planner = createPlanner({ registry: fakeRegistry([a, b]) });
    assert.strictEqual(planner.place('QUEUE', 'k', policy()).carrier, 'a', 'the lower-latency candidate wins');

    b.offer = fakeAdapter('b', 9).offer;   // 10% better: inside the SDK default minGain (15%)
    assert.strictEqual(planner.place('QUEUE', 'k', policy()).carrier, 'a', 'hysteresis keeps the current placement');

    b.offer = fakeAdapter('b', 1).offer;   // far better: move
    assert.strictEqual(planner.place('QUEUE', 'k', policy()).carrier, 'b', 'a clearly better candidate wins');
});

t('planner: an unhealthy valkey-v1 (breaker open) is excluded with a reason and pg-v1 is chosen', async () => {
    const reg = createCarriers({ config: load({ NODE_ENV: 'test' }), valkey: fakeValkey });
    const planner = createPlanner({ registry: reg });
    assert.strictEqual(planner.place('QUEUE', 'k', policy()).carrier, 'valkey-v1', 'valkey-v1 is preferred while healthy');
    const v = reg.get('valkey-v1');
    for (let i = 0; i < 5; i++) { v.signals.record('queue', false); v.signals.record('topic', false); }
    assert.strictEqual(v.healthy(), false, 'the breaker is open after consecutive errors');
    const ex = planner.explain('QUEUE', 'k', policy());
    assert.strictEqual(ex.carrier, 'pg-v1', 'pg-v1 delivers when valkey-v1 is down');
    const cand = ex.result.candidates.find((c) => c.id === 'valkey-v1');
    assert.strictEqual(cand.eligible, false);
    assert.match(cand.excluded_because, /breaker open/);
});

// ── Explain, stored decisions, pinning, at_most_once ────────────────────────

let h;
// pg-v1 only: these tests are about the planner's choice, the stored decision and pinning, deterministically on the
// poll; valkey-v1 has its own tests below (and the selection tests above use the registry directly).
t('boot', async () => { h = await boot({ env: { EVENTS_CARRIERS: 'pg-v1' } }); });

t('explain: shape, a stored decision explains a past delivery, and pinning keeps a backlog key', async () => {
    const stub = await subscriber(() => 204);
    const sub = await subscribe(h.base, 'fab', 'live.fab.*', stub.url);
    // durability optional + an ordering key: the QUEUE class (STREAM needs durability required).
    await putPolicy(h.base, 'live.fab.*', { class: 'domain', durability: 'optional', delivery_semantics: 'at_least_once', ordering: { scope: 'key', key: 'subject' } });

    // E0 fans out (worker not run): a key with a backlog. Its carrier is forced to a sentinel to prove pinning reads
    // the backlog's carrier, not the plan's choice.
    const E0 = envelope('live', { event_type: 'live.fab.x', subject: { type: 'stream', id: 'K' } });
    await publish(h.base, E0);
    await h.store.db.prepare("UPDATE deliveries SET carrier = 'sentinel-v1' WHERE event_id = ? AND subscription_id = ?").run(E0.event_id, sub.id);

    const E1 = envelope('live', { event_type: 'live.fab.x', subject: { type: 'stream', id: 'K' } });
    await publish(h.base, E1);
    const d1 = await h.store.getDelivery(E1.event_id, sub.id);
    assert.strictEqual(d1.ordering_key, 'stream:K');
    assert.strictEqual(d1.carrier, 'sentinel-v1', 'E1 is pinned to the carrier E0 still has a backlog on');

    // The explain endpoint: validated placement-result@1, non-empty reasons, candidates, and the stored decision.
    const ex = await request(h.base, 'GET', '/api/v1/placement?event_type=live.fab.x&key=stream:K', { token: admin });
    assert.strictEqual(ex.status, 200, ex.text);
    assert.strictEqual(ex.body.carrier_class, 'QUEUE');
    assert.strictEqual(ex.body.ordering_key, 'stream:K');
    assert.strictEqual(ex.body.pattern, 'live.fab.*');
    assert.ok(ex.body.placement.reasons.length > 0, 'reasons are non-empty');
    assert.ok(ex.body.placement.candidates.some((c) => c.id === 'pg-v1' && c.eligible));
    assert.ok(validate('platform.placement-result@1', ex.body.placement).valid, JSON.stringify(ex.body.placement));
    assert.ok(ex.body.last_decision && ex.body.last_decision.carrier === 'pg-v1', 'a decision was recorded');

    // The delivery is explained from the decision in force when it was created.
    const de = await request(h.base, 'GET', `/api/v1/placement/deliveries/${E1.event_id}/${sub.id}`, { token: admin });
    assert.strictEqual(de.status, 200, de.text);
    assert.strictEqual(de.body.carrier, 'sentinel-v1');
    assert.ok(de.body.decision && de.body.decision.carrier === 'pg-v1', 'the recorded decision explains the delivery');

    assert.strictEqual((await request(h.base, 'GET', '/api/v1/placement/deliveries/evt_nope/sub_nope', { token: admin })).status, 404);
    assert.strictEqual((await request(h.base, 'GET', '/api/v1/placement?event_type=live.fab.x', { token: reader })).status, 403);
    assert.strictEqual((await request(h.base, 'GET', '/api/v1/placement?event_type=live.fab.x')).status, 401);

    await h.store.setSubscriptionEnabled(sub.id, false);
    await stub.close();
});

t('at_most_once: one failed attempt is final (dead, no retry)', async () => {
    const sub = await subscribe(h.base, 'amo', 'live.amo.*', 'http://127.0.0.1:9/hook');
    await putPolicy(h.base, 'live.amo.*', { class: 'critical', durability: 'none', delivery_semantics: 'at_most_once' });
    let attempts = 0;
    const fetchImpl = async () => { attempts++; return new Response(null, { status: 500 }); };
    const w = createWorker({ store: h.store, config: h.config, clock: h.clock, fetchImpl, log: silent });
    const { event_id } = await publish(h.base, envelope('live', { event_type: 'live.amo.x', subject: { type: 'stream', id: 'A' } }));
    await w.drain();
    const d = await h.store.getDelivery(event_id, sub.id);
    assert.strictEqual(attempts, 1, 'only one attempt');
    assert.strictEqual(d.status, 'dead', 'a failed at_most_once attempt is dead, never retried');
    assert.strictEqual(d.next_attempt_at, null);
    await h.store.setSubscriptionEnabled(sub.id, false);
});

t('prune: placement history past the retention goes, except each key\'s latest decision', async () => {
    const DAY = 86400000;
    const now = Date.now() + 400 * DAY;
    const ins = h.store.db.prepare('INSERT INTO delivery_placements (carrier_class, ordering_key, carrier, result, decided_at) VALUES (?, ?, ?, ?::jsonb, ?)');
    for (const [key, age] of [['prune:a', 90], ['prune:a', 60], ['prune:a', 45], ['prune:b', 80]]) await ins.run('QUEUE', key, 'pg-v1', '{}', now - age * DAY);
    const r = await h.store.prune({ retentionDays: 30, now });
    assert.ok(r.placements >= 2, JSON.stringify(r));
    const left = await h.store.db.prepare("SELECT ordering_key, decided_at FROM delivery_placements WHERE ordering_key LIKE 'prune:%' ORDER BY ordering_key").all();
    assert.deepStrictEqual(left.map((x) => [x.ordering_key, Math.round((now - Number(x.decided_at)) / DAY)]), [['prune:a', 45], ['prune:b', 80]]);
});

t('stop', async () => { await h.stop(); });

// ── valkey-v1 (Valkey only: test:pg) ────────────────────────────────────────

t('valkey-v1 QUEUE: two instances, exactly once, per-key order, fast; a down carrier leaves nothing lost', async () => {
    if (!HAS_VALKEY) return;   // the skip line is printed once, below
    const shared = await testDb();
    const clock = manualClock();
    const prefix = `ov:events-test:fabric:${Date.now()}:`;
    const env = { VALKEY_URL: process.env.OV_TEST_VALKEY_URL, VALKEY_PREFIX: prefix, EVENTS_WORKER_INTERVAL_MS: '5000' };
    const a = await boot({ db: shared.db, clock, env, worker: 'on' });
    const b = await boot({ db: shared.db, clock, env, worker: 'on' });
    const stub = await subscriber(() => 204);
    try {
        const sub = await subscribe(a.base, 'fabq', 'live.q.*', stub.url);
        await putPolicy(a.base, 'live.q.*', keyed('subject'));
        const KEYS = ['K0', 'K1', 'K2', 'K3', 'K4'];
        const byId = new Map();
        const firstIds = new Map(KEYS.map((k) => [k, []]));
        const expectedIds = new Map(KEYS.map((k) => [k, []]));
        stub.calls.length = 0;
        const seen = new Map();
        // Publish 50 events over 5 keys, recording the publish time per event id.
        const publishedAt = new Map();
        for (let i = 0; i < 10; i++) {
            for (const k of KEYS) {
                const env2 = envelope('live', { event_type: 'live.q.x', subject: { type: 'stream', id: k } });
                publishedAt.set(env2.event_id, Date.now());
                expectedIds.get(k).push(env2.event_id);
                await publish(a.base, env2);
            }
        }
        // Wait for every delivery (carrier first; the 5 s poll is the floor).
        await waitFor(() => {
            for (const c of stub.calls) {
                const id = c.body.event.event_id;
                if (!seen.has(id)) { seen.set(id, { at: Date.now() }); byId.set(id, (byId.get(id) || 0) + 1); const L = firstIds.get(c.body.event.subject.id); if (L && !L.includes(id)) L.push(id); }
            }
            return seen.size >= 50;
        }, 9000);
        assert.strictEqual(byId.size, 50, 'every event of every key was delivered');
        assert.ok([...byId.values()].every((n) => n === 1), 'each delivery was sent exactly once');
        for (const k of KEYS) assert.deepStrictEqual(firstIds.get(k), expectedIds.get(k), `${k}: first deliveries follow publish order`);
        const lats = [...publishedAt.entries()].map(([id, at]) => seen.get(id).at - at).sort((x, y) => x - y);
        const median = lats[Math.floor(lats.length / 2)];
        assert.ok(median < 5000, `median publish->send latency ${median}ms is below the 5 s poll interval`);

        // Take the carrier down (consecutive signal errors open the breaker): the remaining deliveries still arrive.
        for (const inst of [a, b]) { const v = inst.carriers.get('valkey-v1'); for (let i = 0; i < 5; i++) { v.signals.record('queue', false); v.signals.record('topic', false); } assert.strictEqual(v.healthy(), false); }
        const before = seen.size;
        for (let i = 0; i < 5; i++) await publish(a.base, envelope('live', { event_type: 'live.q.x', subject: { type: 'stream', id: 'K9' } }));
        await waitFor(() => {
            for (const c of stub.calls) { const id = c.body.event.event_id; if (!seen.has(id)) { seen.set(id, { at: Date.now() }); byId.set(id, (byId.get(id) || 0) + 1); } }
            return seen.size >= before + 5;
        }, 12000);
        assert.ok([...byId.values()].every((n) => n === 1), 'nothing was lost or doubled when the carrier went down');
        await a.store.setSubscriptionEnabled(sub.id, false);
    } finally {
        await stub.close();
        await a.stop(); await b.stop();
        await shared.close();
    }
});

t('valkey-v1 TOPIC: an event published on one instance reaches an SSE client on another, exactly once', async () => {
    if (!HAS_VALKEY) return;
    const shared = await testDb();
    const clock = manualClock();
    const prefix = `ov:events-test:fabric:${Date.now()}:`;
    const env = { VALKEY_URL: process.env.OV_TEST_VALKEY_URL, VALKEY_PREFIX: prefix };
    const a = await boot({ db: shared.db, clock, env, worker: 'off' });
    const b = await boot({ db: shared.db, clock, env, worker: 'off' });
    const { sse } = require('./helpers');
    const serviceViewer = serviceToken('media', ['events.event.read']);
    const cService = await sse(b.base, '/realtime/stream?topics=live.rt.*', { headers: { Authorization: `Bearer ${serviceViewer}` } });
    const U1 = ids.newId('user');
    const u1 = userToken({ subjectId: U1 });
    const u2 = userToken({ subjectId: ids.newId('user') });
    const c1 = await sse(b.base, '/realtime/stream?topics=live.rt.*', { headers: { Authorization: `Bearer ${u1}` } });
    const c2 = await sse(b.base, '/realtime/stream?topics=live.rt.*', { headers: { Authorization: `Bearer ${u2}` } });
    try {
        await putPolicy(a.base, 'live.rt.*', { class: 'realtime_ephemeral', durability: 'none', delivery_semantics: 'at_least_once' });
        const E = envelope('live', { event_type: 'live.rt.x', visibility: 'internal' });
        await publish(a.base, E);
        await cService.waitFor((c) => c.events().some((m) => m.event.event_id === E.event_id), 5000);
        await sleep(200);
        assert.strictEqual(cService.events().filter((m) => m.event.event_id === E.event_id).length, 1, 'exactly once on the remote gateway');

        // A subject-visibility event reaches only its subject on the remote gateway.
        const S = envelope('live', { event_type: 'live.rt.x', visibility: 'subject', subject: { type: 'user', id: U1 }, actor: { type: 'user', id: U1 } });
        await publish(a.base, S);
        await c1.waitFor((c) => c.events().some((m) => m.event.event_id === S.event_id), 5000);
        await sleep(200);
        assert.strictEqual(c2.events().filter((m) => m.event.event_id === S.event_id).length, 0, 'another subject sees nothing');
    } finally {
        cService.close(); c1.close(); c2.close();
        await a.stop(); await b.stop();
        await shared.close();
    }
});

if (!HAS_VALKEY) console.log('fabric valkey: skipped (VALKEY_URL not set: run npm run test:pg with the test services up)');

t.run();
