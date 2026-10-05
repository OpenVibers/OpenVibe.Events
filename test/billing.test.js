'use strict';
// Billing readings (plan T5 step 14, server/billing.js): a closed hour's queue operations per production project
// (each publish and each delivered webhook counts ceil(bytes / 64 KiB), at least one) become one
// platform.usage-sample@1 reading, stored once under a deterministic id and never edited; the sender posts it to
// Billing with the service token, marks it sent on a 2xx (Billing's replay too), refuses it for good on a 4xx about
// the reading, and keeps it pending with backoff on a 5xx. Off by default.
const assert = require('assert');
const { ids, validate } = require('openvibe-contracts');
const apps = require('../server/apps');
const { createBillingReadings, keyOf, HOUR_MS, GRACE_MS } = require('../server/billing');
const { boot, request, appToken, envelope, serviceToken, suite } = require('./helpers');

const t = suite('billing');
const project = () => {
    const prj = `prj_${ids.ulid()}`, appId = ids.newId('app');
    const key = apps.projectKey(prj), source = apps.appSource(`app:${appId}`);
    return {
        prj, key, source,
        token: (env = 'production') => appToken({ appId, projectId: prj, env, cap: ['events.app.publish', 'events.app.subscribe'] }),
        event: (payload = { n: 1 }) => ({
            event_id: ids.newId('event'), event_type: `app.${key}.order.created`, version: 1, source,
            actor: { type: 'app', id: appId }, timestamp: new Date().toISOString(), subject: { type: 'order', id: 'o1' }, payload,
        }),
    };
};
const P1 = project(), P2 = project(), P3 = project(), SANDBOX = project();
const BILLING = 'http://billing.test';
const CONFIG = { intervalMs: 60000, url: BILLING, audience: 'openvibe.billing', clientId: 'events', clientSecret: 's'.repeat(32), timeoutMs: 5000 };

let h, H0, statuses = [], ev = {};
const readings = async () => await h.db.prepare('SELECT * FROM billing_readings ORDER BY id').all();
const publish = async (p, body, env = 'production') => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: p.token(env), body });
    assert.strictEqual(r.status, 201, r.text);
};

/** A Network token endpoint and a Billing that answers from `answers` (a status per call, 201 when empty). */
function stubBilling(answers = []) {
    const calls = { token: [], usage: [] };
    const fetchImpl = async (url, init) => {
        if (String(url).endsWith('/oauth/token')) {
            calls.token.push(Object.fromEntries(new URLSearchParams(String(init.body))));
            return new Response(JSON.stringify({ access_token: 'tok-billing', token_type: 'Bearer', expires_in: 300 }), { status: 200, headers: { 'Content-Type': 'application/json' } });
        }
        assert.strictEqual(url, `${BILLING}/api/v1/usage`);
        calls.usage.push({ auth: init.headers.Authorization, body: JSON.parse(init.body) });
        const status = answers.length ? answers.shift() : 201;
        if (status === 'throw') throw new Error('connect ECONNREFUSED');
        return new Response(JSON.stringify(status < 300 ? { record: {} } : { code: 'billing.x', status }), { status, headers: { 'Content-Type': 'application/json' } });
    };
    return { calls, fetchImpl };
}
const sender = (fetchImpl, config = CONFIG) => createBillingReadings({ db: h.db, config, networkUrl: 'http://network.test', clock: h.clock, fetchImpl, log: { log() {}, warn() {}, error() {} } });

t('boot: off by default, nothing aggregated or sent', async () => {
    H0 = Math.floor(Date.now() / HOUR_MS) * HOUR_MS;
    h = await boot({
        env: { EVENTS_MAX_PAYLOAD_BYTES: String(200 * 1024) },
        appPost: async () => ({ status: statuses.length ? statuses.shift() : 204 }),
    });
    h.clock.t = H0 + 5 * 60 * 1000;
    assert.strictEqual(h.config.billing.intervalMs, 0, 'EVENTS_BILLING_INTERVAL_MS defaults to 0');
    assert.strictEqual(h.config.billing.url, '', 'no OV_BILLING_INTERNAL_URL');
    assert.strictEqual(h.billing.enabled, false);
    assert.strictEqual(h.billing.running, false, 'no timer');
    assert.strictEqual(h.billing.sending, false, 'no Billing client');
    assert.strictEqual(h.billing.start(), false, 'start() stays off');
    await h.store.createSubscription({ id: `sub_${ids.ulid()}`, consumer: P1.source, topicPattern: `app.${P1.key}.*`, endpoint: 'https://hooks.example.com/h', secret: 'a'.repeat(40), projectId: P1.prj, env: 'production' });
});

t('the hour\'s publishes and delivered webhooks are counted per 64 KiB; sandbox, first-party and failed deliveries are not', async () => {
    ev = { small: P1.event(), big: P1.event({ blob: 'x'.repeat(70000) }), at64: P1.event(), over64: P1.event() };
    await publish(P1, { events: [ev.small, ev.big, ev.at64, ev.over64] });
    // The exact boundary: 65536 bytes is one operation, 65537 two.
    await h.db.prepare('UPDATE events SET size_bytes = ? WHERE id = ?').run(65536, ev.at64.event_id);
    await h.db.prepare('UPDATE events SET size_bytes = ? WHERE id = ?').run(65537, ev.over64.event_id);
    const big = await h.db.prepare('SELECT size_bytes FROM events WHERE id = ?').get(ev.big.event_id);
    assert.ok(big.size_bytes > 65536 && big.size_bytes <= 2 * 65536, `two operations: ${big.size_bytes}`);
    await publish(P2, P2.event());
    await publish(P3, { events: [P3.event(), P3.event()] });
    await publish(SANDBOX, SANDBOX.event(), 'sandbox');
    const r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.event.publish']), body: envelope('live') });
    assert.strictEqual(r.status, 201, r.text);
    statuses = [204, 204, 204, 500];   // small, big and at64 delivered; over64 fails
    await h.worker.drain();
    const d = await h.db.prepare("SELECT status FROM deliveries WHERE event_id = ?").get(ev.over64.event_id);
    assert.notStrictEqual(d.status, 'delivered');
});

t('an open hour cannot be aggregated', async () => {
    await assert.rejects(h.billing.aggregate(H0), /has not closed/);
    h.clock.advance(HOUR_MS);   // H0 + 65 min: closed once past H0 + 1 h + GRACE_MS
    assert.ok(H0 + HOUR_MS + GRACE_MS <= h.clock.now());
    assert.strictEqual(await h.db.prepare('SELECT COUNT(*) AS n FROM billing_readings').get().then(r => r.n), 0, 'nothing on the hot path');
});

t('a closed hour: one valid reading per production project, quantities in 64 KiB operations', async () => {
    const out = await h.billing.aggregateClosed();
    assert.strictEqual(out.created, 3);
    const rows = await readings();
    assert.deepStrictEqual(rows.map(r => r.project_id).sort(), [P1.prj, P2.prj, P3.prj].sort(), 'no sandbox, no first-party');
    const q = Object.fromEntries(rows.map(r => [r.project_id, r.reading.quantity]));
    // P1 publishes 1 + 2 + 1 + 2, delivers small 1 + big 2 + at64 1 (over64 failed).
    assert.deepStrictEqual([q[P1.prj], q[P2.prj], q[P3.prj]], [10, 1, 2]);
    for (const row of rows) {
        const v = validate('platform.usage-sample@1', row.reading);
        assert.ok(v.valid, JSON.stringify(v.errors));
        assert.deepStrictEqual(row.reading, {
            id: keyOf(row.project_id, H0), idempotency_key: keyOf(row.project_id, H0), service: 'events', project: row.project_id,
            resource: 'queue-operation-64kb', provider: 'local', operation: 'events.queue', quantity: row.reading.quantity,
            unit: 'queue-operation-64kb', at: new Date(H0).toISOString(), source: 'openvibe.events',
        });
        assert.strictEqual(row.id, `events:queue-operation-64kb:${row.project_id}:${new Date(H0).toISOString()}`);
        assert.deepStrictEqual([row.state, Number(row.attempts), row.period_start, row.metric], ['pending', 0, H0, 'queue-operation-64kb']);
    }
});

t('re-running a closed hour creates nothing; a reading is never edited', async () => {
    const before = await readings();
    assert.strictEqual((await h.billing.aggregate(H0)).created, 0);
    assert.strictEqual((await h.billing.aggregateClosed()).created, 0, 'the hour is marked aggregated');
    assert.deepStrictEqual(await readings(), before);
    await assert.rejects(h.db.prepare('UPDATE billing_readings SET reading = ? WHERE id = ?').run(JSON.stringify({ quantity: 1 }), before[0].id), /never edited/);
});

t('without OV_BILLING_INTERNAL_URL the readings stay queued', async () => {
    const out = await sender(globalThis.fetch, { ...CONFIG, url: '' }).send();
    assert.deepStrictEqual(out, { sent: 0, refused: 0, retried: 0 });
    assert.ok((await readings()).every(r => r.state === 'pending' && Number(r.attempts) === 0));
});

t('a 5xx keeps the reading pending with backoff and stops the batch', async () => {
    const b = stubBilling([503]);
    const out = await sender(b.fetchImpl).send();
    assert.deepStrictEqual(out, { sent: 0, refused: 0, retried: 1 });
    assert.strictEqual(b.calls.usage.length, 1, 'the rest waits');
    assert.deepStrictEqual([b.calls.token[0].client_id, b.calls.token[0].audience, b.calls.token[0].grant_type], ['events', 'openvibe.billing', 'client_credentials']);
    assert.strictEqual(b.calls.usage[0].auth, 'Bearer tok-billing');
    const rows = await readings();
    const tried = rows.find(r => r.id === b.calls.usage[0].body.idempotency_key);
    assert.deepStrictEqual([tried.state, Number(tried.attempts)], ['pending', 1]);
    assert.ok(/^503/.test(tried.last_error) && tried.next_attempt_at > h.clock.now());
    assert.ok(rows.every(r => r.lease_until === null), 'claims released');
    assert.strictEqual(rows.filter(r => Number(r.attempts) === 0).length, 2);
    // A network failure is the same: pending, next time.
    h.clock.advance(31 * 1000);
    const c = stubBilling(['throw']);
    assert.deepStrictEqual(await sender(c.fetchImpl).send(), { sent: 0, refused: 0, retried: 1 });
});

t('2xx (and Billing\'s replay of the key) marks sent; a 4xx about the reading refuses it for good', async () => {
    h.clock.advance(10 * 60 * 1000);
    const b = stubBilling([201, 200, 422]);
    assert.deepStrictEqual(await sender(b.fetchImpl).send(), { sent: 2, refused: 1, retried: 0 });
    const rows = await readings();
    const refused = rows.filter(r => r.state === 'refused');
    assert.strictEqual(refused.length, 1);
    assert.ok(/^422/.test(refused[0].last_error));
    for (const r of rows.filter(x => x.state === 'sent')) assert.ok(r.sent_at && r.last_error === null);
    for (const c of b.calls.usage) assert.deepStrictEqual(c.body, rows.find(r => r.id === c.body.id).reading, 'posted as stored');
    h.clock.advance(2 * HOUR_MS);
    const again = stubBilling();
    assert.deepStrictEqual(await sender(again.fetchImpl).send(), { sent: 0, refused: 0, retried: 0 });
    assert.strictEqual(again.calls.usage.length, 0, 'sent and refused readings are never posted again');
    const s = await h.billing.status();
    assert.deepStrictEqual([s.pending, s.sent, s.refused], [0, 2, 1]);
});

t('two senders never post one reading twice', async () => {
    const H1 = Math.floor(h.clock.now() / HOUR_MS) * HOUR_MS;
    await publish(P2, P2.event());
    h.clock.t = H1 + HOUR_MS + GRACE_MS + 1000;
    // The hours between H0 and H1 are aggregated too (nothing billable in them); H1 has P2's one publish.
    assert.strictEqual((await h.billing.aggregateClosed()).created, 1);
    let gate;
    const wait = new Promise(r => { gate = r; });
    const slow = stubBilling();
    const slowFetch = async (url, init) => { if (!String(url).endsWith('/oauth/token')) await wait; return slow.fetchImpl(url, init); };
    const a = sender(slowFetch).send(), b = sender(slowFetch).send();
    await new Promise(r => setTimeout(r, 50));
    gate();
    const [ra, rb] = await Promise.all([a, b]);
    assert.strictEqual(ra.sent + rb.sent, 1);
    assert.strictEqual(slow.calls.usage.length, 1);
    assert.strictEqual(slow.calls.usage[0].body.id, keyOf(P2.prj, H1));
});

t('the loop runs when EVENTS_BILLING_INTERVAL_MS is set', async () => {
    const h2 = await boot({ env: { EVENTS_BILLING_INTERVAL_MS: '60000' } });
    try {
        assert.strictEqual(h2.billing.enabled, true);
        assert.strictEqual(h2.billing.running, true);
        assert.strictEqual(h2.billing.sending, false, 'no OV_BILLING_INTERNAL_URL: aggregated, not sent');
        assert.ok(await h2.billing.tick());
    } finally { await h2.stop(); }
    assert.strictEqual(h2.billing.running, false, 'stopped with the process');
});

t('teardown', async () => { await h.stop(); });

t.run();
