'use strict';
// Project usage rollups (WS-N task 4, server/usage.js): an app's stored events and its webhook
// delivery attempts are counted per project, environment and hour in the transactions that do the
// work, refusals as errors with their code and trace id; once the hour closes each rollup is stored
// exactly once as events.usage.recorded (common.usage-recorded@1), delivered to first-party
// subscribers and never shown to apps; a count in an hour already sent re-sends it as revision 2.
const assert = require('assert');
const { ids, validate } = require('openvibe-contracts');
const apps = require('../server/apps');
const { boot, request, appToken, subscriber, suite } = require('./helpers');

const t = suite('usage');
const HOUR = 60 * 60 * 1000;
const prj = `prj_${ids.ulid()}`;
const appId = ids.newId('app');
const key = apps.projectKey(prj);
const source = apps.appSource(`app:${appId}`);
const token = appToken({ appId, projectId: prj, env: 'sandbox', cap: ['events.app.publish', 'events.app.read'] });
const appEvent = (over = {}) => ({
    event_id: ids.newId('event'), event_type: `app.${key}.order.created`, version: 1, source,
    actor: { type: 'app', id: appId }, timestamp: new Date().toISOString(), subject: { type: 'order', id: 'o1' }, payload: { n: 1 }, ...over,
});
const TRACE = '4bf92f3577b34da6a3ce929d0e0e4736';

let h, network, statuses = [];
const rows = () => h.db.prepare('SELECT * FROM app_usage ORDER BY capability').all();

t('boot: an app subscription and a first-party usage subscriber', async () => {
    // The hour starts at a round time so the test controls when it closes.
    const clockStart = Math.floor(Date.now() / HOUR) * HOUR + 5 * 60 * 1000;
    h = await boot({
        env: { EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE: '5' },
        appPost: async () => ({ status: statuses.length ? statuses.shift() : 204 }),
    });
    h.clock.t = clockStart;
    network = await subscriber();
    h.store.createSubscription({ id: `sub_${ids.ulid()}`, consumer: source, topicPattern: `app.${key}.*`, endpoint: 'https://hooks.example.com/h', secret: 'a'.repeat(40), projectId: prj, env: 'sandbox' });
    h.store.createSubscription({ id: `sub_${ids.ulid()}`, consumer: 'network', topicPattern: 'events.usage.recorded', endpoint: network.url, secret: 'b'.repeat(40) });
});

t('stored app events count; a repeated event_id does not', async () => {
    const batch = [appEvent(), appEvent(), appEvent()];
    let r = await request(h.base, 'POST', '/api/v1/events', { token, body: { events: batch } });
    assert.strictEqual(r.status, 201);
    r = await request(h.base, 'POST', '/api/v1/events', { token, body: batch[0] });
    assert.strictEqual(r.status, 200, 'a duplicate');
    const [pub] = rows();
    assert.deepStrictEqual([pub.project_id, pub.env, pub.capability, pub.unit, pub.quantity, pub.errors], [prj, 'sandbox', 'events.app.publish', 'events', 3, 0]);
    assert.strictEqual(pub.window_start % HOUR, 0, 'an hour window');
});

t('refusals count as errors with code, status, trace id and event id; nothing of them as quantity', async () => {
    const wrong = appEvent({ event_type: 'live.stream.started' });
    let r = await request(h.base, 'POST', '/api/v1/events', { token, body: wrong, headers: { traceparent: `00-${TRACE}-00f067aa0ba902b7-01` } });
    assert.strictEqual(r.status, 403);
    r = await request(h.base, 'POST', '/api/v1/events', { token, body: { events: [appEvent(), appEvent(), appEvent()] } });
    assert.strictEqual(r.status, 429, 'past the per-minute quota: the whole batch is refused');
    const [pub] = rows();
    assert.strictEqual(pub.quantity, 3);
    assert.strictEqual(pub.errors, 2);
    assert.deepStrictEqual(JSON.parse(pub.error_codes), { 'events.type_not_allowed': 1, 'events.quota_exceeded': 1 });
    const samples = JSON.parse(pub.samples);
    assert.deepStrictEqual(samples.map(s => [s.code, s.status]), [['events.quota_exceeded', 429], ['events.type_not_allowed', 403]], 'newest first');
    assert.strictEqual(samples[1].trace_id, TRACE, 'the refused request\'s trace id');
    assert.strictEqual(samples[1].ref, wrong.event_id, 'the refused event');
});

t('webhook delivery attempts count, a non-2xx as an error with the event and its trace', async () => {
    statuses = [500];
    await h.worker.drain();
    const sub = rows().find(r => r.capability === 'events.app.subscribe');
    assert.deepStrictEqual([sub.unit, sub.quantity, sub.errors], ['deliveries', 3, 1]);
    assert.deepStrictEqual(JSON.parse(sub.error_codes), { 'events.delivery.http_500': 1 });
    const [s] = JSON.parse(sub.samples);
    assert.ok(/^evt_/.test(s.ref) && /^[0-9a-f]{32}$/.test(s.trace_id) && s.status === 500);
});

t('nothing is sent before the hour closes', async () => {
    const out = h.flushUsage();
    assert.strictEqual(out.stored.length, 0);
    assert.strictEqual(h.store.usage.pending(), 2);
});

t('after the hour: one events.usage.recorded per rollup, valid, internal, delivered to first-party subscribers', async () => {
    h.clock.advance(HOUR);
    const out = h.flushUsage();
    assert.strictEqual(out.stored.length, 2);
    for (const row of out.stored) {
        assert.deepStrictEqual([row.event_type, row.source, row.visibility, row.priority, row.subject_type, row.subject_id, row.project_id], ['events.usage.recorded', 'events', 'internal', 'low', 'project', prj, null]);
        const payload = JSON.parse(row.payload);
        assert.ok(validate('events.usage.recorded@1', payload).valid, JSON.stringify(validate('events.usage.recorded@1', payload).errors));
        const text = JSON.stringify(payload);
        assert.ok(!text.includes(appId) && !text.includes(source) && !text.includes('order.created'), 'no app, source or event content');
    }
    assert.strictEqual(h.flushUsage().stored.length, 0, 'sent once');
    await h.worker.drain();
    const got = network.calls.map(c => c.body.event);
    assert.strictEqual(got.length, 2);
    const pub = got.find(e => e.payload.capability === 'events.app.publish').payload;
    assert.deepStrictEqual([pub.quantity, pub.errors, pub.window, pub.env], [3, 2, 'hour', 'sandbox']);
    assert.strictEqual(Date.parse(pub.window_end) - Date.parse(pub.window_start), HOUR);
});

t('apps never see the rollups', async () => {
    const r = await request(h.base, 'GET', '/api/v1/events?topic=events.*&after_seq=0', { token });
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.events.length, 0);
});

t('a count in an hour already sent re-sends it as revision 2', async () => {
    const sent = rows().find(r => r.capability === 'events.app.publish');
    h.store.usage.record({ projectId: prj, env: 'sandbox', capability: 'events.app.publish', unit: 'events', quantity: 1, at: sent.window_start + 1000 });
    const out = h.flushUsage();
    assert.strictEqual(out.stored.length, 1);
    const p = JSON.parse(out.stored[0].payload);
    assert.deepStrictEqual([p.quantity, p.revision], [4, 2], 'the corrected totals');
});

t('EVENTS_USAGE=off counts nothing', async () => {
    const h2 = await boot({ env: { EVENTS_USAGE: 'off' } });
    try {
        const r = await request(h2.base, 'POST', '/api/v1/events', { token, body: appEvent() });
        assert.strictEqual(r.status, 201);
        assert.strictEqual(h2.db.prepare('SELECT COUNT(*) AS n FROM app_usage').get().n, 0);
    } finally { await h2.stop(); }
});

t('teardown', async () => { await network.close(); await h.stop(); });

t.run();
