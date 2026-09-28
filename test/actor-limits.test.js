'use strict';
// Per-actor rate limits at the capability routes (server/actor-limits.js, roadmap WS-R task 4): past its
// limit one principal gets 429 problem+json `rate_limited` with Retry-After, before the route does any
// work, while another principal still passes; the window reopens on the clock; publishing has its own
// numbers for services and apps, and Network's publishes (revocations, cutoffs, deletions) are never
// refused; refusals are logged and counted; EVENTS_LIMITS=off turns every limit off.
const assert = require('assert');
const { ids } = require('openvibe-contracts');
const apps = require('../server/apps');
const { load } = require('../server/config');
const { createLimits } = require('../server/actor-limits');
const { boot, request, serviceToken, appToken, envelope, manualClock, suite } = require('./helpers');

const t = suite('actor-limits');
let h;
// 15 s into a minute, so the minute window has 45 s left.
const clock = manualClock(Date.UTC(2026, 8, 27, 12, 0, 15));

const readerA = serviceToken('search', ['events.event.read']);
const readerB = serviceToken('community', ['events.event.read']);
const live = serviceToken('live', ['events.event.publish']);
const network = serviceToken('network', ['events.event.publish']);
const appId = ids.newId('app');
const projectId = `prj_${ids.ulid()}`;
const app = appToken({ appId, projectId, env: 'production', cap: ['events.app.publish'] });
const appEvent = () => envelope('x', {
    source: apps.appSource(appId), event_type: `app.${apps.projectKey(projectId)}.order.created`,
    actor: { type: 'app', id: appId }, subject: { type: 'order', id: '42' }, visibility: 'internal',
});
const get = (token, p) => request(h.base, 'GET', p, { token });
const publish = (token, body) => request(h.base, 'POST', '/api/v1/events', { token, body });

t('boot (EVENTS_LIMITS_MINUTE=3; the app event quota off, so only the per-actor limit answers)', async () => {
    h = await boot({ clock, env: { EVENTS_LIMITS_MINUTE: '3', EVENTS_LIMITS_HOUR: '100', EVENTS_APP_PUBLISH_PER_MINUTE: '0' } });
});

t('a read route: 3 a minute per principal, then 429 rate_limited with Retry-After; another principal passes', async () => {
    const one = `/api/v1/events/${ids.newId('event')}`;
    for (let i = 0; i < 3; i++) assert.strictEqual((await get(readerA, one)).status, 404);
    const r = await get(readerA, one);
    assert.strictEqual(r.status, 429, r.text);
    assert.strictEqual(r.headers.get('retry-after'), '45');
    assert.ok(/^application\/problem\+json/.test(r.headers.get('content-type')));
    assert.deepStrictEqual([r.body.code, r.body.status, r.body.retry_after_seconds], ['rate_limited', 429, 45]);
    assert.ok(r.body.detail.includes('events.event.read'), r.body.detail);
    assert.strictEqual((await get(readerB, one)).status, 404, 'another principal still passes');
    assert.strictEqual((await get(readerA, '/api/v1/checkpoints?topic=live.*')).status, 200, 'each route counts on its own');
    assert.strictEqual((await get(null, one)).status, 401, 'no token: 401 from the guard, never counted');
    clock.advance(45 * 1000);
    assert.strictEqual((await get(readerA, one)).status, 404, 'the next minute opens the window again');
});

t('health, ready, release.json and metrics are never limited', async () => {
    for (let i = 0; i < 10; i++) {
        assert.strictEqual((await get(null, '/api/health')).status, 200);
        assert.notStrictEqual((await get(null, '/api/ready')).status, 429);
        assert.strictEqual((await get(null, '/release.json')).status, 200);
        assert.strictEqual((await get(null, '/metrics')).status, 200);
    }
});

t('publish: an app gets 60 requests a minute; the 61st is refused before anything is stored', async () => {
    clock.t = Date.UTC(2026, 8, 27, 12, 5, 0);
    for (let i = 0; i < 60; i++) {
        const r = await publish(app, appEvent());
        assert.strictEqual(r.status, 201, r.text);
    }
    const before = h.store.lastSeq();
    const r = await publish(app, appEvent());
    assert.deepStrictEqual([r.status, r.body.code, r.headers.get('retry-after')], [429, 'rate_limited', '60']);
    assert.strictEqual(h.store.lastSeq(), before, 'nothing stored');
    assert.strictEqual((await publish(live, envelope('live'))).status, 201, 'a service still publishes');
});

t('publish: a service gets 600 requests a minute; Network is never refused', async () => {
    clock.t = Date.UTC(2026, 8, 27, 12, 10, 0);
    for (let i = 0; i < 600; i++) assert.strictEqual((await publish(live, envelope('live'))).status, 201);
    const r = await publish(live, envelope('live'));
    assert.deepStrictEqual([r.status, r.body.code], [429, 'rate_limited']);
    for (let i = 0; i < 610; i++) {
        const n = await publish(network, envelope('network', { event_type: 'network.app.revoked' }));
        assert.strictEqual(n.status, 201, `Network publish ${i + 1}: ${n.text}`);
    }
});

t('refusals are counted in events_rate_limited_total', async () => {
    const m = (await get(null, '/metrics')).text;
    assert.ok(/events_rate_limited_total\{limit="events.event.read",window="minute"\} 1/.test(m), m.split('\n').filter((l) => l.includes('rate_limited')).join('\n'));
    assert.ok(/events_rate_limited_total\{limit="events.app.publish",window="minute"\} 1/.test(m));
    assert.ok(/events_rate_limited_total\{limit="events.event.publish",window="minute"\} 1/.test(m));
    await h.stop();
});

t('refusals are logged once each: the limit, the principal and the window, never a token', async () => {
    const warned = [];
    const limits = createLimits({ config: load({ EVENTS_LIMITS_MINUTE: '1' }), clock: manualClock(Date.UTC(2026, 8, 27, 12, 0, 0)), log: { warn: (...a) => warned.push(a.join(' ')) } });
    const mw = limits('events.event.read');
    const req = { principal: { kind: 'service', sub: 'svc:search' }, headers: { authorization: 'Bearer secret-token' } };
    const res = { headers: {}, statusCode: 200, setHeader(k, v) { this.headers[k.toLowerCase()] = v; }, end(b) { this.body = b; } };
    let passed = 0;
    mw(req, res, () => { passed++; });
    mw(req, res, () => { passed++; });
    assert.deepStrictEqual([passed, res.statusCode, res.headers['retry-after']], [1, 429, '60']);
    assert.deepStrictEqual(warned, ['[limits] events.event.read: svc:search refused, over 1 per minute']);
});

t('EVENTS_LIMITS=off: no limit answers', async () => {
    const off = await boot({ clock: manualClock(Date.UTC(2026, 8, 27, 12, 0, 15)), env: { EVENTS_LIMITS: 'off', EVENTS_LIMITS_MINUTE: '1' } });
    for (let i = 0; i < 5; i++) assert.strictEqual((await request(off.base, 'GET', `/api/v1/events/${ids.newId('event')}`, { token: readerA })).status, 404);
    await off.stop();
});

t.run();
