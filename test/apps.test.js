'use strict';
// Developer apps (ADR-014): events.app.publish / events.app.read / events.app.subscribe with app
// tokens, project scoping, sandbox separation, the public-endpoint SSRF guard, quotas, revocation.
const assert = require('assert');
const nodeHttp = require('http');
const { ids } = require('openvibe-contracts');
const { verifyDelivery } = require('../lib/client');
const apps = require('../server/apps');
const { createGuardedPost, isPublicAddress } = require('../server/egress');
const { load } = require('../server/config');
const { boot, request, serviceToken, appToken, envelope, subscriber, suite, sleep } = require('./helpers');

const t = suite('apps');
const ALL = ['events.app.publish', 'events.app.read', 'events.app.subscribe'];

// ── Fake DNS: the test controls what every hostname resolves to ──
const dnsTable = new Map([['hooks.example.com', '93.184.216.34'], ['evil.example.com', '10.0.0.7'], ['rebind.example.com', '93.184.216.35']]);
function fakeLookup(host, opts, cb) {
    if (typeof opts === 'function') { cb = opts; opts = {}; }
    const ip = dnsTable.get(host);
    if (!ip) return cb(Object.assign(new Error('not found'), { code: 'ENOTFOUND' }));
    const family = ip.includes(':') ? 6 : 4;
    return opts.all ? cb(null, [{ address: ip, family }]) : cb(null, ip, family);
}

// A receiving server on 127.0.0.1 stands in for the app's public https endpoint: deliveries go through
// the real guarded poster, with DNS answered by the table above, loopback allowed only for this
// fake, and plain http underneath (the code path under test is the guard and the pinned lookup).
const seen = [];
const receiver = nodeHttp.createServer((req, res) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
        seen.push({ url: req.url, headers: req.headers, rawBody: Buffer.concat(chunks) });
        if (req.url.startsWith('/redirect')) { res.writeHead(302, { Location: 'http://127.0.0.1:1/metadata' }); return res.end(); }
        res.statusCode = 204;
        res.end();
    });
});
let receiverPort;
const pinned = [];
const testPost = createGuardedPost({
    lookup: (host, opts, cb) => fakeLookup(host, opts, (err, a, f) => {
        if (err) return cb(err);
        // Every public test host is served by the local receiver.
        const list = (Array.isArray(a) ? a : [{ address: a, family: f }]).map(x => ({ ...x, address: isPublicAddress(x.address) ? '127.0.0.1' : x.address }));
        pinned.push(list[0].address);
        return opts.all ? cb(null, list) : cb(null, list[0].address, list[0].family);
    }),
    requestImpl: (options, cb) => nodeHttp.request({ ...options, port: receiverPort }, cb),
    isAllowed: (ip) => ip === '127.0.0.1' || isPublicAddress(ip),
});

let h;
const prjA = `prj_${ids.ulid()}`;
const prjB = `prj_${ids.ulid()}`;
const appA = ids.newId('app');
const appA2 = ids.newId('app');   // a production app of the same project
const appB = ids.newId('app');
const keyA = apps.projectKey(prjA);
const keyB = apps.projectKey(prjB);
const tokA = (over = {}) => appToken({ appId: appA, projectId: prjA, env: 'sandbox', cap: ALL, ...over });
const tokA2 = (over = {}) => appToken({ appId: appA2, projectId: prjA, env: 'production', cap: ALL, ...over });
const tokB = (over = {}) => appToken({ appId: appB, projectId: prjB, env: 'sandbox', cap: ALL, ...over });
const live = serviceToken('live', ['events.event.publish']);
const reader = serviceToken('search', ['events.event.read']);

function appEvent(appId, projectId, name = 'order.created', over = {}) {
    return envelope('x', {
        source: apps.appSource(appId), event_type: `app.${apps.projectKey(projectId)}.${name}`,
        actor: { type: 'app', id: appId }, subject: { type: 'order', id: '42' }, visibility: 'internal', ...over,
    });
}
const publish = (token, body) => request(h.base, 'POST', '/api/v1/events', { token, body });
const pull = (token, topic, after = 0) => request(h.base, 'GET', `/api/v1/events?topic=${encodeURIComponent(topic)}&after_seq=${after}`, { token });

t('boot', async () => {
    await new Promise(r => receiver.listen(0, '127.0.0.1', r));
    receiverPort = receiver.address().port;
    h = await boot({
        appPost: testPost, dnsLookup: fakeLookup,
        env: { EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE: '50', EVENTS_APP_SANDBOX_MAX_SUBSCRIPTIONS: '3' },
    });
});

t('project_key and app source are defined exactly (and fit the envelope contract)', async () => {
    assert.strictEqual(apps.projectKey('prj_01JAB2C3D4E5F6G7H8J9K0MNPQ'), 'p01jab2c3d4e5f6g7h8j9k0mnpq');
    assert.strictEqual(apps.appSource('app:app_01JAB2C3D4E5F6G7H8J9K0MNPQ'), 'app-01jab2c3d4e5f6g7h8j9k0mnpq');
    assert.strictEqual(apps.projectKey('prj_bad'), null);
    assert.strictEqual(apps.appSource('svc:live'), null);
    assert.throws(() => load({ EVENTS_SOURCE_PREFIXES: JSON.stringify({ app: ['app.'] }) }), /reserved for developer apps/);
    assert.throws(() => load({ EVENTS_SOURCE_PREFIXES: JSON.stringify({ 'app-x1': ['app-x1.'] }) }), /reserved/);
});

t('publish: an app publishes app.<project_key>.* only, as its own source and actor', async () => {
    let r = await publish(tokA(), appEvent(appA, prjA));
    assert.strictEqual(r.status, 201, r.text);
    const row = h.store.getEvent(r.body.event_id);
    assert.strictEqual(row.project_id, prjA);
    assert.strictEqual(row.env, 'sandbox');
    assert.strictEqual(row.publisher, `app:${appA}`);

    r = await publish(tokA(), appEvent(appA, prjB));
    assert.strictEqual(r.status, 403); assert.strictEqual(r.body.code, 'events.type_not_allowed', 'never another project\'s types');
    r = await publish(tokA(), appEvent(appA, prjA, 'x', { event_type: 'live.stream.started' }));
    assert.strictEqual(r.body.code, 'events.type_not_allowed', 'never a first-party namespace');
    r = await publish(tokA(), appEvent(appA, prjA, 'x', { event_type: 'app.other.thing' }));
    assert.strictEqual(r.body.code, 'events.type_not_allowed');
    r = await publish(tokA(), appEvent(appA, prjA, 'x', { source: 'live' }));
    assert.strictEqual(r.body.code, 'events.source_mismatch', 'never a first-party source');
    r = await publish(tokA(), appEvent(appA, prjA, 'x', { source: apps.appSource(appB) }));
    assert.strictEqual(r.body.code, 'events.source_mismatch');
    r = await publish(tokA(), appEvent(appA, prjA, 'x', { actor: { type: 'service', id: 'live' } }));
    assert.strictEqual(r.body.code, 'events.actor_mismatch');
    const user = ids.newId('user');
    r = await publish(tokA({ onBehalfOf: user }), appEvent(appA, prjA, 'order.paid', { actor: { type: 'user', id: user } }));
    assert.strictEqual(r.status, 201, 'the user the token acts for may be the actor');
    r = await publish(appToken({ appId: appA, projectId: prjA, cap: ['events.app.read'] }), appEvent(appA, prjA));
    assert.strictEqual(r.status, 403, 'events.app.publish is required');
    // A first-party service can never publish into app.*: 'app' is not a source it can own.
    r = await publish(live, envelope('live', { event_type: `app.${keyA}.order.created` }));
    assert.strictEqual(r.status, 403);
});

t('sandbox tokens: accepted on the app routes only', async () => {
    const admin = await request(h.base, 'GET', '/api/v1/deliveries', { token: appToken({ cap: ['events.delivery.admin'], env: 'sandbox' }) });
    assert.strictEqual(admin.status, 401); assert.strictEqual(admin.body.code, 'token.sandbox_refused');
    const prodAdmin = await request(h.base, 'GET', '/api/v1/deliveries', { token: appToken({ cap: ['events.delivery.admin'], env: 'production' }) });
    assert.strictEqual(prodAdmin.status, 403, 'apps never hold operator capabilities');
    const sbxService = serviceToken('live', ['events.event.publish']);
    const withEnv = require('openvibe-contracts').serviceAuth.signServiceToken({
        ...JSON.parse(Buffer.from(sbxService.split('.')[1], 'base64url')), env: 'sandbox',
    }, require('./helpers').privateKey);
    const r = await publish(withEnv, envelope('live'));
    assert.strictEqual(r.status, 401); assert.strictEqual(r.body.code, 'token.sandbox_refused', 'a sandbox service token is refused');
    const rt = await request(h.base, 'GET', '/realtime/stream?topics=live.*', { token: tokA() });
    assert.strictEqual(rt.status, 401, 'realtime refuses sandbox app tokens');
});

t('read: own project in the same environment, plus public first-party events', async () => {
    const pub = await publish(live, envelope('live', { event_type: 'live.stream.started', visibility: 'public' }));
    const internal = await publish(live, envelope('live', { event_type: 'live.stream.ended', visibility: 'internal' }));
    const prodA = await publish(tokA2(), appEvent(appA2, prjA, 'order.created'));
    assert.strictEqual(prodA.status, 201, prodA.text);
    const sbxB = await publish(tokB(), appEvent(appB, prjB, 'order.created'));
    assert.strictEqual(sbxB.status, 201);

    let r = await pull(tokA(), `app.${keyA}.*`);
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(r.body.events.length >= 2 && r.body.events.every(e => e.event.event_type.startsWith(`app.${keyA}.`)));
    assert.ok(!r.body.events.some(e => e.event.event_id === prodA.body.event_id), 'a sandbox app never sees production events');
    r = await pull(tokA2(), `app.${keyA}.*`);
    assert.deepStrictEqual(r.body.events.map(e => e.event.event_id), [prodA.body.event_id], 'a production app never sees sandbox events');

    r = await pull(tokA(), `app.${keyB}.*`);
    assert.strictEqual(r.status, 403); assert.strictEqual(r.body.code, 'events.topic_not_allowed');
    for (const bad of ['*', '*.created', 'app.*', `live.*,app.${keyB}.*`]) {
        r = await pull(tokA(), bad);
        assert.strictEqual(r.status, 403, bad);
    }
    r = await pull(tokA(), 'live.*');
    assert.deepStrictEqual(r.body.events.map(e => e.event.event_id), [pub.body.event_id], 'public first-party only, never internal');

    r = await request(h.base, 'GET', `/api/v1/events/${sbxB.body.event_id}`, { token: tokA() });
    assert.strictEqual(r.status, 404, 'another project\'s event does not exist for this app');
    r = await request(h.base, 'GET', `/api/v1/events/${internal.body.event_id}`, { token: tokA() });
    assert.strictEqual(r.status, 404);
    r = await request(h.base, 'GET', `/api/v1/events/${pub.body.event_id}`, { token: tokA() });
    assert.strictEqual(r.status, 200);

    // First-party readers: never sandbox; app events only through an app.* pattern.
    r = await pull(reader, '*');
    assert.ok(!r.body.events.some(e => e.event.event_type.startsWith('app.')), 'no app events through *');
    r = await pull(reader, 'app.*');
    assert.deepStrictEqual(r.body.events.map(e => e.event.event_id), [prodA.body.event_id], 'production app events only');
    r = await request(h.base, 'GET', `/api/v1/events/${sbxB.body.event_id}`, { token: reader });
    assert.strictEqual(r.status, 404, 'sandbox events are invisible to first-party readers');

    // Checkpoints are per app and follow the same scope.
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: tokA(), body: { topic: `app.${keyA}.*`, cursor: 3 } });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.consumer, `app:${appA}`);
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: tokA(), body: { topic: `app.${keyB}.*`, cursor: 3 } });
    assert.strictEqual(r.status, 403);
    r = await request(h.base, 'GET', `/api/v1/checkpoints?topic=app.${keyA}.*`, { token: tokB() });
    assert.strictEqual(r.status, 403);
});

let subA;
t('subscribe: scoped patterns, public https endpoints only', async () => {
    const sub = (token, body) => request(h.base, 'POST', '/api/v1/subscriptions', { token, body });
    for (const endpoint of ['http://hooks.example.com/x', 'https://127.0.0.1/x', 'https://[::1]/x', 'https://10.1.2.3/x', 'https://169.254.169.254/latest',
        'https://evil.example.com/x', 'https://localhost/x', 'https://u:p@hooks.example.com/x', 'https://nowhere.example.com/x', 'https://intranet/x']) {
        const r = await sub(tokA(), { topic_pattern: `app.${keyA}.*`, endpoint });
        assert.strictEqual(r.status, 422, endpoint);
        assert.strictEqual(r.body.code, 'events.endpoint_not_allowed', endpoint);
    }
    let r = await sub(tokA(), { topic_pattern: `app.${keyB}.*`, endpoint: 'https://hooks.example.com/a' });
    assert.strictEqual(r.status, 403);
    r = await sub(tokA(), { topic_pattern: '*', endpoint: 'https://hooks.example.com/a' });
    assert.strictEqual(r.status, 403);
    r = await sub(appToken({ appId: appA, projectId: prjA, cap: ['events.app.read'] }), { topic_pattern: `app.${keyA}.*`, endpoint: 'https://hooks.example.com/a' });
    assert.strictEqual(r.status, 403, 'events.app.subscribe is required');

    r = await sub(tokA(), { topic_pattern: `app.${keyA}.*`, endpoint: 'https://hooks.example.com/a' });
    assert.strictEqual(r.status, 201, r.text);
    assert.strictEqual(r.body.consumer, `app:${appA}`);
    assert.strictEqual(r.body.project_id, prjA);
    assert.strictEqual(r.body.env, 'sandbox');
    subA = r.body;
    r = await sub(tokA(), { topic_pattern: 'live.*', endpoint: 'https://hooks.example.com/live' });
    assert.strictEqual(r.status, 201);
    const list = await request(h.base, 'GET', '/api/v1/subscriptions', { token: tokA() });
    assert.strictEqual(list.body.subscriptions.length, 2);
    assert.strictEqual((await request(h.base, 'GET', '/api/v1/subscriptions', { token: tokB() })).body.subscriptions.length, 0);
    assert.strictEqual((await request(h.base, 'GET', `/api/v1/subscriptions/${subA.id}`, { token: tokB() })).status, 404);
    assert.strictEqual((await request(h.base, 'POST', `/api/v1/subscriptions/${subA.id}/disable`, { token: tokA2() })).status, 404, 'another app of the project cannot touch it');
    // A first-party consumer never sees app subscriptions either.
    const media = serviceToken('media', ['events.subscription.manage']);
    assert.strictEqual((await request(h.base, 'GET', `/api/v1/subscriptions/${subA.id}`, { token: media })).status, 404);
    // Project quota on subscriptions (sandbox: 3 in this test).
    r = await sub(tokA(), { topic_pattern: `app.${keyA}.order.*`, endpoint: 'https://hooks.example.com/b' });
    assert.strictEqual(r.status, 201);
    r = await sub(tokA(), { topic_pattern: `app.${keyA}.order.paid`, endpoint: 'https://hooks.example.com/c' });
    assert.strictEqual(r.status, 429); assert.strictEqual(r.body.code, 'events.quota_exceeded');
});

t('delivery: signed, scoped, sandbox never meets production', async () => {
    // A production subscription of the same project, and one of another project.
    const prodSub = await request(h.base, 'POST', '/api/v1/subscriptions', { token: tokA2(), body: { topic_pattern: `app.${keyA}.*`, endpoint: 'https://hooks.example.com/prod' } });
    assert.strictEqual(prodSub.status, 201, prodSub.text);
    const bSub = await request(h.base, 'POST', '/api/v1/subscriptions', { token: tokB(), body: { topic_pattern: `app.${keyB}.*`, endpoint: 'https://hooks.example.com/b-project' } });
    assert.strictEqual(bSub.status, 201);
    const firstParty = await subscriber();
    const svcSub = await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken('ops', ['events.subscription.manage']), body: { topic_pattern: 'app.*', endpoint: firstParty.url } });
    assert.strictEqual(svcSub.status, 201);
    seen.length = 0;

    const e1 = await publish(tokA(), appEvent(appA, prjA, 'order.shipped'));
    const pub = await publish(live, envelope('live', { event_type: 'live.stream.started', visibility: 'public' }));
    await publish(live, envelope('live', { event_type: 'live.stream.ended', visibility: 'internal' }));
    await h.worker.drain();

    const byPath = (p) => seen.filter(g => g.url === p).map(g => g.headers['x-openvibe-event-id']);
    assert.deepStrictEqual(byPath('/a'), [e1.body.event_id], 'own sandbox event reached the sandbox subscription');
    assert.deepStrictEqual(byPath('/b'), [e1.body.event_id], 'app.<key>.order.* matches order.shipped');
    assert.deepStrictEqual(byPath('/live'), [pub.body.event_id], 'public first-party events only, never internal');
    assert.deepStrictEqual(byPath('/prod'), [], 'a sandbox event never reaches a production subscription');
    assert.deepStrictEqual(byPath('/b-project'), [], 'never another project');
    assert.strictEqual(firstParty.calls.length, 0, 'first-party subscriptions never get sandbox events');
    const a = seen.find(g => g.url === '/a');
    assert.ok(verifyDelivery(a.rawBody, a.headers['x-openvibe-signature'], subA.secret), 'HMAC-signed with the subscription secret');
    assert.strictEqual(JSON.parse(a.rawBody).event.source, apps.appSource(appA));
    assert.ok(pinned.length && pinned.every(ip => ip === '127.0.0.1'), 'the socket connected to the address the guard checked');

    // A production app event reaches the production subscription and first-party app.* subscribers, not sandbox ones.
    seen.length = 0;
    const e2 = await publish(tokA2(), appEvent(appA2, prjA, 'order.created'));
    await h.worker.drain();
    assert.deepStrictEqual(byPath('/prod'), [e2.body.event_id]);
    assert.deepStrictEqual(byPath('/a'), []);
    assert.deepStrictEqual(firstParty.calls.map(c => c.headers['x-openvibe-event-id']), [e2.body.event_id]);
    await firstParty.close();
});

t('delivery-time SSRF guard: re-resolved on every attempt, redirects never followed', async () => {
    // Real guard (no test allowances): the socket's lookup refuses, so nothing is ever sent.
    seen.length = 0;
    const strict = createGuardedPost({ lookup: fakeLookup, requestImpl: (o, cb) => nodeHttp.request({ ...o, port: receiverPort }, cb) });
    await assert.rejects(strict('https://evil.example.com/x', { headers: {}, body: '{}' }), e => e.permanent === true && /non-public/.test(e.message));
    assert.strictEqual(seen.length, 0);
    await assert.rejects(strict('https://10.0.0.1/x', { headers: {}, body: '{}' }), e => e.permanent === true);
    await assert.rejects(strict('http://hooks.example.com/x', { headers: {}, body: '{}' }), e => e.permanent === true);

    // DNS rebinding: public when subscribed, private when delivered.
    const r = await request(h.base, 'POST', '/api/v1/subscriptions', { token: tokB(), body: { topic_pattern: `app.${keyB}.rebind.*`, endpoint: 'https://rebind.example.com/hook' } });
    assert.strictEqual(r.status, 201, r.text);
    const redir = await request(h.base, 'POST', '/api/v1/subscriptions', { token: tokB(), body: { topic_pattern: `app.${keyB}.redirect.*`, endpoint: 'https://hooks.example.com/redirect' } });
    assert.strictEqual(redir.status, 201, redir.text);
    dnsTable.set('rebind.example.com', '10.0.0.9');
    seen.length = 0;
    const ev = await publish(tokB(), appEvent(appB, prjB, 'rebind.test'));
    const ev2 = await publish(tokB(), appEvent(appB, prjB, 'redirect.test'));
    await h.worker.drain();
    const d = h.store.getDelivery(ev.body.event_id, r.body.id);
    assert.strictEqual(d.status, 'dead', 'a private address at delivery time is refused for good');
    assert.match(d.last_error, /non-public/);
    const d2 = h.store.getDelivery(ev2.body.event_id, redir.body.id);
    assert.strictEqual(d2.status, 'failed'); assert.strictEqual(d2.last_status, 302);
    assert.deepStrictEqual(seen.map(s => s.url).filter(u => u !== '/b-project'), ['/redirect'], 'one request, the redirect was not followed, nothing reached the rebound host');
});

t('quotas: per-project publish rate and retained bytes', async () => {
    const h2 = await boot({ appPost: testPost, dnsLookup: fakeLookup, env: { EVENTS_APP_PUBLISH_PER_MINUTE: '3', EVENTS_APP_SANDBOX_RETAINED_BYTES: '3000' } });
    try {
        const pub = (token, body) => request(h2.base, 'POST', '/api/v1/events', { token, body });
        for (let i = 0; i < 3; i++) assert.strictEqual((await pub(tokA2(), appEvent(appA2, prjA))).status, 201);
        let r = await pub(tokA2(), appEvent(appA2, prjA));
        assert.strictEqual(r.status, 429); assert.strictEqual(r.body.code, 'events.quota_exceeded');
        assert.strictEqual(r.body.quota, 'publish_rate'); assert.strictEqual(r.headers.get('retry-after'), '60');
        r = await pub(appToken({ projectId: prjB, env: 'production', cap: ALL, appId: appB }), appEvent(appB, prjB));
        assert.strictEqual(r.status, 201, 'another project has its own quota');
        h2.clock.advance(61 * 1000);
        assert.strictEqual((await pub(tokA2(), appEvent(appA2, prjA))).status, 201, 'the window moves on');

        const big = { blob: 'x'.repeat(1200) };
        assert.strictEqual((await pub(tokA(), appEvent(appA, prjA, 'big.one', { payload: big }))).status, 201);
        assert.strictEqual((await pub(tokA(), appEvent(appA, prjA, 'big.two', { payload: big }))).status, 201);
        r = await pub(tokA(), appEvent(appA, prjA, 'big.three', { payload: big }));
        assert.strictEqual(r.status, 429); assert.strictEqual(r.body.quota, 'retained_bytes');
        r = await pub(tokA(), { events: [appEvent(appA, prjA, 'small.one')] });
        assert.strictEqual(r.status, 201, 'small events still fit');
    } finally {
        await h2.stop();
    }
});

t('revocation from Network stops an app\'s subscriptions and old tokens', async () => {
    const network = serviceToken('network', ['events.event.publish']);
    const future = new Date(Date.now() + 2000).toISOString();
    let r = await publish(network, envelope('network', {
        event_type: 'network.grant.changed', actor: { type: 'system', id: 'network' }, timestamp: future,
        subject: { type: 'app', id: appB }, payload: { project_id: prjB, capability: 'events.app.subscribe', audience: 'openvibe.events', from: 'approved', to: 'revoked' },
    }));
    assert.strictEqual(r.status, 201, r.text);
    const subsB = (await request(h.base, 'GET', '/api/v1/subscriptions', { token: tokB() })).body.subscriptions;
    assert.ok(subsB.length && subsB.every(s => !s.enabled), 'every subscription of the app is disabled');
    r = await request(h.base, 'POST', `/api/v1/subscriptions/${subsB[0].id}/enable`, { token: tokB() });
    assert.strictEqual(r.status, 403, 'a token issued before the revocation cannot re-enable');
    r = await request(h.base, 'POST', '/api/v1/subscriptions', { token: tokB(), body: { topic_pattern: `app.${keyB}.again`, endpoint: 'https://hooks.example.com/again' } });
    assert.strictEqual(r.status, 403, '...nor create one');
    assert.strictEqual((await publish(tokB(), appEvent(appB, prjB))).status, 201, 'publishing is a different grant');
    const later = tokB({ iat: Math.floor(Date.now() / 1000) + 10 });
    r = await request(h.base, 'POST', `/api/v1/subscriptions/${subsB[0].id}/enable`, { token: later });
    assert.strictEqual(r.status, 200, 'a token issued after a re-approval may');

    r = await publish(network, envelope('network', {
        event_type: 'network.app.revoked', actor: { type: 'system', id: 'network' }, timestamp: future,
        subject: { type: 'app', id: appA }, payload: { project_id: prjA, environment: 'sandbox', reason: 'test' },
    }));
    assert.strictEqual(r.status, 201, r.text);
    assert.ok(h.store.listSubscriptions(`app:${appA}`).every(s => !s.enabled));
    r = await pull(tokA(), `app.${keyA}.*`);
    assert.strictEqual(r.status, 401); assert.strictEqual(r.body.code, 'token.revoked');
    r = await publish(tokA(), appEvent(appA, prjA));
    assert.strictEqual(r.status, 401);
    // Only svc:network speaks for Network, and the hook ignores app-published look-alikes.
    assert.strictEqual((await pull(tokA2(), `app.${keyA}.*`)).status, 200, 'other apps of the project are untouched');
});

t('realtime never streams app events, to anyone', async () => {
    const { sse } = require('./helpers');
    const svc = await sse(h.base, '/realtime/stream?topics=app.*,live.*', { headers: { Authorization: `Bearer ${serviceToken('ops', ['events.event.read'])}` } });
    const anon = await sse(h.base, `/realtime/stream?topics=app.${keyA}.*,live.*`);
    await publish(tokA2(), appEvent(appA2, prjA, 'order.realtime', { visibility: 'public' }));
    const marker = await publish(live, envelope('live', { event_type: 'live.stream.started', visibility: 'public' }));
    await svc.waitFor(c => c.events().some(e => e.event.event_id === marker.body.event_id));
    await anon.waitFor(c => c.events().some(e => e.event.event_id === marker.body.event_id));
    assert.ok(!svc.events().some(e => e.event.event_type.startsWith('app.')));
    assert.ok(!anon.events().some(e => e.event.event_type.startsWith('app.')));
    svc.close(); anon.close();
});

t('stop', async () => {
    await h.stop();
    await new Promise(r => receiver.close(r));
    await sleep(10);
});

t.run();
