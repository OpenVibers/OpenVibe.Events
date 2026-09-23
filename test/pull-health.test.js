'use strict';
const assert = require('assert');
const crypto = require('crypto');
const nodeHttp = require('http');
const topics = require('../server/topics');
const { hasCap, allows } = require('../server/auth');
const { load } = require('../server/config');
const { boot, request, serviceToken, envelope, publicKey, suite, sleep } = require('./helpers');

const t = suite('pull-health');
let h;
const live = serviceToken('live', ['events.event.publish']);
const reader = serviceToken('games', ['events.event.read']);

t('topic patterns: * spans one or more segments', () => {
    const yes = [['media.vod.*', 'media.vod.ready'], ['media.vod.*', 'media.vod.clip.cut'], ['*.created', 'community.post.created'],
        ['media.*.ready', 'media.vod.ready'], ['media.*.ready', 'media.a.b.ready'], ['*', 'live.stream.started'], ['live.stream.started', 'live.stream.started']];
    const no = [['media.vod.*', 'media.vod'], ['media.vod.*', 'media.vodx.ready'], ['*.created', 'community.post.created_at'],
        ['live.stream.started', 'live.stream.started.x'], ['media.*.ready', 'media.ready']];
    for (const [p, e] of yes) assert.ok(topics.matches(p, e), `${p} ~ ${e}`);
    for (const [p, e] of no) assert.ok(!topics.matches(p, e), `${p} !~ ${e}`);
    for (const bad of ['', 'Media.x', 'a..b', 'a.*.*', 'a.b*', 'a/b', 'x'.repeat(201)]) assert.ok(!topics.isValidPattern(bad), bad);
});

t('capability grants: exact or family wildcard; contracts decide ids they know', () => {
    assert.ok(hasCap({ cap: ['events.event.publish'] }, 'events.event.publish'));
    assert.ok(hasCap({ cap: ['events.*'] }, 'events.delivery.admin'));
    assert.ok(!hasCap({ cap: ['events.pub*'] }, 'events.event.publish'));
    assert.ok(!hasCap({ cap: ['event.*'] }, 'events.event.publish'));
    assert.ok(!hasCap({}, 'events.event.read'));
    assert.strictEqual(allows({ cap: ['events.event.read'] }, 'events.event.read').allowed, true);
    assert.strictEqual(allows({ cap: ['media.object.upload'] }, 'media.object.upload').allowed, true, 'known ids go through contracts');
});

t('config: every source prefix must start with the source', () => {
    assert.throws(() => load({ EVENTS_SOURCE_PREFIXES: JSON.stringify({ live: ['media.'] }) }), /must start with "live\."/);
    const c = load({ EVENTS_SOURCE_PREFIXES: JSON.stringify({ live: ['live.stream.'] }) });
    assert.deepStrictEqual(c.sourcePrefixes, { live: ['live.stream.'] });
    assert.strictEqual(load({}).port, 4300);
    assert.strictEqual(load({}).retentionDays, 30);
});

t('boot', async () => { h = await boot(); });

t('pull: cursor, topic filter, internal events included for services', async () => {
    const s1 = (await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.vod.ready' }) })).body.seq;
    await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.chat.sent' }) });
    const s3 = (await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.vod.deleted' }) })).body.seq;

    let r = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*&after_seq=0&limit=1', { token: reader });
    assert.strictEqual(r.status, 200, r.text);
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [s1]);
    assert.strictEqual(r.body.events[0].event.visibility, 'internal');
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after_seq=${r.body.next_after_seq}`, { token: reader });
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [s3]);
    assert.strictEqual(r.body.next_after_seq, s3);
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after_seq=${r.body.next_after_seq}`, { token: reader });
    assert.deepStrictEqual(r.body.events, []);
    assert.strictEqual(r.body.latest_seq, s3);

    const id3 = h.db.prepare('SELECT id FROM events WHERE seq = ?').get(s3).id;
    r = await request(h.base, 'GET', `/api/v1/events/${id3}`, { token: reader });
    assert.strictEqual(r.body.seq, s3);
    r = await request(h.base, 'GET', '/api/v1/events?topic=live.*', { token: live });
    assert.strictEqual(r.status, 403, 'events.event.read is required');
    r = await request(h.base, 'GET', '/api/v1/events?limit=5000', { token: reader });
    assert.strictEqual(r.status, 400);
});

t('pull: a cursor older than retention reports the gap', async () => {
    const before = h.store.lastSeq();
    h.store.prune({ retentionDays: 30, now: Date.now() + 31 * 86400000 });
    const s = (await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope() })).body.seq;
    const r = await request(h.base, 'GET', '/api/v1/events?after_seq=1', { token: reader });
    assert.deepStrictEqual(r.body.gap, { from_seq: 2, to_seq: before });
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [s]);
});

t('checkpoints: per consumer and topic', async () => {
    let r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: reader });
    assert.strictEqual(r.body.cursor, 0);
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: 42 } });
    assert.strictEqual(r.body.cursor, 42);
    r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: reader });
    assert.strictEqual(r.body.cursor, 42);
    assert.strictEqual(r.body.consumer, 'games');
    r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: serviceToken('tools', ['events.event.read']) });
    assert.strictEqual(r.body.cursor, 0, 'another consumer has its own');
});

t('health and ready', async () => {
    let r = await request(h.base, 'GET', '/api/health');
    assert.deepStrictEqual([r.status, r.body.status, r.body.service], [200, 'ok', 'openvibe-events']);
    r = await request(h.base, 'GET', '/api/ready');
    assert.strictEqual(r.status, 200);
    assert.deepStrictEqual(r.body.checks, { db: true, worker: true, key: true });
    r = await request(h.base, 'GET', '/nope');
    assert.strictEqual(r.status, 404);
    assert.strictEqual(r.body.code, 'events.not_found');
    assert.ok(r.headers.get('x-openvibe-request-id'));
});

t('stop', async () => { await h.stop(); });

t('key loader: JWKS {keys:[jwk]} and legacy {public_key}; ready is 503 until it loads', async () => {
    const jwk = crypto.createPublicKey(publicKey).export({ format: 'jwk' });
    let shape = 'none';
    const net = nodeHttp.createServer((req, res) => {
        if (req.url !== '/api/.well-known/jwks' || shape === 'none') { res.statusCode = 503; return res.end(); }
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(shape === 'jwks' ? { keys: [{ ...jwk, alg: 'RS256', kid: 'k1' }] } : { public_key: publicKey, algorithm: 'RS256' }));
    });
    await new Promise(r => net.listen(0, '127.0.0.1', r));
    const url = `http://127.0.0.1:${net.address().port}`;
    for (const s of ['jwks', 'pem']) {
        shape = 'none';
        const x = await boot({ env: { OV_NETWORK_PUBLIC_KEY: '', OV_NETWORK_INTERNAL_URL: url, OV_NETWORK_URL: url, OV_NETWORK_ISSUER: 'https://openvibe.network' }, worker: 'on' });
        await x.keyLoaded;
        let r = await request(x.base, 'GET', '/api/ready');
        assert.strictEqual(r.status, 503);
        assert.strictEqual(r.body.checks.key, false);
        r = await request(x.base, 'POST', '/api/v1/events', { token: live, body: envelope() });
        assert.strictEqual(r.status, 503, 'no key yet: service unavailable, not unauthorized');
        shape = s;
        await x.keys.fetchOnce();
        r = await request(x.base, 'GET', '/api/ready');
        assert.strictEqual(r.status, 200, s);
        r = await request(x.base, 'POST', '/api/v1/events', { token: live, body: envelope() });
        assert.strictEqual(r.status, 201, s);
        await x.stop();
    }
    net.close();
    await sleep(10);
});

t.run();
