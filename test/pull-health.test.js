'use strict';
const assert = require('assert');
const crypto = require('crypto');
const nodeHttp = require('http');
const cursor = require('../server/cursor');
const topics = require('../server/topics');
const { hasCap, allows } = require('../server/auth');
const { load } = require('../server/config');
const { validate } = require('openvibe-contracts');
const { boot, request, serviceToken, envelope, publicKey, suite, sleep, cursorAt } = require('./helpers');

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

t('config: network-wide prefixes are shared, source namespaces stay owned', () => {
    assert.deepStrictEqual(load({}).sharedPrefixes, ['provider.']);
    const c = load({ EVENTS_SHARED_PREFIXES: 'provider.,storage.' });
    assert.deepStrictEqual(c.sharedPrefixes, ['provider.', 'storage.']);
    assert.throws(() => load({ EVENTS_SHARED_PREFIXES: 'provider' }), /bad prefix/);
    assert.throws(() => load({ EVENTS_SHARED_PREFIXES: 'media.' }), /belongs to source "media"/);
    assert.throws(() => load({ EVENTS_SHARED_PREFIXES: 'app.' }), /reserved for developer apps/);
});

t('boot', async () => { h = await boot(); });

t('pull: cursor, topic filter, internal events included for services', async () => {
    const s1 = (await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.vod.ready' }) })).body.seq;
    await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.chat.sent' }) });
    const s3 = (await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.vod.deleted' }) })).body.seq;

    let r = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*&limit=1', { token: reader });
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(validate('events.read-result@1', r.body).valid, JSON.stringify(validate('events.read-result@1', r.body).errors));
    for (const e of r.body.events) assert.deepStrictEqual(cursor.decode(e.cursor), { seq: e.seq, epoch: await h.store.epoch() });
    assert.strictEqual(r.body.next_cursor, await cursorAt(h, s1));
    assert.ok(!('next_after_seq' in r.body) && !('latest_seq' in r.body));
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [s1]);
    assert.strictEqual(r.body.events[0].event.visibility, 'internal');
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after=${r.body.next_cursor}`, { token: reader });
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [s3]);
    assert.strictEqual(r.body.next_cursor, await cursorAt(h, s3));
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after=${r.body.next_cursor}`, { token: reader });
    assert.deepStrictEqual(r.body.events, []);
    assert.strictEqual(r.body.latest_cursor, await cursorAt(h, s3));
    assert.match(r.body.latest_cursor, /^c1\./, 'the head as an opaque cursor');
    {
        // Starting at the head with latest_cursor reads nothing old, and the next event after it arrives.
        const head = r.body.latest_cursor;
        const fromHead = await request(h.base, "GET", `/api/v1/events?topic=live.vod.*&after=${encodeURIComponent(head)}`, { token: reader });
        assert.strictEqual(fromHead.status, 200);
        assert.deepStrictEqual(fromHead.body.events, []);
        assert.strictEqual(fromHead.body.next_cursor, head);
    }

    const id3 = (await h.db.prepare('SELECT id FROM events WHERE seq = ?').get(s3)).id;
    r = await request(h.base, 'GET', `/api/v1/events/${id3}`, { token: reader });
    assert.strictEqual(r.body.seq, s3);
    r = await request(h.base, 'GET', '/api/v1/events?topic=live.*', { token: live });
    assert.strictEqual(r.status, 403, 'events.event.read is required');
    r = await request(h.base, 'GET', '/api/v1/events?limit=5000', { token: reader });
    assert.strictEqual(r.status, 400);
});

t('pull: a cursor older than retention reports the gap', async () => {
    const before = await h.store.lastSeq();
    await h.store.prune({ retentionDays: 30, now: Date.now() + 31 * 86400000 });
    const s = (await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope() })).body.seq;
    const r = await request(h.base, 'GET', `/api/v1/events?after=${await cursorAt(h, 1)}`, { token: reader });
    assert.deepStrictEqual(r.body.gap, { from_seq: 2, to_seq: before });
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [s]);
});

t('pull: after_seq is refused even at zero', async () => {
    const r = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*&after_seq=0', { token: reader });
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'events.bad_request');
});

t('checkpoints: per consumer and topic', async () => {
    let r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: reader });
    assert.strictEqual(r.body.cursor, null);
    assert.strictEqual(r.body.carrier, null);
    assert.strictEqual(r.body.updated_at, null);
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: 42 } });
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'events.bad_request');
    const position = await cursorAt(h, 42);
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: position } });
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.cursor, position);
    assert.ok(validate('events.read-result@1', r.body).valid, 'the checkpoint answer is a contract checkpoint');
    r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: reader });
    assert.strictEqual(r.body.cursor, position);
    assert.strictEqual(r.body.consumer, 'games');
    r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: serviceToken('tools', ['events.event.read']) });
    assert.strictEqual(r.body.cursor, null, 'another consumer has its own');
});

t('health and ready', async () => {
    let r = await request(h.base, 'GET', '/api/health');
    assert.deepStrictEqual([r.status, r.body.status, r.body.service], [200, 'ok', 'openvibe-events']);
    r = await request(h.base, 'GET', '/api/ready');
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.ready, true);
    assert.deepStrictEqual(Object.keys(r.body.checks), ['db', 'valkey', 'network_jwks', 'dlq'], 'worker off in this boot: no delivery_worker check');
    assert.strictEqual(r.body.checks.db.status, 'ok');
    assert.strictEqual(r.body.checks.network_jwks.status, 'ok');
    r = await request(h.base, 'GET', '/nope');
    assert.strictEqual(r.status, 404);
    assert.strictEqual(r.body.code, 'events.not_found');
    assert.ok(r.headers.get('x-openvibe-request-id'));
});

t('limits.json: the developer limits, read from config (WS-N task 7)', async () => {
    const r = await request(h.base, 'GET', '/limits.json');
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.headers.get('access-control-allow-origin'), '*');
    const by = Object.fromEntries(r.body.limits.map((l) => [l.id, l]));
    assert.deepStrictEqual([by.publish_per_minute.production, by.publish_per_minute.sandbox], [120, 30]);
    assert.deepStrictEqual([by.retention_days.production, by.retention_days.sandbox], [30, 7]);
    assert.strictEqual(by.subscriptions.capability, 'events.app.subscribe');
    for (const l of r.body.limits) assert.ok(Number.isInteger(l.production) && Number.isInteger(l.sandbox) && l.exceeded, l.id);
    // The per-actor rate limits the routes declared (WS-R task 4), each with its numbers.
    assert.ok(Array.isArray(r.body.rate_limits) && r.body.rate_limits.length > 3, JSON.stringify(r.body.rate_limits));
    for (const l of r.body.rate_limits) assert.ok(/^events\./.test(l.id) && l.minute > 0 && l.hour >= l.minute && l.exceeded === '429 rate_limited', JSON.stringify(l));
    const { limitsOf } = require('../server/limits');
    assert.strictEqual(limitsOf(load({ NODE_ENV: 'test', EVENTS_APP_MAX_SUBSCRIPTIONS: '0', OV_NETWORK_PUBLIC_KEY: publicKey })).limits.find((l) => l.id === 'subscriptions').production, null, '0 (off) is null');
    const custom = limitsOf(load({ NODE_ENV: 'test', EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE: '7', OV_NETWORK_PUBLIC_KEY: publicKey }));
    assert.strictEqual(custom.limits.find((l) => l.id === 'publish_per_minute').sandbox, 7, 'an env override is what the page shows');
});

t('stop', async () => { await h.stop(); });

t('key loader: JWKS {keys:[jwk]} and legacy {public_key}; ready is 503 until it loads', async () => {
    const jwk = crypto.createPublicKey(publicKey).export({ format: 'jwk' });
    let shape = 'none';
    // One Network per shape: openvibe-sdk/auth keeps one JWKS client per URL for the process, so a second boot on
    // the same URL would start with the first one's keys (as a restarted verifier in one process should).
    for (const s of ['jwks', 'pem']) {
        const net = nodeHttp.createServer((req, res) => {
            if (req.url !== '/api/.well-known/jwks' || shape === 'none') { res.statusCode = 503; return res.end(); }
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(shape === 'jwks' ? { keys: [{ ...jwk, alg: 'RS256', kid: 'k1' }] } : { public_key: publicKey, algorithm: 'RS256' }));
        });
        await new Promise(r => net.listen(0, '127.0.0.1', r));
        const url = `http://127.0.0.1:${net.address().port}`;
        shape = 'none';
        const x = await boot({ env: { OV_NETWORK_PUBLIC_KEY: '', OV_NETWORK_INTERNAL_URL: url, OV_NETWORK_URL: url, OV_NETWORK_ISSUER: 'https://openvibe.network' }, worker: 'on' });
        await x.keyLoaded;
        let r = await request(x.base, 'GET', '/api/ready');
        assert.strictEqual(r.status, 503);
        assert.strictEqual(r.body.checks.network_jwks.status, 'fail');
        assert.deepStrictEqual(r.body.failed, ['network_jwks']);
        r = await request(x.base, 'POST', '/api/v1/events', { token: live, body: envelope() });
        assert.strictEqual(r.status, 503, 'no key yet: service unavailable, not unauthorized');
        shape = s;
        await x.keys.refresh();
        r = await request(x.base, 'GET', '/api/ready');
        assert.strictEqual(r.status, 200, s);
        r = await request(x.base, 'POST', '/api/v1/events', { token: live, body: envelope() });
        assert.strictEqual(r.status, 201, s);
        await x.stop();
        net.close();
    }
    await sleep(10);
});

t('key rotation: a token naming a new kid verifies after one refetch; an unpublished key never does', async () => {
    const pair = () => crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
    const k1 = pair(); const k2 = pair(); const rogue = pair();
    const jwkOf = (k, kid) => ({ ...k.publicKey.export({ format: 'jwk' }), alg: 'RS256', use: 'sig', kid });
    // Network's tokens carry the kid of the key that signed them.
    const sign = (k, kid) => {
        const now = Math.floor(Date.now() / 1000);
        const enc = (o) => Buffer.from(JSON.stringify(o)).toString('base64url');
        const input = `${enc({ alg: 'RS256', typ: 'JWT', kid })}.${enc({
            iss: 'https://openvibe.network', sub: 'svc:live', actor_type: 'service', aud: ['openvibe.events'], cap: ['events.event.publish'], ns: [],
            iat: now, exp: now + 300, jti: `tok_${crypto.randomBytes(8).toString('hex')}`,
        })}`;
        return `${input}.${crypto.sign('RSA-SHA256', Buffer.from(input), k.privateKey).toString('base64url')}`;
    };
    let published = [jwkOf(k1, 'k1')];
    let fetches = 0;
    const net = nodeHttp.createServer((req, res) => {
        if (req.url !== '/api/.well-known/jwks') { res.statusCode = 404; return res.end(); }
        fetches++;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ keys: published }));
    });
    await new Promise(r => net.listen(0, '127.0.0.1', r));
    const url = `http://127.0.0.1:${net.address().port}`;
    const x = await boot({ env: { OV_NETWORK_PUBLIC_KEY: '', OV_NETWORK_INTERNAL_URL: url, OV_NETWORK_URL: url, OV_NETWORK_ISSUER: 'https://openvibe.network' } });
    await x.keyLoaded;
    try {
        let r = await request(x.base, 'POST', '/api/v1/events', { token: sign(k1, 'k1'), body: envelope() });
        assert.strictEqual(r.status, 201, JSON.stringify(r.body));
        const before = fetches;
        published = [jwkOf(k2, 'k2'), jwkOf(k1, 'k1')];
        r = await request(x.base, 'POST', '/api/v1/events', { token: sign(k2, 'k2'), body: envelope() });
        assert.strictEqual(r.status, 201, `the rotated key verifies: ${JSON.stringify(r.body)}`);
        assert.strictEqual(fetches, before + 1, 'one refetch for the unknown kid');
        r = await request(x.base, 'POST', '/api/v1/events', { token: sign(k1, 'k1'), body: envelope() });
        assert.strictEqual(r.status, 201, 'the old key still verifies while Network publishes it');
        r = await request(x.base, 'POST', '/api/v1/events', { token: sign(rogue, 'k9'), body: envelope() });
        assert.strictEqual(r.status, 401, 'a key Network never published');
        assert.strictEqual(r.body.code, 'token.bad_signature');
        r = await request(x.base, 'POST', '/api/v1/events', { token: sign(rogue, 'k1'), body: envelope() });
        assert.strictEqual(r.status, 401, 'a published kid with another key behind it');
    } finally {
        await x.stop();
        net.close();
    }
    await sleep(10);
});

t.run();
