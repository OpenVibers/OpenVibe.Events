'use strict';
// Developer apps must not be able to hurt first-party event flow: the loop guard's hop count is
// kept per tenancy (an app cannot poison a first-party trace), and app deliveries cannot hold every
// delivery slot (bounded time per attempt, a capped share of the in-flight slots).
const assert = require('assert');
const net = require('net');
const nodeHttp = require('http');
const { ids } = require('openvibe-contracts');
const apps = require('../server/apps');
const { createGuardedPost, isPublicAddress } = require('../server/egress');
const { boot, request, serviceToken, appToken, envelope, subscriber, suite, sleep } = require('./helpers');

const t = suite('apps-abuse');
const ALL = ['events.app.publish', 'events.app.read', 'events.app.subscribe'];
const prj = `prj_${ids.ulid()}`;
const app = ids.newId('app');
const key = apps.projectKey(prj);
const tok = () => appToken({ appId: app, projectId: prj, env: 'sandbox', cap: ALL });
const appEvent = (over = {}) => envelope('x', {
    source: apps.appSource(app), event_type: `app.${key}.poke`, actor: { type: 'app', id: app }, subject: { type: 'thing', id: '1' }, ...over,
});

t('loop guard: an app cannot poison a first-party trace it has seen', async () => {
    const h = await boot({ env: { EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE: '0' } });
    try {
        const live = serviceToken('live', ['events.event.publish']);
        const media = serviceToken('media', ['events.event.publish']);
        const trace = 'ab'.repeat(16);
        // A public first-party event: every app can read it, trace_id included.
        let r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { trace_id: trace, visibility: 'public' }) });
        assert.strictEqual(r.status, 201, r.text);
        // The app republishes into that trace until its own hop count reaches the maximum (8).
        for (let i = 0; i < 8; i++) {
            r = await request(h.base, 'POST', '/api/v1/events', { token: tok(), body: appEvent({ trace_id: trace }) });
            assert.strictEqual(r.status, 201, r.text);
        }
        // A first-party consumer reacting to the live event in the same trace must still publish.
        r = await request(h.base, 'POST', '/api/v1/events', {
            token: media, body: envelope('media', { event_type: 'media.recording.started', trace_id: trace }),
        });
        assert.strictEqual(r.status, 201, `first-party publish refused: ${r.text}`);
        // And a first-party loop is still caught.
        let last;
        for (let i = 0; i < 9; i++) {
            last = await request(h.base, 'POST', '/api/v1/events', {
                token: media, body: envelope('media', { event_type: 'media.recording.started', trace_id: trace }),
            });
        }
        assert.strictEqual(last.status, 409); assert.strictEqual(last.body.code, 'events.loop_detected');
    } finally {
        await h.stop();
    }
});

// ── Slow endpoints ──
function fakeLookup(host, opts, cb) {
    if (typeof opts === 'function') { cb = opts; opts = {}; }
    return opts.all ? cb(null, [{ address: '127.0.0.1', family: 4 }]) : cb(null, '127.0.0.1', 4);
}

/** A raw TCP server that answers slowly: 'headers' drips header lines, 'body' drips the body. */
async function dripServer(mode) {
    const sockets = new Set();
    const server = net.createServer((sock) => {
        sockets.add(sock);
        sock.on('error', () => {});
        sock.once('data', () => {
            if (mode === 'headers') sock.write('HTTP/1.1 200 OK\r\n');
            else sock.write('HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n');
            const timer = setInterval(() => { if (!sock.destroyed) sock.write(mode === 'headers' ? 'X-Drip: 1\r\n' : 'x'); }, 50);
            sock.on('close', () => { clearInterval(timer); sockets.delete(sock); });
        });
    });
    await new Promise(r => server.listen(0, '127.0.0.1', r));
    return {
        port: server.address().port,
        close: () => new Promise(r => { for (const s of sockets) s.destroy(); server.close(() => r()); }),
    };
}

t('egress: a slow endpoint cannot hold a delivery past its timeout', async () => {
    for (const mode of ['headers', 'body']) {
        const srv = await dripServer(mode);
        try {
            const post = createGuardedPost({
                lookup: fakeLookup,
                requestImpl: (o, cb) => nodeHttp.request({ ...o, port: srv.port }, cb),
                isAllowed: (ip) => ip === '127.0.0.1' || isPublicAddress(ip),
            });
            const started = Date.now();
            const outcome = await Promise.race([
                post('https://hooks.example.com/x', { headers: {}, body: '{}', timeoutMs: 300 }).then(v => ({ v }), e => ({ e })),
                sleep(2000).then(() => ({ hung: true })),
            ]);
            assert.ok(!outcome.hung, `${mode}: the delivery was still in flight after 2 s (timeout 300 ms)`);
            assert.ok(Date.now() - started < 1500, mode);
        } finally {
            await srv.close();
        }
    }
});

t('worker: app deliveries never take every in-flight slot', async () => {
    const pending = [];
    const appPost = () => new Promise((resolve) => pending.push(resolve));   // an app endpoint that never answers
    const h = await boot({
        appPost, dnsLookup: (host, opts, cb) => cb(null, [{ address: '93.184.216.34', family: 4 }]),
        env: { EVENTS_MAX_INFLIGHT: '2', EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE: '0', EVENTS_APP_SANDBOX_MAX_SUBSCRIPTIONS: '0' },
    });
    const firstParty = await subscriber();
    try {
        for (const p of ['a', 'b', 'c']) {
            const r = await request(h.base, 'POST', '/api/v1/subscriptions', { token: tok(), body: { topic_pattern: `app.${key}.*`, endpoint: `https://hooks.example.com/${p}` } });
            assert.strictEqual(r.status, 201, r.text);
        }
        const s = await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken('chat', ['events.subscription.manage']), body: { topic_pattern: 'live.*', endpoint: firstParty.url } });
        assert.strictEqual(s.status, 201, s.text);
        // Critical app events are queued before the first-party one.
        for (let i = 0; i < 3; i++) {
            const r = await request(h.base, 'POST', '/api/v1/events', { token: tok(), body: appEvent({ priority: 'critical' }) });
            assert.strictEqual(r.status, 201, r.text);
        }
        const r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.event.publish']), body: envelope('live') });
        assert.strictEqual(r.status, 201);
        h.worker.dispatch();
        for (let i = 0; i < 40 && !firstParty.calls.length; i++) await sleep(25);
        assert.strictEqual(firstParty.calls.length, 1, 'the first-party delivery got a slot');
        assert.ok(pending.length >= 1, 'app deliveries still run');
    } finally {
        for (const resolve of pending) resolve({ status: 204 });
        await firstParty.close();
        await h.stop();
    }
});

t.run();
