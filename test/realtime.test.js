'use strict';
const assert = require('assert');
const { ids } = require('openvibe-contracts');
const { boot, request, serviceToken, userToken, envelope, sse, sleep, suite } = require('./helpers');

const t = suite('realtime');
let h;
const live = serviceToken('live', ['events.publish']);
const network = serviceToken('network', ['events.publish']);
const alice = ids.newId('user');
const bob = ids.newId('user');
const open = [];

async function stream(q, headers) {
    const c = await sse(h.base, `/realtime/stream?${q}`, { headers });
    open.push(c);
    return c;
}
async function publish(token, env) {
    const r = await request(h.base, 'POST', '/api/v1/events', { token, body: env });
    assert.strictEqual(r.status, 201, r.text);
    return r.body.seq;
}
const quiet = () => sleep(150);

t('boot', async () => { h = await boot({ env: { REALTIME_HEARTBEAT_MS: '100' } }); });

t('visibility: public to everyone, subject only to that user, internal only to services', async () => {
    const anon = await stream('topics=live.stream.*,network.notification.*');
    const a = await stream('topics=live.stream.*,network.notification.*', { Cookie: `theme=x; ov_token=${userToken({ subjectId: alice })}` });
    const b = await stream('topics=live.stream.*,network.notification.*', { Authorization: `Bearer ${userToken({ subjectId: bob })}` });
    const svc = await stream('topics=live.stream.*,network.notification.*', { Authorization: `Bearer ${serviceToken('live', ['events.read'])}` });
    assert.strictEqual(anon.status, 200);
    assert.match(anon.headers['content-type'], /text\/event-stream/);
    await sleep(50);
    assert.ok(a.comments.includes('connected user'));
    assert.ok(svc.comments.includes('connected service'));

    const pub = await publish(live, envelope('live', { visibility: 'public' }));
    const internal = await publish(live, envelope('live', { visibility: 'internal' }));
    const toAliceAsSubject = await publish(network, envelope('network', {
        event_type: 'network.notification.created', visibility: 'subject', subject: { type: 'user', id: alice }, payload: { text: 'hi alice' },
    }));
    const byAliceAsActor = await publish(live, envelope('live', { visibility: 'subject', actor: { type: 'user', id: alice }, event_type: 'live.stream.followed' }));
    await svc.waitFor(c => c.events().length === 4);
    await quiet();

    const seqs = (c) => c.events().map(e => e.seq);
    assert.deepStrictEqual(seqs(anon), [pub]);
    assert.deepStrictEqual(seqs(a), [pub, toAliceAsSubject, byAliceAsActor]);
    assert.deepStrictEqual(seqs(b), [pub], 'bob sees nothing addressed to alice');
    assert.deepStrictEqual(seqs(svc), [pub, internal, toAliceAsSubject, byAliceAsActor]);
    assert.strictEqual(a.messages.find(m => m.data.includes('hi alice')).id, String(toAliceAsSubject), 'SSE id is the seq');
    for (const c of [anon, a, b, svc]) c.close();
});

t('a guessed private topic yields no data', async () => {
    const guess = await stream('topics=network.notification.*,*', { Authorization: `Bearer ${userToken({ subjectId: bob })}` });
    const anon = await stream('topics=*');
    await publish(network, envelope('network', { event_type: 'network.notification.created', visibility: 'subject', subject: { type: 'user', id: alice } }));
    await publish(network, envelope('network', { event_type: 'network.wallet.credited', visibility: 'internal' }));
    await quiet();
    assert.deepStrictEqual(guess.events(), []);
    assert.deepStrictEqual(anon.events(), []);
    guess.close(); anon.close();
});

t('Last-Event-ID resume replays what was missed, in order, then goes live', async () => {
    const first = await publish(live, envelope('live', { visibility: 'public', event_type: 'live.stream.a' }));
    const missed1 = await publish(live, envelope('live', { visibility: 'public', event_type: 'live.stream.b' }));
    await publish(live, envelope('live', { visibility: 'public', event_type: 'live.chat.not_subscribed' }));
    await publish(live, envelope('live', { visibility: 'internal', event_type: 'live.stream.c' }));
    const missed2 = await publish(live, envelope('live', { visibility: 'public', event_type: 'live.stream.d' }));
    const c = await stream('topics=live.stream.*', { 'Last-Event-ID': String(first) });
    await c.waitFor(x => x.events().length === 2);
    assert.deepStrictEqual(c.events().map(e => e.seq), [missed1, missed2]);
    assert.deepStrictEqual(c.gaps(), []);
    const liveSeq = await publish(live, envelope('live', { visibility: 'public', event_type: 'live.stream.e' }));
    await c.waitFor(x => x.events().length === 3);
    assert.strictEqual(c.events()[2].seq, liveSeq);
    c.close();
    // Query-string form for clients that cannot set headers.
    const q = await stream(`topics=live.stream.*&last_event_id=${missed2}`);
    await q.waitFor(x => x.events().length === 1);
    assert.strictEqual(q.events()[0].seq, liveSeq);
    q.close();
});

t('a cursor older than retention gets `event: gap` first', async () => {
    const old = await publish(live, envelope('live', { visibility: 'public' }));
    h.store.prune({ retentionDays: 30, now: Date.now() + 31 * 86400000 });
    const kept = await publish(live, envelope('live', { visibility: 'public' }));
    const c = await stream('topics=live.stream.*', { 'Last-Event-ID': String(old - 1) });
    await c.waitFor(x => x.events().length === 1);
    const gaps = c.gaps();
    assert.strictEqual(gaps.length, 1);
    assert.strictEqual(gaps[0].reason, 'retention');
    assert.strictEqual(gaps[0].from_seq, old);
    assert.strictEqual(gaps[0].to_seq, kept - 1);
    assert.ok(c.messages.findIndex(m => m.event === 'gap') < c.messages.findIndex(m => !m.event), 'gap comes before events');
    assert.strictEqual(c.events()[0].seq, kept);
    c.close();
    const ahead = await stream('topics=live.stream.*', { 'Last-Event-ID': String(kept + 1000) });
    await ahead.waitFor(x => x.gaps().length === 1);
    assert.strictEqual(ahead.gaps()[0].reason, 'cursor_ahead');
    ahead.close();
});

t('heartbeat comments keep the stream alive', async () => {
    const c = await stream('topics=live.stream.*');
    await c.waitFor(x => x.comments.includes('hb'), 1000);
    c.close();
});

t('limits and auth errors', async () => {
    const topics = Array.from({ length: 21 }, (_, i) => `live.t${i}.*`).join(',');
    let r = await request(h.base, 'GET', `/realtime/stream?topics=${topics}`);
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'realtime.too_many_topics');
    r = await request(h.base, 'GET', '/realtime/stream?topics=Live..x');
    assert.strictEqual(r.status, 400);
    r = await request(h.base, 'GET', '/realtime/stream');
    assert.strictEqual(r.status, 400);
    r = await request(h.base, 'GET', '/realtime/stream?topics=live.*', { headers: { Authorization: 'Bearer not.a.token' } });
    assert.strictEqual(r.status, 401, 'a bad Bearer is refused, not downgraded');
    r = await request(h.base, 'GET', '/realtime/stream?topics=live.*', { token: serviceToken('live', ['events.publish']) });
    assert.strictEqual(r.status, 403, 'service tokens need events.read');
    const expired = await stream('topics=live.*', { Cookie: `ov_token=${userToken({ exp: Math.floor(Date.now() / 1000) - 3600 })}` });
    await sleep(30);
    assert.ok(expired.comments.includes('connected anonymous'), 'an expired cookie degrades to anonymous');
    expired.close();
});

t('CORS: credentials only for https://*.openvibe.* origins', async () => {
    let r = await fetch(`${h.base}/realtime/stream?topics=live.*`, { method: 'OPTIONS', headers: { Origin: 'https://openvibe.live', 'Access-Control-Request-Method': 'GET' } });
    assert.strictEqual(r.status, 204);
    assert.strictEqual(r.headers.get('access-control-allow-origin'), 'https://openvibe.live');
    assert.strictEqual(r.headers.get('access-control-allow-credentials'), 'true');
    for (const origin of ['https://evil.example', 'http://openvibe.live', 'https://openvibe.live.evil.example']) {
        r = await fetch(`${h.base}/realtime/stream?topics=live.*`, { method: 'OPTIONS', headers: { Origin: origin } });
        assert.strictEqual(r.headers.get('access-control-allow-origin'), null, origin);
    }
    const c = await stream('topics=live.*', { Origin: 'https://live.openvibe.network' });
    assert.strictEqual(c.headers['access-control-allow-origin'], 'https://live.openvibe.network');
    c.close();
});

t('global connection cap', async () => {
    const capped = await boot({ env: { REALTIME_MAX_CONNECTIONS: '2' } });
    const c1 = await sse(capped.base, '/realtime/stream?topics=live.*');
    const c2 = await sse(capped.base, '/realtime/stream?topics=live.*');
    const r = await request(capped.base, 'GET', '/realtime/stream?topics=live.*');
    assert.strictEqual(r.status, 503);
    assert.strictEqual(r.body.code, 'realtime.over_capacity');
    c1.close();
    await sleep(50);
    const c3 = await sse(capped.base, '/realtime/stream?topics=live.*');
    assert.strictEqual(c3.status, 200, 'a closed connection frees its slot');
    c2.close(); c3.close();
    await capped.stop();
});

t('stop', async () => { for (const c of open) c.close(); await h.stop(); });

t.run();
