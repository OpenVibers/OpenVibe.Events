'use strict';
// Private bypass (roadmap WS-R task 5). With the seeded world of test/security-world.js (public,
// internal and per-user subject first-party events; app events of project PA in sandbox and in
// production and of project PB in sandbox, each with a distinct marker), every read path is tried by
// every caller and the markers it gets back are compared with what it may see:
//   - pull reads (GET /api/v1/events with every topic pattern, GET /api/v1/events/:id for every id):
//     first-party readers see first-party production events and app events only in production and only
//     through app.* patterns; an app sees its own project in its own environment plus public first-party
//     events, never another project, never the other environment, never internal or subject events;
//     people and anonymous callers pull nothing
//   - realtime (SSE, replayed from the start): anonymous and people see public events, a person also
//     their own subject events (ticket, cookie or Bearer), services everything first-party, nobody any
//     app event; an app token is never a service viewer, whatever capabilities it carries
//   - DLQ views and replay are the operator's; replay never widens a subscription's scope
//   - subscription listings, single reads and checkpoints are the caller's own
//   node test/security-private.test.js
const assert = require('assert');
const { suite, appToken, request } = require('./helpers');
const { buildWorld } = require('./security-world');
const crawler = require('./security-crawl');

const t = suite('security-private');
let w;
const seenIn = (text) => Object.entries(w.markers).filter(([, m]) => text.includes(m)).map(([k]) => k).sort();

t('world', async () => { w = await buildWorld(); });

t('pull reads: each caller gets exactly its scope, by every topic pattern and every id', async () => {
    const { tok, ids, ev } = w;
    const FIRST_PARTY = ['appA2', 'internal', 'public', 'subjectX', 'subjectY'];
    const expected = {
        live: FIRST_PARTY, reader: FIRST_PARTY, ops: [], userX: [], anonymous: [],
        appA: ['appA', 'public'], appA2: ['appA2', 'public'], appB: ['appB', 'public'],
    };
    const topicsList = ['*', 'live.*', 'network.*', 'network.notification.*', 'app.*', `app.${ids.keyA}.*`, `app.${ids.keyB}.*`, `app.${ids.keyA}.*,app.${ids.keyB}.*`, '*.*.*', 'live.*,network.*,app.*'];
    const bad = [];
    for (const [who, want] of Object.entries(expected)) {
        const token = who === 'anonymous' ? undefined : tok[who] || tok.userX;
        let got = [];
        for (const topic of topicsList) {
            const r = await request(w.base, 'GET', `/api/v1/events?topic=${encodeURIComponent(topic)}&after_seq=0&limit=1000`, { token });
            got = got.concat(seenIn(r.text));
        }
        for (const [name, id] of Object.entries(ev)) {
            const r = await request(w.base, 'GET', `/api/v1/events/${id}`, { token });
            const s = seenIn(r.text);
            if (s.length && !want.includes(name)) bad.push(`${who} reads ${name} by id (${r.status})`);
            if (!s.length && want.includes(name) && r.status !== 200) bad.push(`${who} cannot read its own ${name} by id (${r.status})`);
            got = got.concat(s);
        }
        const gotSet = [...new Set(got)].sort();
        if (JSON.stringify(gotSet) !== JSON.stringify([...want].sort())) bad.push(`${who}: sees ${gotSet.join(',') || 'nothing'}; may see ${want.join(',') || 'nothing'}`);
    }
    assert.deepStrictEqual(bad, [], bad.join('\n'));
});

t('realtime: visibility by viewer, replayed from the start, app events never streamed', async () => {
    const { tok, ids } = w;
    const topics = encodeURIComponent('live.*,network.notification.*,app.*');
    const open = async (headers = {}, extra = '') => crawler.hit(w.base, 'GET', `/realtime/stream?topics=${topics}&last_event_id=0${extra}`, { headers, streamMs: 400 });
    const cases = [
        ['anonymous', {}, '', ['public']],
        ['x by ticket', {}, `&ticket=${w.realtimeTicket({ subjectId: ids.x })}`, ['public', 'subjectX']],
        ['x by cookie', { cookie: `ov_token=${tok.userX}` }, '', ['public', 'subjectX']],
        ['y by Bearer', { authorization: `Bearer ${tok.userY}` }, '', ['public', 'subjectY']],
        ['service', { authorization: `Bearer ${tok.live}` }, '', ['internal', 'public', 'subjectX', 'subjectY']],
    ];
    const bad = [];
    for (const [who, headers, extra, want] of cases) {
        const r = await open(headers, extra);
        if (r.status !== 200) { bad.push(`${who}: ${r.status}`); continue; }
        const got = seenIn(r.text);
        if (JSON.stringify(got) !== JSON.stringify(want)) bad.push(`${who}: sees ${got.join(',') || 'nothing'}; may see ${want.join(',')}`);
    }
    // App tokens are never service viewers: not with the app's own capabilities, not in production,
    // not even when a first-party read capability is in the token.
    for (const [who, token] of [
        ['sandbox app', tok.appA],
        ['production app', tok.appA2],
        ['production app carrying events.event.read', appToken({ appId: ids.A2, projectId: ids.PA, env: 'production', cap: ['events.event.read', 'events.app.read'] })],
        ['sandbox app carrying events.event.read', appToken({ appId: ids.A, projectId: ids.PA, env: 'sandbox', cap: ['events.event.read'] })],
    ]) {
        const r = await open({ authorization: `Bearer ${token}` });
        const got = seenIn(r.text);
        if (got.some((k) => k !== 'public')) bad.push(`${who}: ${r.status}, sees ${got.join(',')}`);
    }
    assert.deepStrictEqual(bad, [], bad.join('\n'));
});

t('DLQ views and replay are the operator\'s, and replay never widens a subscription', async () => {
    const { tok, subs, ev } = w;
    for (const who of ['live', 'reader', 'appA', 'appA2', 'appB', 'userX']) {
        const r = await request(w.base, 'GET', '/api/v1/deliveries?limit=1000', { token: tok[who] });
        assert.ok(r.status === 401 || r.status === 403, `${who} reads no deliveries (${r.status})`);
        const rp = await request(w.base, 'POST', '/api/v1/deliveries/replay', { token: tok[who], body: { subscription_id: subs.appB.id, from_seq: 0 } });
        assert.ok(rp.status === 401 || rp.status === 403, `${who} replays nothing (${rp.status})`);
    }
    assert.strictEqual((await request(w.base, 'GET', '/api/v1/deliveries?limit=1000')).status, 401);
    const ops = await request(w.base, 'GET', '/api/v1/deliveries?limit=1000', { token: tok.ops });
    assert.strictEqual(ops.status, 200, 'the operator reads them (positive control)');
    assert.deepStrictEqual(seenIn(ops.text), [], 'the DLQ view carries delivery state, not event payloads');
    // Replay every event into B's subscription, by id and from the start: only B's own go out.
    w.appSeen.length = 0;
    let r = await request(w.base, 'POST', '/api/v1/deliveries/replay', { token: tok.ops, body: { subscription_id: subs.appB.id, event_ids: Object.values(ev) } });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.queued, 1, 'only B\'s own event is requeued');
    r = await request(w.base, 'POST', '/api/v1/deliveries/replay', { token: tok.ops, body: { subscription_id: subs.appA.id, from_seq: 0 } });
    assert.strictEqual(r.status, 200); assert.strictEqual(r.body.queued, 1, 'a sandbox subscription gets its own sandbox event only');
    r = await request(w.base, 'POST', '/api/v1/deliveries/replay', { token: tok.ops, body: { subscription_id: subs.live.id, event_ids: Object.values(ev) } });
    assert.strictEqual(r.body.queued, 2, 'a first-party subscription gets its own topic, never app or sandbox events');
    await w.h.worker.drain();
    const byPath = {};
    for (const s of w.appSeen) byPath[s.url] = (byPath[s.url] || []).concat(seenIn(s.rawBody));
    assert.deepStrictEqual(byPath, { '/a': ['appA'], '/b': ['appB'] });
});

t('subscriptions and checkpoints are the caller\'s own', async () => {
    const { tok, subs, ids } = w;
    const own = { live: [subs.live.id], media: [subs.media.id], appA: [subs.appA.id], appA2: [subs.appA2.id], appB: [subs.appB.id] };
    const bad = [];
    for (const [who, mine] of Object.entries(own)) {
        const r = await request(w.base, 'GET', '/api/v1/subscriptions', { token: tok[who] });
        const listed = (r.body && r.body.subscriptions || []).map((s) => s.id).sort();
        if (JSON.stringify(listed) !== JSON.stringify(mine)) bad.push(`${who} lists ${listed.join(',')}`);
        for (const id of Object.values(subs).map((s) => s.id)) {
            const one = await request(w.base, 'GET', `/api/v1/subscriptions/${id}`, { token: tok[who] });
            if ((one.status === 200) !== mine.includes(id)) bad.push(`${who} GET ${id} → ${one.status}`);
        }
    }
    for (const who of ['reader', 'ops', 'userX']) {
        const r = await request(w.base, 'GET', `/api/v1/subscriptions/${subs.live.id}`, { token: tok[who] });
        if (r.status < 400) bad.push(`${who} reads live's subscription (${r.status})`);
    }
    // Checkpoints are keyed by the calling principal.
    let r = await request(w.base, 'PUT', '/api/v1/checkpoints', { token: tok.live, body: { topic: 'live.*', cursor: 5 } });
    assert.strictEqual(r.status, 200);
    r = await request(w.base, 'PUT', '/api/v1/checkpoints', { token: tok.appA, body: { topic: `app.${ids.keyA}.*`, cursor: 3 } });
    assert.strictEqual(r.status, 200);
    for (const [who, topic] of [['media', 'live.*'], ['reader', 'live.*'], ['appA2', `app.${ids.keyA}.*`]]) {
        r = await request(w.base, 'GET', `/api/v1/checkpoints?topic=${encodeURIComponent(topic)}`, { token: tok[who] });
        if (r.status === 200 && r.body.cursor !== 0) bad.push(`${who} reads another consumer's checkpoint (${r.body.cursor})`);
    }
    r = await request(w.base, 'GET', `/api/v1/checkpoints?topic=${encodeURIComponent(`app.${ids.keyA}.*`)}`, { token: tok.appB });
    assert.strictEqual(r.status, 403, 'another project\'s pattern is refused');
    r = await request(w.base, 'GET', '/api/v1/checkpoints?topic=live.*', { token: tok.live });
    assert.strictEqual(r.body.cursor, 5, 'positive control');
    assert.deepStrictEqual(bad, [], bad.join('\n'));
});

t('stop', async () => { await w.stop(); });

t.run();
