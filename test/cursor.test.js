'use strict';
/**
 * ADR-042 decision 7: opaque cursors beside the global seq. A cursor is `c1.<epoch>.<base64url(seq)>`;
 * every answer that carries a seq also carries its cursor, pull accepts `after=<cursor>` beside
 * `after_seq`, the SSE `id` is the cursor (a bare seq is accepted for one release), and checkpoints
 * store and return the position's epoch. The contract answers are validated against the pinned
 * openvibe-contracts (0.81.0, the release that added the cursor fields).
 */
const assert = require('assert');
const { validate } = require('openvibe-contracts');
const cursor = require('../server/cursor');
const { boot, request, serviceToken, envelope, sse, suite } = require('./helpers');

const t = suite('cursor');
let h;
const live = serviceToken('live', ['events.event.publish']);
const reader = serviceToken('games', ['events.event.read']);
const readerSvc = serviceToken('search', ['events.event.read']);

const publish = async (over = {}) => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.vod.ready', ...over }) });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
};

t('boot', async () => { h = await boot(); });

t('encode/decode round trip (never throws on malformed input)', () => {
    for (const [seq, epoch] of [[0, 0], [1, 0], [42, 7], [Number.MAX_SAFE_INTEGER, 3]]) {
        const c = cursor.encode(seq, epoch);
        assert.match(c, /^c1\.\d+\.[A-Za-z0-9_-]+$/, c);
        assert.deepStrictEqual(cursor.decode(c), { seq, epoch });
    }
    assert.strictEqual(cursor.encode(42, 0), `c1.0.${Buffer.from('42').toString('base64url')}`);
    for (const bad of [null, undefined, 42, {}, [], '', 'c1', 'c1.0', 'c1.0.MQ.MQ', 'c2.0.MQ', 'c1.x.MQ', 'c1.-1.MQ',
        'c1.0.!!!', 'c1.0.MA==', 'c1.00.MQ', 'c1.0.' + 'A'.repeat(300), 'c1.99999999999999999999.MQ',
        'c1.0.' + Buffer.from('01').toString('base64url')]) {   // the last one is non-canonical for seq 1
        assert.strictEqual(cursor.decode(bad), null, JSON.stringify(bad));
    }
});

t('publish and read answers carry cursors, validated against events.publish-result@1 / read-result@1', async () => {
    const epoch = await h.store.epoch();
    const one = await publish();
    const pv = validate('events.publish-result@1', one);
    assert.ok(pv.valid, JSON.stringify(pv.errors));
    assert.deepStrictEqual(cursor.decode(one.cursor), { seq: one.seq, epoch });

    const batch = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events: [envelope('live', { event_type: 'live.vod.ready' }), envelope('live', { event_type: 'live.vod.ready' })] } });
    assert.ok(validate('events.publish-result@1', batch.body).valid);
    for (const r of batch.body.results) assert.deepStrictEqual(cursor.decode(r.cursor), { seq: r.seq, epoch });

    const page = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*', { token: reader });
    const rv = validate('events.read-result@1', page.body);
    assert.ok(rv.valid, JSON.stringify(rv.errors));
    assert.ok(page.body.events.length >= 3);
    for (const e of page.body.events) assert.deepStrictEqual(cursor.decode(e.cursor), { seq: e.seq, epoch });
    assert.deepStrictEqual(cursor.decode(page.body.next_cursor), { seq: page.body.next_after_seq, epoch });

    const single = await request(h.base, 'GET', `/api/v1/events/${one.event_id}`, { token: reader });
    assert.ok(validate('events.read-result@1', single.body).valid);
    assert.deepStrictEqual(cursor.decode(single.body.cursor), { seq: single.body.seq, epoch });
});

t('pull accepts after=<cursor> beside after_seq; next_cursor round-trips a page', async () => {
    let r = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*&limit=1', { token: reader });
    assert.strictEqual(r.body.events.length, 1);
    const first = r.body.events[0];
    assert.strictEqual(r.body.next_after_seq, first.seq);
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after=${encodeURIComponent(r.body.next_cursor)}`, { token: reader });
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(r.body.events.length >= 1 && r.body.events.every(e => e.seq > first.seq));
    // after_seq still works, unchanged.
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after_seq=${first.seq}`, { token: reader });
    assert.ok(r.body.events.every(e => e.seq > first.seq));
});

t('a malformed after= is refused, not treated as a position', async () => {
    for (const bad of ['not-a-cursor', 'c1.0!!!', 'c2.0.MQ']) {
        const r = await request(h.base, 'GET', `/api/v1/events?after=${encodeURIComponent(bad)}`, { token: reader });
        assert.strictEqual(r.status, 400, bad);
        assert.strictEqual(r.body.code, 'events.bad_request');
    }
});

t('a cursor from another epoch answers the gap shape, never a silent restart', async () => {
    const before = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*', { token: reader });
    const stale = before.body.next_cursor;
    const olderEpoch = cursor.decode(stale).epoch;
    await h.store.bumpEpoch();
    assert.strictEqual(await h.store.epoch(), olderEpoch + 1);
    const r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after=${encodeURIComponent(stale)}`, { token: reader });
    assert.strictEqual(r.status, 200);
    assert.ok(r.body.gap, JSON.stringify(r.body));
    assert.ok(Number.isInteger(r.body.gap.from_seq) && Number.isInteger(r.body.gap.to_seq));
    assert.ok(r.body.events.length >= 1, 'the retained events are still served after the gap');
    assert.strictEqual(cursor.decode(r.body.next_cursor).epoch, olderEpoch + 1, 'position moves in the new epoch');
});

t('SSE id is the cursor; Last-Event-ID resumes from a cursor, and a bare seq still works', async () => {
    const epoch = await h.store.epoch();
    const c1 = await sse(h.base, '/realtime/stream?topics=live.stream.*', { headers: { Authorization: `Bearer ${readerSvc}` } });
    const s1 = await publish({ visibility: 'public', event_type: 'live.stream.a' });
    await c1.waitFor(x => x.events().length === 1);
    const msg = c1.messages.find(m => !m.event);
    assert.deepStrictEqual(cursor.decode(msg.id), { seq: s1.seq, epoch });
    c1.close();

    const s2 = await publish({ visibility: 'public', event_type: 'live.stream.b' });
    const byCursor = await sse(h.base, `/realtime/stream?topics=live.stream.*&last_event_id=${encodeURIComponent(msg.id)}`, { headers: { Authorization: `Bearer ${readerSvc}` } });
    await byCursor.waitFor(x => x.events().length === 1);
    assert.deepStrictEqual(byCursor.events().map(e => e.seq), [s2.seq]);
    assert.deepStrictEqual(byCursor.gaps(), []);
    byCursor.close();

    // A bare seq is still accepted for this release.
    const bySeq = await sse(h.base, `/realtime/stream?topics=live.stream.*&last_event_id=${s2.seq}`, { headers: { Authorization: `Bearer ${readerSvc}` } });
    const s3 = await publish({ visibility: 'public', event_type: 'live.stream.c' });
    await bySeq.waitFor(x => x.events().length === 1);
    assert.deepStrictEqual(bySeq.events().map(e => e.seq), [s3.seq]);
    bySeq.close();
});

t('SSE reports a gap when the cursor belongs to another epoch', async () => {
    const c = await sse(h.base, '/realtime/stream?topics=live.stream.*', { headers: { Authorization: `Bearer ${readerSvc}` } });
    const s = await publish({ visibility: 'public', event_type: 'live.stream.d' });
    await c.waitFor(x => x.events().length === 1);
    const stale = c.messages.find(m => !m.event).id;
    c.close();
    await h.store.bumpEpoch();
    const again = await sse(h.base, `/realtime/stream?topics=live.stream.*&last_event_id=${encodeURIComponent(stale)}`, { headers: { Authorization: `Bearer ${readerSvc}` } });
    await again.waitFor(x => x.gaps().length >= 1);
    assert.strictEqual(again.gaps()[0].reason, 'epoch');
    // The retained matching event is replayed after the gap.
    await again.waitFor(x => x.events().some(e => e.seq === s.seq));
    again.close();
});

t('checkpoint round trip: an opaque cursor in, the position and epoch out', async () => {
    const pub = await publish();
    const epoch = cursor.decode(pub.cursor).epoch;
    let r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: pub.cursor } });
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(validate('events.read-result@1', r.body).valid, JSON.stringify(r.body));
    assert.strictEqual(r.body.cursor, pub.seq, 'cursor stays the numeric position');
    assert.strictEqual(r.body.epoch, epoch);
    assert.strictEqual(r.body.next_cursor, pub.cursor, 'the opaque cursor comes back, ready for after=');

    r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: reader });
    assert.strictEqual(r.body.cursor, pub.seq);
    assert.strictEqual(r.body.epoch, epoch);
    assert.strictEqual(r.body.next_cursor, pub.cursor);
    assert.strictEqual(cursor.encode(r.body.cursor, r.body.epoch), pub.cursor);

    // A plain integer still works and takes the current epoch.
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: 5 } });
    assert.strictEqual(r.body.cursor, 5);
    assert.strictEqual(r.body.epoch, await h.store.epoch());
    assert.deepStrictEqual(cursor.decode(r.body.next_cursor), { seq: 5, epoch: await h.store.epoch() });

    // A malformed cursor string is refused.
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: 'nope' } });
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'events.bad_request');
});

t('stop', async () => { await h.stop(); });

t.run();
