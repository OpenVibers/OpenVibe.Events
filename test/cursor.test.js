'use strict';
/**
 * ADR-042 decision 7: consumer positions are cursors only. A cursor is `c1.<epoch>.<base64url(seq)>`;
 * event answers retain seq and carry a cursor, pull accepts `after=<cursor>`, SSE resumes from
 * cursor IDs, and checkpoints store and return opaque cursor strings. Unchanged publish and single
 * event answers are validated against the pinned openvibe-contracts.
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

t('publish and read answers carry cursors', async () => {
    const epoch = await h.store.epoch();
    const one = await publish();
    const pv = validate('events.publish-result@1', one);
    assert.ok(pv.valid, JSON.stringify(pv.errors));
    assert.deepStrictEqual(cursor.decode(one.cursor), { seq: one.seq, epoch });

    const batch = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events: [envelope('live', { event_type: 'live.vod.ready' }), envelope('live', { event_type: 'live.vod.ready' })] } });
    assert.ok(validate('events.publish-result@1', batch.body).valid);
    for (const r of batch.body.results) assert.deepStrictEqual(cursor.decode(r.cursor), { seq: r.seq, epoch });

    const page = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*', { token: reader });
    assert.ok(typeof page.body.next_cursor === 'string' && typeof page.body.latest_cursor === 'string');
    assert.ok(page.body.events.length >= 3);
    for (const e of page.body.events) assert.deepStrictEqual(cursor.decode(e.cursor), { seq: e.seq, epoch });
    assert.deepStrictEqual(cursor.decode(page.body.next_cursor), { seq: page.body.events.at(-1).seq, epoch });
    assert.ok(!('next_after_seq' in page.body) && !('latest_seq' in page.body));

    const single = await request(h.base, 'GET', `/api/v1/events/${one.event_id}`, { token: reader });
    assert.ok(validate('events.read-result@1', single.body).valid);
    assert.deepStrictEqual(cursor.decode(single.body.cursor), { seq: single.body.seq, epoch });
});

t('pull accepts after=<cursor>; next_cursor round-trips a page and after_seq is refused', async () => {
    let r = await request(h.base, 'GET', '/api/v1/events?topic=live.vod.*&limit=1', { token: reader });
    assert.strictEqual(r.body.events.length, 1);
    const first = r.body.events[0];
    assert.strictEqual(r.body.next_cursor, first.cursor);
    r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after=${encodeURIComponent(r.body.next_cursor)}`, { token: reader });
    assert.strictEqual(r.status, 200, r.text);
    assert.ok(r.body.events.length >= 1 && r.body.events.every(e => e.seq > first.seq));
    for (const value of ['0', String(first.seq)]) {
        r = await request(h.base, 'GET', `/api/v1/events?topic=live.vod.*&after_seq=${value}`, { token: reader });
        assert.strictEqual(r.status, 400);
        assert.strictEqual(r.body.code, 'events.bad_request');
    }
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

t('SSE id is the cursor; Last-Event-ID resumes from a cursor, and a bare seq starts at the head', async () => {
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

    const bySeq = await sse(h.base, `/realtime/stream?topics=live.stream.*&last_event_id=${s1.seq}`, { headers: { Authorization: `Bearer ${readerSvc}` } });
    assert.deepStrictEqual(bySeq.events(), [], 'a bare number does not replay missed events');
    assert.deepStrictEqual(bySeq.gaps(), []);
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

t('checkpoint round trip: an opaque cursor in and out', async () => {
    const pub = await publish();
    let r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: pub.cursor } });
    assert.strictEqual(r.status, 200, r.text);
    assert.strictEqual(r.body.topic, 'live.vod.*');
    assert.strictEqual(r.body.cursor, pub.cursor);
    assert.ok(!('epoch' in r.body) && !('next_cursor' in r.body));

    r = await request(h.base, 'GET', '/api/v1/checkpoints?topic=live.vod.*', { token: reader });
    assert.strictEqual(r.body.cursor, pub.cursor);
    assert.ok(!('epoch' in r.body) && !('next_cursor' in r.body));

    // A plain integer is no longer a consumer position.
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: 5 } });
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'events.bad_request');

    // A malformed cursor string is refused.
    r = await request(h.base, 'PUT', '/api/v1/checkpoints', { token: reader, body: { topic: 'live.vod.*', cursor: 'nope' } });
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'events.bad_request');
});

t('stop', async () => { await h.stop(); });

t.run();
