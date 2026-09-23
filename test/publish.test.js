'use strict';
const assert = require('assert');
const { ids } = require('openvibe-contracts');
const { boot, request, serviceToken, envelope, suite } = require('./helpers');

const t = suite('publish');
let h;
const live = serviceToken('live', ['events.event.publish']);

t('boot', async () => { h = await boot(); });

t('no token -> 401, wrong audience -> 401, missing capability -> 403', async () => {
    let r = await request(h.base, 'POST', '/api/v1/events', { body: envelope() });
    assert.strictEqual(r.status, 401);
    assert.strictEqual(r.body.code, 'token.missing');
    assert.match(r.headers.get('content-type'), /problem\+json/);
    r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.event.publish'], { aud: 'openvibe.media' }), body: envelope() });
    assert.strictEqual(r.status, 401);
    assert.strictEqual(r.body.code, 'token.wrong_audience');
    r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.event.read']), body: envelope() });
    assert.strictEqual(r.status, 403);
    assert.strictEqual(r.body.code, 'capability.denied');
});

t('a family grant (events.*) covers events.event.publish', async () => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('live', ['events.*']), body: envelope() });
    assert.strictEqual(r.status, 201);
});

t('valid envelope -> 201 with a seq; defaults filled', async () => {
    const env = envelope('live');
    delete env.priority; delete env.visibility;
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env, headers: { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' } });
    assert.strictEqual(r.status, 201, r.text);
    assert.strictEqual(r.body.event_id, env.event_id);
    assert.ok(Number.isInteger(r.body.seq) && r.body.seq > 0);
    assert.strictEqual(r.body.duplicate, false);
    const row = h.store.getEvent(env.event_id);
    assert.strictEqual(row.priority, 'important');
    assert.strictEqual(row.visibility, 'internal');
    assert.strictEqual(row.trace_id, '0af7651916cd43dd8448eb211c80319c', 'trace taken from traceparent');
    assert.strictEqual(row.publisher, 'svc:live');
});

t('bad envelope -> 422 events.invalid_envelope with errors', async () => {
    for (const bad of [
        { ...envelope(), event_id: 'nope' },
        { ...envelope(), event_type: 'live.started' },            // needs 3+ segments
        { ...envelope(), actor: { type: 'user', id: 57 } },
        { ...envelope(), extra: true },
        (() => { const e = envelope(); delete e.subject; return e; })(),
        { ...envelope(), visibility: 'everyone' },
    ]) {
        const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: bad });
        assert.strictEqual(r.status, 422, JSON.stringify(bad));
        assert.strictEqual(r.body.code, 'events.invalid_envelope');
    }
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, raw: '{not json', headers: { 'Content-Type': 'application/json' } });
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'events.bad_json');
});

t('source must be the calling service', async () => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('media') });
    assert.strictEqual(r.status, 403);
    assert.strictEqual(r.body.code, 'events.source_mismatch');
});

t('event_type must use a prefix the source owns', async () => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'media.vod.ready' }) });
    assert.strictEqual(r.status, 403);
    assert.strictEqual(r.body.code, 'events.type_not_allowed');
});

t('unknown source (not in the manifest map) -> 403', async () => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: serviceToken('stranger', ['events.event.publish']), body: envelope('stranger') });
    assert.strictEqual(r.status, 403);
    assert.strictEqual(r.body.code, 'events.unknown_source');
});

t('app principals cannot publish', async () => {
    const r = await request(h.base, 'POST', '/api/v1/events', {
        token: serviceToken('x', ['events.event.publish'], { sub: `app:${ids.newId('app')}` }), body: envelope('live'),
    });
    assert.strictEqual(r.status, 403);
});

t('idempotent repeat: same event_id -> 200, same seq, stored once', async () => {
    const env = envelope();
    const a = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    const b = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.strictEqual(a.status, 201);
    assert.strictEqual(b.status, 200);
    assert.strictEqual(b.body.duplicate, true);
    assert.strictEqual(b.body.seq, a.body.seq);
    assert.strictEqual(h.db.prepare('SELECT COUNT(*) AS n FROM events WHERE id = ?').get(env.event_id).n, 1);
    const c = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { ...env, event_type: 'live.stream.ended' } });
    assert.strictEqual(c.status, 409);
    assert.strictEqual(c.body.code, 'events.id_conflict');
});

t('batch: atomic, <= 100, results per event', async () => {
    const events = [envelope(), envelope(), envelope()];
    let r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events } });
    assert.strictEqual(r.status, 201);
    assert.strictEqual(r.body.results.length, 3);
    assert.ok(r.body.results[0].seq < r.body.results[1].seq && r.body.results[1].seq < r.body.results[2].seq);

    const before = h.store.lastSeq();
    r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events: [envelope(), envelope('media')] } });
    assert.strictEqual(r.status, 403);
    assert.strictEqual(r.body.index, 1);
    assert.strictEqual(h.store.lastSeq(), before, 'nothing of a rejected batch is stored');

    r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events: Array.from({ length: 101 }, () => envelope()) } });
    assert.strictEqual(r.status, 413);
    assert.strictEqual(r.body.code, 'events.batch_too_large');
});

t('payload size limit', async () => {
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { payload: { big: 'x'.repeat(70 * 1024) } }) });
    assert.strictEqual(r.status, 413);
    assert.strictEqual(r.body.code, 'events.payload_too_large');
});

t('loop guard: a trace bouncing between services is refused at 8 hops', async () => {
    const trace = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';
    const media = serviceToken('media', ['events.event.publish']);
    let last;
    for (let i = 0; i < 9; i++) {
        const src = i % 2 ? 'media' : 'live';
        last = await request(h.base, 'POST', '/api/v1/events', {
            token: src === 'live' ? live : media,
            body: envelope(src, { event_type: `${src}.ping.pong`, trace_id: trace }),
        });
        if (i < 8) assert.strictEqual(last.status, 201, `hop ${i + 1}: ${last.text}`);
    }
    assert.strictEqual(last.status, 409);
    assert.strictEqual(last.body.code, 'events.loop_detected');
    assert.strictEqual(last.body.max_hops, 8);
});

t('loop guard: one service re-triggering itself on the same subject', async () => {
    const trace = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb';
    let last;
    for (let i = 0; i < 9; i++) {
        last = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.chat.echoed', trace_id: trace }) });
    }
    assert.strictEqual(last.status, 409);
    assert.strictEqual(last.body.code, 'events.loop_detected');
    // …but a batch of siblings in one request is not a loop.
    const siblings = Array.from({ length: 20 }, () => envelope('live', { event_type: 'live.chat.imported', trace_id: 'cccccccccccccccccccccccccccccccc' }));
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events: siblings } });
    assert.strictEqual(r.status, 201, r.text);
});

t('publish receipts outlive retention: a pruned id is still a duplicate', async () => {
    const env = envelope();
    await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    h.store.prune({ retentionDays: 30, receiptRetentionDays: 90, now: Date.now() + 31 * 86400000 });
    assert.strictEqual(h.store.getEvent(env.event_id), null);
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.duplicate, true);
    assert.strictEqual(r.body.pruned, true);
});

t('stop', async () => { await h.stop(); });

t.run();
