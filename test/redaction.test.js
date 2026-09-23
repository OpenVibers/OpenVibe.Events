'use strict';
/**
 * Redaction (server/redaction.js) and the public replay window (server/realtime.js).
 *
 * The leak this closes: a chat message deleted in OpenVibe.Chat stayed replayable here, text and
 * anon id included, for the whole retention, to anyone (public visibility, anonymous SSE replay).
 * Now Chat's chat.message.deleted carries payload.redacts, the stored chat.message.created becomes
 * a tombstone at the same seq on every read path, and browsers are replayed public events of the
 * last few minutes only.
 */
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { ids } = require('openvibe-contracts');
const apps = require('../server/apps');
const { parseDirective } = require('../server/redaction');
const { boot, request, serviceToken, userToken, appToken, envelope, subscriber, sse, sleep, suite } = require('./helpers');

const t = suite('redaction');
let h;
const chat = serviceToken('chat', ['events.event.publish']);
const live = serviceToken('live', ['events.event.publish']);
const reader = serviceToken('search', ['events.event.read']);
const subscriberSvc = serviceToken('search', ['events.subscription.manage']);
const alice = ids.newId('user');
const open = [];
const SECRET_TEXT = 'probe: my phone number is 555-0199';

async function stream(q, headers) {
    const c = await sse(h.base, `/realtime/stream?${q}`, { headers });
    open.push(c);
    return c;
}
async function publish(token, body, status = 201) {
    const r = await request(h.base, 'POST', '/api/v1/events', { token, body });
    assert.strictEqual(r.status, status, r.text);
    return r.body;
}
/** chat.message.created as OpenVibe.Chat publishes it (server/db/database.js saveChatMessage). */
function created(messageId, over = {}) {
    return envelope('chat', {
        event_type: 'chat.message.created', visibility: 'public', actor: { type: 'user', id: alice },
        subject: { type: 'chat_message', id: String(messageId) },
        payload: {
            message_id: messageId, room: { type: 'global' }, message_type: 'chat', user_id: null, user_subject: null,
            anon_id: 'anon4242', username: 'anon4242', text: SECRET_TEXT, source_platform: null, reply_to_id: null,
        },
        ...over,
    });
}
/** chat.message.deleted as Chat publishes it: the ids, and the redaction directive. */
function deleted(messageIds, over = {}) {
    return envelope('chat', {
        event_type: 'chat.message.deleted', visibility: 'public', actor: { type: 'service', id: 'chat' },
        subject: { type: 'chat_message', id: String(messageIds[0]) },
        payload: { message_ids: messageIds, redacts: { subject_type: 'chat_message', subject_ids: messageIds.map(String) } },
        ...over,
    });
}
const leaks = (x) => JSON.stringify(x).includes('555-0199') || JSON.stringify(x).includes('anon4242') || JSON.stringify(x).includes(alice);

function assertTombstone(event, byEventId) {
    assert.strictEqual(event.payload.redacted, true, JSON.stringify(event));
    assert.strictEqual(event.payload.redacted_by, byEventId);
    assert.match(event.payload.redacted_at, /^\d{4}-\d\d-\d\dT/);
    assert.deepStrictEqual(Object.keys(event.payload).sort(), ['redacted', 'redacted_at', 'redacted_by']);
    assert.deepStrictEqual(event.actor, { type: 'service', id: 'chat' }, 'the author is no longer linked');
    assert.strictEqual(event.event_type, 'chat.message.created', 'the envelope stays');
    assert.ok(!leaks(event), 'no text, anon id or author subject');
}

t('directive parsing', () => {
    assert.strictEqual(parseDirective({ text: 'x' }), null);
    assert.deepStrictEqual(parseDirective({ redacts: { subject_type: 'chat_message', subject_ids: ['1', '1', '2'] } }),
        { eventIds: [], subjectType: 'chat_message', subjectIds: ['1', '2'] });
    const id = ids.newId('event');
    assert.deepStrictEqual(parseDirective({ redacts: { event_ids: [id] } }), { eventIds: [id], subjectType: null, subjectIds: [] });
    for (const bad of [null, [], 'x', {}, { event_ids: ['nope'] }, { subject_type: 'chat_message' }, { subject_ids: ['1'] },
        { subject_type: 'Chat', subject_ids: ['1'] }, { subject_type: 'chat_message', subject_ids: [1] }, { event_ids: [] },
        { subject_type: 'chat_message', subject_ids: Array.from({ length: 1001 }, (_, i) => String(i)) }, { event_ids: [id], extra: 1 }]) {
        assert.ok(parseDirective({ redacts: bad }).error, JSON.stringify(bad));
    }
});

t('boot', async () => { h = await boot({ env: { REALTIME_PUBLIC_REPLAY_SECONDS: '300' } }); });

let msgSeq, delSeq, msgEventId, delEventId, sub, stub;

t('a first-party subscriber is queued the original before the deletion', async () => {
    stub = await subscriber();
    const r = await request(h.base, 'POST', '/api/v1/subscriptions', { token: subscriberSvc, body: { topic_pattern: 'chat.message.*', endpoint: stub.url } });
    assert.strictEqual(r.status, 201, r.text);
    sub = r.body;
});

t('before deletion the message is replayable (the leak as it was)', async () => {
    const env = created(101);
    msgEventId = env.event_id;
    msgSeq = (await publish(chat, env)).seq;
    const anon = await stream('topics=chat.message.*', { 'Last-Event-ID': String(msgSeq - 1) });
    await anon.waitFor(c => c.events().length === 1);
    assert.strictEqual(anon.events()[0].event.payload.text, SECRET_TEXT);
    anon.close();
});

t('chat.message.deleted turns the stored event into a tombstone at the same seq', async () => {
    const env = deleted([101]);
    delEventId = env.event_id;
    const r = await publish(chat, env);
    delSeq = r.seq;
    assert.strictEqual(delSeq, msgSeq + 1);
    const row = h.store.getEvent(msgEventId);
    assert.strictEqual(row.seq, msgSeq, 'seq unchanged');
    assert.strictEqual(row.redacted_by, delEventId);
    assert.ok(row.redacted_at > 0);
    assert.ok(!row.payload.includes('555-0199') && !row.actor.includes(alice));
    assert.strictEqual(h.store.getEvent(delEventId).redacts, 1);
    assert.ok(!h.store.getEvent(delEventId).redacted_at, 'the deletion event itself is kept as is');
});

t('anonymous SSE replay: the tombstone, never the text', async () => {
    const anon = await stream('topics=chat.message.*', { 'Last-Event-ID': String(msgSeq - 1) });
    await anon.waitFor(c => c.events().length === 2);
    const [a, b] = anon.events();
    assert.deepStrictEqual([a.seq, b.seq], [msgSeq, delSeq], 'sequence intact');
    assertTombstone(a.event, delEventId);
    assert.deepStrictEqual(b.event.payload.message_ids, [101]);
    assert.ok(!leaks(anon.body));
    anon.close();
});

t('signed-in SSE replay (cookie and Bearer) and service replay: the tombstone too', async () => {
    const byCookie = await stream('topics=chat.message.*', { Cookie: `ov_token=${userToken({ subjectId: alice })}`, 'Last-Event-ID': String(msgSeq - 1) });
    const byBearer = await stream('topics=chat.*', { Authorization: `Bearer ${userToken()}`, 'Last-Event-ID': String(msgSeq - 1) });
    const svc = await stream('topics=chat.message.*', { Authorization: `Bearer ${serviceToken('search', ['events.event.read'])}`, 'Last-Event-ID': String(msgSeq - 1) });
    for (const c of [byCookie, byBearer, svc]) {
        await c.waitFor(x => x.events().length === 2);
        assert.deepStrictEqual(c.events().map(e => e.seq), [msgSeq, delSeq]);
        assertTombstone(c.events()[0].event, delEventId);
        assert.ok(!leaks(c.body));
        c.close();
    }
});

t('pull (/api/v1/events), by id, and developer-app reads: the tombstone', async () => {
    let r = await request(h.base, 'GET', `/api/v1/events?topic=chat.message.*&after_seq=${msgSeq - 1}`, { token: reader });
    assert.strictEqual(r.status, 200);
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [msgSeq, delSeq], 'no hole in the sequence');
    assertTombstone(r.body.events[0].event, delEventId);
    r = await request(h.base, 'GET', `/api/v1/events/${msgEventId}`, { token: reader });
    assertTombstone(r.body.event, delEventId);
    assert.strictEqual(r.body.seq, msgSeq);
    const app = appToken({ env: 'production', cap: ['events.app.read'] });
    r = await request(h.base, 'GET', `/api/v1/events?topic=chat.*&after_seq=${msgSeq - 1}`, { token: app });
    assert.strictEqual(r.status, 200, r.text);
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [msgSeq, delSeq]);
    assertTombstone(r.body.events[0].event, delEventId);
    assert.ok(!leaks(r.body));
});

t('durable delivery: the consumer gets the tombstone and then the deletion, in order', async () => {
    await h.worker.drain();
    const got = stub.calls.map(c => c.body);
    assert.deepStrictEqual(got.map(b => b.seq), [msgSeq, delSeq]);
    assertTombstone(got[0].event, delEventId);
    assert.strictEqual(got[1].event.event_type, 'chat.message.deleted');
    assert.ok(!leaks(stub.calls.map(c => c.rawBody.toString())));
    // A DLQ replay of the original sends the tombstone as well.
    const admin = serviceToken('ops', ['events.delivery.admin']);
    const rq = await request(h.base, 'POST', '/api/v1/deliveries/replay', { token: admin, body: { subscription_id: sub.id, event_ids: [msgEventId] } });
    assert.strictEqual(rq.body.queued, 1);
    await h.worker.drain();
    assertTombstone(stub.calls.at(-1).body.event, delEventId);
});

t('created and deleted in one batch: live SSE subscribers get the tombstone, not the text', async () => {
    const anon = await stream('topics=chat.message.*');
    const svc = await stream('topics=chat.message.*', { Authorization: `Bearer ${serviceToken('search', ['events.event.read'])}` });
    await sleep(30);
    const c = created(102);
    const d = deleted([102]);
    const r = await publish(chat, { events: [c, d] });
    assert.deepStrictEqual(r.results.map(x => x.duplicate), [false, false]);
    for (const s of [anon, svc]) {
        await s.waitFor(x => x.events().length === 2);
        assertTombstone(s.events()[0].event, d.event_id);
        assert.ok(!leaks(s.body));
    }
});

t('re-publishing a deletion is a duplicate; a second deletion leaves tombstones and deletions alone', async () => {
    const again = await request(h.base, 'POST', '/api/v1/events', { token: chat, body: { ...deleted([101]), event_id: delEventId } });
    assert.strictEqual(again.status, 200);
    assert.strictEqual(again.body.duplicate, true);
    const second = deleted([101]);
    await publish(chat, second);
    assert.strictEqual(h.store.getEvent(msgEventId).redacted_by, delEventId, 'first redaction stands');
    assert.ok(!h.store.getEvent(delEventId).redacted_at, 'a deletion event is never redacted by a later one');
});

t('only the owning service can redact', async () => {
    const env = created(201);
    const seq = (await publish(chat, env)).seq;
    // Live names Chat's event by id: refused, and nothing of the batch is stored.
    const liveEvent = envelope('live', { payload: { redacts: { event_ids: [env.event_id] } } });
    let r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: { events: [envelope('live'), liveEvent] } });
    assert.strictEqual(r.status, 403, r.text);
    assert.strictEqual(r.body.code, 'events.redaction_not_allowed');
    assert.strictEqual(h.store.getEvent(liveEvent.event_id), null);
    // Live by subject: its own namespace only, so Chat's event is untouched.
    await publish(live, envelope('live', { payload: { redacts: { subject_type: 'chat_message', subject_ids: ['201'] } } }));
    // Nor can Live publish Chat's deletion (source must be the caller).
    r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: deleted([201]) });
    assert.strictEqual(r.status, 403);
    assert.strictEqual(r.body.code, 'events.source_mismatch');
    // A developer app cannot reach first-party events.
    const appId = ids.newId('app');
    const prj = `prj_${ids.ulid()}`;
    const appEvent = envelope('x', {
        source: apps.appSource(appId), event_type: `app.${apps.projectKey(prj)}.thing.deleted`, actor: { type: 'app', id: appId },
        subject: { type: 'chat_message', id: '201' }, payload: { redacts: { event_ids: [env.event_id] } },
    });
    r = await request(h.base, 'POST', '/api/v1/events', { token: appToken({ appId, projectId: prj, env: 'production', cap: ['events.app.publish'] }), body: appEvent });
    assert.strictEqual(r.status, 403, r.text);
    assert.strictEqual(r.body.code, 'events.redaction_not_allowed');
    r = await request(h.base, 'POST', '/api/v1/events', {
        token: appToken({ appId, projectId: prj, env: 'production', cap: ['events.app.publish'] }),
        body: { ...appEvent, event_id: ids.newId('event'), payload: { redacts: { subject_type: 'chat_message', subject_ids: ['201'] } } },
    });
    assert.strictEqual(r.status, 201, r.text);
    // A malformed directive is refused before anything is stored.
    r = await request(h.base, 'POST', '/api/v1/events', { token: chat, body: deleted([201], { payload: { redacts: { subject_type: 'chat_message' } } }) });
    assert.strictEqual(r.status, 422);
    assert.strictEqual(r.body.code, 'events.invalid_redaction');
    const row = h.store.getEvent(env.event_id);
    assert.ok(!row.redacted_at, 'still intact after every refused or foreign attempt');
    assert.ok(row.payload.includes('555-0199'));
    // The owner can.
    await publish(chat, deleted([201]));
    assert.ok(h.store.getEvent(env.event_id).redacted_at);
    assert.strictEqual(h.store.getEvent(env.event_id).seq, seq);
});

t('the text is gone from the database file, not just hidden', async () => {
    h.db.pragma('wal_checkpoint(TRUNCATE)');
    const bytes = fs.readFileSync(path.join(h.dir, 'events.db'));
    assert.ok(!bytes.includes(Buffer.from('555-0199')), 'secure_delete zeroed the old payloads');
    assert.ok(!bytes.includes(Buffer.from('anon4242')));
});

t('operator redaction (backfill): same owner rule, idempotent', async () => {
    const env = created(301);
    await publish(chat, env);
    const other = envelope('live', { subject: { type: 'chat_message', id: '301' } });
    await publish(live, other);
    const done = h.store.redact('chat', { subject_type: 'chat_message', subject_ids: ['301'] }, { by: 'backfill' });
    assert.deepStrictEqual(done.map(d => d.id), [env.event_id]);
    assert.strictEqual(h.store.getEvent(env.event_id).redacted_by, 'backfill');
    assert.ok(!h.store.getEvent(other.event_id).redacted_at, "another source's event with the same subject is untouched");
    assert.deepStrictEqual(h.store.redact('chat', { subject_type: 'chat_message', subject_ids: ['301'] }, { by: 'backfill' }), [], 'second run: nothing');
    assert.throws(() => h.store.redact('chat', { event_ids: [other.event_id] }), /may redact only its own/);
});

t('public replay window: browsers get a gap for public events older than 5 minutes', async () => {
    const w = await boot({ env: { REALTIME_PUBLIC_REPLAY_SECONDS: '300' } });
    try {
        const post = async (token, body) => (await request(w.base, 'POST', '/api/v1/events', { token, body })).body.seq;
        const net = serviceToken('network', ['events.event.publish']);
        const oldPublic = await post(chat, created(1));
        const oldMine = await post(net, envelope('network', { event_type: 'network.notification.created', visibility: 'subject', subject: { type: 'user', id: alice } }));
        w.clock.advance(301 * 1000);
        const recent = await post(chat, created(2));
        const topics = 'topics=chat.message.*,network.notification.*';

        const anon = await sse(w.base, `/realtime/stream?${topics}`, { headers: { 'Last-Event-ID': String(oldPublic - 1) } });
        await anon.waitFor(c => c.events().length === 1);
        assert.deepStrictEqual(anon.events().map(e => e.seq), [recent]);
        const gap = anon.gaps()[0];
        assert.strictEqual(gap.reason, 'public_window');
        assert.deepStrictEqual([gap.from_seq, gap.to_seq, gap.window_seconds], [oldPublic, recent - 1, 300]);
        assert.ok(anon.messages.findIndex(m => m.event === 'gap') < anon.messages.findIndex(m => !m.event), 'gap first');

        const user = await sse(w.base, `/realtime/stream?${topics}`, { headers: { Cookie: `ov_token=${userToken({ subjectId: alice })}`, 'Last-Event-ID': String(oldPublic - 1) } });
        await user.waitFor(c => c.events().length === 2);
        assert.deepStrictEqual(user.events().map(e => e.seq), [oldMine, recent], 'own subject events keep the whole retention; old public ones do not');
        assert.strictEqual(user.gaps()[0].reason, 'public_window');

        const svc = await sse(w.base, `/realtime/stream?${topics}`, { headers: { Authorization: `Bearer ${serviceToken('search', ['events.event.read'])}`, 'Last-Event-ID': String(oldPublic - 1) } });
        await svc.waitFor(c => c.events().length === 3);
        assert.deepStrictEqual(svc.gaps(), [], 'services are not windowed');

        const fresh = await sse(w.base, `/realtime/stream?${topics}`, { headers: { 'Last-Event-ID': String(recent - 1) } });
        await fresh.waitFor(c => c.events().length === 1);
        assert.deepStrictEqual(fresh.gaps(), [], 'a reconnect inside the window is seamless');
        for (const c of [anon, user, svc, fresh]) c.close();

        const none = await boot({ env: { REALTIME_PUBLIC_REPLAY_SECONDS: '0' } });
        try {
            const s = await (async () => (await request(none.base, 'POST', '/api/v1/events', { token: chat, body: created(3) })).body.seq)();
            const c = await sse(none.base, '/realtime/stream?topics=chat.message.*', { headers: { 'Last-Event-ID': String(s - 1) } });
            await c.waitFor(x => x.gaps().length === 1);
            await sleep(50);
            assert.deepStrictEqual(c.events(), [], 'window 0: no public replay at all');
            c.close();
        } finally { await none.stop(); }
    } finally { await w.stop(); }
});

t('stop', async () => { for (const c of open) c.close(); if (stub) await stub.close(); await h.stop(); });

t.run();
