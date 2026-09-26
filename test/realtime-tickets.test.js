'use strict';
// Realtime tickets (ADR-005 amendment 2; roadmap WS-E task 3, WS-F task 1): a page on any OpenVibe site
// opens /realtime/stream?ticket=… with a two-minute, single-use ticket Network signed for the signed-in
// person, and sees what that person's session would: public events and subject events addressed to them.
// A ticket for Alice never receives Bob's network.notification.created, on any pattern; expired, foreign,
// reused and session tokens are refused as tickets, and a ticket is never a session. A stream resumes
// from last_event_id with a fresh ticket, and a cursor past retention is reported as a gap.
const assert = require('assert');
const crypto = require('crypto');
const { ids } = require('openvibe-contracts');
const { boot, request, serviceToken, userToken, realtimeTicket, envelope, sse, sleep, suite, ISSUER } = require('./helpers');

const t = suite('realtime-tickets');
let h;
const network = serviceToken('network', ['events.event.publish']);
const live = serviceToken('live', ['events.event.publish']);
const alice = ids.newId('user');
const bob = ids.newId('user');
const open = [];
const TOPIC = 'network.notification.*';

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
/** network.notification.created for `who`, as Network's outbox writes it. */
const notified = (who, unread = 1) => envelope('network', {
    event_type: 'network.notification.created', visibility: 'subject', actor: { type: 'system', id: 'network' },
    subject: { type: 'user', id: who }, priority: 'low',
    payload: { notification_id: crypto.randomUUID(), type: 'STREAM_LIVE', category: 'stream', priority: 'normal', service: 'live', created_at: new Date().toISOString(), unread_count: unread },
});
const refused = async (q, code, headers) => {
    const r = await request(h.base, 'GET', `/realtime/stream?${q}`, { headers });
    assert.strictEqual(r.status, 401, `${q.slice(0, 60)}: ${r.text}`);
    assert.strictEqual(r.body.code, code);
    assert.ok(!r.text.includes('ticket=') && !r.text.includes('eyJ'), 'the ticket is not echoed');
    return r;
};
const quiet = () => sleep(150);

t('boot', async () => { h = await boot({ env: { REALTIME_HEARTBEAT_MS: '100' } }); });

t('a ticket opens a stream as its person: their subject events and public ones, nobody else\'s', async () => {
    const a = await stream(`topics=${TOPIC},live.stream.*&ticket=${realtimeTicket({ subjectId: alice })}`);
    const b = await stream(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: bob })}`);
    assert.strictEqual(a.status, 200);
    await sleep(30);
    assert.ok(a.comments.includes('connected user'));
    const toAlice = await publish(network, notified(alice));
    const toBob = await publish(network, notified(bob));
    const pub = await publish(live, envelope('live', { visibility: 'public' }));
    await a.waitFor(c => c.events().length === 2);
    await b.waitFor(c => c.events().length === 1);
    await quiet();
    assert.deepStrictEqual(a.events().map(e => e.seq), [toAlice, pub]);
    assert.deepStrictEqual(b.events().map(e => e.seq), [toBob]);
    assert.strictEqual(a.events()[0].event.payload.unread_count, 1);
    a.close(); b.close();
});

t('a guessed user:<other> topic yields nothing: refused as a pattern, and no pattern reaches Bob\'s events', async () => {
    let r = await request(h.base, 'GET', `/realtime/stream?topics=user:${bob}&ticket=${realtimeTicket({ subjectId: alice })}`);
    assert.strictEqual(r.status, 400);
    assert.strictEqual(r.body.code, 'realtime.bad_topic');
    const guess = await stream(`topics=${TOPIC},*,network.*,*.created&ticket=${realtimeTicket({ subjectId: alice })}`);
    const anon = await stream(`topics=${TOPIC},*`);
    await publish(network, notified(bob));
    await publish(network, envelope('network', { event_type: 'network.notification.created', visibility: 'subject', actor: { type: 'user', id: bob }, subject: { type: 'user', id: bob } }));
    await publish(network, envelope('network', { event_type: 'network.wallet.credited', visibility: 'internal', subject: { type: 'user', id: alice } }));
    await quiet();
    assert.deepStrictEqual(guess.events(), [], 'Alice\'s ticket sees none of Bob\'s events and no internal one');
    assert.deepStrictEqual(anon.events(), []);
    // Replay too: from before those events, Alice's ticket still gets none of them.
    const replay = await stream(`topics=*&ticket=${realtimeTicket({ subjectId: alice })}&last_event_id=0`);
    await quiet();
    assert.ok(replay.events().every(e => e.event.subject.id !== bob && e.event.visibility !== 'internal'), 'replay never includes Bob\'s or internal events');
    guess.close(); anon.close(); replay.close();
});

t('refused as a ticket: expired, wrong purpose, typ or audience, a session\'s issuer, too long-lived, foreign key, reused, a session JWT', async () => {
    const now = Math.floor(Date.now() / 1000);
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, iat: now - 300, exp: now - 180 })}`, 'ticket.expired');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, purpose: 'session' })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, typ: undefined })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, aud: ['openvibe.network'] })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, aud: ['openvibe.events', 'openvibe.live'] })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, iss: ISSUER })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, iat: now, exp: now + 3600 })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: '57' })}`, 'ticket.invalid');
    const other = crypto.generateKeyPairSync('rsa', { modulusLength: 2048, privateKeyEncoding: { type: 'pkcs8', format: 'pem' }, publicKeyEncoding: { type: 'spki', format: 'pem' } });
    await refused(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice, key: other.privateKey })}`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=not-a-ticket`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=`, 'ticket.invalid');
    await refused(`topics=${TOPIC}&ticket=${userToken({ subjectId: alice, aud: ['openvibe.events'] })}`, 'ticket.invalid');

    // Single use: the second stream with the same ticket is refused, whoever asks.
    const once = realtimeTicket({ subjectId: alice });
    const first = await stream(`topics=${TOPIC}&ticket=${once}`);
    assert.strictEqual(first.status, 200);
    await refused(`topics=${TOPIC}&ticket=${once}`, 'ticket.used');
    first.close();
    await refused(`topics=${TOPIC}&ticket=${once}`, 'ticket.used');
});

t('a ticket is never a session: refused as Bearer, anonymous as a cookie', async () => {
    const tk = realtimeTicket({ subjectId: alice });
    const r = await request(h.base, 'GET', `/realtime/stream?topics=${TOPIC}`, { token: tk });
    assert.strictEqual(r.status, 401);
    assert.strictEqual(r.body.code, 'token.invalid');
    const c = await stream(`topics=${TOPIC}`, { Cookie: `ov_token=${realtimeTicket({ subjectId: alice })}` });
    await publish(network, notified(alice));
    await quiet();
    assert.ok(c.comments.includes('connected anonymous'), 'a ticket in the cookie is no session');
    assert.deepStrictEqual(c.events(), []);
    c.close();
    // A session JWT still works as before (Bearer and cookie), and a ticket beats a Bearer when both come.
    const s = await stream(`topics=${TOPIC}`, { Authorization: `Bearer ${userToken({ subjectId: alice })}` });
    await sleep(30);
    assert.ok(s.comments.includes('connected user'));
    s.close();
    const both = await request(h.base, 'GET', `/realtime/stream?topics=${TOPIC}&ticket=expired.not.valid`, { token: userToken({ subjectId: alice }) });
    assert.strictEqual(both.status, 401, 'a bad ticket is refused even beside a good session');
});

t('disconnect and resume from the cursor with a fresh ticket: the missed events, in order, then live', async () => {
    const a1 = await stream(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice })}`);
    const seen = await publish(network, notified(alice, 1));
    await a1.waitFor(c => c.events().length === 1);
    const cursor = a1.events()[0].seq;
    assert.strictEqual(cursor, seen);
    a1.close();                                              // the tab went to sleep, or Events restarted
    await sleep(30);
    const missed1 = await publish(network, notified(alice, 2));
    await publish(network, notified(bob, 9));
    const missed2 = await publish(network, notified(alice, 3));
    const a2 = await stream(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice })}&last_event_id=${cursor}`);
    await a2.waitFor(c => c.events().length === 2);
    assert.deepStrictEqual(a2.events().map(e => e.seq), [missed1, missed2]);
    assert.deepStrictEqual(a2.events().map(e => e.event.payload.unread_count), [2, 3]);
    assert.deepStrictEqual(a2.gaps(), []);
    const now = await publish(network, notified(alice, 4));
    await a2.waitFor(c => c.events().length === 3);
    assert.strictEqual(a2.events()[2].seq, now);
    a2.close();
});

t('a cursor older than retention is reported as a gap before the events kept', async () => {
    const old = await publish(network, notified(alice, 5));
    h.store.prune({ retentionDays: 30, now: Date.now() + 31 * 86400000 });
    const kept = await publish(network, notified(alice, 6));
    const c = await stream(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice })}&last_event_id=${old - 1}`);
    await c.waitFor(x => x.events().length === 1);
    assert.strictEqual(c.gaps().length, 1);
    assert.strictEqual(c.gaps()[0].reason, 'retention');
    assert.ok(c.messages.findIndex(m => m.event === 'gap') < c.messages.findIndex(m => !m.event), 'the gap comes first');
    assert.strictEqual(c.events()[0].seq, kept);
    c.close();
});

t('CORS: a ticket stream needs no credentials, from https://*.openvibe.* origins', async () => {
    const c = await stream(`topics=${TOPIC}&ticket=${realtimeTicket({ subjectId: alice })}`, { Origin: 'https://openvibe.live' });
    assert.strictEqual(c.status, 200);
    assert.strictEqual(c.headers['access-control-allow-origin'], 'https://openvibe.live');
    c.close();
});

t('stop', async () => { for (const c of open) c.close(); await h.stop(); });

t.run();
