'use strict';
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const nodeHttp = require('http');
const { createDb, sql } = require('openvibe-sdk/db');
const { createTestDb } = require('openvibe-sdk/testing');
const { serviceAuth } = require('openvibe-contracts');
const events = require('..');
const { boot, request, serviceToken, envelope, subscriber, suite } = require('./helpers');

const t = suite('outbox-inbox');
let h;
let network;
let tokenClient;
const admin = serviceToken('ops', ['events.delivery.admin']);

async function countIn(id) {
    return (await h.db.prepare('SELECT COUNT(*) AS n FROM events WHERE id = ?').get(id)).n;   // Events' own database (PostgreSQL)
}

t('boot Events + a stub Network token endpoint', async () => {
    h = await boot();
    let issued = 0;
    network = nodeHttp.createServer((req, res) => {
        let body = '';
        req.on('data', c => { body += c; });
        req.on('end', () => {
            const p = new URLSearchParams(body);
            assert.strictEqual(p.get('grant_type'), 'client_credentials');
            assert.strictEqual(p.get('audience'), 'openvibe.events');
            issued++;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ access_token: serviceToken(p.get('client_id'), ['events.event.publish']), token_type: 'Bearer', expires_in: 300 }));
        });
    });
    await new Promise(r => network.listen(0, '127.0.0.1', r));
    network.issued = () => issued;
    tokenClient = serviceAuth.createTokenClient({
        tokenUrl: `http://127.0.0.1:${network.address().port}/oauth/token`, clientId: 'live', clientSecret: 's', audience: 'openvibe.events',
    });
});

t('publisher fills event_id, timestamp and trace (from traceparent)', async () => {
    const publisher = events.createPublisher({ eventsUrl: h.base, tokenClient });
    const out = await publisher.publish({
        event_type: 'live.stream.started', source: 'live', actor: { type: 'service', id: 'live' },
        subject: { type: 'stream', id: '7' }, payload: {},
    }, { traceparent: '00-11111111111111111111111111111111-2222222222222222-01' });
    assert.match(out.event_id, /^evt_/);
    assert.strictEqual((await h.store.getEvent(out.event_id)).trace_id, '11111111111111111111111111111111');
    assert.strictEqual(network.issued(), 1);
    const batch = await publisher.publish([envelope(), envelope()]);
    assert.strictEqual(batch.results.length, 2);
    assert.strictEqual(network.issued(), 1, 'token cached');
    await assert.rejects(publisher.publish(envelope('media')), (err) => err.status === 403 && err.code === 'events.source_mismatch' && err.permanent);
});

// The producer's own database: embedded PostgreSQL, like a service's (openvibe-sdk/db).
async function producerDb(extra = '') {
    const db = createDb({ pglite: true });
    await db.query(`${extra}${events.outboxSchema()}${events.inboxSchema()}`);
    return db;
}

t('outbox: enqueue inside a rolled-back transaction publishes nothing; committed publishes exactly once', async () => {
    const db = await producerDb('CREATE TABLE vods (id bigint PRIMARY KEY, status text);');
    const outbox = events.createPgOutbox(db, { events: events.createPublisher({ eventsUrl: h.base, tokenClient }) });
    try {
        await assert.rejects(outbox.enqueue(null, envelope()), /transaction handle/);

        let rolledBack;
        await assert.rejects(db.tx(async (t) => {
            await t.exec(sql`INSERT INTO vods (id, status) VALUES (1, 'ready')`);
            rolledBack = await outbox.enqueue(t, envelope('live', { event_type: 'live.vod.ready' }));
            throw new Error('domain write failed');
        }), /domain write failed/);
        assert.strictEqual(await outbox.pending(), 0);
        let r = await outbox.flush();
        assert.deepStrictEqual(r, { sent: 0, failed: 0, rejected: 0 });
        assert.strictEqual(await countIn(rolledBack.event_id), 0, 'rolled back: never published');

        const committed = await db.tx(async (t) => {
            await t.exec(sql`INSERT INTO vods (id, status) VALUES (2, 'ready')`);
            return outbox.enqueue(t, envelope('live', { event_type: 'live.vod.ready' }));
        });
        assert.strictEqual(await outbox.pending(), 1);
        r = await outbox.flush();
        assert.strictEqual(r.sent, 1);
        r = await outbox.flush();
        assert.strictEqual(r.sent, 0, 'sent rows are not sent again');
        assert.strictEqual(await countIn(committed.event_id), 1);

        // Relay crashed after Events accepted but before the row was marked: at-least-once resend is a duplicate.
        await db.query('UPDATE event_outbox SET sent_at = NULL, next_attempt_at = 0 WHERE event_id = $1', [committed.event_id]);
        r = await outbox.flush();
        assert.strictEqual(r.sent, 1);
        assert.strictEqual(await countIn(committed.event_id), 1, 'still exactly one event');
        assert.ok(Number((await db.one('SELECT seq FROM event_outbox WHERE event_id = $1', [committed.event_id])).seq) > 0);
    } finally { await db.close(); }
});

t('outbox: a poisoned row is isolated and rejected; a down Events is retried later', async () => {
    const db = await producerDb();
    let clock = Date.now();
    const outbox = events.createPgOutbox(db, { events: events.createPublisher({ eventsUrl: h.base, tokenClient }), now: () => clock });
    try {
        const [good, bad] = await db.tx(async (t) => [
            await outbox.enqueue(t, envelope()),
            await outbox.enqueue(t, envelope('media')),       // live may not publish as media
        ]);
        const r = await outbox.flush();
        assert.deepStrictEqual(r, { sent: 1, failed: 0, rejected: 1 });
        assert.strictEqual(await countIn(good.event_id), 1);
        assert.ok((await db.one('SELECT rejected_at FROM event_outbox WHERE event_id = $1', [bad.event_id])).rejected_at);

        const down = events.createPgOutbox(db, { events: events.createPublisher({ eventsUrl: 'http://127.0.0.1:9', tokenClient }), now: () => clock });
        const later = await db.tx((t) => down.enqueue(t, envelope()));
        const d = await down.flush();
        assert.deepStrictEqual(d, { sent: 0, failed: 1, rejected: 0 });
        assert.strictEqual(await down.pending(), 1);
        const row = await db.one('SELECT * FROM event_outbox WHERE event_id = $1', [later.event_id]);
        assert.strictEqual(Number(row.next_attempt_at), clock + 1000, 'backoff after the first failure');
        // Events is back: the regular relay picks it up once due.
        clock += 1000;
        assert.strictEqual((await outbox.flush()).sent, 1);
        assert.strictEqual(await countIn(later.event_id), 1);
    } finally { await db.close(); }
});

t('consumer crash mid-processing + redelivery + replay -> exactly one effect (createPgInbox)', async () => {
    const consumerDb = await producerDb('CREATE TABLE credits (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, event_id text, amount integer);');
    const inbox = events.createPgInbox(consumerDb);
    let secret = null;
    let mode = 'crash-after-commit';
    const stub = await subscriber(async (call) => {
        if (!events.verifyDeliveryV2(call.rawBody, call.headers, secret, { now: h.clock.now() })) return 401;
        const ev = call.body.event;
        if (mode === 'crash-before-commit') {
            mode = 'ok';
            try {
                await inbox.once('media', ev.event_id, async (t) => {
                    await t.query('INSERT INTO credits (event_id, amount) VALUES ($1, $2)', [ev.event_id, ev.payload.amount]);
                    throw new Error('process died mid-transaction');
                });
            } catch { /* the transaction rolled back */ }
            return 'destroy';
        }
        await inbox.once('media', ev.event_id, async (t) => {
            await t.query('INSERT INTO credits (event_id, amount) VALUES ($1, $2)', [ev.event_id, ev.payload.amount]);
        });
        if (mode === 'crash-after-commit') { mode = 'ok'; return 'destroy'; }  // effect committed, no response sent
        return 204;
    });
    try {
        const media = serviceToken('media', ['events.subscription.manage']);
        const sub = (await request(h.base, 'POST', '/api/v1/subscriptions', { token: media, body: { topic_pattern: 'live.tip.*', endpoint: stub.url } })).body;
        secret = sub.secret;
        const live = serviceToken('live', ['events.event.publish']);
        const effects = async (id) => Number((await consumerDb.one('SELECT COUNT(*) AS n FROM credits WHERE event_id = $1', [id])).n);

        for (const first of ['crash-after-commit', 'crash-before-commit']) {
            mode = first;
            const env = envelope('live', { event_type: 'live.tip.sent', payload: { amount: 5 } });
            await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
            await h.worker.drain();
            const d = await h.store.getDelivery(env.event_id, sub.id);
            assert.strictEqual(d.status, 'failed', `${first}: the crashed attempt is retried`);
            assert.strictEqual(await effects(env.event_id), first === 'crash-after-commit' ? 1 : 0);
            h.clock.advance(1000);
            await h.worker.drain();
            assert.strictEqual((await h.store.getDelivery(env.event_id, sub.id)).status, 'delivered');
            assert.strictEqual(await effects(env.event_id), 1, `${first}: exactly one effect after redelivery`);

            const rp = await request(h.base, 'POST', '/api/v1/deliveries/replay', { token: admin, body: { subscription_id: sub.id, event_ids: [env.event_id] } });
            assert.strictEqual(rp.body.queued, 1);
            await h.worker.drain();
            assert.strictEqual(await effects(env.event_id), 1, `${first}: replay does not repeat the effect`);
        }
        assert.strictEqual(await inbox.seen('media', 'evt_x'), false);
    } finally {
        await stub.close();
        await consumerDb.close();
    }
});

// A consumer's database for the ordered inbox, migrated like a service's: PGlite, or with EVENTS_TEST_STORE=pg the
// containers' PostgreSQL through PgBouncer (a runtime role, a pool of 4: concurrent applies really race).
async function orderedConsumerDb(extra = '') {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ov-inbox-'));
    fs.writeFileSync(path.join(dir, '0001_consumer.sql'), `-- phase: expand\n${extra}\n${events.inboxSchema()}\n${events.inboxHeadsSchema()}\n`);
    try {
        return await createTestDb({ migrations: dir, store: process.env.EVENTS_TEST_STORE || 'pglite', service: 'consumer' });
    } finally { fs.rmSync(dir, { recursive: true, force: true }); }
}

function rev(id, revision, extra = {}) {
    return envelope('live', { event_type: 'live.stream.updated', subject: { type: 'stream', id, revision }, ...extra });
}

t('ordered inbox: an older or equal revision of a key is stale and never applied; keys are independent', async () => {
    const { db, close } = await orderedConsumerDb('CREATE TABLE titles (stream text PRIMARY KEY, revision integer);');
    const inbox = events.createPgOrderedInbox(db);
    const write = (ev) => async (t) => {
        await t.query('INSERT INTO titles (stream, revision) VALUES ($1, $2) ON CONFLICT (stream) DO UPDATE SET revision = EXCLUDED.revision', [ev.subject.id, ev.subject.revision]);
        return ev.subject.revision;
    };
    const title = async (id) => Number((await db.one('SELECT revision FROM titles WHERE stream = $1', [id])).revision);
    try {
        assert.strictEqual(events.subjectKey(rev('a', 1)), '["stream","a"]');
        const r2 = rev('a', 2);
        assert.deepStrictEqual(await inbox.apply('media', r2, write(r2)), { duplicate: false, stale: false, result: 2 });
        const r1 = rev('a', 1);
        assert.deepStrictEqual(await inbox.apply('media', r1, write(r1)), { duplicate: false, stale: true, head: { revision: 2, event_id: r2.event_id } });
        assert.strictEqual(await title('a'), 2, 'the older revision did not overwrite');
        assert.deepStrictEqual(await inbox.apply('media', r1, write(r1)), { duplicate: true }, 'a stale event keeps its receipt');
        assert.deepStrictEqual(await inbox.apply('media', r2, write(r2)), { duplicate: true });
        const again = rev('a', 2);
        assert.strictEqual((await inbox.apply('media', again, write(again))).stale, true, 'an equal revision is stale');
        const r3 = rev('a', 3);
        assert.strictEqual((await inbox.apply('media', r3, write(r3))).result, 3);
        assert.deepStrictEqual(await inbox.head('media', events.subjectKey(rev('a', 0))), { revision: 3, event_id: r3.event_id });

        const b1 = rev('b', 1);
        assert.strictEqual((await inbox.apply('media', b1, write(b1))).stale, false, 'another key has its own head');
        assert.strictEqual((await inbox.apply('network', r1, write(r1))).stale, false, 'another consumer has its own heads');
        const subj = (type, id, revision) => envelope('live', { event_type: 'live.stream.updated', subject: { type, id, revision } });
        const long = 'x'.repeat(250);
        const pairs = [[subj('stream', `${long}1`, 9), subj('stream', `${long}2`, 1)], [subj('a:b', 'c', 9), subj('a', 'b:c', 1)]];
        for (const [hi, lo] of pairs) {
            assert.notStrictEqual(events.subjectKey(hi), events.subjectKey(lo), 'distinct subjects have distinct keys');
            assert.ok(events.subjectKey(hi).length <= 200);
            assert.strictEqual((await inbox.apply('media', hi, async () => 'hi')).result, 'hi');
            assert.deepStrictEqual(await inbox.apply('media', lo, async () => 'lo'), { duplicate: false, stale: false, result: 'lo' },
                'a higher revision of one subject never makes another stale');
        }
        const custom = rev('a', 0);
        assert.strictEqual((await inbox.apply('media', custom, async () => 'k', { key: 'owner:9', revision: 7 })).result, 'k');
        assert.strictEqual((await inbox.head('media', 'owner:9')).revision, 7);

        const unordered = envelope('live', { subject: undefined });
        assert.deepStrictEqual(await inbox.apply('media', unordered, async () => 'ran'), { duplicate: false, stale: false, result: 'ran' });
        assert.deepStrictEqual(await inbox.apply('media', unordered, async () => 'ran'), { duplicate: true }, 'without a revision: deduplicated only');
        assert.strictEqual(await inbox.seen('media', unordered.event_id), true);

        await assert.rejects(inbox.apply('media', rev('c', -1), async () => {}), /revision/);
        await assert.rejects(inbox.apply('media', rev('c', 1), async () => {}, { key: 'x'.repeat(201) }), /key/);
        await assert.rejects(inbox.apply('media', {}, async () => {}), /event_id/);
        assert.throws(() => events.createPgOrderedInbox(db, { headsTable: 'bad-name' }), /table name/);
    } finally { await close(); }
});

t('ordered inbox: a failed handler moves neither the receipt nor the head; concurrent revisions apply in order', async () => {
    const { db, close } = await orderedConsumerDb('CREATE TABLE effects (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, revision integer);');
    const inbox = events.createPgOrderedInbox(db);
    const effect = (ev) => (t) => t.query('INSERT INTO effects (revision) VALUES ($1)', [ev.subject.revision]);
    try {
        const r5 = rev('a', 5);
        await assert.rejects(inbox.apply('media', r5, async (t) => {
            await effect(r5)(t);
            throw new Error('crashed mid-transaction');
        }), /crashed/);
        assert.strictEqual(await inbox.head('media', events.subjectKey(rev('a', 0))), null);
        assert.strictEqual(await inbox.seen('media', r5.event_id), false);
        assert.strictEqual(Number(await db.value('SELECT count(*) FROM effects')), 0);

        const evs = [3, 8, 1, 6, 2, 7, 4, 5].map(n => rev('b', n));
        const rs = await Promise.all(evs.map(ev => inbox.apply('media', ev, effect(ev))));
        assert.ok(rs.every(r => r.duplicate === false));
        assert.strictEqual((await inbox.head('media', events.subjectKey(rev('b', 0)))).revision, 8);
        const applied = (await db.query('SELECT revision FROM effects ORDER BY id')).rows.map(r => Number(r.revision));
        assert.strictEqual(applied.length, rs.filter(r => !r.stale).length);
        assert.ok(applied.every((n, i) => i === 0 || n > applied[i - 1]), `applied in increasing revisions: ${applied}`);
        assert.strictEqual(applied[applied.length - 1], 8);
    } finally { await close(); }
});

t('ordered inbox end to end: replaying a dead older revision after a newer one is acknowledged, not applied', async () => {
    const { db: consumerDb, close } = await orderedConsumerDb('CREATE TABLE titles (stream text PRIMARY KEY, revision integer);');
    const inbox = events.createPgOrderedInbox(consumerDb);
    let secret = null;
    let failNext = true;
    const outcomes = [];
    const stub = await subscriber(async (call) => {
        if (!events.verifyDeliveryV2(call.rawBody, call.headers, secret, { now: h.clock.now() })) return 401;
        if (failNext) { failNext = false; return 503; }
        const ev = call.body.event;
        const r = await inbox.apply('media', ev, (t) => t.query(
            'INSERT INTO titles (stream, revision) VALUES ($1, $2) ON CONFLICT (stream) DO UPDATE SET revision = EXCLUDED.revision', [ev.subject.id, ev.subject.revision]));
        outcomes.push(r.duplicate ? 'duplicate' : r.stale ? 'stale' : 'applied');
        return 204;
    });
    try {
        const media = serviceToken('media', ['events.subscription.manage']);
        const sub = (await request(h.base, 'POST', '/api/v1/subscriptions', {
            token: media, body: { topic_pattern: 'live.stream.updated', endpoint: stub.url, retry_policy: { max_attempts: 1 } },
        })).body;
        secret = sub.secret;
        const live = serviceToken('live', ['events.event.publish']);
        const older = rev('e2e', 1);
        const newer = rev('e2e', 2);
        await request(h.base, 'POST', '/api/v1/events', { token: live, body: older });
        await h.worker.drain();
        assert.strictEqual((await h.store.getDelivery(older.event_id, sub.id)).status, 'dead');
        await request(h.base, 'POST', '/api/v1/events', { token: live, body: newer });
        await h.worker.drain();
        assert.strictEqual((await h.store.getDelivery(newer.event_id, sub.id)).status, 'delivered');

        const rp = await request(h.base, 'POST', '/api/v1/deliveries/replay', { token: admin, body: { subscription_id: sub.id, event_ids: [older.event_id, newer.event_id] } });
        assert.strictEqual(rp.body.queued, 2);
        await h.worker.drain();
        assert.strictEqual((await h.store.getDelivery(older.event_id, sub.id)).status, 'delivered', 'the stale event is acknowledged');
        assert.deepStrictEqual(outcomes.sort(), ['applied', 'duplicate', 'stale']);
        assert.strictEqual(Number((await consumerDb.one('SELECT revision FROM titles WHERE stream = $1', ['e2e'])).revision), 2);
    } finally {
        await stub.close();
        await close();
    }
});

t('stop', async () => { await h.stop(); network.close(); });

t.run();
