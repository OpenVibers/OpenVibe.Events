'use strict';
const assert = require('assert');
const path = require('path');
const nodeHttp = require('http');
const Database = require('better-sqlite3');
const { serviceAuth } = require('openvibe-contracts');
const events = require('..');
const { boot, request, serviceToken, envelope, subscriber, suite, tmpDir } = require('./helpers');

const t = suite('outbox-inbox');
let h;
let network;
let tokenClient;
const admin = serviceToken('ops', ['events.delivery.admin']);

function countIn(id) {
    return h.db.prepare('SELECT COUNT(*) AS n FROM events WHERE id = ?').get(id).n;
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
    assert.strictEqual(h.store.getEvent(out.event_id).trace_id, '11111111111111111111111111111111');
    assert.strictEqual(network.issued(), 1);
    const batch = await publisher.publish([envelope(), envelope()]);
    assert.strictEqual(batch.results.length, 2);
    assert.strictEqual(network.issued(), 1, 'token cached');
    await assert.rejects(publisher.publish(envelope('media')), (err) => err.status === 403 && err.code === 'events.source_mismatch' && err.permanent);
});

t('outbox: enqueue inside a rolled-back transaction publishes nothing; committed publishes exactly once', async () => {
    const db = new Database(path.join(tmpDir(), 'producer.db'));
    db.exec('CREATE TABLE vods (id INTEGER PRIMARY KEY, status TEXT)');
    const outbox = events.createOutbox(db, { publisher: events.createPublisher({ eventsUrl: h.base, tokenClient }) });
    outbox.ensureSchema();

    assert.throws(() => outbox.enqueue(envelope()), /inside the transaction/);

    let rolledBack;
    assert.throws(() => db.transaction(() => {
        db.prepare('INSERT INTO vods (id, status) VALUES (1, ?)').run('ready');
        rolledBack = outbox.enqueue(envelope('live', { event_type: 'live.vod.ready' }));
        throw new Error('domain write failed');
    })(), /domain write failed/);
    assert.strictEqual(outbox.pending(), 0);
    let r = await outbox.flush();
    assert.deepStrictEqual(r, { sent: 0, failed: 0, rejected: 0 });
    assert.strictEqual(countIn(rolledBack.event_id), 0, 'rolled back: never published');

    let committed;
    db.transaction(() => {
        db.prepare('INSERT INTO vods (id, status) VALUES (2, ?)').run('ready');
        committed = outbox.enqueue(envelope('live', { event_type: 'live.vod.ready' }));
    })();
    assert.strictEqual(outbox.pending(), 1);
    r = await outbox.flush();
    assert.strictEqual(r.sent, 1);
    r = await outbox.flush();
    assert.strictEqual(r.sent, 0, 'sent rows are not sent again');
    assert.strictEqual(countIn(committed.event_id), 1);

    // Relay crashed after Events accepted but before the row was marked: at-least-once resend is a duplicate.
    db.prepare('UPDATE event_outbox SET sent_at = NULL WHERE event_id = ?').run(committed.event_id);
    r = await outbox.flush();
    assert.strictEqual(r.sent, 1);
    assert.strictEqual(countIn(committed.event_id), 1, 'still exactly one event');
    assert.ok(db.prepare('SELECT seq FROM event_outbox WHERE event_id = ?').get(committed.event_id).seq > 0);
    db.close();
});

t('outbox: a poisoned row is isolated and rejected; a down Events is retried later', async () => {
    const db = new Database(path.join(tmpDir(), 'producer2.db'));
    let clock = Date.now();
    const outbox = events.createOutbox(db, { publisher: events.createPublisher({ eventsUrl: h.base, tokenClient }), now: () => clock });
    outbox.ensureSchema();
    let good; let bad;
    db.transaction(() => {
        good = outbox.enqueue(envelope());
        bad = outbox.enqueue(envelope('media'));      // live may not publish as media
    })();
    const r = await outbox.flush();
    assert.deepStrictEqual(r, { sent: 1, failed: 0, rejected: 1 });
    assert.strictEqual(countIn(good.event_id), 1);
    assert.ok(db.prepare('SELECT rejected_at FROM event_outbox WHERE event_id = ?').get(bad.event_id).rejected_at);

    const down = events.createOutbox(db, { publisher: events.createPublisher({ eventsUrl: 'http://127.0.0.1:9', tokenClient }), now: () => clock });
    let later;
    db.transaction(() => { later = down.enqueue(envelope()); })();
    const d = await down.flush();
    assert.deepStrictEqual(d, { sent: 0, failed: 1, rejected: 0 });
    assert.strictEqual(down.pending(), 1);
    const row = db.prepare('SELECT * FROM event_outbox WHERE event_id = ?').get(later.event_id);
    assert.strictEqual(row.next_attempt_at, clock + 1000, 'backoff after the first failure');
    // Events is back: the regular relay picks it up once due.
    clock += 1000;
    assert.strictEqual((await outbox.flush()).sent, 1);
    assert.strictEqual(countIn(later.event_id), 1);
    db.close();
});

t('consumer crash mid-processing + redelivery + replay -> exactly one effect (createInbox)', async () => {
    const consumerDb = new Database(path.join(tmpDir(), 'consumer.db'));
    consumerDb.exec('CREATE TABLE credits (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT, amount INTEGER)');
    const inbox = events.createInbox(consumerDb);
    inbox.ensureSchema();
    let secret = null;
    let mode = 'crash-after-commit';
    const stub = await subscriber((call) => {
        if (!events.verifyDelivery(call.rawBody, call.headers['x-openvibe-signature'], secret)) return 401;
        const ev = call.body.event;
        if (mode === 'crash-before-commit') {
            mode = 'ok';
            try {
                inbox.once('media', ev.event_id, () => {
                    consumerDb.prepare('INSERT INTO credits (event_id, amount) VALUES (?, ?)').run(ev.event_id, ev.payload.amount);
                    throw new Error('process died mid-transaction');
                });
            } catch { /* the transaction rolled back */ }
            return 'destroy';
        }
        inbox.once('media', ev.event_id, () => {
            consumerDb.prepare('INSERT INTO credits (event_id, amount) VALUES (?, ?)').run(ev.event_id, ev.payload.amount);
        });
        if (mode === 'crash-after-commit') { mode = 'ok'; return 'destroy'; }  // effect committed, no response sent
        return 204;
    });
    const media = serviceToken('media', ['events.subscription.manage']);
    const sub = (await request(h.base, 'POST', '/api/v1/subscriptions', { token: media, body: { topic_pattern: 'live.tip.*', endpoint: stub.url } })).body;
    secret = sub.secret;
    const live = serviceToken('live', ['events.event.publish']);
    const effects = (id) => consumerDb.prepare('SELECT COUNT(*) AS n FROM credits WHERE event_id = ?').get(id).n;

    for (const first of ['crash-after-commit', 'crash-before-commit']) {
        mode = first;
        const env = envelope('live', { event_type: 'live.tip.sent', payload: { amount: 5 } });
        await request(h.base, 'POST', '/api/v1/events', { token: live, body: env });
        await h.worker.drain();
        const d = h.store.getDelivery(env.event_id, sub.id);
        assert.strictEqual(d.status, 'failed', `${first}: the crashed attempt is retried`);
        assert.strictEqual(effects(env.event_id), first === 'crash-after-commit' ? 1 : 0);
        h.clock.advance(1000);
        await h.worker.drain();
        assert.strictEqual(h.store.getDelivery(env.event_id, sub.id).status, 'delivered');
        assert.strictEqual(effects(env.event_id), 1, `${first}: exactly one effect after redelivery`);

        const rp = await request(h.base, 'POST', '/api/v1/deliveries/replay', { token: admin, body: { subscription_id: sub.id, event_ids: [env.event_id] } });
        assert.strictEqual(rp.body.queued, 1);
        await h.worker.drain();
        assert.strictEqual(effects(env.event_id), 1, `${first}: replay does not repeat the effect`);
    }
    assert.throws(() => inbox.once('media', 'evt_x', () => Promise.resolve()), /synchronous/);
    assert.strictEqual(inbox.seen('media', 'evt_x'), false, 'an async fn is rolled back, not recorded');
    await stub.close();
    consumerDb.close();
});

t('stop', async () => { await h.stop(); network.close(); });

t.run();
