'use strict';
// Tiered retention (ADR-042 decision 8): hot (`events`) → replay (`events_archive`, same database) → archive (monthly
// NDJSON objects, scripts/events-archive.js). Reads span hot and replay with one cursor; `gap` only outside both.
const assert = require('assert');
const http = require('http');
const zlib = require('zlib');
const { ids } = require('openvibe-contracts');
const apps = require('../server/apps');
const cursor = require('../server/cursor');
const { load } = require('../server/config');
const { limitsOf } = require('../server/limits');
const { createStore } = require('../server/store');
const { exportMonth, restoreMonth, objectKey, manifestKey } = require('../scripts/events-archive');
const { localStorage, s3Storage, fromEnv, signV4 } = require('../scripts/archive-storage');
const { boot, request, serviceToken, appToken, envelope, sse, suite, tmpDir, cursorAt } = require('./helpers');

const t = suite('archive');
const DAY = 86400000;
let h;
const live = serviceToken('live', ['events.event.publish']);
const media = serviceToken('media', ['events.event.publish']);
const reader = serviceToken('games', ['events.event.read']);
const T0 = Date.now();
const later = (days) => T0 + days * DAY;
const tiered = (days, over = {}) => ({ retentionDays: 30, replayRetentionDays: 365, now: later(days), ...over });
const open = [];

async function publish(token, body) {
    const r = await request(h.base, 'POST', '/api/v1/events', { token, body });
    assert.strictEqual(r.status, 201, r.text);
    return r.body;
}
const pull = async (q, token = reader) => await request(h.base, 'GET', `/api/v1/events?${q}`, { token });
const count = async (table, id) => (await h.db.prepare(`SELECT COUNT(*)::int AS n FROM ${table} WHERE id = ?`).get(id)).n;

t('boot', async () => { h = await boot(); });

t('a hot-pruned event is readable through replay: by id and by pull, unchanged', async () => {
    const e = await publish(live, envelope('live', { event_type: 'live.tier.one', visibility: 'public', payload: { n: 1, s: 'é ✓' } }));
    const before = await request(h.base, 'GET', `/api/v1/events/${e.event_id}`, { token: reader });
    const r = await h.store.prune(tiered(31));
    assert.ok(r.archived >= 1 && r.events >= r.archived, JSON.stringify(r));
    assert.deepStrictEqual([await count('events', e.event_id), await count('events_archive', e.event_id)], [0, 1], 'moved, not copied');
    const after = await request(h.base, 'GET', `/api/v1/events/${e.event_id}`, { token: reader });
    assert.strictEqual(after.status, 200, after.text);
    assert.deepStrictEqual(after.body, before.body, 'the same seq, cursor and envelope from the replay tier');
    const p = await pull('topic=live.tier.one');
    assert.deepStrictEqual(p.body.events.map(x => x.event), [before.body.event]);
    assert.strictEqual(p.body.gap, undefined);
    // A publish of the same id is a duplicate with its position, never a second row.
    const again = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_id: e.event_id, event_type: 'live.tier.one' }) });
    assert.strictEqual(again.status, 200, again.text);
    assert.deepStrictEqual([again.body.duplicate, again.body.seq, again.body.cursor], [true, e.seq, e.cursor]);
});

t('a cursor across the hot/replay boundary reports no false gap (pull and SSE)', async () => {
    const a = await publish(live, envelope('live', { event_type: 'live.tier.cross' }));
    const b = await publish(live, envelope('live', { event_type: 'live.tier.cross' }));
    await h.store.prune(tiered(31));
    assert.strictEqual(await count('events_archive', b.event_id), 1);
    const c = await publish(live, envelope('live', { event_type: 'live.tier.cross' }));

    let r = await pull(`topic=live.tier.cross&after=${a.cursor}&limit=1`);
    assert.deepStrictEqual([r.body.events.map(x => x.seq), r.body.gap], [[b.seq], undefined], 'replay');
    assert.strictEqual(r.body.events[0].cursor, b.cursor);
    r = await pull(`topic=live.tier.cross&after=${r.body.next_cursor}&limit=1`);
    assert.deepStrictEqual([r.body.events.map(x => x.seq), r.body.gap], [[c.seq], undefined], 'then hot, no gap between');
    r = await pull(`topic=*&after=${await cursorAt(h, a.seq - 1)}`);
    assert.deepStrictEqual(r.body.events.map(x => x.seq), [a.seq, b.seq, c.seq]);
    assert.strictEqual(r.body.gap, undefined);

    const s = await sse(h.base, '/realtime/stream?topics=live.tier.cross', { headers: { Authorization: `Bearer ${serviceToken('live', ['events.event.read'])}`, 'Last-Event-ID': a.cursor } });
    open.push(s);
    await s.waitFor(x => x.events().length === 2);
    assert.deepStrictEqual(s.events().map(x => x.seq), [b.seq, c.seq]);
    assert.deepStrictEqual(s.gaps(), []);
    assert.ok(s.messages.some(m => m.id === b.cursor), 'the SSE id of a replayed event is its cursor');
    s.close();
});

t('redaction reaches replay rows (by id and by subject); the owner rule holds there too', async () => {
    const x = await publish(live, envelope('live', { event_type: 'live.tier.secret', subject: { type: 'stream', id: 'tier-red-1' }, payload: { secret: 'a' } }));
    const y = await publish(live, envelope('live', { event_type: 'live.tier.secret', subject: { type: 'stream', id: 'tier-red-2' }, payload: { secret: 'b' } }));
    await h.store.prune(tiered(31));
    assert.strictEqual(await count('events_archive', x.event_id) + await count('events_archive', y.event_id), 2);
    const refused = await request(h.base, 'POST', '/api/v1/events', { token: media, body: envelope('media', { event_type: 'media.tier.redact', payload: { redacts: { event_ids: [x.event_id] } } }) });
    assert.deepStrictEqual([refused.status, refused.body.code], [403, 'events.redaction_not_allowed']);
    await publish(live, envelope('live', { event_type: 'live.tier.redact', payload: { redacts: { event_ids: [y.event_id], subject_type: 'stream', subject_ids: ['tier-red-1'] } } }));
    for (const e of [x, y]) {
        const r = await request(h.base, 'GET', `/api/v1/events/${e.event_id}`, { token: reader });
        assert.strictEqual(r.body.event.payload.redacted, true, JSON.stringify(r.body));
        assert.deepStrictEqual(r.body.event.actor, { type: 'service', id: 'live' });
        assert.strictEqual(r.body.seq, e.seq);
    }
    const p = await pull('topic=live.tier.secret');
    assert.ok(p.body.events.every(e => e.event.payload.redacted === true && !('secret' in e.event.payload)));
});

t('sandbox and environment rules apply to replay rows exactly as to hot ones', async () => {
    const prj = `prj_${ids.ulid()}`;
    const [prodApp, sandApp] = [ids.newId('app'), ids.newId('app')];
    const cap = ['events.app.publish', 'events.app.read'];
    const prod = appToken({ appId: prodApp, projectId: prj, env: 'production', cap });
    const sand = appToken({ appId: sandApp, projectId: prj, env: 'sandbox', cap });
    const appEvent = (appId) => envelope('x', { source: apps.appSource(appId), event_type: `app.${apps.projectKey(prj)}.order.created`, actor: { type: 'app', id: appId }, subject: { type: 'order', id: '1' }, visibility: 'internal' });
    const pe = await publish(prod, appEvent(prodApp));
    const se = await publish(sand, appEvent(sandApp));
    await h.store.prune(tiered(31, { sandboxRetentionDays: 7 }));
    assert.deepStrictEqual([await count('events_archive', pe.event_id), await count('events_archive', se.event_id), await count('events', se.event_id)], [1, 0, 0],
        'a production event moves; a sandbox event is deleted, never kept in replay');
    const topic = `topic=app.${apps.projectKey(prj)}.*`;
    assert.deepStrictEqual((await pull(topic, prod)).body.events.map(e => e.seq), [pe.seq], 'the app reads its own replay rows');
    assert.deepStrictEqual((await pull(topic, sand)).body.events, [], 'never across environments');
    assert.deepStrictEqual((await pull(topic)).body.events.map(e => e.seq), [pe.seq], 'first-party: production app events through app.*');
    assert.deepStrictEqual((await pull('topic=*')).body.events.filter(e => e.event.event_id === pe.event_id), [], '…and only through app.*');
    assert.strictEqual((await request(h.base, 'GET', `/api/v1/events/${pe.event_id}`, { token: sand })).status, 404);
    assert.strictEqual((await request(h.base, 'GET', `/api/v1/events/${pe.event_id}`, { token: prod })).status, 200);
});

t('two concurrent prunes move each event exactly once', async () => {
    await h.store.prune(tiered(31));   // the other tests' hot rows first: the race below is over these 60 only
    const mine = [];
    for (let i = 0; i < 60; i++) mine.push((await publish(live, envelope('live', { event_type: 'live.tier.race', payload: { i } }))).event_id);
    // Two stores on one database: separate pooled connections, as two Events processes would have.
    const s1 = createStore(h.db, { clock: h.clock });
    const s2 = createStore(h.db, { clock: h.clock });
    const [r1, r2] = await Promise.all([s1.prune(tiered(31, { batchSize: 7 })), s2.prune(tiered(31, { batchSize: 7 }))]);
    assert.strictEqual(r1.archived + r2.archived, 60, JSON.stringify([r1, r2]));
    const rows = await h.db.prepare('SELECT id, seq FROM events_archive WHERE id = ANY(?) ORDER BY seq').all(mine);
    assert.deepStrictEqual(rows.map(r => r.id), mine, 'every one in replay, once, in order');
    assert.strictEqual((await h.db.prepare('SELECT COUNT(*)::int AS n FROM events WHERE id = ANY(?)').get(mine)).n, 0);
    const again = await h.store.prune(tiered(31));
    assert.strictEqual(again.archived, 0, 'nothing left to move');
});

t('export → restore round-trips byte-identical rows; the rows leave replay only after the object verifies', async () => {
    const storage = localStorage({ dir: tmpDir() });
    const month = new Date(h.clock.now()).toISOString().slice(0, 7);
    const exportAt = later(400);   // the month is closed and older than the cutoff
    const inMonth = await h.db.prepare('SELECT * FROM events_archive ORDER BY seq').all();
    assert.ok(inMonth.length >= 60);
    const before = await Promise.all(inMonth.map(r => h.store.getEvent(r.id)));

    const r = await exportMonth({ db: h.db, storage, month, now: exportAt, olderThanDays: 90 });
    assert.deepStrictEqual([r.exported, r.count, r.deleted, r.changed], [inMonth.length, inMonth.length, inMonth.length, 0]);
    assert.strictEqual((await h.db.prepare('SELECT COUNT(*)::int AS n FROM events_archive').get()).n, 0);
    const lines = zlib.gunzipSync(await storage.get(objectKey(month))).toString('utf8').trim().split('\n').map(l => JSON.parse(l));
    assert.strictEqual(lines.length, inMonth.length);
    assert.strictEqual(JSON.parse(await storage.get(manifestKey(month))).count, inMonth.length);
    assert.strictEqual((await request(h.base, 'GET', `/api/v1/events/${inMonth[0].id}`, { token: reader })).status, 404, 'the archive tier is offline');
    const gapped = await pull(`topic=*&after=${await cursorAt(h, 0)}`);
    assert.ok(gapped.body.gap, 'a cursor into what went to the archive tier is a gap');

    // A rerun is idempotent: the object keeps its rows, nothing is deleted twice.
    const rerun = await exportMonth({ db: h.db, storage, month, now: exportAt, olderThanDays: 90 });
    assert.deepStrictEqual([rerun.exported, rerun.count, rerun.deleted], [0, inMonth.length, 0]);

    const back = await restoreMonth({ db: h.db, storage, month, now: exportAt, holdDays: 30 });
    assert.deepStrictEqual([back.count, back.inserted, back.skipped], [inMonth.length, inMonth.length, 0]);
    assert.deepStrictEqual((await restoreMonth({ db: h.db, storage, month, now: exportAt })).inserted, 0, 'idempotent on event_id');
    const after = await Promise.all(inMonth.map(r => h.store.getEvent(r.id)));
    assert.deepStrictEqual(after, before, 'every column, payload and actor text byte for byte');
    const restored = await h.db.prepare('SELECT * FROM events_archive ORDER BY seq').all();
    for (let i = 0; i < restored.length; i++) {
        assert.strictEqual(zlib.gunzipSync(restored[i].body).toString('utf8'), zlib.gunzipSync(inMonth[i].body).toString('utf8'));
        assert.deepStrictEqual({ ...restored[i], body: null, hold_until: null }, { ...inMonth[i], body: null, hold_until: null });
    }
    // Held: a replay prune leaves restored rows alone until hold_until, whatever their age.
    const held = async () => (await h.db.prepare('SELECT COUNT(*)::int AS n FROM events_archive WHERE id = ANY(?)').get(inMonth.map(x => x.id))).n;
    await h.store.prune({ ...tiered(0), now: exportAt });
    assert.strictEqual(await held(), inMonth.length);
    await h.store.prune({ ...tiered(0), now: exportAt + 31 * DAY });
    assert.strictEqual(await held(), 0);

    // A tampered object is refused before anything is restored.
    const gz = await storage.get(objectKey(month));
    gz[gz.length - 5] ^= 0xff;
    await storage.put(objectKey(month), gz);
    await assert.rejects(restoreMonth({ db: h.db, storage, month, now: exportAt }), /sha256/);
    await assert.rejects(exportMonth({ db: h.db, storage, month: '2999-01', now: exportAt }), /not ended/);
});

t('replay pruning answers `gap` (the existing shape), pull and SSE', async () => {
    const a = await publish(live, envelope('live', { event_type: 'live.tier.gone', visibility: 'public' }));
    await publish(live, envelope('live', { event_type: 'live.tier.gone', visibility: 'public' }));
    await h.store.prune(tiered(31));
    const r0 = await pull(`topic=live.tier.gone&after=${a.cursor}`);
    assert.strictEqual(r0.body.gap, undefined, 'still in replay: no gap');
    await h.store.prune(tiered(366));
    const kept = await publish(live, envelope('live', { event_type: 'live.tier.gone' }));
    const r = await pull(`topic=live.tier.gone&after=${a.cursor}`);
    assert.deepStrictEqual(r.body.gap, { from_seq: a.seq + 1, to_seq: kept.seq - 1 });
    assert.deepStrictEqual(r.body.events.map(e => e.seq), [kept.seq]);
    const s = await sse(h.base, '/realtime/stream?topics=live.tier.gone', { headers: { Authorization: `Bearer ${serviceToken('live', ['events.event.read'])}`, 'Last-Event-ID': a.cursor } });
    open.push(s);
    await s.waitFor(x => x.events().length === 1);
    assert.deepStrictEqual(s.gaps().map(g => [g.reason, g.from_seq, g.to_seq]), [['retention', a.seq + 1, kept.seq - 1]]);
    s.close();
});

t('EVENTS_REPLAY_RETENTION_DAYS=0 behaves exactly as before the tiers', async () => {
    assert.strictEqual(load({}).replayRetentionDays, 365);
    assert.strictEqual(load({ EVENTS_REPLAY_RETENTION_DAYS: '0' }).replayRetentionDays, 0);
    const x = await boot({ env: { EVENTS_REPLAY_RETENTION_DAYS: '0' } });
    try {
        const pub = async () => (await request(x.base, 'POST', '/api/v1/events', { token: live, body: envelope('live', { event_type: 'live.tier.zero' }) })).body;
        const old = await pub();
        // A leftover replay row (the tier was on before) is emptied by the next prune: no rule would prune it otherwise.
        await x.store.prune({ ...tiered(31), replayRetentionDays: 365 });
        assert.strictEqual((await x.db.prepare('SELECT COUNT(*)::int AS n FROM events_archive').get()).n, 1);
        const r = await x.store.prune({ retentionDays: x.config.retentionDays, replayRetentionDays: x.config.replayRetentionDays, now: later(31) });
        assert.deepStrictEqual([r.archived, r.replay], [0, 1]);
        const old2 = await pub();
        const r2 = await x.store.prune({ retentionDays: 30, replayRetentionDays: 0, now: later(31) });
        assert.deepStrictEqual([r2.events, r2.archived], [1, 0], 'deleted, not moved');
        assert.strictEqual((await x.db.prepare('SELECT COUNT(*)::int AS n FROM events_archive').get()).n, 0);
        const kept = await pub();
        const p = await request(x.base, 'GET', `/api/v1/events?topic=live.tier.zero&after=${old.cursor}`, { token: reader });
        assert.deepStrictEqual(p.body.gap, { from_seq: old.seq + 1, to_seq: kept.seq - 1 });
        assert.strictEqual((await request(x.base, 'GET', `/api/v1/events/${old2.event_id}`, { token: reader })).status, 404);
        const lim = limitsOf(x.config).limits.find(l => l.id === 'replay_retention_days');
        assert.deepStrictEqual([lim.production, lim.sandbox], [0, 0]);
    } finally {
        await x.stop();
    }
    const lim = limitsOf(h.config).limits.find(l => l.id === 'replay_retention_days');
    assert.deepStrictEqual([lim.production, lim.sandbox, lim.unit], [365, 0, 'days']);
});

t('archive storage: SigV4 matches AWS\'s published vectors; the s3 adapter round-trips through a signing stub', async () => {
    const empty = 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
    assert.match(signV4({ method: 'GET', path: '/', headers: { host: 'example.amazonaws.com', 'x-amz-date': '20150830T123600Z' }, payloadHash: empty,
        region: 'us-east-1', service: 'service', accessKeyId: 'AKIDEXAMPLE', secretAccessKey: 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY', amzDate: '20150830T123600Z' }),
    /Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31$/);
    assert.match(signV4({ method: 'GET', path: '/test.txt', headers: { host: 'examplebucket.s3.amazonaws.com', range: 'bytes=0-9', 'x-amz-content-sha256': empty, 'x-amz-date': '20130524T000000Z' },
        payloadHash: empty, region: 'us-east-1', service: 's3', accessKeyId: 'AKIAIOSFODNN7EXAMPLE', secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', amzDate: '20130524T000000Z' }),
    /SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41$/);

    // A stub S3: checks each request's signature with the same secret and keeps objects in memory.
    const objects = new Map();
    const creds = { accessKeyId: 'AKIDTEST', secretAccessKey: 'secret-test' };
    const server = http.createServer((req, res) => {
        const chunks = [];
        req.on('data', c => chunks.push(c));
        req.on('end', () => {
            const body = Buffer.concat(chunks);
            const headers = { host: req.headers.host, 'x-amz-content-sha256': req.headers['x-amz-content-sha256'], 'x-amz-date': req.headers['x-amz-date'] };
            const want = signV4({ method: req.method, path: req.url, headers, payloadHash: require('crypto').createHash('sha256').update(body).digest('hex'),
                region: 'eu-west-3', service: 's3', ...creds, amzDate: req.headers['x-amz-date'] });
            if (req.headers.authorization !== want) { res.writeHead(403); return res.end(); }
            if (req.method === 'PUT') { objects.set(req.url, body); res.writeHead(200); return res.end(); }
            if (!objects.has(req.url)) { res.writeHead(404); return res.end(); }
            res.writeHead(200); return res.end(objects.get(req.url));
        });
    });
    await new Promise(r => server.listen(0, '127.0.0.1', r));
    try {
        const env = { EVENTS_ARCHIVE_S3_BUCKET: 'ov-events', EVENTS_ARCHIVE_S3_ENDPOINT: `http://127.0.0.1:${server.address().port}`, EVENTS_ARCHIVE_S3_REGION: 'eu-west-3',
            EVENTS_ARCHIVE_S3_ACCESS_KEY_ID: creds.accessKeyId, EVENTS_ARCHIVE_S3_SECRET_ACCESS_KEY: creds.secretAccessKey };
        const s3 = fromEnv(env);
        assert.match(s3.name, /^s3:/);
        await s3.put('2026/events-2026-01.json', Buffer.from('{"a":1}'));
        assert.deepStrictEqual([...objects.keys()], ['/ov-events/events/2026/events-2026-01.json']);
        assert.strictEqual((await s3.get('2026/events-2026-01.json')).toString(), '{"a":1}');
        assert.strictEqual(await s3.get('2026/missing.json'), null);
        await assert.rejects(fromEnv({ ...env, EVENTS_ARCHIVE_S3_SECRET_ACCESS_KEY: 'wrong' }).put('x.json', Buffer.from('1')), /answered 403/);
        assert.throws(() => s3Storage({ ...creds, endpoint: 'http://s3.example.com', bucket: 'b' }), /https/);
        assert.match(fromEnv({ EVENTS_ARCHIVE_DIR: tmpDir() }).name, /^local:/);
        await assert.rejects(s3.put('../escape', Buffer.from('1')), /bad object key/);
    } finally {
        server.close();
    }
});

t('stop', async () => { for (const c of open) c.close(); await h.stop(); });

t.run();
