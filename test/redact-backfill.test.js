'use strict';
/**
 * scripts/redact-backfill.js: redacts the stored chat events of messages Chat deleted before it
 * published chat.message.deleted. Dry run by default, --apply only with a verified --backup,
 * idempotent, first-party chat events only.
 */
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const { ids } = require('openvibe-contracts');
const { openDb, createStore } = require('../server/store');
const { main } = require('../scripts/redact-backfill');
const { tmpDir, envelope, suite } = require('./helpers');

const t = suite('redact-backfill');
const dir = tmpDir();
const eventsPath = path.join(dir, 'events.db');
const chatPath = path.join(dir, 'chat.db');
const user = ids.newId('user');
let store, db;
const ev = {};

function created(messageId, text) {
    return envelope('chat', {
        event_type: 'chat.message.created', visibility: 'public', actor: { type: 'user', id: user },
        subject: { type: 'chat_message', id: String(messageId) }, payload: { message_id: messageId, anon_id: 'anon77', text },
    });
}
async function run(...argv) {
    const lines = [];
    const code = await main(['--db', eventsPath, '--chat-db', chatPath, ...argv], (l) => lines.push(l));
    return { code, out: lines.join('\n') };
}

t('setup: an Events store with chat events, a Chat database with deleted messages', () => {
    db = openDb(eventsPath);
    store = createStore(db);
    const batch = [created(1, 'deleted one'), created(2, 'still here'), created(3, 'expired one'), created(4, 'never in chat'),
        envelope('live', { subject: { type: 'chat_message', id: '1' }, payload: { note: 'live about 1' } })];
    [ev.m1, ev.m2, ev.m3, ev.m4, ev.live] = batch.map(e => e.event_id);
    store.insertBatch(batch.map(e => ({ ...e, trace_id: 'a'.repeat(32), priority: 'important', visibility: e.visibility || 'internal' })), { publisher: 'svc:test' });
    const chat = new Database(chatPath);
    chat.exec(`CREATE TABLE chat_messages (id INTEGER PRIMARY KEY, message TEXT, is_deleted INTEGER DEFAULT 0, auto_delete_at DATETIME);
        INSERT INTO chat_messages (id, message, is_deleted, auto_delete_at) VALUES
            (1, 'deleted one', 1, NULL), (2, 'still here', 0, '2999-01-01 00:00:00'), (3, 'expired one', 0, '2000-01-01 00:00:00');`);
    chat.close();
});

t('dry run: counts, changes nothing', async () => {
    const r = await run();
    assert.strictEqual(r.code, 0, r.out);
    assert.match(r.out, /not redacted: 4 events, 4 messages/);
    assert.match(r.out, /deleted in Chat: 2 messages, 2 events to redact/);
    assert.match(r.out, /not in Chat's database: 1 messages \(left alone; --include-missing redacts them\)/);
    assert.match(r.out, /dry run: nothing changed/);
    for (const id of [ev.m1, ev.m3]) assert.ok(!store.getEvent(id).redacted_at);
});

t('--apply refuses without --backup, and with an existing backup file', async () => {
    let r = await run('--apply');
    assert.strictEqual(r.code, 2);
    assert.match(r.out, /refusing --apply without --backup/);
    const existing = path.join(dir, 'exists.db');
    fs.writeFileSync(existing, 'x');
    r = await run('--apply', '--backup', existing);
    assert.strictEqual(r.code, 2);
    assert.match(r.out, /already exists/);
    assert.ok(!store.getEvent(ev.m1).redacted_at, 'nothing changed');
    r = await main(['--db', eventsPath], () => {});
    assert.strictEqual(r, 2, '--chat-db is required');
});

t('--apply --backup: verified 0600 backup first, then exactly the deleted messages are redacted', async () => {
    const backup = path.join(dir, 'backup-1.db');
    const r = await run('--apply', '--backup', backup);
    assert.strictEqual(r.code, 0, r.out);
    assert.match(r.out, /redacted {3}2 events/);
    assert.strictEqual(fs.statSync(backup).mode & 0o777, 0o600);
    const b = new Database(backup, { readonly: true });
    assert.match(b.prepare('SELECT payload FROM events WHERE id = ?').get(ev.m1).payload, /deleted one/, 'the backup is the state before');
    b.close();
    for (const id of [ev.m1, ev.m3]) {
        const row = store.getEvent(id);
        assert.strictEqual(row.redacted_by, 'backfill');
        assert.deepStrictEqual(Object.keys(JSON.parse(row.payload)).sort(), ['redacted', 'redacted_at', 'redacted_by']);
        assert.deepStrictEqual(JSON.parse(row.actor), { type: 'service', id: 'chat' });
    }
    assert.match(store.getEvent(ev.m2).payload, /still here/, 'a live message is untouched');
    assert.match(store.getEvent(ev.m4).payload, /never in chat/, 'unknown messages are left alone');
    assert.ok(!store.getEvent(ev.live).redacted_at, "another source's event about the same id is untouched");
    const seqs = db.prepare('SELECT seq FROM events ORDER BY seq').pluck().all();
    assert.deepStrictEqual(seqs, [1, 2, 3, 4, 5], 'no row removed, sequence intact');
});

t('--include-missing: a message Chat no longer has (hard-deleted) is redacted too', async () => {
    const r = await run('--apply', '--include-missing', '--backup', path.join(dir, 'backup-missing.db'));
    assert.strictEqual(r.code, 0, r.out);
    assert.match(r.out, /not in Chat's database: 1 messages \(redacted: --include-missing\)/);
    const row = store.getEvent(ev.m4);
    assert.strictEqual(row.redacted_by, 'backfill');
    assert.ok(!/never in chat/.test(row.payload));
    assert.match(store.getEvent(ev.m2).payload, /still here/, 'a live message is still untouched');
});

t('idempotent: a second run finds nothing and changes nothing', async () => {
    const before = db.prepare('SELECT id, payload, redacted_at FROM events ORDER BY seq').all();
    const r = await run('--apply', '--backup', path.join(dir, 'backup-2.db'));
    assert.strictEqual(r.code, 0, r.out);
    assert.match(r.out, /deleted in Chat: 0 messages, 0 events to redact/);
    assert.match(r.out, /redacted {3}0 events/);
    assert.deepStrictEqual(db.prepare('SELECT id, payload, redacted_at FROM events ORDER BY seq').all(), before);
});

t('refuses a database from before redaction', async () => {
    const old = path.join(dir, 'old.db');
    const o = new Database(old);
    o.exec('CREATE TABLE events (id TEXT PRIMARY KEY, seq INTEGER, payload TEXT)');
    o.close();
    const lines = [];
    const code = await main(['--db', old, '--chat-db', chatPath], (l) => lines.push(l));
    assert.strictEqual(code, 2);
    assert.match(lines.join('\n'), /no redaction columns/);
});

t('close', () => { db.close(); });

t.run();
