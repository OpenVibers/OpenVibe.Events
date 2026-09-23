#!/usr/bin/env node
/**
 * One-off backfill: redact the stored chat events of messages OpenVibe.Chat deleted before Chat
 * published chat.message.deleted (server/redaction.js does it for every deletion since).
 *
 *   node scripts/redact-backfill.js --chat-db <chat.db>                             # dry run: counts only
 *   node scripts/redact-backfill.js --chat-db <chat.db> --apply --backup <new file>  # backup, then redact
 *
 * Options:
 *   --chat-db <file>  Chat's database, opened read-only (production: /var/lib/openvibe-chat/chat.db)
 *   --db <file>       the Events database (default: $EVENTS_DB_PATH, else data/events.db; relative
 *                     paths from the repo root; production: /var/lib/openvibe-events/events.db)
 *   --batch <n>       messages per write transaction (default 500, at most 1000)
 *
 * A chat event is redacted when it is a first-party `chat` event about a `chat_message` subject
 * that Chat marks deleted (is_deleted = 1, or an auto-delete time that has passed). It becomes the
 * same tombstone a live chat.message.deleted produces (payload { redacted, redacted_at,
 * redacted_by: "backfill" }, actor service:chat), at the same seq. Messages Chat does not have at
 * all are counted and left alone. Idempotent: redacted events are never selected again, so a
 * second run changes nothing.
 *
 * --apply refuses to run without --backup <file>: a new file, mode 0600, an sqlite online backup of
 * the whole Events database, verified with quick_check and a row count before anything is changed.
 * That backup still holds the text being removed: delete it once the run is verified. Safe while
 * Events is up (short transactions; secure_delete zeroes the old payloads). Run as the service
 * user, with the Events release that knows redaction deployed (it adds the columns).
 */
'use strict';
const fs = require('fs');
const path = require('path');
const Database = require('better-sqlite3');
const { createStore } = require('../server/store');

const ROOT = path.join(__dirname, '..');
const HELP_LINES = 26; // header lines 3..26
const BY = 'backfill';

function parseArgs(argv) {
    const a = { apply: false, backup: null, chatDb: null, db: null, batch: 500 };
    for (let i = 0; i < argv.length; i++) {
        const k = argv[i];
        const val = () => { if (i + 1 >= argv.length) throw new Error(`${k} needs a value`); return argv[++i]; };
        if (k === '--apply') a.apply = true;
        else if (k === '--backup') a.backup = val();
        else if (k === '--chat-db') a.chatDb = val();
        else if (k === '--db') a.db = val();
        else if (k === '--batch') a.batch = Number(val());
        else if (k === '-h' || k === '--help') a.help = true;
        else throw new Error(`unknown option ${k}`);
    }
    if (!Number.isInteger(a.batch) || a.batch < 1 || a.batch > 1000) throw new Error('--batch must be 1..1000');
    return a;
}

/** The database the server uses: --db, else $EVENTS_DB_PATH, else data/events.db (relative to the repo root). */
function defaultDbPath(args, env = process.env) {
    return path.resolve(ROOT, args.db || env.EVENTS_DB_PATH || path.join('data', 'events.db'));
}

const mb = (n) => `${(n / 1048576).toFixed(1)} MB`;
function fileBytes(f) { try { return fs.statSync(f).size; } catch { return 0; } }
function freeBytes(dir) { try { const s = fs.statfsSync(dir); return s.bavail * s.bsize; } catch { return null; } }
const chunks = (list, n) => Array.from({ length: Math.ceil(list.length / n) }, (_, i) => list.slice(i * n, (i + 1) * n));

/** Unredacted first-party chat events about chat messages: [{ id, seq, subject_id }]. */
function candidates(db) {
    return db.prepare(`SELECT id, seq, subject_id FROM events
        WHERE source = 'chat' AND subject_type = 'chat_message' AND project_id IS NULL AND redacted_at IS NULL AND redacts = 0
        ORDER BY seq`).all();
}

/** Split message ids into those Chat marks deleted and those it does not have. */
function classify(chat, messageIds) {
    const deleted = new Set();
    const present = new Set();
    for (const part of chunks(messageIds, 500)) {
        const marks = part.map(() => '?').join(',');
        for (const r of chat.prepare(`SELECT id, is_deleted,
                (auto_delete_at IS NOT NULL AND auto_delete_at <= CURRENT_TIMESTAMP) AS expired
                FROM chat_messages WHERE id IN (${marks})`).all(...part.map(Number))) {
            present.add(String(r.id));
            if (r.is_deleted || r.expired) deleted.add(String(r.id));
        }
    }
    return { deleted: [...deleted], missing: messageIds.filter(id => !present.has(id)) };
}

async function main(argv, log = console.log) {
    let args;
    try { args = parseArgs(argv); } catch (e) { log(`error: ${e.message}`); return 2; }
    if (args.help) { log(fs.readFileSync(__filename, 'utf8').split('\n').slice(2, HELP_LINES).join('\n')); return 0; }
    if (!args.chatDb) { log('error: --chat-db <chat.db> is required'); return 2; }
    const dbPath = defaultDbPath(args);
    const chatPath = path.resolve(args.chatDb);
    for (const f of [dbPath, chatPath]) if (!fs.existsSync(f)) { log(`error: ${f} does not exist`); return 2; }
    if (args.apply && !args.backup) {
        log('refusing --apply without --backup <new file> (an sqlite online backup of the Events database)');
        return 2;
    }

    const db = new Database(dbPath, args.apply ? { fileMustExist: true } : { readonly: true, fileMustExist: true });
    const chat = new Database(chatPath, { readonly: true, fileMustExist: true });
    try {
        db.pragma('busy_timeout = 5000');
        chat.pragma('busy_timeout = 5000');
        const cols = new Set(db.prepare('PRAGMA table_info(events)').all().map(c => c.name));
        if (!cols.has('redacted_at') || !cols.has('redacts')) {
            log('error: this Events database has no redaction columns yet; deploy and start the Events release with redaction first');
            return 2;
        }
        const rows = candidates(db);
        const byMessage = new Map();
        for (const r of rows) {
            if (!byMessage.has(r.subject_id)) byMessage.set(r.subject_id, []);
            byMessage.get(r.subject_id).push(r);
        }
        const { deleted, missing } = classify(chat, [...byMessage.keys()]);
        const toRedact = deleted.flatMap(id => byMessage.get(id)).sort((x, y) => x.seq - y.seq);
        const walBytes = fileBytes(dbPath + '-wal');
        log(`events db  ${dbPath} (${mb(fileBytes(dbPath))} + ${mb(walBytes)} WAL)`);
        log(`chat db    ${chatPath} (read-only)`);
        log(`chat events about messages, not redacted: ${rows.length} events, ${byMessage.size} messages`);
        log(`deleted in Chat: ${deleted.length} messages, ${toRedact.length} events to redact${toRedact.length ? ` (seq ${toRedact[0].seq}..${toRedact.at(-1).seq})` : ''}`);
        log(`not in Chat's database (left alone): ${missing.length} messages`);

        if (!args.apply) {
            log('\ndry run: nothing changed. Re-run with --apply --backup <new file>.');
            return 0;
        }

        const target = path.resolve(args.backup);
        if (fs.existsSync(target)) { log(`error: backup target ${target} already exists; choose a new file`); return 2; }
        const need = fileBytes(dbPath) + walBytes;
        const free = freeBytes(path.dirname(target));
        if (free != null && free < need * 1.1) { log(`error: ${mb(free)} free at ${path.dirname(target)}, the backup needs about ${mb(need)}`); return 2; }
        const total = db.prepare('SELECT COUNT(*) FROM events').pluck().get();
        // A full copy of the event store, deleted text included: owner-only from the first byte.
        const umask = process.umask(0o077);
        try { await db.backup(target); } finally { process.umask(umask); }
        fs.chmodSync(target, 0o600);
        const b = new Database(target, { readonly: true });
        try {
            const ok = b.pragma('quick_check', { simple: true });
            const n = b.prepare('SELECT COUNT(*) FROM events').pluck().get();
            if (ok !== 'ok' || n < total) { log(`error: backup check failed (quick_check=${ok}, events=${n} of ${total}); nothing changed`); return 1; }
        } finally { b.close(); }
        log(`backup     ${target} (${mb(fileBytes(target))}, mode 0600, quick_check ok). It still holds the deleted text: remove it once this run is verified.`);

        db.pragma('secure_delete = ON');
        const store = createStore(db);
        let redacted = 0;
        for (const part of chunks(deleted, args.batch)) {
            redacted += store.redact('chat', { subject_type: 'chat_message', subject_ids: part }, { by: BY }).length;
        }
        log(`redacted   ${redacted} events`);
        try { db.pragma('wal_checkpoint(TRUNCATE)'); } catch { /* the server's next checkpoint does it */ }
        const left = classify(chat, [...new Set(candidates(db).map(r => r.subject_id))]).deleted.length;
        log(`now        ${left} deleted messages with unredacted events`);
        return left ? 1 : 0;
    } finally {
        chat.close();
        db.close();
    }
}

if (require.main === module) {
    main(process.argv.slice(2)).then((code) => process.exit(code), (err) => { console.error(err); process.exit(1); });
}

module.exports = { main, parseArgs, defaultDbPath };
