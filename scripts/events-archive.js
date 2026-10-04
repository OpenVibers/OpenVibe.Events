#!/usr/bin/env node
'use strict';
/**
 * The archive tier (ADR-042 decision 8): monthly NDJSON objects outside the database, written and read back only by
 * this operator job. Nothing runs it automatically and no online path reads an object.
 *
 *   node scripts/events-archive.js export  --month YYYY-MM [--older-than-days 90]
 *   node scripts/events-archive.js restore --month YYYY-MM [--hold-days 30]
 *   node scripts/events-archive.js status
 *
 * export   the replay-tier rows (events_archive) received in that closed month and older than --older-than-days go
 *          into one object, <YYYY>/events-<YYYY-MM>.ndjson.gz (one event row per line: the hot row's exact column
 *          values — payload and actor as their stored JSON text — plus its epoch and archived_at), and a manifest
 *          beside it, <YYYY>/events-<YYYY-MM>.json ({ format, month, count, sha256, bytes, exported_at }). The rows
 *          are deleted from events_archive only after the object was read back and matched the manifest's count and
 *          sha256, and only those still as exported (a row redacted meanwhile stays; rerun the export). A rerun, or an
 *          export of what is left of a month, merges into the month's object: rows already in it are kept, a row
 *          still in the database replaces its line.
 * restore  reads the month's object back (its sha256 and count checked against the manifest first) into
 *          events_archive, idempotent on event_id, held for --hold-days from now: the replay prune leaves a restored
 *          row alone until then, whatever its age. Pulls see restored rows of the current epoch.
 * status   rows per month in the replay tier, and the hot store's oldest event.
 *
 * Storage: scripts/archive-storage.js — EVENTS_ARCHIVE_DIR (local directory, default data/archive), or an
 * S3-compatible bucket when EVENTS_ARCHIVE_S3_BUCKET is set. The database is DATABASE_URL (PostgreSQL, ADR-035).
 * Export and restore of one month are serialised by a PostgreSQL advisory lock.
 */
const crypto = require('crypto');
const zlib = require('zlib');
const archive = require('../server/archive');

const DAY_MS = 24 * 60 * 60 * 1000;
const FORMAT = 'openvibe.events.archive@1';
const LINE = [...archive.COLUMNS, ...archive.BODY, 'epoch', 'archived_at'];
const CHUNK = 1000;

const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');

/** [start, end) of a UTC month, in ms. Throws on anything but YYYY-MM. */
function monthRange(month) {
    const m = /^(\d{4})-(0[1-9]|1[0-2])$/.exec(String(month || ''));
    if (!m) throw new Error('--month must be YYYY-MM');
    return [Date.UTC(Number(m[1]), Number(m[2]) - 1, 1), Date.UTC(Number(m[1]), Number(m[2]), 1)];
}
const objectKey = (month) => `${month.slice(0, 4)}/events-${month}.ndjson.gz`;
const manifestKey = (month) => `${month.slice(0, 4)}/events-${month}.json`;

/** An events_archive row → its NDJSON line object, keys in a fixed order. */
function toLine(a) {
    const row = archive.unpack(a);
    const out = {};
    for (const k of LINE) out[k] = k === 'archived_at' ? a.archived_at : row[k];
    return out;
}

/** The month's object as line objects, after checking it against its manifest; null when there is none. */
async function readObject(storage, month) {
    const [manifestBuf, gz] = [await storage.get(manifestKey(month)), await storage.get(objectKey(month))];
    if (!manifestBuf && !gz) return null;
    if (!manifestBuf || !gz) throw new Error(`archive ${month}: object and manifest must both exist (found only the ${gz ? 'object' : 'manifest'})`);
    const manifest = JSON.parse(manifestBuf.toString('utf8'));
    if (manifest.format !== FORMAT || manifest.month !== month) throw new Error(`archive ${month}: the manifest is not a ${FORMAT} manifest of this month`);
    if (sha256(gz) !== manifest.sha256) throw new Error(`archive ${month}: sha256 of the object does not match its manifest`);
    const text = zlib.gunzipSync(gz).toString('utf8');
    const lines = text ? text.split('\n').filter(Boolean).map((l) => JSON.parse(l)) : [];
    if (lines.length !== manifest.count) throw new Error(`archive ${month}: the object holds ${lines.length} rows, its manifest says ${manifest.count}`);
    return { manifest, lines };
}

/** Serialise export and restore of one month across processes (inside the caller's transaction). */
const lockMonth = (db, month) => db.prepare("SELECT pg_advisory_xact_lock(hashtext('events-archive:' || ?)) AS ok").get(month);

/**
 * Export `month` (see the header). Returns { month, exported, count, deleted, changed, object } — exported: rows read
 * from the database this run; count: rows in the object; deleted: rows removed from events_archive; changed: rows
 * left in place because they changed (were redacted) after they were read.
 */
async function exportMonth({ db, storage, month, now = Date.now(), olderThanDays = 90 }) {
    const [start, end] = monthRange(month);
    if (end > now) throw new Error(`archive ${month}: the month has not ended yet`);
    if (!(Number.isInteger(olderThanDays) && olderThanDays >= 0)) throw new Error('--older-than-days must be an integer >= 0');
    const cutoff = Math.min(end, now - olderThanDays * DAY_MS);
    return await db.tx(async () => {
        await lockMonth(db, month);
        const fresh = [];
        if (cutoff > start) {
            const page = db.prepare(`SELECT * FROM events_archive WHERE received_at >= ? AND received_at < ? AND (epoch, seq) > (?, ?)
                ORDER BY epoch, seq LIMIT ?`);
            let at = [-1, -1];
            for (;;) {
                const rows = await page.all(start, cutoff, at[0], at[1], CHUNK);
                for (const r of rows) fresh.push(toLine(r));
                if (rows.length < CHUNK) break;
                at = [rows[rows.length - 1].epoch, rows[rows.length - 1].seq];
            }
        }
        const existing = await readObject(storage, month);
        if (!fresh.length && !existing) return { month, exported: 0, count: 0, deleted: 0, changed: 0, object: null };
        const merged = new Map((existing ? existing.lines : []).map((l) => [l.id, l]));
        for (const l of fresh) merged.set(l.id, l);
        const lines = [...merged.values()].sort((x, y) => x.epoch - y.epoch || x.seq - y.seq);
        const gz = zlib.gzipSync(Buffer.from(lines.map((l) => `${JSON.stringify(l)}\n`).join(''), 'utf8'));
        const manifest = { format: FORMAT, month, count: lines.length, sha256: sha256(gz), bytes: gz.length, exported_at: new Date(now).toISOString() };
        await storage.put(objectKey(month), gz);
        await storage.put(manifestKey(month), Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`));

        // Verified before anything is deleted: the object as stored has the manifest's sha256 and count, and every
        // row read from the database this run is in it, line for line.
        const back = await readObject(storage, month);
        if (!back || back.manifest.sha256 !== manifest.sha256) throw new Error(`archive ${month}: the stored object is not the one written; nothing deleted`);
        const stored = new Map(back.lines.map((l) => [l.id, JSON.stringify(l)]));
        for (const l of fresh) {
            if (stored.get(l.id) !== JSON.stringify(l)) throw new Error(`archive ${month}: row ${l.id} is missing from the stored object; nothing deleted`);
        }

        // Delete only what is still as exported: a row redacted since it was read keeps its (tombstoned) replay copy.
        let deleted = 0;
        let changed = 0;
        const lock = db.prepare('SELECT id, redacted_at FROM events_archive WHERE id = ANY(?) FOR UPDATE');
        const drop = db.prepare('DELETE FROM events_archive WHERE id = ANY(?)');
        for (let i = 0; i < fresh.length; i += CHUNK) {
            const part = fresh.slice(i, i + CHUNK);
            const exported = new Map(part.map((l) => [l.id, l.redacted_at ?? null]));
            const current = await lock.all(part.map((l) => l.id));
            const same = current.filter((r) => (r.redacted_at ?? null) === exported.get(r.id)).map((r) => r.id);
            changed += current.length - same.length;
            if (same.length) deleted += (await drop.run(same)).changes;
        }
        return { month, exported: fresh.length, count: lines.length, deleted, changed, object: objectKey(month) };
    });
}

/** Restore `month` into events_archive (see the header). Returns { month, count, inserted, skipped }. */
async function restoreMonth({ db, storage, month, now = Date.now(), holdDays = 30 }) {
    monthRange(month);
    if (!(Number.isInteger(holdDays) && holdDays >= 1)) throw new Error('--hold-days must be an integer >= 1');
    return await db.tx(async () => {
        await lockMonth(db, month);
        const obj = await readObject(storage, month);
        if (!obj) throw new Error(`archive ${month}: no object in ${storage.name}`);
        const put = db.prepare(`INSERT INTO events_archive (${archive.COLUMNS.join(', ')}, epoch, body, archived_at, hold_until)
            VALUES (${archive.COLUMNS.map(() => '?').join(', ')}, ?, ?, ?, ?) ON CONFLICT (id) DO NOTHING`);
        let inserted = 0;
        for (const l of obj.lines) {
            inserted += (await put.run(...archive.COLUMNS.map((c) => l[c]), l.epoch, archive.pack(l), l.archived_at, now + holdDays * DAY_MS)).changes;
        }
        return { month, count: obj.lines.length, inserted, skipped: obj.lines.length - inserted };
    });
}

/** Rows per UTC month in the replay tier, and the hot store's oldest event. */
async function status({ db }) {
    const months = await db.prepare(`SELECT to_char(to_timestamp(received_at / 1000.0) AT TIME ZONE 'UTC', 'YYYY-MM') AS month,
        COUNT(*)::bigint AS rows, MIN(seq) AS first_seq, MAX(seq) AS last_seq, COUNT(hold_until)::bigint AS held
        FROM events_archive GROUP BY 1 ORDER BY 1`).all();
    const hot = await db.prepare('SELECT MIN(seq) AS oldest_seq, MIN(received_at) AS oldest_received_at, COUNT(*)::bigint AS rows FROM events').get();
    return { replay: months, hot };
}

async function main(argv = process.argv.slice(2)) {
    const [command, ...rest] = argv;
    const opt = (k, d) => { const i = rest.indexOf(k); return i >= 0 ? rest[i + 1] : d; };
    if (!['export', 'restore', 'status'].includes(command)) {
        console.error('usage: events-archive.js export --month YYYY-MM [--older-than-days 90] | restore --month YYYY-MM [--hold-days 30] | status');
        return 2;
    }
    require('dotenv').config();
    const { load } = require('../server/config');
    const { openDb } = require('../server/store');
    const { fromEnv } = require('./archive-storage');
    const db = await openDb(load(), { log: { log() {}, warn: console.warn } });
    try {
        const storage = fromEnv(process.env);
        let r;
        if (command === 'export') r = await exportMonth({ db, storage, month: opt('--month'), olderThanDays: Number(opt('--older-than-days', '90')) });
        else if (command === 'restore') r = await restoreMonth({ db, storage, month: opt('--month'), holdDays: Number(opt('--hold-days', '30')) });
        else r = await status({ db });
        console.log(JSON.stringify({ storage: command === 'status' ? undefined : storage.name, ...r }, null, 2));
        return 0;
    } finally {
        await db.close();
    }
}

if (require.main === module) {
    main().then((code) => process.exit(code), (err) => { console.error(`events-archive: ${err.message}`); process.exit(1); });
}

module.exports = { exportMonth, restoreMonth, status, monthRange, objectKey, manifestKey, FORMAT };
