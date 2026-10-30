'use strict';
/**
 * Account export and deletion → Events (ADR-033; openvibe-sdk/account-data).
 *
 * Events is the bus: it stores the events the services publish, and two of its columns name a person — the actor (the
 * SubjectRef of whoever caused the event) and the subject (what the event is about). The payload is the publishing
 * service's record, and the platform's redaction mechanism (server/redaction.js) is what takes a copy of it back.
 *
 *   export   the events the person caused and the events about them: the envelope (id, type, source, subject,
 *            visibility, timestamps), never the payload — that is the publisher's record and may name other people.
 *   erase    the events the person caused and the events about them become tombstones, exactly as a redaction does:
 *            the payload goes and the actor becomes the producer, so no person stays linked to them. The envelope keeps
 *            its id, seq, type, subject and timestamps, so the log stays continuous and the replay tier stays in step.
 *            Both tiers are answered: the hot store and the replay archive (its body holds the actor, so its rows are
 *            opened to find it).
 *
 * Telemetry samples that name a person are covered by the SDK itself (telemetry.json, then subject NULL).
 */
const { createAccountData, TOPICS } = require('openvibe-sdk/account-data');
const archive = require('./archive');
const redaction = require('./redaction');

const TABLES = [];

// The envelope the person gets: the event's place in the log, not what it carried.
const ENVELOPE = ['id', 'seq', 'event_type', 'source', 'subject_type', 'subject_id', 'visibility', 'project_id', 'occurred_at', 'received_at'];
const ROW_LIMIT = 5000;

/** The person in a SubjectRef JSON column: {type: 'user', id: 'usr_…'}. */
function actorIs(row, subject) {
    if (!row || !row.actor) return false;
    let a = null;
    try { a = typeof row.actor === 'string' ? JSON.parse(row.actor) : row.actor; } catch { return false; }
    return Boolean(a) && a.type === 'user' && a.id === subject;
}

/** The event is about the person. */
function subjectIs(row, subject) {
    return row && row.subject_type === 'user' && row.subject_id === subject;
}

/** The events that name the person, in the hot store. */
async function hotRows(db, subject) {
    return await db.many(
        `SELECT ${ENVELOPE.join(', ')} FROM events
         WHERE (actor::jsonb ->> 'type' = 'user' AND actor::jsonb ->> 'id' = $1) OR (subject_type = 'user' AND subject_id = $1)
         ORDER BY received_at DESC LIMIT ${ROW_LIMIT}`, [subject]);
}

/** The events that name the person, in the replay tier: the actor lives in the packed body, so the rows are opened. */
async function archivedRows(db, subject) {
    const out = [];
    for (const a of await db.many('SELECT * FROM events_archive WHERE redacted_at IS NULL')) {
        const r = archive.unpack(a);
        if (actorIs(r, subject) || subjectIs(r, subject)) out.push(r);
    }
    return out;
}

/** The person's part of an account export: the envelope of the events they caused and the events about them. */
async function extraExport(db, subject) {
    const hot = await hotRows(db, subject);
    const files = [];
    if (hot.length) files.push({ name: 'events.json', content: hot });
    const archived = await archivedRows(db, subject);
    if (archived.length) {
        files.push({
            name: 'events-replay.json',
            content: archived.slice(0, ROW_LIMIT).map((r) => Object.fromEntries(ENVELOPE.map((c) => [c, r[c] === undefined ? null : r[c]]))),
        });
    }
    return files;
}

/** Turn the person's events into tombstones, as a redaction does: the payload goes, the actor becomes the producer. */
async function extraErase(t, subjects, counts) {
    const now = Date.now();
    for (const subject of subjects) {
        const by = `account-deletion:${subject}`;
        const hot = await t.many('SELECT * FROM events WHERE (actor::jsonb ->> \'type\' = \'user\' AND actor::jsonb ->> \'id\' = $1) OR (subject_type = \'user\' AND subject_id = $1) FOR UPDATE', [subject]);
        for (const r of hot) {
            const cols = redaction.tombstone(r, { by, at: now });
            await t.exec('UPDATE events SET payload = $1, actor = $2, size_bytes = $3, redacted_at = $4, redacted_by = $5 WHERE id = $6',
                [cols.payload, cols.actor, cols.size_bytes, cols.redacted_at, cols.redacted_by, r.id]);
        }
        counts.add(counts.erased, 'events', hot.length);
        // The replay tier: the body holds the actor, so its rows are opened, tombstoned and packed again.
        const archived = await archivedRows(t, subject);
        for (const r of archived) {
            const cols = redaction.tombstone(r, { by, at: now });
            // The replay tier keeps the redaction's who in the packed body; the table itself has only the marker.
            await t.exec('UPDATE events_archive SET body = $1, redacted_at = $2 WHERE id = $3',
                [archive.pack({ ...r, ...cols }), cols.redacted_at, r.id]);
        }
        counts.add(counts.erased, 'events_replay', archived.length);
    }
}

function create({ db, log = console } = {}) {
    return createAccountData({ db, service: 'events', tables: TABLES, extraExport, extraErase, log });
}

module.exports = { create, TABLES, TOPICS, extraExport, extraErase };
