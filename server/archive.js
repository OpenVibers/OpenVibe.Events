'use strict';
/**
 * The replay tier's row shape (ADR-042 decision 8; migrations/0006_events_archive.sql). An archived row keeps as columns
 * what the read path filters and orders on (COLUMNS); every other column of the hot row (BODY) is one gzip'd JSON
 * object in `body`. unpack() gives back the hot row's exact column values — actor and payload are the stored JSON
 * text, not re-serialised — so rowToEnvelope, the visibility and redaction rules and the export see one shape for
 * both tiers. Compression is node:zlib gzip: no dependency, and the export (scripts/events-archive.js) uses it too.
 */
const zlib = require('zlib');

/** Columns events_archive keeps as they are in `events`. */
const COLUMNS = ['id', 'seq', 'event_type', 'source', 'subject_type', 'subject_id', 'visibility', 'project_id', 'env', 'received_at', 'redacted_at', 'redacts'];
/** Columns of `events` that live in the compressed body. */
const BODY = ['version', 'actor', 'subject_revision', 'trace_id', 'priority', 'occurred_at', 'payload', 'hops', 'publisher', 'request_id', 'size_bytes', 'redacted_by'];

/** The compressed body of a hot row (or of an unpacked one). */
function pack(row) {
    const body = {};
    for (const c of BODY) body[c] = row[c] === undefined ? null : row[c];
    return zlib.gzipSync(Buffer.from(JSON.stringify(body), 'utf8'));
}

/** An events_archive row → the hot row it was, plus the epoch its position belongs to. */
function unpack(a) {
    const body = JSON.parse(zlib.gunzipSync(a.body).toString('utf8'));
    const row = {};
    for (const c of COLUMNS) row[c] = a[c];
    for (const c of BODY) row[c] = body[c] === undefined ? null : body[c];
    row.epoch = a.epoch;
    return row;
}

module.exports = { pack, unpack, COLUMNS, BODY };
