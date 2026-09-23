'use strict';
/**
 * Redaction: a producer takes back what it published earlier (a deleted chat message, a removed
 * post). Any event may carry a directive in its payload:
 *
 *   "payload": { …, "redacts": { "event_ids": ["evt_…"], "subject_type": "chat_message", "subject_ids": ["123"] } }
 *
 * `event_ids` names events directly; `subject_type` + `subject_ids` names every stored event of the
 * same source about those subjects. Either or both, at most MAX_IDS of each. When the event is
 * stored, every target is rewritten in the same transaction into a tombstone:
 *
 *   payload  -> { redacted: true, redacted_at, redacted_by: <the redacting event_id> }
 *   actor    -> the producer itself (service:<source>, or the app), so no person stays linked to it
 *
 * The envelope keeps its event_id, seq, type, subject, timestamp and visibility, so sequences stay
 * continuous: pull readers and replays get the tombstone at the same seq, and a delivery still
 * queued (or replayed from the DLQ) sends the tombstone, never the original.
 *
 * Authority is the publish rule: only events of the redacting event's own source (and, for a
 * developer app, its own project and environment) can be redacted. Naming another source's event
 * by id refuses the whole publish (403 events.redaction_not_allowed); a subject match never reaches
 * another source. Events that carry a directive themselves are never redacted (they hold only ids),
 * nor is the redacting event.
 */

const MAX_IDS = 1000;
const EVENT_ID_RE = /^evt_[0-9A-HJKMNP-TV-Z]{26}$/;
const SUBJECT_TYPE_RE = /^[a-z][a-z0-9_]{0,63}$/;

/**
 * The directive in `payload`, normalized: { eventIds, subjectType, subjectIds }; null when there is
 * none; { error } when it is malformed.
 */
function parseDirective(payload) {
    if (!payload || typeof payload !== 'object' || !Object.prototype.hasOwnProperty.call(payload, 'redacts')) return null;
    const r = payload.redacts;
    if (!r || typeof r !== 'object' || Array.isArray(r)) return { error: 'payload.redacts must be an object' };
    const extra = Object.keys(r).filter(k => !['event_ids', 'subject_type', 'subject_ids'].includes(k));
    if (extra.length) return { error: `payload.redacts: unknown field ${extra[0]}` };
    const eventIds = r.event_ids === undefined ? [] : r.event_ids;
    if (!Array.isArray(eventIds) || eventIds.length > MAX_IDS || !eventIds.every(id => typeof id === 'string' && EVENT_ID_RE.test(id))) {
        return { error: `payload.redacts.event_ids must be at most ${MAX_IDS} event ids (evt_…)` };
    }
    const hasType = r.subject_type !== undefined;
    const hasIds = r.subject_ids !== undefined;
    if (hasType !== hasIds) return { error: 'payload.redacts: subject_type and subject_ids go together' };
    let subjectType = null;
    let subjectIds = [];
    if (hasType) {
        if (typeof r.subject_type !== 'string' || !SUBJECT_TYPE_RE.test(r.subject_type)) return { error: 'payload.redacts.subject_type must be a subject type ([a-z][a-z0-9_]*)' };
        if (!Array.isArray(r.subject_ids) || r.subject_ids.length > MAX_IDS
            || !r.subject_ids.every(id => typeof id === 'string' && id.length > 0 && id.length <= 200)) {
            return { error: `payload.redacts.subject_ids must be at most ${MAX_IDS} non-empty strings` };
        }
        subjectType = r.subject_type;
        subjectIds = [...new Set(r.subject_ids)];
    }
    if (!eventIds.length && !subjectIds.length) return { error: 'payload.redacts names nothing to redact' };
    return { eventIds: [...new Set(eventIds)], subjectType, subjectIds };
}

/** May an event stored as `by` (a row: source, project_id, env) redact `target` (a row)? */
function sameOwner(by, target) {
    return target.source === by.source
        && (target.project_id || null) === (by.project_id || null)
        && (target.env || 'production') === (by.env || 'production');
}

/** The actor a tombstone carries: the producer itself. */
function tombstoneActor(row) {
    if (row.project_id && /^app-[0-9a-hjkmnp-tv-z]{26}$/.test(row.source)) return { type: 'app', id: `app_${row.source.slice(4).toUpperCase()}` };
    return { type: 'service', id: row.source };
}

/** Column values that turn `row` into a tombstone. */
function tombstone(row, { by, at }) {
    const payload = JSON.stringify({ redacted: true, redacted_at: new Date(at).toISOString(), redacted_by: by });
    const actor = JSON.stringify(tombstoneActor(row));
    return {
        payload,
        actor,
        size_bytes: Buffer.byteLength(payload) + Buffer.byteLength(actor) + row.event_type.length + row.subject_id.length,
        redacted_at: at,
        redacted_by: by,
    };
}

module.exports = { parseDirective, sameOwner, tombstone, tombstoneActor, MAX_IDS };
