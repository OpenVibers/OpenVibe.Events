'use strict';
/**
 * The delivery policy (ADR-042 decisions 1 and 4; plan T7): the only semantic vocabulary for how an event type is
 * carried. Policies live in `delivery_policies`, keyed by an event-type pattern using the same matcher subscriptions
 * use for `topic_pattern` (../topics.js). The most specific matching pattern wins: an exact type beats any wildcard;
 * among wildcards the longer literal part wins; ties break by pattern string order (deterministic). With no policy row
 * behaviour is exactly today's DEFAULT_POLICY: no ordering key, one delivery in flight per subscription.
 *
 * The carrier class (TOPIC | QUEUE | STREAM) is an internal, derived label — never a contract enum, never in an
 * envelope. It is answered by the explain/resolve API only.
 *
 * Publisher intent: the publish contract (events.publish-request@1 -> events.event-envelope@1) has no intent field, so
 * none is read here; a class can only be raised by writing a policy. Do not add a field for it.
 */
const topics = require('../topics');

const DEFAULT_POLICY = { class: 'domain', durability: 'required', delivery_semantics: 'at_least_once' };
const MAX_KEY_LEN = 200;

/**
 * Which carrier class a policy's deliveries use. STREAM only when durability is required AND the policy has an
 * ordering key (retained, ordered per key); TOPIC for the fan-out class; QUEUE for everything else.
 */
function carrierClass(policy) {
    if (policy && policy.durability === 'required' && policy.ordering && typeof policy.ordering.key === 'string' && policy.ordering.key) return 'STREAM';
    if (policy && policy.class === 'realtime_ephemeral') return 'TOPIC';
    return 'QUEUE';
}

function subjectForm(envelope) {
    const s = envelope && envelope.subject;
    if (s && typeof s.type === 'string' && s.type && typeof s.id === 'string' && s.id) return `${s.type}:${s.id}`;
    return null;
}

/** The value at a dotted path (`subject.id`, `payload.a.b`, `actor.id`) or null when it is not there. */
function pathValue(envelope, path) {
    let cur = envelope;
    for (const seg of String(path).split('.')) {
        if (cur == null || typeof cur !== 'object') return null;
        cur = cur[seg];
    }
    return cur;
}

/**
 * The ordering key of an envelope under a policy, or null when the policy has no ordering (the default). `'subject'`
 * is `<subject.type>:<subject.id>`; a dotted path starting with subject./actor./payload. is that value when it is a
 * string or a finite number; anything unresolvable falls back to the subject form, then to the event type. Capped at
 * 200 characters. Never the seq.
 */
function orderingKey(policy, envelope) {
    if (!policy || !policy.ordering || typeof policy.ordering.key !== 'string' || !policy.ordering.key) return null;
    const key = policy.ordering.key;
    let value = null;
    if (key === 'subject') {
        value = subjectForm(envelope);
    } else if (/^(subject|actor|payload)\./.test(key)) {
        const raw = pathValue(envelope, key);
        if (typeof raw === 'string' || (typeof raw === 'number' && Number.isFinite(raw))) value = String(raw);
    }
    if (value == null || value === '') value = subjectForm(envelope) || (envelope && envelope.event_type) || null;
    if (value == null) return null;
    return value.length > MAX_KEY_LEN ? value.slice(0, MAX_KEY_LEN) : value;
}

/** Exact patterns beat wildcards; among wildcards the longer literal part wins. */
function specificity(pattern) {
    return pattern.includes('*') ? pattern.replace(/\*/g, '').length : Infinity;
}

function bestMatch(rows, eventType) {
    let best = null;
    for (const row of rows) {
        if (!row.compiled.test(eventType)) continue;
        if (!best) { best = row; continue; }
        const a = specificity(row.pattern);
        const b = specificity(best.pattern);
        if (a > b || (a === b && row.pattern < best.pattern)) best = row;
    }
    return best;
}

function normalize(row) {
    const policy = typeof row.policy === 'string' ? JSON.parse(row.policy) : row.policy;
    return { pattern: row.pattern, policy, revision: Number(row.revision), updated_at: row.updated_at, updated_by: row.updated_by || null };
}

/**
 * @param {object} db  the Events database (openvibe-sdk/db handle)
 * @param {{ now(): number }} [o.clock]
 * @param {number} [o.reloadMs]  how long a compiled cache is trusted before it is dropped (a second worker process
 *                               picks up another process's writes; this process also drops it on every write)
 */
function createPolicyLibrary(db, { clock = { now: () => Date.now() }, reloadMs = 60000, log = console } = {}) {
    let cache = null;   // { rows, at }

    async function load() {
        let raw = [];
        try {
            raw = await db.prepare('SELECT pattern, policy, revision, updated_at, updated_by FROM delivery_policies').all();
        } catch (err) {
            log.warn(`[policy] could not load delivery_policies: ${err.message}`);
            raw = [];
        }
        const rows = raw.map((r) => {
            const row = normalize(r);
            row.compiled = topics.compile(row.pattern);
            return row;
        });
        cache = { rows, at: clock.now() };
        return rows;
    }

    /** Drop the compiled cache; the next resolve reloads from the database. */
    function invalidate() { cache = null; }

    async function all() {
        if (!cache || clock.now() - cache.at >= reloadMs) await load();
        return cache.rows;
    }

    /** { policy, pattern, revision }; pattern is null and revision 0 for the default (no matching row). */
    async function resolve(eventType) {
        const rows = await all();
        const m = bestMatch(rows, eventType);
        return m ? { policy: m.policy, pattern: m.pattern, revision: m.revision } : { policy: DEFAULT_POLICY, pattern: null, revision: 0 };
    }

    /** The ordering key an envelope of `eventType` takes, or null (no policy ordering). */
    async function keyFor(eventType, envelope) {
        const { policy } = await resolve(eventType);
        return orderingKey(policy, envelope);
    }

    /** Upsert a policy row; revision +1 on an existing pattern. Returns the stored row. */
    async function put(pattern, policy, updatedBy) {
        const row = await db.prepare(`INSERT INTO delivery_policies (pattern, policy, revision, updated_at, updated_by)
            VALUES (?, ?, 1, ?, ?)
            ON CONFLICT(pattern) DO UPDATE SET policy = excluded.policy, revision = delivery_policies.revision + 1,
                updated_at = excluded.updated_at, updated_by = excluded.updated_by
            RETURNING pattern, policy, revision, updated_at, updated_by`).get(pattern, JSON.stringify(policy), clock.now(), updatedBy);
        invalidate();
        return normalize(row);
    }

    /** Remove a policy row. Returns the number of rows deleted. */
    async function remove(pattern) {
        const changes = (await db.prepare('DELETE FROM delivery_policies WHERE pattern = ?').run(pattern)).changes;
        invalidate();
        return changes;
    }

    const timer = setInterval(() => { invalidate(); }, reloadMs);
    timer.unref?.();

    return { resolve, keyFor, all, put, remove, invalidate, reload: load, stop() { clearInterval(timer); } };
}

module.exports = { DEFAULT_POLICY, carrierClass, orderingKey, specificity, createPolicyLibrary };
