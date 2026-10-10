'use strict';
/**
 * The authority resource index (ADR-048 section 3; capability events.resource.read):
 *
 *   GET /api/v1/resources[?project=&kind=&owner=&cursor=&limit=] -> common.resource-list-result@1
 *   GET /api/v1/resources/:ovrn                            -> common.resource-summary@1
 *
 * It pages the resources OpenVibe.Events owns — today its subscriptions (sub_), the kind
 * events.subscription — as common.resource-summary@1, the shape OpenVibe.Services fans out over and merges
 * (openvibe-sdk/resources' createResourceIndex). An event is not a resource: evt_<ULID> is exactly what
 * common.resource-name@1 refuses by design. A project's queue is a derived carrier class, not a stored row,
 * so events.queue joins the index only when Events stores a queue row (ADR-048, amended 2026-10-07).
 *
 * Tenancy: `?project=prj_…` is the caller's tenancy boundary. With it, only that project's subscriptions
 * answer — the project-less ones are not its rows and are never mixed in. Without it the first-party caller
 * (the capability is first-party, resourceConstraints none) sees every subscription, which is what an
 * authority-wide fan-out needs; a subscription without a project (a service consumer's, first-party) is
 * exactly the row only that caller sees. `?owner=usr_…` narrows the result to subscriptions whose consumer
 * is that raw user subject; this index does not emit agent owners.
 *
 * OVRN: a summary's ovrn is computed with openvibe-contracts' contracts.resources.nameOf, the one
 * formatter, so it is present exactly when the subscription is a nameable resource — a project-scoped one is
 * ovrn:events:<prj_…>:subscription/sub_…. A project-less subscription has no project segment and so no name.
 * That is also what GET /api/v1/resources/:ovrn can read: only a resource whose computed ovrn equals the one
 * asked for answers, so an OVRN naming another project, or a project-less subscription's id, is a 404.
 *
 * The capability is `active` in openvibe-contracts since v0.110.0; the guard is the same service-token check
 * every other first-party route uses.
 */
const express = require('express');
const contracts = require('openvibe-contracts');
const { CAPS } = require('../auth');

const SERVICE = 'events';
const SUBSCRIPTION_KIND = 'events.subscription';
const KINDS = [SUBSCRIPTION_KIND];
const PROJECT_ID_RE = /^prj_[0-9A-HJKMNP-TV-Z]{26}$/;
const SUBJECT_ID_RE = /^(usr|agt)_[0-9A-HJKMNP-TV-Z]{26}$/;
const USER_SUBJECT_RE = /^usr_[0-9A-HJKMNP-TV-Z]{26}$/;
const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

/** A subject-ref {type:'user', id} for a stored subject, or null when it is not a usr_ id. */
const userRef = (subject) => (USER_SUBJECT_RE.test(String(subject || '')) ? { type: 'user', id: subject } : null);

/**
 * common.resource-summary@1 for a subscriptions row. Only the fields the schema allows: the row's id and
 * kind, the pattern as its name (a subscription has no other human-readable one), the state its `enabled`
 * flag describes, its creation time, and — as the one stored subject — the consumer, when it is a person.
 */
function subscriptionSummary(s) {
    const owner = userRef(s.consumer);
    return {
        id: s.id, kind: SUBSCRIPTION_KIND, service: SERVICE,
        ...(s.project_id ? { project_id: s.project_id } : {}),
        ...(owner ? { owner } : {}),
        name: s.topic_pattern,
        state: s.enabled ? 'active' : 'disabled',
        created_at: new Date(s.created_at).toISOString(),
    };
}

/** The summary's ovrn, or null when it has none (a project-less subscription has no project segment). */
function ovrnOf(summary) {
    return contracts.resources.nameOf(summary);
}

/** The summary with its ovrn attached when it has one. */
function named(summary) {
    const ovrn = ovrnOf(summary);
    return ovrn ? { ...summary, ovrn } : summary;
}

/** A stable (kind, id) ordering, so a cursor can be a position in it. */
const order = (x, y) => (x.kind < y.kind ? -1 : x.kind > y.kind ? 1 : x.id < y.id ? -1 : x.id > y.id ? 1 : 0);

/**
 * The summaries matching the filters: `project` scopes tenancy, `kind` picks one kind, `owner` picks a
 * person whose subject is stored as the subscription consumer. Sorted by (kind, id).
 *
 * Scope: events.resource.read is first-party (resourceConstraints none), so its holder sees every project and
 * ?project= only narrows. If it is ever granted to a non-first-party principal, derive the scope from that
 * principal's grants here instead of trusting the query.
 */
async function collect(store, { project = null, kind = null, owner = null } = {}) {
    const out = [];
    if ((!kind || kind === SUBSCRIPTION_KIND) && (!owner || USER_SUBJECT_RE.test(owner))) {
        const rows = project ? await store.listProjectSubscriptions(project, owner) : await store.listSubscriptions(owner);
        for (const s of rows) out.push(named(subscriptionSummary(s)));
    }
    return out.sort(order);
}

/** A cursor is an opaque base64url [kind, id] position; only one this index issued decodes to that. */
const encodeCursor = (s) => Buffer.from(JSON.stringify([s.kind, s.id])).toString('base64url');
function decodeCursor(raw) {
    let v;
    try { v = JSON.parse(Buffer.from(String(raw), 'base64url').toString('utf8')); } catch { return null; }
    return Array.isArray(v) && v.length === 2 && typeof v[0] === 'string' && typeof v[1] === 'string' ? v : null;
}
const afterCursor = (s, [kind, id]) => s.kind > kind || (s.kind === kind && s.id > id);

/** The query as filters, or { error } for a value that cannot be honoured. An unknown kind is kept (it matches nothing). */
function filtersOf(query) {
    const project = typeof query.project === 'string' && query.project !== '' ? query.project : null;
    if (project && !PROJECT_ID_RE.test(project)) return { error: 'project must be a prj_ id' };
    const owner = query.owner === undefined || query.owner === '' ? null : query.owner;
    if (owner !== null && (typeof owner !== 'string' || !SUBJECT_ID_RE.test(owner))) return { error: 'owner must be a usr_ or agt_ id' };
    const kind = typeof query.kind === 'string' && query.kind !== '' ? query.kind : null;
    let limit = DEFAULT_LIMIT;
    if (typeof query.limit === 'string' && query.limit !== '') {
        if (!/^\d+$/.test(query.limit) || Number(query.limit) < 1 || Number(query.limit) > MAX_LIMIT) return { error: `limit must be an integer 1-${MAX_LIMIT}` };
        limit = Number(query.limit);
    }
    let cursor = null;
    if (typeof query.cursor === 'string' && query.cursor !== '') {
        cursor = decodeCursor(query.cursor);
        if (!cursor) return { error: 'cursor is not one this index issued' };
    }
    return { project, kind, owner, limit, cursor };
}

function resourcesRouter({ store, auth }) {
    const guard = auth.requireCap(CAPS.resourceRead);
    const open = (res) => res.set('Cache-Control', 'private, max-age=60');
    const bad = (res, req, detail) => contracts.http.sendProblem(res, 400, 'resources.bad_query', { detail, ctx: req.ov });
    const unknown = (res, req, name) => contracts.http.sendProblem(res, 404, 'resources.unknown_resource', { detail: `no resource named ${name}`, ctx: req.ov });

    /** One common.resource-list-result@1 page: the filtered, sorted summaries from the cursor, then `limit` of them. */
    async function page(req, res) {
        const f = filtersOf(req.query);
        if (f.error) return bad(res, req, f.error);
        const all = await collect(store, f);
        const rest = f.cursor ? all.filter((s) => afterCursor(s, f.cursor)) : all;
        const resources = rest.slice(0, f.limit);
        const next_cursor = rest.length > f.limit ? encodeCursor(resources[resources.length - 1]) : null;
        return open(res).json({ resources, next_cursor });
    }

    /** GET /api/v1/resources/:ovrn: the summary whose computed ovrn is exactly the one asked for. */
    async function one(req, res) {
        const name = String(req.params.ovrn);
        const parsed = contracts.resources.parse(name);
        let summary = null;
        if (parsed && parsed.service === SERVICE && parsed.type === 'subscription') {
            const row = await store.getSubscription(parsed.id);
            if (row) summary = named(subscriptionSummary(row));
        }
        if (!summary || summary.ovrn !== name) return unknown(res, req, name);
        return open(res).json(summary);
    }

    const router = express.Router();
    router.get('/api/v1/resources', guard, page);
    router.get('/api/v1/resources/:ovrn', guard, one);
    return router;
}

module.exports = { resourcesRouter, SERVICE, KINDS, SUBSCRIPTION_KIND, DEFAULT_LIMIT, MAX_LIMIT, subscriptionSummary, ovrnOf, collect, encodeCursor };
