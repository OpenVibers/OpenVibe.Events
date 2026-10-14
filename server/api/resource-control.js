'use strict';
/**
 * Events' control API (ADR-048, plan T13 step 10): POST /api/v1/resources/control — one
 * common.resource-control-request@1 from a first-party service (OpenVibe.Services) holding events.resource.control,
 * acting for the person in on_behalf_of, on an events.subscription named by its OVRN. The answer is a
 * common.resource-control-result@1 (200 for every outcome; a malformed request is a 400 problem).
 *
 *   suspend   disable the subscription: nothing is delivered to it until it resumes (events it matches are kept as
 *             they always are, and can be replayed)
 *   resume    enable it again
 *
 * Any other action is refused `control.unsupported_action` (Events has no deletion of a subscription). Only the
 * subscription's consumer may act on it (on_behalf_of names them; another person's subscription is refused as not
 * found, never confirmed to exist), and only inside the project the OVRN names. `dry_run` decides and explains without
 * applying. The first answer per (caller, project, idempotency_key) is kept in events_control_log and returned for a
 * repeat; the same key with another request is refused `control.idempotency_conflict`.
 */
const crypto = require('crypto');
const express = require('express');
const contracts = require('openvibe-contracts');

const SERVICE = 'events';
const CAPABILITY = 'events.resource.control';
const ACTIONS = { suspend: false, resume: true };
const USER_SUBJECT_RE = /^usr_[0-9A-HJKMNP-TV-Z]{26}$/;

function canonical(v) {
    if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
    if (v && typeof v === 'object') return `{${Object.keys(v).sort().filter((k) => v[k] !== undefined).map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`;
    return JSON.stringify(v);
}
const digestOf = (request) => crypto.createHash('sha256')
    .update(canonical({ ...request, idempotency_key: undefined, confirmation_id: undefined, trace_id: undefined })).digest('hex');

function resourceControlRouter({ store, auth, now = () => Date.now() }) {
    const router = express.Router();
    const guard = auth.requireCap(CAPABILITY, { requireService: true });
    const json = express.json({ limit: '16kb' });
    const db = store.db;
    const iso = () => new Date(now()).toISOString();
    const result = (request, state, extra = {}) => ({ action: request.action, ...(request.resource ? { resource: request.resource } : {}), state, at: iso(), ...extra });
    const problem = (status, code, detail) => ({ type: `https://openvibe.network/problems/${code}`, title: code, status, code, detail });

    async function decide(request) {
        const parsed = contracts.resources.parse(request.resource || '');
        if (!parsed || parsed.service !== SERVICE || parsed.type !== 'subscription') {
            return result(request, 'refused', { problem: problem(404, 'resources.unknown_resource', `no subscription named ${request.resource || request.resource_kind}`) });
        }
        if (!(request.action in ACTIONS)) {
            return result(request, 'refused', { problem: problem(422, 'control.unsupported_action', `a subscription takes suspend and resume, not ${request.action}`) });
        }
        const person = request.on_behalf_of;
        if (!person || person.type !== 'user' || !USER_SUBJECT_RE.test(String(person.id || ''))) {
            return result(request, 'refused', { problem: problem(403, 'control.no_person', 'on_behalf_of must name the person whose subscription it is') });
        }
        const row = await store.getSubscription(parsed.id);
        if (!row || row.consumer !== person.id || row.project_id !== parsed.project_id || parsed.project_id !== request.project_id) {
            return result(request, 'refused', { problem: problem(404, 'resources.unknown_resource', `no subscription named ${request.resource} of this person in this project`) });
        }
        const target = ACTIONS[request.action];
        const enabled = Boolean(Number(row.enabled));
        const status = (on) => (on ? 'active' : 'disabled');
        if (enabled === target) return result(request, 'done', { result: { status: status(enabled), changed: false } });
        if (request.dry_run) return result(request, 'done', { result: { dry_run: true, would: target ? 'enable the subscription' : 'disable the subscription' } });
        const updated = await store.setSubscriptionEnabled(row.id, target);
        return result(request, 'done', { result: { status: status(Boolean(Number(updated.enabled))), changed: true } });
    }

    router.post('/api/v1/resources/control', guard, json, async (req, res, next) => {
        try {
            const request = req.body;
            const check = contracts.resources.checkControlRequest(request || {});
            if (!check.valid) return contracts.http.sendProblem(res, 400, 'control.bad_request', { detail: JSON.stringify(check.errors).slice(0, 500), ctx: req.ov });
            const caller = req.principal.sub;
            const digest = digestOf(request);
            const seen = await db.prepare('SELECT digest, response FROM events_control_log WHERE caller = ? AND project_id = ? AND idempotency_key = ?')
                .get(caller, request.project_id, request.idempotency_key);
            if (seen) {
                if (seen.digest !== digest) return contracts.http.sendProblem(res, 409, 'control.idempotency_conflict', { detail: 'this idempotency_key was used for another request', ctx: req.ov });
                return res.json(typeof seen.response === 'string' ? JSON.parse(seen.response) : seen.response);
            }
            const answer = await decide(request);
            await db.prepare(`INSERT INTO events_control_log (caller, project_id, idempotency_key, digest, response, created_at)
                VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`).run(caller, request.project_id, request.idempotency_key, digest, JSON.stringify(answer), now());
            return res.json(answer);
        } catch (err) { return next(err); }
    });

    return router;
}

module.exports = { resourceControlRouter, CAPABILITY, digestOf };
