'use strict';

/**
 * GitHub webhooks in (AL5: a ticket follows its pull request).
 *
 *   POST /api/v1/in/github
 *     X-GitHub-Delivery: the delivery GUID (idempotency: a redelivery answers the stored event ids)
 *     X-GitHub-Event:    check_run | check_suite | workflow_run | pull_request |
 *                        pull_request_review | pull_request_review_comment | issue_comment | ping
 *     X-Hub-Signature-256: sha256=<hex HMAC of the raw body> with one of config.github.secrets
 *
 * A delivery is normalized to one source-'github' envelope per pull request and stored through
 * store.insertBatch like any other event, so Codes (and the portal) subscribes to it with the usual
 * Events subscription — nothing polls GitHub:
 *
 *   github.check.completed   a check_run, check_suite or workflow_run finished → { repo, kind, conclusion,
 *                            check_name, head_sha, html_url, details_url, output } (Codes decides what is red;
 *                            output is the failing job's log tail when GitHub sends one)
 *   github.pr.updated        a pull_request synchronize|reopened|closed|opened → { repo, pr_number, action,
 *                            merged, mergeable, mergeable_state, head_sha, head_ref, base, base_ref,
 *                            changed_files, additions, deletions, draft, title, html_url, files_url }
 *                            (mergeable_state is GitHub's, often null on the webhook: conflicts through it are
 *                            best-effort; files_url is where a worker can list the changed files)
 *   github.pr.commented      an issue_comment on a PR, a submitted review or a review comment →
 *                            { repo, pr_number, author, author_type, author_association, state, body,
 *                            comment_id, path, line, head_sha, html_url }
 *
 * The subject of every envelope is { type: 'pr', id: '<owner>/<name>#<number>' } with the slug lowercased,
 * the key Codes' pr_bindings matches on. 202 answers { event_ids } (or { ignored } for ping and event kinds
 * with no pull request); a redelivery answers { event_ids, duplicate: true }; 401 on a bad signature; 404 while
 * EVENTS_GITHUB_WEBHOOK_SECRET is unset. Nothing here prints or stores a secret.
 *
 *   const ingress = createGithubIngress({ config, store, log });
 *   app.post('/api/v1/in/github', ingress.raw, ingress.handler);   // before the JSON body parser
 */
const crypto = require('crypto');
const express = require('express');
const { ids, validate, http } = require('openvibe-contracts');

const SOURCE = 'github';
const ACTOR = Object.freeze({ type: 'service', id: 'github' });
const MAX_BODY = 4000;          // a comment/review body kept on an envelope
const MAX_LOG_TAIL = 8_000;     // a check's output text (the log tail GitHub sends) kept on an envelope
const MAX_DELIVERIES = 20;      // a check run is fanned out per pull request, at most this many
const MIN_SECRET = 16;

const cut = (v, n) => (typeof v === 'string' && v.length > n ? v.slice(0, n) : (typeof v === 'string' ? v : undefined));
const lower = (v) => (typeof v === 'string' ? v.toLowerCase() : null);
const repoOf = (p) => lower(p && p.repository && p.repository.full_name);
const num = (v) => (Number.isInteger(v) && v > 0 ? v : null);

/** pull request numbers a check payload names (check_run, check_suite, workflow_run). */
function checkPrNumbers(p) {
    const out = [];
    const seen = new Set();
    const take = (n) => { const k = num(n); if (k && !seen.has(k) && out.length < MAX_DELIVERIES) { seen.add(k); out.push(k); } };
    const run = p.check_run || p.check_suite || p.workflow_run;
    for (const pr of (run && run.pull_requests) || []) {
        if (num(pr.number)) take(pr.number);
        else if (typeof pr.url === 'string') { const m = /\/pulls\/(\d+)(?:\/|$)/.exec(pr.url); if (m) take(Number(m[1])); }
    }
    return out;
}

/** A check payload finished: conclusion, name, links and its output log tail, or null when it did not finish. */
function checkCompleted(ghEvent, p) {
    const run = p.check_run || p.check_suite || p.workflow_run;
    if (!run || run.status !== 'completed') return null;
    const url = run.html_url || run.url || (p.check_run && p.check_run.details_url) || null;
    const out = run.output && typeof run.output === 'object' && !Array.isArray(run.output) ? run.output : null;
    const output = out ? {
        title: cut(out.title, 500),
        summary: cut(out.summary, 4_000),
        text: cut(out.text, MAX_LOG_TAIL),
        annotations: Array.isArray(out.annotations) ? out.annotations.length : undefined,
    } : null;
    return {
        event_type: 'github.check.completed',
        check: {
            kind: p.check_run ? 'check_run' : p.check_suite ? 'check_suite' : 'workflow_run',
            conclusion: typeof run.conclusion === 'string' ? run.conclusion.slice(0, 40) : null,
            check_name: cut(run.name, 200),
            head_sha: typeof run.head_sha === 'string' ? run.head_sha.slice(0, 64) : null,
            html_url: typeof url === 'string' ? url.slice(0, 2000) : null,
            details_url: cut(p.check_run && p.check_run.details_url, 2000),
            started_at: cut(run.started_at, 40),
            completed_at: cut(run.completed_at, 40),
            output,
        },
        prNumbers: checkPrNumbers(p),
    };
}

function prUpdated(p) {
    const pr = p.pull_request;
    if (!pr || !num(p.number)) return null;
    const head = pr.head && typeof pr.head === 'object' ? pr.head : {};
    const base = pr.base && typeof pr.base === 'object' ? pr.base : {};
    return {
        event_type: 'github.pr.updated',
        update: {
            action: typeof p.action === 'string' ? p.action.slice(0, 40) : null,
            merged: pr.merged === true,
            mergeable: pr.mergeable === true ? true : pr.mergeable === false ? false : null,
            mergeable_state: typeof pr.mergeable_state === 'string' ? pr.mergeable_state.slice(0, 40) : null,
            draft: pr.draft === true,
            title: cut(pr.title, 500),
            head_sha: cut(head.sha, 64),
            head_ref: cut(head.ref, 200),
            base: cut(base.ref, 200),
            base_ref: cut(base.ref, 200),
            base_sha: cut(base.sha, 64),
            changed_files: num(pr.changed_files),
            additions: num(pr.additions),
            deletions: num(pr.deletions),
            html_url: cut(pr.html_url, 2000),
            files_url: cut(pr.url ? `${String(pr.url).replace(/\/+$/, '')}/files` : null, 2000),
        },
        prNumbers: [p.number],
    };
}

const userOf = (u) => (u && typeof u.login === 'string' ? { login: u.login.slice(0, 200), type: u.type === 'Bot' ? 'Bot' : 'User', association: cut(u.author_association, 40) } : null);

/** A comment id; GitHub sends it as a number. */
const commentId = (v) => (Number.isInteger(v) && v > 0 ? v : null);

function prCommented(ghEvent, p) {
    const repo = repoOf(p);
    if (!repo) return null;
    if (ghEvent === 'issue_comment') {
        // An issue_comment is a PR comment only when the issue is a pull request. Only a newly posted one wakes
        // anyone: an edited or deleted comment is not a new instruction (and a deleted "stop" must not end a ticket).
        if (p.action !== 'created') return null;
        if (!p.issue || !p.issue.pull_request || !num(p.issue.number)) return null;
        const by = userOf(p.comment && p.comment.user);
        if (!by || typeof (p.comment && p.comment.body) !== 'string') return null;
        const pr = p.issue.pull_request || {};
        return {
            event_type: 'github.pr.commented',
            comment: {
                author: by.login, author_type: by.type, author_association: by.association, state: 'commented',
                body: cut(p.comment.body, MAX_BODY),
                comment_id: commentId(p.comment.id),
                head_sha: cut(pr.head && pr.head.sha, 64),
                html_url: cut(p.comment.html_url, 2000),
            },
            prNumbers: [p.issue.number],
        };
    }
    if (ghEvent === 'pull_request_review') {
        const pr = p.pull_request;
        if (!pr || !num(pr.number)) return null;
        // A dismissal is not a new instruction; a bare approval is not a comment. A review that asks for changes
        // without a summary still wakes the worker, with a note to read the review on the pull request.
        if (p.action !== 'submitted') return null;
        const review = p.review && typeof p.review === 'object' ? p.review : {};
        const by = userOf(review.user);
        if (!by) return null;
        const state = cut(review.state, 40) || 'commented';
        let body = cut(review.body, MAX_BODY) || '';
        if (state === 'approved' && !body) return null;
        if (!body && state === 'changes_requested') body = 'The reviewer requested changes without a summary; read the review on the pull request.';
        if (!body) return null;
        return {
            event_type: 'github.pr.commented',
            comment: {
                author: by.login, author_type: by.type, author_association: by.association, state,
                body,
                comment_id: commentId(review.id),
                head_sha: cut(pr.head && pr.head.sha, 64),
                html_url: cut(review.html_url || pr.html_url, 2000),
            },
            prNumbers: [pr.number],
        };
    }
    if (ghEvent === 'pull_request_review_comment') {
        const pr = p.pull_request;
        if (!pr || !num(pr.number)) return null;
        if (p.action !== 'created') return null;
        const by = userOf(p.comment && p.comment.user);
        if (!by || typeof (p.comment && p.comment.body) !== 'string') return null;
        const c = p.comment;
        return {
            event_type: 'github.pr.commented',
            comment: {
                author: by.login, author_type: by.type, author_association: by.association, state: 'commented',
                body: cut(c.body, MAX_BODY),
                comment_id: commentId(c.id),
                path: cut(c.path, 500),
                line: num(c.line) || num(c.original_line),
                head_sha: cut(pr.head && pr.head.sha, 64),
                html_url: cut(c.html_url, 2000),
            },
            prNumbers: [pr.number],
        };
    }
    return null;
}

/**
 * One normalized result per envelope: { event_type, prNumber, payload }. null: nothing to store
 * (an unfinished check, a non-PR issue, an uninteresting action). Throws on a malformed envelope.
 */
function normalize(ghEvent, p) {
    if (!p || typeof p !== 'object' || Array.isArray(p)) throw new Error('the webhook body must be a JSON object');
    const repo = repoOf(p);
    if (!repo) throw new Error('the webhook body names no repository');
    if (ghEvent === 'check_run' || ghEvent === 'check_suite' || ghEvent === 'workflow_run') {
        const c = checkCompleted(ghEvent, p);
        if (!c) return [];
        return c.prNumbers.map((n) => ({
            event_type: c.event_type, prNumber: n,
            payload: {
                repo, kind: c.check.kind, conclusion: c.check.conclusion, check_name: c.check.check_name,
                head_sha: c.check.head_sha, html_url: c.check.html_url, details_url: c.check.details_url,
                started_at: c.check.started_at, completed_at: c.check.completed_at, output: c.check.output || null,
            },
        }));
    }
    if (ghEvent === 'pull_request') {
        if (!['synchronize', 'reopened', 'closed', 'opened'].includes(p.action)) return [];
        const u = prUpdated(p);
        if (!u) return [];
        return u.prNumbers.map((n) => ({
            event_type: u.event_type, prNumber: n,
            payload: { repo, pr_number: n, ...u.update },
        }));
    }
    if (ghEvent === 'issue_comment' || ghEvent === 'pull_request_review' || ghEvent === 'pull_request_review_comment') {
        const c = prCommented(ghEvent, p);
        if (!c) return [];
        return c.prNumbers.map((n) => ({
            event_type: c.event_type, prNumber: n,
            payload: { repo, pr_number: n, ...c.comment },
        }));
    }
    return [];
}

/** sha256=<hex> over the raw body with one of the secrets, compared in constant time. */
function verifies(raw, header, secrets) {
    if (typeof header !== 'string') return false;
    const m = /^sha256=([0-9a-f]{64})$/.exec(header.trim());
    if (!m) return false;
    const got = Buffer.from(m[1], 'hex');
    return (secrets || []).some((s) => {
        if (typeof s !== 'string' || !s.length) return false;
        const want = crypto.createHmac('sha256', s).update(raw).digest();
        return want.length === got.length && crypto.timingSafeEqual(want, got);
    });
}

/** A tiny per-address sliding window (GitHub delivers from many addresses; this stops one stuck sender). */
function createLimiter({ perMinute = 300 } = {}) {
    const hits = new Map();
    return (ip) => {
        const now = Date.now();
        const list = (hits.get(ip) || []).filter((t) => now - t < 60_000);
        list.push(now);
        if (hits.size > 10000 && !hits.has(ip)) hits.delete(hits.keys().next().value);
        hits.set(ip, list);
        return list.length <= perMinute;
    };
}

function createGithubIngress({ config, store, log = console, now = () => Date.now() } = {}) {
    const db = store.db;
    const secrets = ((config.github && config.github.secrets) || []).filter((s) => typeof s === 'string' && s.length >= MIN_SECRET);
    const limited = createLimiter();
    const stats = { received: 0, stored: 0, duplicates: 0, ignored: 0, refused: 0 };

    async function handler(req, res) {
        const ctx = http.requestContext(req.headers);
        const problem = (status, code, detail) => http.sendProblem(res, status, code, { detail, ctx });
        if (!secrets.length) return problem(404, 'events.github_disabled', 'GitHub webhooks are not configured');
        if (!limited(req.ip || req.socket.remoteAddress || 'unknown')) { stats.refused++; return problem(429, 'events.rate_limited', 'too many webhook deliveries'); }
        const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
        if (!verifies(raw, req.headers['x-hub-signature-256'], secrets)) { stats.refused++; return problem(401, 'events.bad_signature', 'X-Hub-Signature-256 does not verify'); }
        const ghEvent = String(req.headers['x-github-event'] || '');
        const delivery = String(req.headers['x-github-delivery'] || '').slice(0, 100);
        if (!/^[0-9a-f-]{8,100}$/i.test(delivery)) { stats.refused++; return problem(400, 'events.bad_delivery', 'X-GitHub-Delivery is required'); }
        stats.received++;
        if (ghEvent === 'ping') return res.status(202).json({ ignored: 'ping' });
        let payload;
        try { payload = JSON.parse(raw.toString('utf8')); } catch { stats.refused++; return problem(400, 'events.bad_delivery', 'the webhook body is not JSON'); }
        let normalized;
        try { normalized = normalize(ghEvent, payload); } catch (e) { stats.refused++; return problem(400, 'events.bad_delivery', e.message); }
        if (!normalized.length) { stats.ignored++; return res.status(202).json({ ignored: ghEvent || 'unknown' }); }
        const at = new Date(now()).toISOString();
        const trace = crypto.randomBytes(16).toString('hex');
        const items = normalized.map((n) => ({
            event_id: ids.newId('event', now()), event_type: n.event_type, version: 1, source: SOURCE,
            actor: ACTOR, timestamp: at, trace_id: trace, priority: 'important', visibility: 'internal',
            subject: { type: 'pr', id: `${repoOf(payload)}#${n.prNumber}` }, payload: n.payload,
        }));
        for (const env of items) {
            const v = validate('events.event-envelope@1', env);
            if (!v.valid) {
                stats.refused++;
                return problem(400, 'events.bad_delivery', 'the webhook names no pull request Codes can follow');
            }
        }
        // One claim per delivery (it may name several pull requests, and they must be claimed together): a
        // redelivery answers these ids without storing again. insertBatch runs its own transaction after the
        // claim commits; a failure releases the claim so GitHub's retry stores the events.
        const eventIds = items.map((i) => i.event_id);
        const seen = await db.maybe('SELECT event_ids FROM github_deliveries WHERE delivery_id = $1', [delivery]);
        if (seen) {
            // A claim is written just before the batch. If the process died in that window the claimed events do
            // not exist; release the stale claim and store them now instead of answering a duplicate for nothing.
            const claimed = Array.isArray(seen.event_ids) ? seen.event_ids : [];
            const present = await Promise.all(claimed.map((id) => store.getEvent(id).catch(() => null)));
            if (claimed.length && present.every(Boolean)) {
                stats.duplicates++;
                return res.status(202).json({ event_ids: claimed, duplicate: true });
            }
            await db.exec('DELETE FROM github_deliveries WHERE delivery_id = $1', [delivery]).catch(() => {});
        }
        try {
            await db.exec(
                'INSERT INTO github_deliveries (delivery_id, event_ids, event_types) VALUES ($1, $2::jsonb, $3::jsonb)',
                [delivery, JSON.stringify(eventIds), JSON.stringify(items.map((i) => i.event_type))],
            );
        } catch (err) {
            const twin = await db.maybe('SELECT event_ids FROM github_deliveries WHERE delivery_id = $1', [delivery]).catch(() => null);
            if (twin) {
                stats.duplicates++;
                return res.status(202).json({ event_ids: twin.event_ids, duplicate: true });
            }
            // A concurrent twin claimed it first and has not committed yet: GitHub retries onto the committed row.
            log.error(`[github] ingress claim failed for ${delivery}:`, err.message);
            return problem(503, 'events.ingress_failed', 'the delivery was not stored; GitHub will retry it');
        }
        try {
            await store.insertBatch(items, { publisher: 'github-ingress', requestId: `github:${delivery}` });
        } catch (err) {
            // The claim is released so the redelivery stores the events (a duplicate delivery onto committed
            // envelopes answers duplicate from the receipts instead of storing twice).
            await db.exec('DELETE FROM github_deliveries WHERE delivery_id = $1', [delivery]).catch(() => {});
            log.error(`[github] ingress failed for ${delivery}:`, err.message);
            return problem(503, 'events.ingress_failed', 'the delivery was not stored; GitHub will retry it');
        }
        stats.stored += items.length;
        return res.status(202).json({ event_ids: eventIds });
    }

    const raw = express.raw({ type: () => true, limit: '1mb' });
    return { handler, raw, normalize, verifies, stats: () => ({ ...stats, enabled: secrets.length > 0 }) };
}

module.exports = { createGithubIngress, normalize, verifies, SOURCE };
