'use strict';
/**
 * Authentication and capability checks.
 *
 *   - Service principals: RS256 client-credentials tokens from OpenVibe.Network (audience
 *     openvibe.events), verified with openvibe-contracts serviceAuth.verifyServiceToken.
 *   - Browsers: Network user JWTs (RS256, same key), cookie `ov_token` or Bearer; or, on
 *     /realtime/stream only, a realtime ticket (?ticket=): a two-minute, single-use RS256 JWT Network
 *     signs for the signed-in person (identity.realtime-ticket-claims@1, ADR-005 amendment 2), so a
 *     page on any OpenVibe site can open an EventSource without a third-party cookie. A ticket is
 *     never a session (issuer <network>/realtime, typ, audience openvibe.events only) and a session
 *     token is never a ticket.
 *
 *   - Developer apps (ADR-014): app tokens (sub app:app_<ULID>, project_id, env) holding
 *     events.app.publish / events.app.read / events.app.subscribe, on the routes that accept them
 *     (appOrService guard). Only there is a sandbox token (env=sandbox) accepted; every other route
 *     refuses it with token.sandbox_refused.
 *
 * The service capabilities are in openvibe-contracts since v0.7; events.app.* arrive in v0.28.
 * capabilities.check() answers capability.unknown for an id the installed release does not know;
 * until then hasCap() decides with the same grant rule (exact id, or a `family.*` grant).
 */
const crypto = require('crypto');
const { serviceAuth, capabilities, http } = require('openvibe-contracts');
const apps = require('./apps');

/** The capability ids this service enforces. */
const CAPS = Object.freeze({
    publish: 'events.event.publish',
    subscribe: 'events.subscription.manage',
    read: 'events.event.read',
    admin: 'events.delivery.admin',
    appPublish: 'events.app.publish',
    appRead: 'events.app.read',
    appSubscribe: 'events.app.subscribe',
});

// ── Network public key ─────────────────────────────────────

/**
 * Loads the Network signing key from GET /api/.well-known/jwks ({ keys: [jwk] } or the older
 * { public_key: PEM }), retrying every 30 s until it loads and refreshing every 6 h after that.
 * A PEM given in config (OV_NETWORK_PUBLIC_KEY) is used as is and never fetched.
 */
function createKeyStore({ urls = [], pem = null, fetchImpl = globalThis.fetch, log = console } = {}) {
    let key = pem ? toPem(pem) : null;
    let retryTimer = null;
    let refreshTimer = null;

    function toPem(value) {
        return crypto.createPublicKey(value).export({ type: 'spki', format: 'pem' });
    }

    async function fetchOnce() {
        for (const base of urls) {
            if (!base) continue;
            const url = `${base}/api/.well-known/jwks`;
            try {
                const res = await fetchImpl(url, { signal: AbortSignal.timeout(8000) });
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const body = await res.json();
                const jwk = (body.keys || []).find(k => k.kty === 'RSA');
                if (jwk) key = toPem({ key: jwk, format: 'jwk' });
                else if (typeof body.public_key === 'string' && body.public_key.includes('BEGIN')) key = toPem(body.public_key);
                else throw new Error('no RSA key in response');
                log.log(`[auth] Network public key loaded from ${base}`);
                return key;
            } catch (err) {
                log.warn(`[auth] key fetch from ${url} failed: ${err.message}`);
            }
        }
        return null;
    }

    function start() {
        if (pem) return Promise.resolve(key);
        const attempt = async () => {
            const k = await fetchOnce();
            if (!k && !key) {
                retryTimer = setTimeout(attempt, 30 * 1000);
                retryTimer.unref?.();
            }
            return k;
        };
        refreshTimer = setInterval(() => { fetchOnce().catch(() => {}); }, 6 * 60 * 60 * 1000);
        refreshTimer.unref?.();
        return attempt();
    }

    function stop() {
        clearTimeout(retryTimer);
        clearInterval(refreshTimer);
    }

    return { get: () => key, loaded: () => Boolean(key), start, stop, fetchOnce };
}

// ── Capabilities ───────────────────────────────────────────

/** Exact id or `family.*` grant (the rule openvibe-contracts applies to its known capabilities). */
function hasCap(claims, id) {
    const granted = claims && Array.isArray(claims.cap) ? claims.cap : [];
    return granted.some(g => g === id || (g.endsWith('.*') && id.startsWith(g.slice(0, -1))));
}

/** Contracts decide once they know the id; until then the local rule does. */
function allows(claims, id) {
    if (!hasCap(claims, id)) return { allowed: false, code: 'capability.denied', reason: `${id} not granted` };
    const c = capabilities.check(claims, id);
    if (c.code === 'capability.unknown' && !capabilities.get(id)) return { allowed: true, code: null, reason: null };
    return c;
}

// ── Tokens ─────────────────────────────────────────────────

const b64json = (s) => JSON.parse(Buffer.from(String(s), 'base64url').toString('utf8'));

/** Network user JWT (RS256). Returns claims or null. Service tokens are never accepted as users. */
function verifyUserJwt(token, { publicKey, issuer, audiences, now = Date.now() }) {
    if (!publicKey || typeof token !== 'string') return null;
    const parts = token.split('.');
    if (parts.length !== 3) return null;
    try {
        const header = b64json(parts[0]);
        if (header.alg !== 'RS256') return null;
        const ok = crypto.verify('RSA-SHA256', Buffer.from(`${parts[0]}.${parts[1]}`), publicKey, Buffer.from(parts[2], 'base64url'));
        if (!ok) return null;
        const claims = b64json(parts[1]);
        const t = Math.floor(now / 1000);
        if (typeof claims.exp !== 'number' || claims.exp + 30 < t) return null;
        if (issuer && claims.iss !== issuer) return null;
        const aud = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
        if (!aud.some(a => audiences.includes(a))) return null;
        if (claims.actor_type || /^(svc|app|mod):/.test(String(claims.sub))) return null;
        // A typed token (a realtime ticket, a FedCM assertion) is never a session.
        if (claims.typ !== undefined || claims.purpose !== undefined) return null;
        return claims;
    } catch {
        return null;
    }
}

const TICKET_LIFETIME_MAX_S = 300;
const TICKET_SKEW_S = 30;
const SUBJECT_RE = /^usr_[0-9A-HJKMNP-TV-Z]{26}$/;
const TICKET_JTI_RE = /^rtk_[0-9a-f]{24}$/;

/**
 * A realtime ticket (identity.realtime-ticket-claims@1). Returns { ok: true, claims } or
 * { ok: false, code, reason }. Checks the signature, iss (<issuer>/realtime), aud (openvibe.events),
 * typ and purpose (realtime), sub (a usr_ subject), jti, expiry and a lifetime of at most 300 s.
 * Single use is the caller's (createAuth keeps the jtis it accepted).
 */
function verifyRealtimeTicket(token, { publicKey, issuer, now = Date.now() }) {
    const bad = (reason) => ({ ok: false, code: 'ticket.invalid', reason });
    if (!publicKey) return { ok: false, code: 'token.unavailable', reason: 'signing key not loaded yet' };
    if (typeof token !== 'string' || token.length > 4096) return bad('not a ticket');
    const parts = token.split('.');
    if (parts.length !== 3) return bad('not a ticket');
    let header; let claims;
    try {
        header = b64json(parts[0]);
        claims = b64json(parts[1]);
    } catch {
        return bad('not a ticket');
    }
    if (!header || header.alg !== 'RS256' || !claims || typeof claims !== 'object') return bad('not a ticket');
    let signed = false;
    try { signed = crypto.verify('RSA-SHA256', Buffer.from(`${parts[0]}.${parts[1]}`), publicKey, Buffer.from(parts[2], 'base64url')); } catch { signed = false; }
    if (!signed) return bad('signature does not verify');
    if (claims.iss !== `${issuer}/realtime`) return bad('not a realtime ticket (issuer)');
    if (claims.typ !== 'realtime' || claims.purpose !== 'realtime') return bad('not a realtime ticket (purpose)');
    const aud = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
    if (aud.length !== 1 || aud[0] !== 'openvibe.events') return bad('not for openvibe.events');
    if (!SUBJECT_RE.test(String(claims.sub)) || !TICKET_JTI_RE.test(String(claims.jti))) return bad('bad claims');
    const t = Math.floor(now / 1000);
    if (!Number.isInteger(claims.iat) || !Number.isInteger(claims.exp)) return bad('bad claims');
    if (claims.exp - claims.iat > TICKET_LIFETIME_MAX_S || claims.exp <= claims.iat) return bad('lifetime over 300 s');
    if (claims.iat > t + TICKET_SKEW_S) return bad('issued in the future');
    if (claims.exp <= t) return { ok: false, code: 'ticket.expired', reason: 'the ticket expired; ask Network for a new one' };
    return { ok: true, claims };
}

function bearer(req) {
    const h = String(req.headers.authorization || '');
    return h.startsWith('Bearer ') ? h.slice(7).trim() : null;
}

function cookie(req, name) {
    const raw = String(req.headers.cookie || '');
    for (const part of raw.split(';')) {
        const i = part.indexOf('=');
        if (i > 0 && part.slice(0, i).trim() === name) {
            try { return decodeURIComponent(part.slice(i + 1).trim()); } catch { return null; }
        }
    }
    return null;
}

/** Service slug for a principal sub: svc:live -> 'live'; app:/mod: principals have none. */
function serviceSlug(sub) {
    const m = /^svc:([a-z][a-z0-9-]{1,39})$/.exec(String(sub || ''));
    return m ? m[1] : null;
}

function createAuth({ config, keys, store = null }) {
    // Realtime tickets are single use: the jtis accepted while they are still valid (in memory; a
    // restart forgets them, and a ticket lives two minutes at most anyway).
    const usedTickets = new Map();   // jti -> exp (ms)
    function spendTicket(jti, expSec, now = Date.now()) {
        if (usedTickets.size > 5000) for (const [k, exp] of usedTickets) if (exp <= now) usedTickets.delete(k);
        if (usedTickets.has(jti)) return false;
        usedTickets.set(jti, expSec * 1000);
        return true;
    }

    // Token lifetimes are judged on wall-clock time, never on the worker's (injectable) clock.
    function verifyService(token, { acceptSandbox = false } = {}) {
        const publicKey = keys.get();
        if (!publicKey) return { ok: false, code: 'token.unavailable', reason: 'signing key not loaded yet' };
        const r = serviceAuth.verifyServiceToken(token, { publicKey, issuer: config.issuer, audience: config.audience, acceptSandbox });
        // Belt and braces for a contracts release that does not know `env` yet.
        if (r.ok && r.claims.env === 'sandbox' && !acceptSandbox) return { ok: false, code: 'token.sandbox_refused', reason: 'sandbox tokens are not accepted here' };
        return r;
    }

    function verifyUser(token) {
        return verifyUserJwt(token, { publicKey: keys.get(), issuer: config.issuer, audiences: config.userAudiences });
    }

    /**
     * Express guard for service routes. Sets req.principal = { sub, service, cap, ns, jti }.
     * `requireService` refuses app/mod principals (publish and subscribe act as a service).
     */
    function requireCap(id, { requireService = false } = {}) {
        return function capGuard(req, res, next) {
            const ctx = req.ov;
            const token = bearer(req);
            if (!token) return http.sendProblem(res, 401, 'token.missing', { detail: 'a service token is required', ctx });
            const r = verifyService(token);
            if (!r.ok) return http.sendProblem(res, r.code === 'token.unavailable' ? 503 : 401, r.code, { detail: r.reason, ctx });
            // Developer apps only ever act through events.app.* (appOrService); a first-party
            // capability in an app token is never honoured.
            if (/^app:/.test(String(r.claims.sub))) return http.sendProblem(res, 403, 'capability.denied', { detail: 'app tokens are not accepted on this route', ctx });
            const c = allows(r.claims, id);
            if (!c.allowed) return http.sendProblem(res, 403, c.code, { detail: c.reason, ctx });
            const service = serviceSlug(r.claims.sub);
            if (requireService && !service) return http.sendProblem(res, 403, 'capability.denied', { detail: 'only service principals may do this', ctx });
            req.principal = { kind: 'service', sub: r.claims.sub, service, cap: r.claims.cap, ns: r.claims.ns || [], jti: r.claims.jti };
            return next();
        };
    }

    /**
     * A route shared by first-party services (serviceCap) and developer apps (appCap). An app token
     * is judged only on appCap and becomes req.principal = { kind: 'app', sub, appId, projectId,
     * projectKey, source, prefix, env, onBehalfOf }; sandbox tokens are accepted for apps only.
     * Anything else goes through requireCap(serviceCap).
     */
    function appOrService(serviceCap, appCap, { requireService = false } = {}) {
        const serviceGuard = requireCap(serviceCap, { requireService });
        return function appOrServiceGuard(req, res, next) {
            const ctx = req.ov;
            const token = bearer(req);
            if (!token || !config.apps.enabled) return serviceGuard(req, res, next);
            const r = verifyService(token, { acceptSandbox: true });
            if (!r.ok || !/^app:/.test(String(r.claims.sub))) return serviceGuard(req, res, next);
            const principal = apps.appPrincipal(r.claims);
            if (principal.error) return http.sendProblem(res, 401, 'token.invalid_claims', { detail: principal.error, ctx });
            // Network revoked this app after the token was issued (network.app.revoked reached us).
            const revoked = store && store.revokedAt(principal.sub, 'app');
            if (revoked && r.claims.iat * 1000 <= revoked) return http.sendProblem(res, 401, 'token.revoked', { detail: 'this app was revoked', ctx });
            const c = allows(r.claims, appCap);
            if (!c.allowed) return http.sendProblem(res, 403, c.code, { detail: c.reason, ctx });
            req.principal = { ...principal, cap: r.claims.cap, jti: r.claims.jti, iat: r.claims.iat };
            return next();
        };
    }

    /**
     * Who is on the other end of a realtime connection:
     *   { kind: 'service', sub } | { kind: 'user', subjectId, sub } | { kind: 'anonymous' }
     * or { error: { status, code, detail } }. A realtime ticket (?ticket=) must verify and be unused;
     * then nothing else is looked at. A Bearer token must verify; an unverifiable cookie (expired
     * session) degrades to anonymous so the browser still gets public events. The ticket is never logged.
     */
    function realtimeViewer(req) {
        const q = req.query || {};
        if (q.ticket !== undefined) {
            const r = verifyRealtimeTicket(Array.isArray(q.ticket) ? null : q.ticket, { publicKey: keys.get(), issuer: config.issuer });
            if (!r.ok) return { error: { status: r.code === 'token.unavailable' ? 503 : 401, code: r.code, detail: r.reason } };
            if (!spendTicket(r.claims.jti, r.claims.exp)) return { error: { status: 401, code: 'ticket.used', detail: 'a ticket opens one stream; ask Network for a new one' } };
            return { kind: 'user', subjectId: r.claims.sub, sub: r.claims.sub, via: 'ticket' };
        }
        const token = bearer(req);
        if (token) {
            const svc = verifyService(token);
            if (svc.ok) {
                const c = allows(svc.claims, CAPS.read);
                if (!c.allowed) return { error: { status: 403, code: c.code, detail: c.reason } };
                return { kind: 'service', sub: svc.claims.sub };
            }
            if (svc.code === 'token.unavailable') return { error: { status: 503, code: svc.code, detail: svc.reason } };
            const user = verifyUser(token);
            if (user) return userViewer(user);
            return { error: { status: 401, code: 'token.invalid', detail: 'token does not verify' } };
        }
        const fromCookie = cookie(req, 'ov_token');
        const user = fromCookie ? verifyUser(fromCookie) : null;
        if (user) return userViewer(user);
        return { kind: 'anonymous' };
    }

    function userViewer(claims) {
        const subjectId = typeof claims.subject_id === 'string' && /^usr_[0-9A-HJKMNP-TV-Z]{26}$/.test(claims.subject_id) ? claims.subject_id : null;
        return { kind: 'user', subjectId, sub: String(claims.sub) };
    }

    return { verifyService, verifyUser, requireCap, appOrService, realtimeViewer };
}

module.exports = { CAPS, createKeyStore, createAuth, hasCap, allows, verifyUserJwt, verifyRealtimeTicket, serviceSlug, bearer, cookie };
