'use strict';
/**
 * Outbound delivery to developer-app endpoints (events.app.subscribe). SSRF guard:
 *
 *   - https only, no credentials in the URL, no trailing-dot or single-label hostnames
 *   - every address the hostname resolves to must be public (no loopback, private, link-local,
 *     CGNAT, multicast, documentation, benchmarking, reserved, unique-local, NAT64/6to4/Teredo
 *     wrapping a non-public v4 address, …)
 *   - checked when the subscription is created AND again at delivery time, inside the socket's own
 *     DNS lookup, so the connection goes to the address that was checked (no rebinding window)
 *   - redirects are never followed (a 3xx is a failed attempt)
 *
 * First-party subscriptions keep the host allow-list in server/endpoints.js.
 */
const dns = require('dns');
const net = require('net');
const https = require('https');

// The public-address rule is openvibe-shared/egress (the one Live and Tools use too); this file keeps the
// app-endpoint syntax rules and the guarded POST on top of it.
const { isPublicAddress, embeddedV4, isInternalName } = require('openvibe-shared/egress');

/** Syntax part of the check: { ok, url } or { ok: false, reason }. No DNS. */
function parseAppEndpoint(endpoint) {
    if (typeof endpoint !== 'string' || endpoint.length > 2048) return { ok: false, reason: 'endpoint must be a URL of at most 2048 characters' };
    let url;
    try { url = new URL(endpoint); } catch { return { ok: false, reason: 'not a URL' }; }
    if (url.protocol !== 'https:') return { ok: false, reason: 'app endpoints must use https' };
    if (url.username || url.password) return { ok: false, reason: 'credentials in the URL are not allowed' };
    let host = url.hostname.toLowerCase();
    if (host.startsWith('[') && host.endsWith(']')) host = host.slice(1, -1);
    if (host.endsWith('.')) return { ok: false, reason: 'trailing-dot hostnames are not allowed' };
    if (net.isIP(host)) {
        if (!isPublicAddress(host)) return { ok: false, reason: `${host} is not a public address` };
        return { ok: true, url, host, literal: true };
    }
    if (isInternalName(host)) {
        return { ok: false, reason: `${host} is not a public hostname` };
    }
    return { ok: true, url, host, literal: false };
}

/** Resolve every address of `host`; { ok, addresses } or { ok: false, reason, transient }. */
function resolvePublic(host, { lookup = dns.lookup } = {}) {
    return new Promise((resolve) => {
        lookup(host, { all: true, verbatim: true }, (err, addresses) => {
            if (err) return resolve({ ok: false, reason: `cannot resolve ${host}: ${err.code || err.message}`, transient: true });
            const list = Array.isArray(addresses) ? addresses : [{ address: addresses, family: net.isIP(addresses) }];
            if (!list.length) return resolve({ ok: false, reason: `${host} has no addresses`, transient: true });
            const bad = list.find(a => !isPublicAddress(a.address));
            if (bad) return resolve({ ok: false, reason: `${host} resolves to a non-public address` });
            return resolve({ ok: true, addresses: list });
        });
    });
}

/** Full subscribe-time check: syntax plus DNS. */
async function checkAppEndpoint(endpoint, { lookup } = {}) {
    const p = parseAppEndpoint(endpoint);
    if (!p.ok || p.literal) return p;
    const r = await resolvePublic(p.host, { lookup });
    return r.ok ? p : { ok: false, reason: r.reason };
}

/**
 * POST to an app endpoint through the guard. Returns { status } or throws (err.permanent = true
 * when the endpoint itself is refused). `requestImpl`/`isAllowed` exist for tests only.
 */
function createGuardedPost({ lookup = dns.lookup, requestImpl = https.request, isAllowed = isPublicAddress } = {}) {
    return function guardedPost(endpoint, { headers, body, timeoutMs = 10000 }) {
        const p = parseAppEndpoint(endpoint);
        if (!p.ok) return Promise.reject(Object.assign(new Error(`endpoint not allowed: ${p.reason}`), { permanent: true }));
        if (p.literal && !isAllowed(p.host)) return Promise.reject(Object.assign(new Error('endpoint not allowed: not a public address'), { permanent: true }));
        return new Promise((resolve, reject) => {
            let settled = false;
            let deadline = null;
            const done = (fn, v) => { if (!settled) { settled = true; clearTimeout(deadline); fn(v); } };
            // The socket's own lookup: the address we connect to is the one we checked.
            const guardedLookup = (hostname, opts, cb) => {
                if (typeof opts === 'function') { cb = opts; opts = {}; }
                lookup(hostname, { all: true, verbatim: true, family: (opts && opts.family) || 0 }, (err, addresses) => {
                    if (err) return cb(err);
                    const list = Array.isArray(addresses) ? addresses : [{ address: addresses, family: net.isIP(addresses) }];
                    if (!list.length || list.some(a => !isAllowed(a.address))) {
                        return cb(Object.assign(new Error(`${hostname} resolves to a non-public address`), { code: 'EOV_NOT_PUBLIC', permanent: true }));
                    }
                    if (opts && opts.all) return cb(null, list.map(a => ({ address: a.address, family: a.family || net.isIP(a.address) })));
                    return cb(null, list[0].address, list[0].family || net.isIP(list[0].address));
                });
            };
            const req = requestImpl({
                hostname: p.host,
                port: p.url.port || undefined,
                path: `${p.url.pathname}${p.url.search}`,
                method: 'POST',
                headers: { ...headers, 'Content-Length': Buffer.byteLength(body) },
                lookup: p.literal ? undefined : guardedLookup,
                servername: p.literal ? undefined : p.host,
                timeout: timeoutMs,
            }, (res) => {
                // The status is all a delivery needs: settle now and drop the body, so an endpoint
                // that drips its response cannot keep this attempt (and its worker slot) open.
                done(resolve, { status: res.statusCode });
                res.destroy();
            });
            const timeout = () => { req.destroy(Object.assign(new Error('timeout'), { name: 'TimeoutError' })); };
            // `timeout` above is only the socket's idle timer; this bounds the whole attempt, however
            // slowly the endpoint trickles its status line and headers.
            deadline = setTimeout(timeout, timeoutMs);
            req.on('timeout', timeout);
            req.on('error', (err) => {
                if (err.code === 'EOV_NOT_PUBLIC' || (err.cause && err.cause.code === 'EOV_NOT_PUBLIC')) err.permanent = true;
                done(reject, err);
            });
            req.end(body);
        });
    };
}

module.exports = { isPublicAddress, parseAppEndpoint, resolvePublic, checkAppEndpoint, createGuardedPost, embeddedV4 };
