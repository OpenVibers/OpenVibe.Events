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

const blocked = new net.BlockList();
for (const [addr, prefix] of [
    ['0.0.0.0', 8], ['10.0.0.0', 8], ['100.64.0.0', 10], ['127.0.0.0', 8], ['169.254.0.0', 16], ['172.16.0.0', 12],
    ['192.0.0.0', 24], ['192.0.2.0', 24], ['192.88.99.0', 24], ['192.168.0.0', 16], ['198.18.0.0', 15],
    ['198.51.100.0', 24], ['203.0.113.0', 24], ['224.0.0.0', 4], ['240.0.0.0', 4],
]) blocked.addSubnet(addr, prefix, 'ipv4');
for (const [addr, prefix] of [
    ['::', 128], ['::1', 128], ['::', 96], ['100::', 64], ['2001::', 23], ['2001:db8::', 32], ['fc00::', 7], ['fe80::', 10],
    ['fec0::', 10], ['ff00::', 8], ['3fff::', 20], ['5f00::', 16],
]) blocked.addSubnet(addr, prefix, 'ipv6');

/** The IPv4 address an IPv6 address carries (mapped, compatible-ish NAT64, 6to4, Teredo), or null. */
function embeddedV4(ip) {
    const words = expandV6(ip);
    if (!words) return null;
    const v4 = (hi, lo) => `${hi >> 8}.${hi & 255}.${lo >> 8}.${lo & 255}`;
    if (words.slice(0, 5).every(w => w === 0) && words[5] === 0xffff) return v4(words[6], words[7]);            // ::ffff:a.b.c.d
    if (words[0] === 0x64 && words[1] === 0xff9b) return v4(words[6], words[7]);                                // 64:ff9b::/96 (and /48)
    if (words[0] === 0x2002) return v4(words[1], words[2]);                                                      // 6to4
    if (words[0] === 0x2001 && words[1] === 0) return v4(words[6] ^ 0xffff, words[7] ^ 0xffff);                 // Teredo client
    return null;
}

function expandV6(ip) {
    let s = String(ip).toLowerCase();
    const pct = s.indexOf('%');
    if (pct >= 0) s = s.slice(0, pct);
    if (net.isIPv6(s) === false) return null;
    // Trailing dotted quad -> two words.
    const dq = /(\d+\.\d+\.\d+\.\d+)$/.exec(s);
    if (dq) {
        const p = dq[1].split('.').map(Number);
        s = s.slice(0, -dq[1].length) + `${((p[0] << 8) | p[1]).toString(16)}:${((p[2] << 8) | p[3]).toString(16)}`;
    }
    const [head, tail] = s.split('::');
    const h = head ? head.split(':') : [];
    const t = tail !== undefined ? (tail ? tail.split(':') : []) : null;
    const words = t === null ? h : [...h, ...Array(8 - h.length - t.length).fill('0'), ...t];
    if (words.length !== 8) return null;
    return words.map(w => parseInt(w || '0', 16));
}

/** True only for a globally routable unicast address. */
function isPublicAddress(ip) {
    const s = String(ip || '');
    const family = net.isIP(s);
    if (family === 4) return !blocked.check(s, 'ipv4');
    if (family === 6) {
        const bare = s.split('%')[0];
        if (blocked.check(bare, 'ipv6')) return false;
        const v4 = embeddedV4(bare);
        if (v4 !== null) return !blocked.check(v4, 'ipv4');
        return true;
    }
    return false;
}

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
    if (!host.includes('.') || host === 'localhost' || host.endsWith('.localhost') || host.endsWith('.local') || host.endsWith('.internal')) {
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
