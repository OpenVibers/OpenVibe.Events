'use strict';
/**
 * Subscription endpoint allow-list (SSRF guard). An endpoint is an http(s) URL without credentials
 * whose hostname matches one of the configured patterns:
 *
 *   127.0.0.1      exactly that host (services on the same machine)
 *   *.openvibe.*   openvibe.<tld> and any subdomain of it (live.openvibe.network, openvibe.live)
 *   exact.host     that hostname only
 *
 * IP literals other than an exact allowed entry never pass, whatever the pattern.
 */
const net = require('net');

const LABEL = '[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?';

function hostPatternToRegex(pattern) {
    const p = String(pattern).toLowerCase();
    if (p === '*.openvibe.*') return new RegExp(`^(?:${LABEL}\\.)*openvibe\\.[a-z]{2,63}$`);
    if (p.startsWith('*.')) {
        const rest = p.slice(2).replace(/[.]/g, '\\.');
        return new RegExp(`^(?:${LABEL}\\.)*${rest}$`);
    }
    return new RegExp(`^${p.replace(/[.]/g, '\\.')}$`);
}

function checkEndpoint(endpoint, allowed) {
    let url;
    try { url = new URL(String(endpoint)); } catch { return { ok: false, reason: 'not a URL' }; }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return { ok: false, reason: 'only http and https endpoints' };
    if (url.username || url.password) return { ok: false, reason: 'credentials in the URL are not allowed' };
    if (String(endpoint).length > 2048) return { ok: false, reason: 'URL too long' };
    let host = url.hostname.toLowerCase();
    if (host.startsWith('[') && host.endsWith(']')) host = host.slice(1, -1);
    if (host.endsWith('.')) return { ok: false, reason: 'trailing-dot hostnames are not allowed' };
    const isIp = net.isIP(host) !== 0;
    for (const pattern of allowed) {
        const p = String(pattern).toLowerCase();
        if (isIp) {
            if (p === host) return { ok: true, url };
            continue;
        }
        if (net.isIP(p)) continue;
        if (hostPatternToRegex(p).test(host)) return { ok: true, url };
    }
    return { ok: false, reason: `host ${host} is not on the endpoint allow-list` };
}

module.exports = { checkEndpoint, hostPatternToRegex };
