'use strict';
// SSRF (roadmap WS-R task 5). A developer app chooses its subscription endpoint, so every spelling
// of an internal address must be refused where the subscription is made AND where each delivery is
// made (server/egress.js), with DNS answered by a stub:
//   loopback, RFC1918, CGNAT, 169.254.169.254, ::1, ::, ::ffff:127.0.0.1, ::ffff:7f00:1, fc00::/7,
//   fe80::, NAT64 64:ff9b::7f00:1, 6to4 2002:7f00:1::, decimal 2130706433, octal 0177.0.0.1, hex
//   0x7f000001, 0.0.0.0, trailing-dot and internal names, userinfo tricks, plain http; names that
//   resolve to any of those, mixed answers, and a name that answers public first and internal next
//   (DNS rebinding: the subscription is accepted, the delivery refused for good, nothing sent).
// Redirects are never followed, for app endpoints and first-party ones alike. First-party
// subscriptions keep their host allow-list (127.0.0.1 and *.openvibe.*): its lookalikes are refused.
// A ratchet lists every outbound call site in server/ (comments stripped) with where it goes and why
// a stranger cannot choose it; a new one fails until it is reviewed here.
//   node test/security-ssrf.test.js
const assert = require('assert');
const fs = require('fs');
const net = require('net');
const path = require('path');
const nodeHttp = require('http');
const { ids } = require('openvibe-contracts');
const { suite, boot, request, serviceToken, appToken, envelope, subscriber } = require('./helpers');
const { createGuardedPost, parseAppEndpoint } = require('../server/egress');
const apps = require('../server/apps');

const t = suite('security-ssrf');
const ALL = ['events.app.publish', 'events.app.read', 'events.app.subscribe'];

// ── DNS stub: what every test name resolves to (a function: called once per lookup) ──
const PUBLIC = '93.184.216.34';
let rebindCalls = 0;
const A = (...addrs) => () => addrs.map((a) => ({ address: a, family: net.isIP(a) }));
const DNS = {
    'hooks.example.com': A(PUBLIC),
    'redirect.example.com': A(PUBLIC),
    'loop.example.com': A('127.0.0.1'),
    'v6loop.example.com': A('::1'),
    'mapped.example.com': A('::ffff:127.0.0.1'),
    'mappedhex.example.com': A('::ffff:7f00:1'),
    'meta.example.com': A('169.254.169.254'),
    'metamapped.example.com': A('::ffff:a9fe:a9fe'),
    'private.example.com': A('10.1.2.3'),
    'cgnat.example.com': A('100.64.1.1'),
    'zero.example.com': A('0.0.0.0'),
    'ula.example.com': A('fd00::1'),
    'linklocal.example.com': A('fe80::1'),
    'nat64.example.com': A('64:ff9b::7f00:1'),
    'sixto4.example.com': A('2002:7f00:1::1'),
    'mixed.example.com': A(PUBLIC, '10.0.0.1'),
    'mixed6.example.com': A(PUBLIC, '::1'),
    'rebind.example.com': () => (rebindCalls++ === 0 ? [{ address: PUBLIC, family: 4 }] : [{ address: '127.0.0.1', family: 4 }]),
};
const INTERNAL_NAMES = Object.keys(DNS).filter((n) => !['hooks.example.com', 'redirect.example.com', 'rebind.example.com'].includes(n));
function dnsLookup(host, opts, cb) {
    if (typeof opts === 'function') { cb = opts; opts = {}; }
    const f = DNS[String(host).toLowerCase()];
    if (!f) return process.nextTick(() => cb(Object.assign(new Error('not found'), { code: 'ENOTFOUND' })));
    const list = f();
    process.nextTick(() => (opts && opts.all ? cb(null, list) : cb(null, list[0].address, list[0].family)));
}
const LITERALS = ['127.0.0.1', '127.1', '10.0.0.1', '172.16.0.1', '192.168.1.1', '100.64.0.1', '169.254.169.254', '0.0.0.0', '[::1]', '[::]',
    '[::ffff:127.0.0.1]', '[::ffff:7f00:1]', '[fc00::1]', '[fd12:3456::1]', '[fe80::1]', '[64:ff9b::7f00:1]', '[2002:7f00:1::]',
    '2130706433', '0177.0.0.1', '0x7f000001', '0x7f.0.0.1', '127.0.0.1.', '[::ffff:a9fe:a9fe]', '[::ffff:169.254.169.254]'];
const NAMES = ['localhost', 'localhost.', 'foo.localhost', 'printer.local', 'db.internal', 'router.home.arpa', 'intranet', 'hooks.example.com.'];
function internalEndpoints(port = 443) {
    return [
        ...LITERALS.map((h) => `https://${h}:${port}/hook`),
        ...NAMES.map((h) => `https://${h}:${port}/hook`),
        ...INTERNAL_NAMES.map((h) => `https://${h}:${port}/hook`),
        `https://user:pass@hooks.example.com:${port}/hook`, `https://hooks.example.com@127.0.0.1:${port}/hook`, `https://hooks.example.com:x@127.0.0.1:${port}/`,
        `http://hooks.example.com:${port}/hook`, `ftp://hooks.example.com/hook`, 'file:///etc/passwd', 'javascript:alert(1)', `//hooks.example.com/hook`, '',
    ];
}

// ── The receiving side: every address the guard approved is served by one local receiver ──
const received = [];
const approved = [];
const receiver = nodeHttp.createServer((req, res) => {
    req.resume();
    req.on('end', () => {
        received.push({ host: req.headers.host, url: req.url });
        if (req.url.startsWith('/redirect')) { res.writeHead(302, { Location: `http://127.0.0.1:${internalPort}/metadata` }); return res.end(); }
        res.statusCode = 204; res.end();
    });
});
let internalHits = 0;
const internal = nodeHttp.createServer((req, res) => { internalHits++; res.end('internal'); });
let receiverPort; let internalPort;
// The real guard (its own lookup and address rule); only the connection itself is redirected to the
// local receiver, and only after the guard approved the address.
const guarded = createGuardedPost({
    lookup: dnsLookup,
    requestImpl: (options, cb) => nodeHttp.request({
        ...options, port: receiverPort,
        lookup: options.lookup && ((h, o, done) => options.lookup(h, o, (err, a, f) => {
            if (err) return done(err);
            const list = Array.isArray(a) ? a : [{ address: a, family: f }];
            approved.push(...list.map((x) => x.address));
            const local = list.map(() => ({ address: '127.0.0.1', family: 4 }));
            return o && o.all ? done(null, local) : done(null, '127.0.0.1', 4);
        })),
        hostname: options.lookup ? options.hostname : '127.0.0.1',   // a literal (only a public one gets here) is served locally too
    }, cb),
});

let h;
const prj = `prj_${ids.ulid()}`;
const app = ids.newId('app');
const key = apps.projectKey(prj);
const tokApp = () => appToken({ appId: app, projectId: prj, env: 'sandbox', cap: ALL });
const sub = async (token, endpoint, topic = `app.${key}.*`) => await request(h.base, 'POST', '/api/v1/subscriptions', { token, body: { topic_pattern: topic, endpoint } });

t('boot', async () => {
    await new Promise((r) => receiver.listen(0, '127.0.0.1', r));
    await new Promise((r) => internal.listen(0, '127.0.0.1', r));
    receiverPort = receiver.address().port; internalPort = internal.address().port;
    // Dozens of subscribe attempts from one app, in one minute of the manual clock: no quota or
    // per-actor limit may answer before the endpoint check under test.
    h = await boot({ appPost: guarded, dnsLookup, env: { EVENTS_APP_SANDBOX_MAX_SUBSCRIPTIONS: '50', EVENTS_LIMITS: 'off' } });
});

t('at creation: every internal spelling is refused (422), a public name accepted', async () => {
    const bad = [];
    for (const endpoint of internalEndpoints()) {
        const r = await sub(tokApp(), endpoint);
        if (r.status !== 422 || r.body.code !== 'events.endpoint_not_allowed') bad.push(`${endpoint} → ${r.status} ${r.text.slice(0, 100)}`);
    }
    assert.deepStrictEqual(bad, [], `accepted:\n${bad.join('\n')}`);
    const ok = await sub(tokApp(), 'https://hooks.example.com/hook');
    assert.strictEqual(ok.status, 201, 'positive control');
});

t('at delivery: the guard refuses every internal spelling itself, and nothing is sent', async () => {
    received.length = 0; approved.length = 0;
    const bad = [];
    for (const endpoint of internalEndpoints(receiverPort)) {
        try {
            await guarded(endpoint, { headers: {}, body: '{}', timeoutMs: 2000 });
            bad.push(`sent to ${endpoint}`);
        } catch (err) {
            if (!err.permanent) bad.push(`${endpoint}: refusal not permanent (${err.message})`);
        }
    }
    assert.deepStrictEqual(bad, [], bad.join('\n'));
    assert.deepStrictEqual(received, [], 'the receiver saw nothing');
    assert.deepStrictEqual(approved, [], 'no internal address was ever approved for a connection');
    // Positive control: a public name goes through the same path.
    const r = await guarded(`https://hooks.example.com:${receiverPort}/ok`, { headers: {}, body: '{}' });
    assert.strictEqual(r.status, 204);
    assert.deepStrictEqual(approved, [PUBLIC], 'the socket connected to the address that was checked');
    // The syntax check alone (no DNS) refuses every literal and internal name.
    for (const endpoint of internalEndpoints().filter((e) => !INTERNAL_NAMES.some((n) => e.includes(n)))) assert.ok(!parseAppEndpoint(endpoint).ok, endpoint);
});

t('DNS rebinding: public at subscribe, internal at delivery: dead at once, nothing sent', async () => {
    const r = await sub(tokApp(), 'https://rebind.example.com/rebind', `app.${key}.rebind.*`);
    assert.strictEqual(r.status, 201, r.text);
    received.length = 0; approved.length = 0;
    const e = await request(h.base, 'POST', '/api/v1/events', { token: tokApp(), body: envelope('x', { source: apps.appSource(app), event_type: `app.${key}.rebind.hit`, actor: { type: 'app', id: app } }) });
    assert.strictEqual(e.status, 201, e.text);
    await h.worker.drain();
    const d = await h.store.getDelivery(e.body.event_id, r.body.id);
    assert.strictEqual(d.status, 'dead', 'refused for good');
    assert.match(d.last_error, /non-public/);
    assert.ok(!received.some((x) => x.url === '/rebind'), 'nothing reached the rebound address');
    assert.ok(!approved.includes('127.0.0.1'));
    assert.ok(rebindCalls >= 2, 'resolved again at delivery time');
});

t('redirects are never followed (app endpoints and first-party ones)', async () => {
    const r = await sub(tokApp(), 'https://redirect.example.com/redirect', `app.${key}.redir.*`);
    assert.strictEqual(r.status, 201, r.text);
    received.length = 0; internalHits = 0;
    const e = await request(h.base, 'POST', '/api/v1/events', { token: tokApp(), body: envelope('x', { source: apps.appSource(app), event_type: `app.${key}.redir.hit`, actor: { type: 'app', id: app } }) });
    await h.worker.drain();
    const d = await h.store.getDelivery(e.body.event_id, r.body.id);
    assert.strictEqual(d.last_status, 302, 'a 3xx is a failed attempt');
    assert.notStrictEqual(d.status, 'delivered');
    assert.strictEqual(received.filter((x) => x.url === '/redirect').length, 1, 'one request to the endpoint, none to where it pointed');
    assert.strictEqual(internalHits, 0, 'the redirect target was never asked');
    // First party: a subscriber on 127.0.0.1 answering 302 to another local service.
    const fp = await subscriber(() => 302);
    const live = serviceToken('live', ['events.event.publish', 'events.subscription.manage']);
    const s = await sub(live, fp.url, 'live.*');
    assert.strictEqual(s.status, 201, s.text);
    const le = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope('live') });
    await h.worker.drain();
    assert.strictEqual(fp.calls.length, 1);
    assert.strictEqual((await h.store.getDelivery(le.body.event_id, s.body.id)).last_status, 302);
    assert.strictEqual(internalHits, 0);
    await fp.close();
});

t('first-party allow-list: 127.0.0.1 and *.openvibe.* only, lookalikes refused', async () => {
    const live = serviceToken('fpcheck', ['events.subscription.manage']);
    const refused = ['http://localhost:4000/x', 'http://[::1]:4000/x', 'http://[::ffff:127.0.0.1]:4000/x', 'http://0.0.0.0:4000/x', 'http://10.0.0.5/x',
        'http://169.254.169.254/latest/meta-data', 'http://[::ffff:a9fe:a9fe]/x', 'https://openvibe.live.evil.test/x', 'https://evilopenvibe.live/x',
        'https://openvibe.live@evil.test/x', 'https://u:p@openvibe.live/x', 'https://openvibe.live./x', 'https://x.openvibe/x', 'https://openvibe.live%2eevil.test/x',
        'ftp://openvibe.live/x', 'http://127.0.0.2/x', 'http://127.0.0.1.nip.io/x'];
    const bad = [];
    let n = 0;
    for (const endpoint of refused) {
        const r = await sub(live, endpoint, `live.check${n++}.*`);
        if (r.status !== 422) bad.push(`${endpoint} → ${r.status}`);
    }
    assert.deepStrictEqual(bad, [], `accepted:\n${bad.join('\n')}`);
    for (const endpoint of ['http://127.0.0.1:4000/x', 'https://hooks.openvibe.live/x', 'https://openvibe.network/x']) {
        assert.strictEqual((await sub(live, endpoint, `live.ok${n++}.*`)).status, 201, endpoint);
    }
});

// ── Ratchet: outbound call sites in server/ ─────────────────────────────
const PATTERNS = {
    fetch: /\bfetch(?:Impl)?\s*\(/g, 'http.request': /\bhttps?\.(?:request|get)\b/g, requestImpl: /\brequestImpl\s*\(/g, appPost: /\bappPost\s*\(/g,
    websocket: /\bnew\s+WebSocket\s*\(/g, axios: /\baxios\b/g, got: /require\(\s*['"]got['"]\s*\)/g, undici: /\bundici\b/g,
    'net.connect': /\b(?:net|tls)\.(?:connect|createConnection)\s*\(/g,
};
const REVIEWED = {
    'server/auth.js fetch': [1, 'the Network signing key: OV_NETWORK_INTERNAL_URL / OV_NETWORK_URL /api/.well-known/jwks (operator config)'],
    'server/worker.js fetch': [1, 'first-party deliveries: endpoints on the host allow-list (server/endpoints.js), checked again at delivery, redirect: manual'],
    'server/worker.js appPost': [1, 'developer-app deliveries: the guarded poster (server/egress.js), covered above'],
    'server/egress.js http.request': [1, 'the guarded poster\'s https.request (default requestImpl): its own lookup pins the checked address'],
    'server/egress.js requestImpl': [1, 'the guarded poster\'s request, after parseAppEndpoint and with the guarded lookup'],
};
/** JS source with comments blanked (strings, template literals and regex literals kept). */
function stripComments(src) {
    let o = '';
    let i = 0;
    let prev = '';
    const n = src.length;
    while (i < n) {
        const c = src[i];
        const d = src[i + 1];
        if (c === '/' && d === '/') { while (i < n && src[i] !== '\n') { o += ' '; i++; } continue; }
        if (c === '/' && d === '*') { const end = src.indexOf('*/', i + 2); const stop = end < 0 ? n : end + 2; o += src.slice(i, stop).replace(/[^\n]/g, ' '); i = stop; continue; }
        if (c === '\'' || c === '"' || c === '`') {
            let j = i + 1;
            while (j < n && src[j] !== c) { if (src[j] === '\\') j++; j++; }
            o += src.slice(i, j + 1); i = j + 1; prev = c; continue;
        }
        if (c === '/' && (prev === '' || '(,=:[!&|?{};+-*%<>~^'.includes(prev))) {
            let j = i + 1; let cls = false;
            while (j < n && src[j] !== '\n') { if (src[j] === '\\') { j += 2; continue; } if (src[j] === '[') cls = true; else if (src[j] === ']') cls = false; else if (src[j] === '/' && !cls) break; j++; }
            o += src.slice(i, j + 1); i = j + 1; prev = '/'; continue;
        }
        o += c;
        if (!/\s/.test(c)) prev = c;
        i++;
    }
    return o;
}

t('ratchet: every outbound call site in server/ is reviewed', async () => {
    assert.strictEqual(stripComments("const a = '/* no */'; // fetch(x)\n/* fetch(y) */ fetch(z); const r = /\\/\\*/;").match(PATTERNS.fetch).length, 1, 'the stripper keeps strings, drops comments');
    const root = path.join(__dirname, '..');
    const inv = {};
    const walk = (d) => {
        for (const f of fs.readdirSync(d)) {
            const p = path.join(d, f);
            if (fs.statSync(p).isDirectory()) { walk(p); continue; }
            if (!p.endsWith('.js')) continue;
            const src = stripComments(fs.readFileSync(p, 'utf8'));
            for (const [kind, re] of Object.entries(PATTERNS)) { const k = (src.match(re) || []).length; if (k) inv[`${path.relative(root, p).split(path.sep).join('/')} ${kind}`] = k; }
        }
    };
    walk(path.join(root, 'server'));
    const unreviewed = Object.entries(inv).filter(([k, c]) => !REVIEWED[k] || REVIEWED[k][0] !== c).map(([k, c]) => `${k}: ${c}${REVIEWED[k] ? ` (reviewed: ${REVIEWED[k][0]})` : ' (not reviewed)'}`);
    assert.deepStrictEqual(unreviewed, [], `outbound call sites changed; review each and update REVIEWED in this file:\n${unreviewed.join('\n')}`);
    assert.deepStrictEqual(Object.keys(REVIEWED).filter((k) => !inv[k]), [], 'reviewed call sites no longer exist; drop them');
});

t('stop', async () => {
    await h.stop();
    await new Promise((r) => receiver.close(r));
    await new Promise((r) => internal.close(r));
});

t.run();
