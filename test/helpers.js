'use strict';
/**
 * Shared test fixtures: a generated Network signing key, token minting, a booted Events service on a
 * random port with a temp database and a manual clock, a stub subscriber, and an SSE reader.
 */
const crypto = require('crypto');
const fs = require('fs');
const os = require('os');
const path = require('path');
const nodeHttp = require('http');
const { serviceAuth, ids } = require('openvibe-contracts');
const { load } = require('../server/config');
const { start } = require('../server/index');

const ISSUER = 'https://openvibe.network';
const { privateKey, publicKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
    publicKeyEncoding: { type: 'spki', format: 'pem' },
});

const silent = { log() {}, warn() {}, error(...a) { if (process.env.DEBUG) console.error(...a); } };

function serviceToken(slug, cap, { aud = 'openvibe.events', exp = Math.floor(Date.now() / 1000) + 300, iss = ISSUER, key = privateKey, sub } = {}) {
    const now = Math.floor(Date.now() / 1000);
    return serviceAuth.signServiceToken({
        iss, sub: sub || `svc:${slug}`, actor_type: 'service', aud: [aud], cap, ns: [], iat: now, exp,
        jti: `tok_${crypto.randomBytes(8).toString('hex')}`,
    }, key);
}

function userToken({ subjectId = ids.newId('user'), aud = ['openvibe.live', 'openvibe.network'], exp = Math.floor(Date.now() / 1000) + 3600, key = privateKey } = {}) {
    return serviceAuth.signServiceToken({
        sub: 57, id: 57, subject_id: subjectId, username: 'viewer', role: 'user', iss: ISSUER, aud, iat: Math.floor(Date.now() / 1000), exp,
    }, key);
}

function manualClock(t = Date.now()) {
    return { t, now() { return this.t; }, advance(ms) { this.t += ms; return this.t; } };
}

const made = [];
process.on('exit', () => { for (const d of made) fs.rmSync(d, { recursive: true, force: true }); });

/** A temp directory removed when the test process exits. */
function tmpDir() {
    const d = fs.mkdtempSync(path.join(os.tmpdir(), 'ov-events-test-'));
    made.push(d);
    return d;
}

/** Boot the service. env overrides go through config.load(); `worker: 'manual'` keeps the loop off. */
async function boot({ env = {}, clock = manualClock(), worker = 'manual', fetchImpl, deliveryFetch } = {}) {
    const dir = tmpDir();
    const config = load({
        NODE_ENV: 'test',
        PORT: '0',
        EVENTS_DB_PATH: path.join(dir, 'events.db'),
        OV_NETWORK_PUBLIC_KEY: publicKey,
        EVENTS_WORKER: worker === 'manual' ? 'off' : 'on',
        ...env,
    });
    const h = await start({ config, clock, log: silent, fetchImpl, deliveryFetch });
    const base = `http://127.0.0.1:${h.server.address().port}`;
    return {
        ...h, base, clock, dir,
        async stop() { await h.close(); fs.rmSync(dir, { recursive: true, force: true }); },
    };
}

async function request(base, method, p, { token, body, headers = {}, raw } = {}) {
    const h = { ...headers };
    if (token) h.Authorization = `Bearer ${token}`;
    if (body !== undefined) h['Content-Type'] = 'application/json';
    const res = await fetch(base + p, { method, headers: h, body: raw !== undefined ? raw : body !== undefined ? JSON.stringify(body) : undefined });
    const text = await res.text();
    let json = null;
    try { json = text ? JSON.parse(text) : null; } catch { json = null; }
    return { status: res.status, body: json, text, headers: res.headers };
}

/** A valid envelope for `source`; override anything. */
function envelope(source = 'live', overrides = {}) {
    return {
        event_id: ids.newId('event'),
        event_type: `${source}.stream.started`,
        version: 1,
        source,
        actor: { type: 'service', id: source },
        timestamp: new Date().toISOString(),
        subject: { type: 'stream', id: 'str_1', revision: 1 },
        payload: { title: 'hello' },
        ...overrides,
    };
}

/** Stub webhook receiver. `respond(req, rawBody, n)` returns a status (default 204). */
async function subscriber(respond = () => 204) {
    const calls = [];
    const server = nodeHttp.createServer((req, res) => {
        const chunks = [];
        req.on('data', c => chunks.push(c));
        req.on('end', () => {
            const rawBody = Buffer.concat(chunks);
            const call = { headers: req.headers, rawBody, body: JSON.parse(rawBody.toString('utf8') || 'null') };
            calls.push(call);
            let status;
            try { status = respond(call, calls.length); } catch (err) { status = 500; call.error = err; }
            if (status === 'destroy') { req.socket.destroy(); return; }
            res.statusCode = status;
            res.end();
        });
    });
    await new Promise(r => server.listen(0, '127.0.0.1', r));
    return {
        calls,
        url: `http://127.0.0.1:${server.address().port}/hook`,
        close: () => new Promise(r => { server.closeAllConnections?.(); server.close(() => r()); }),
    };
}

/** Open an SSE stream and parse it. */
function sse(base, pathAndQuery, { headers = {} } = {}) {
    return new Promise((resolve, reject) => {
        const messages = [];
        const waiters = [];
        const url = new URL(base + pathAndQuery);
        const req = nodeHttp.get({ hostname: url.hostname, port: url.port, path: url.pathname + url.search, headers }, (res) => {
            let buf = '';
            const client = {
                status: res.statusCode,
                headers: res.headers,
                messages,
                body: '',
                events: () => messages.filter(m => !m.event || m.event === 'message').map(m => JSON.parse(m.data)),
                gaps: () => messages.filter(m => m.event === 'gap').map(m => JSON.parse(m.data)),
                comments: [],
                waitFor(pred, ms = 2000) {
                    if (pred(client)) return Promise.resolve(client);
                    return new Promise((ok, fail) => {
                        const w = { pred, ok };
                        waiters.push(w);
                        setTimeout(() => { const i = waiters.indexOf(w); if (i >= 0) { waiters.splice(i, 1); fail(new Error('sse waitFor timed out: ' + JSON.stringify(messages))); } }, ms);
                    });
                },
                close() { req.destroy(); },
            };
            res.setEncoding('utf8');
            res.on('data', (chunk) => {
                client.body += chunk;
                buf += chunk;
                let i;
                while ((i = buf.indexOf('\n\n')) >= 0) {
                    const block = buf.slice(0, i);
                    buf = buf.slice(i + 2);
                    const msg = {};
                    for (const line of block.split('\n')) {
                        if (line.startsWith(':')) { client.comments.push(line.slice(1).trim()); continue; }
                        const c = line.indexOf(':');
                        const k = c < 0 ? line : line.slice(0, c);
                        const v = c < 0 ? '' : line.slice(c + 1).replace(/^ /, '');
                        msg[k] = msg[k] ? msg[k] + '\n' + v : v;
                    }
                    if (msg.data !== undefined) messages.push(msg);
                }
                for (const w of [...waiters]) if (w.pred(client)) { waiters.splice(waiters.indexOf(w), 1); w.ok(client); }
            });
            resolve(client);
        });
        req.on('error', reject);
    });
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** Tiny sequential test runner: t('name', async () => {...}); await t.run(). */
function suite(name) {
    const tests = [];
    const t = (n, fn) => tests.push([n, fn]);
    t.run = async () => {
        let failed = 0;
        for (const [n, fn] of tests) {
            try { await fn(); console.log(`  ok   ${n}`); } catch (err) { failed++; console.log(`  FAIL ${n}\n${err.stack}`); }
        }
        console.log(`${name}: ${tests.length - failed}/${tests.length} passed`);
        if (failed) process.exit(1);
    };
    return t;
}

module.exports = {
    ISSUER, privateKey, publicKey, silent, serviceToken, userToken, manualClock, tmpDir, boot, request,
    envelope, subscriber, sse, sleep, suite,
};
