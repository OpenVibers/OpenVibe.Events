'use strict';
/**
 * A minimal NATS Core client (the text protocol: INFO, CONNECT, PING/PONG, PUB, SUB, UNSUB, MSG, +OK, -ERR) for the
 * nats-v1 TOPIC carrier and the nats-js-v1 STREAM carrier. No headers and no TLS (a server that requires TLS is
 * refused). JetStream is reached through its JSON API over plain request/reply (carriers/jetstream.js), so it needs no
 * client library either. It is kept here, without a dependency, because Events needs exactly publish, a subscription
 * and request/reply; the broker is on loopback or the private network (deploy/nats).
 *
 *   createNatsCore({ url, name, log, reconnectMs, maxReconnectMs, connectTimeoutMs, probe, deniedRetryMs, inboxPrefix, onConnect })
 *     connect()               resolves after the first PONG; rejects on refusal, auth error or timeout. A dropped
 *                             connection reconnects with backoff (from reconnectMs up to maxReconnectMs) and
 *                             re-subscribes; connect() rejecting does not stop the retries.
 *     connected()             true while the handshake is done and the socket open
 *     denied()                the broker's last permissions violation (a refused PUB or SUB, after which NATS keeps
 *                             the connection open and drops the message), else null. The handshake SUBs and PUBs `{}`
 *                             to each `probe` subject before its PING, so a refusal shows up before the first real
 *                             publish. A denied connection is re-opened after deniedRetryMs: a handshake without a
 *                             violation clears it.
 *     publish(subject, obj, reply)       JSON-encodes and writes one PUB (with an optional reply subject); throws
 *                                        when not connected or over max_payload
 *     publishRaw(subject, data, reply)   the same with a string or Buffer as is (a JetStream ack is an empty body)
 *     request(subject, obj, { timeoutMs })  publish with a reply subject under `inboxPrefix` (default `_INBOX.<id>.`)
 *                             and resolve with the first JSON reply; rejects on timeout (a non-JSON reply is
 *                             dropped, so it times out too) or close()
 *     subscribe(subject, fn)  fn(obj, { subject, reply }) per MSG (a non-JSON payload is dropped); returns an
 *                             unsubscribe function
 *     onConnect()             (option) called after every handshake, the first and each reconnect
 *     close()                 stops reconnecting and closes the socket
 *
 * The URL is nats://[user:pass@|token@]host[:4222]. It is never logged (it can carry a credential).
 */
const net = require('net');
const crypto = require('crypto');

const CRLF = '\r\n';
const MAX_LINE = 64 * 1024;
const MAX_BUFFERED = 8 * 1024 * 1024;   // unsent bytes before publish fails (and the breaker counts it)

function parseUrl(raw) {
    let u;
    try { u = new URL(raw); } catch { throw new Error('NATS_URL is not a URL (nats://[user:pass@]host:port)'); }
    if (u.protocol !== 'nats:') throw new Error('NATS_URL must use nats:// (TLS is not supported by nats-v1)');
    const user = decodeURIComponent(u.username || '');
    const pass = decodeURIComponent(u.password || '');
    return {
        host: u.hostname.replace(/^\[|\]$/g, ''),
        port: Number(u.port) || 4222,
        auth: user && pass ? { user, pass } : user ? { auth_token: user } : {},
    };
}

function createNatsCore({ url, name = 'openvibe-events', log = console, reconnectMs = 250, maxReconnectMs = 5000, connectTimeoutMs = 2000, probe = [], deniedRetryMs = 30000,
    inboxPrefix = `_INBOX.${crypto.randomBytes(8).toString('hex')}.`, onConnect = null } = {}) {
    const target = parseUrl(url);
    const where = `${target.host}:${target.port}`;
    const subs = new Map();   // sid -> { subject, fn }
    let nextSid = 1;
    let sock = null;
    let ready = false;
    let stopped = false;
    let timer = null;
    let delay = reconnectMs;
    let maxPayload = 1024 * 1024;
    let first = null;         // { resolve, reject } of the pending connect()
    let denied = null;        // the last permissions violation on the current connection
    let deniedTimer = null;
    const pending = new Map();   // request token -> { resolve, reject, timer }
    let nextToken = 1;
    let inboxSid = 0;            // the one wildcard subscription replies arrive on (made at the first request)

    function settle(err) {
        if (!first) return;
        const f = first; first = null;
        if (err) f.reject(err); else f.resolve();
    }

    function scheduleReconnect() {
        if (stopped || timer) return;
        timer = setTimeout(() => { timer = null; open(); }, delay);
        timer.unref?.();
        delay = Math.min(delay * 2, maxReconnectMs);
    }

    // A permissions change needs a new connection to be seen (and the handshake probe to pass): re-open later.
    function retryDenied(s) {
        if (deniedTimer) return;
        deniedTimer = setTimeout(() => { deniedTimer = null; if (sock === s) s.destroy(); }, deniedRetryMs);
        deniedTimer.unref?.();
    }

    function open() {
        if (stopped) return;
        let buf = Buffer.alloc(0);
        let handshaken = false;
        let violation = null;     // a permissions violation seen during this connection's handshake
        const s = net.connect({ host: target.host, port: target.port });
        sock = s;
        s.setNoDelay(true);
        const deadline = setTimeout(() => { if (!handshaken) s.destroy(new Error(`nats ${where}: connect timed out`)); }, connectTimeoutMs);
        deadline.unref?.();

        s.on('data', (chunk) => {
            buf = buf.length ? Buffer.concat([buf, chunk]) : chunk;
            for (;;) {
                const eol = buf.indexOf(CRLF);
                if (eol < 0) { if (buf.length > MAX_LINE) s.destroy(new Error(`nats ${where}: protocol line too long`)); return; }
                const line = buf.subarray(0, eol).toString('utf8');
                const op = line.split(' ', 1)[0].toUpperCase();
                if (op === 'MSG') {
                    // MSG <subject> <sid> [reply-to] <#bytes>
                    const parts = line.split(' ');
                    const bytes = Number(parts[parts.length - 1]);
                    if (buf.length < eol + 2 + bytes + 2) return;   // wait for the payload
                    const payload = buf.subarray(eol + 2, eol + 2 + bytes);
                    buf = buf.subarray(eol + 2 + bytes + 2);
                    const sub = subs.get(Number(parts[2]));
                    if (sub) {
                        let obj;
                        try { obj = JSON.parse(payload.toString('utf8')); } catch { continue; }
                        const meta = { subject: parts[1], reply: parts.length === 5 ? parts[3] : null };
                        try { sub.fn(obj, meta); } catch (err) { log.warn(`[nats] handler ${sub.subject}: ${err.message}`); }
                    }
                    continue;
                }
                buf = buf.subarray(eol + 2);
                if (op === 'PING') s.write(`PONG${CRLF}`);
                else if (op === 'INFO') {
                    let info = {};
                    try { info = JSON.parse(line.slice(5)); } catch { /* keep the defaults */ }
                    if (info.max_payload) maxPayload = info.max_payload;
                    if (info.tls_required) { s.destroy(new Error(`nats ${where}: the server requires TLS (not supported by nats-v1)`)); return; }
                    if (!handshaken) {
                        const connect = { verbose: false, pedantic: false, lang: 'node', version: '0', name, protocol: 1, headers: false, no_echo: true, ...target.auth };
                        let out = `CONNECT ${JSON.stringify(connect)}${CRLF}`;
                        for (const [sid, sub] of subs) out += `SUB ${sub.subject} ${sid}${CRLF}`;
                        for (const subject of probe) out += `PUB ${subject} 2${CRLF}{}${CRLF}`;
                        s.write(`${out}PING${CRLF}`);
                    }
                } else if (op === 'PONG') {
                    if (!handshaken) {
                        handshaken = true; ready = true; delay = reconnectMs;
                        clearTimeout(deadline);
                        denied = violation;
                        if (denied) retryDenied(s);
                        settle(null);
                        if (onConnect) { try { onConnect(); } catch (err) { log.warn(`[nats] onConnect: ${err.message}`); } }
                    }
                } else if (op === '-ERR') {
                    const why = line.slice(5).trim();
                    log.warn(`[nats] ${where}: ${why}`);
                    // A refused PUB or SUB leaves the connection open (the message is dropped): record it, so the
                    // carrier is unhealthy and placement falls back. Any other -ERR before the handshake is fatal.
                    if (/permissions violation/i.test(why)) {
                        if (!handshaken) violation = why;
                        else if (sock === s) { denied = why; retryDenied(s); }
                    } else if (!handshaken) s.destroy(new Error(`nats ${where}: ${why}`));
                }
                // +OK and anything else: nothing to do
            }
        });
        s.on('error', (err) => { if (!handshaken) settle(err); else log.warn(`[nats] ${where}: ${err.message}`); });
        s.on('close', () => {
            clearTimeout(deadline);
            if (sock === s) { sock = null; ready = false; denied = null; if (deniedTimer) { clearTimeout(deniedTimer); deniedTimer = null; } }
            if (!handshaken) settle(new Error(`nats ${where}: connection closed before the handshake`));
            scheduleReconnect();
        });
    }

    function publishRaw(subject, body, reply) {
        if (!ready || !sock) throw new Error(`nats ${where}: not connected`);
        if (sock.writableLength > MAX_BUFFERED) throw new Error(`nats ${where}: ${sock.writableLength} bytes unsent (the broker is not reading)`);
        const data = Buffer.isBuffer(body) ? body : Buffer.from(String(body ?? ''), 'utf8');
        if (data.length > maxPayload) throw Object.assign(new Error(`nats: payload ${data.length} bytes over max_payload ${maxPayload}`), { code: 'max_payload' });
        sock.write(Buffer.concat([Buffer.from(`PUB ${subject}${reply ? ` ${reply}` : ''} ${data.length}${CRLF}`), data, Buffer.from(CRLF)]));
    }

    function subscribe(subject, fn) {
        const sid = nextSid++;
        subs.set(sid, { subject, fn });
        if (ready && sock) sock.write(`SUB ${subject} ${sid}${CRLF}`);
        return {
            sid,
            async unsubscribe() {
                subs.delete(sid);
                if (ready && sock) sock.write(`UNSUB ${sid}${CRLF}`);
            },
        };
    }

    function onReply(obj, { subject }) {
        const p = pending.get(subject.slice(inboxPrefix.length));
        if (!p) return;   // late (timed out) or not ours
        pending.delete(subject.slice(inboxPrefix.length));
        clearTimeout(p.timer);
        p.resolve(obj);
    }

    return {
        connect() {
            if (ready) return Promise.resolve();
            stopped = false;
            return new Promise((resolve, reject) => {
                first = { resolve, reject };
                if (!sock && !timer) open();
            });
        },
        connected: () => ready,
        denied: () => (ready ? denied : null),
        publish(subject, obj, reply) { publishRaw(subject, JSON.stringify(obj), reply); },
        publishRaw,
        request(subject, obj, { timeoutMs = 2000 } = {}) {
            if (!inboxSid) inboxSid = subscribe(`${inboxPrefix}*`, onReply).sid;
            const token = String(nextToken++);
            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => { pending.delete(token); reject(new Error(`nats ${where}: request ${subject} timed out`)); }, timeoutMs);
                timer.unref?.();
                pending.set(token, { resolve, reject, timer });
                try { publishRaw(subject, JSON.stringify(obj), `${inboxPrefix}${token}`); }
                catch (err) { clearTimeout(timer); pending.delete(token); reject(err); }
            });
        },
        subscribe(subject, fn) { return subscribe(subject, fn).unsubscribe; },
        async close() {
            stopped = true;
            for (const [token, p] of pending) { clearTimeout(p.timer); pending.delete(token); p.reject(new Error('nats: closed')); }
            if (timer) { clearTimeout(timer); timer = null; }
            if (deniedTimer) { clearTimeout(deniedTimer); deniedTimer = null; }
            ready = false;
            settle(new Error('nats: closed'));
            const s = sock;
            sock = null;
            if (s && !s.destroyed) await new Promise((r) => { s.once('close', r); s.end(); setTimeout(() => s.destroy(), 200).unref?.(); });
        },
        get maxPayload() { return maxPayload; },
    };
}

module.exports = { createNatsCore, parseUrl };
