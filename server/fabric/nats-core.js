'use strict';
/**
 * A minimal NATS Core client (the text protocol: INFO, CONNECT, PING/PONG, PUB, SUB, UNSUB, MSG, +OK, -ERR) for the
 * nats-v1 TOPIC carrier. Core NATS only: no JetStream, no headers, no request/reply, no TLS (a server that requires
 * TLS is refused). It is kept here, without a dependency, because Events needs exactly fire-and-forget publish and a
 * subscription; the broker is on loopback or the private network (deploy/nats).
 *
 *   createNatsCore({ url, name, log, reconnectMs, maxReconnectMs, connectTimeoutMs })
 *     connect()               resolves after the first PONG; rejects on refusal, auth error or timeout. A dropped
 *                             connection reconnects with backoff (from reconnectMs up to maxReconnectMs) and
 *                             re-subscribes; connect() rejecting does not stop the retries.
 *     connected()             true while the handshake is done and the socket open
 *     publish(subject, obj)   JSON-encodes and writes one PUB; throws when not connected or over max_payload
 *     subscribe(subject, fn)  fn(obj) per MSG (a non-JSON payload is dropped); returns an unsubscribe function
 *     close()                 stops reconnecting and closes the socket
 *
 * The URL is nats://[user:pass@|token@]host[:4222]. It is never logged (it can carry a credential).
 */
const net = require('net');

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

function createNatsCore({ url, name = 'openvibe-events', log = console, reconnectMs = 250, maxReconnectMs = 5000, connectTimeoutMs = 2000 } = {}) {
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

    function open() {
        if (stopped) return;
        let buf = Buffer.alloc(0);
        let handshaken = false;
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
                        try { sub.fn(obj); } catch (err) { log.warn(`[nats] handler ${sub.subject}: ${err.message}`); }
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
                        s.write(`${out}PING${CRLF}`);
                    }
                } else if (op === 'PONG') {
                    if (!handshaken) {
                        handshaken = true; ready = true; delay = reconnectMs;
                        clearTimeout(deadline);
                        settle(null);
                    }
                } else if (op === '-ERR') {
                    const why = line.slice(5).trim();
                    log.warn(`[nats] ${where}: ${why}`);
                    if (!handshaken) s.destroy(new Error(`nats ${where}: ${why}`));
                }
                // +OK and anything else: nothing to do
            }
        });
        s.on('error', (err) => { if (!handshaken) settle(err); else log.warn(`[nats] ${where}: ${err.message}`); });
        s.on('close', () => {
            clearTimeout(deadline);
            if (sock === s) { sock = null; ready = false; }
            if (!handshaken) settle(new Error(`nats ${where}: connection closed before the handshake`));
            scheduleReconnect();
        });
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
        publish(subject, obj) {
            if (!ready || !sock) throw new Error(`nats ${where}: not connected`);
            if (sock.writableLength > MAX_BUFFERED) throw new Error(`nats ${where}: ${sock.writableLength} bytes unsent (the broker is not reading)`);
            const data = Buffer.from(JSON.stringify(obj), 'utf8');
            if (data.length > maxPayload) throw Object.assign(new Error(`nats: payload ${data.length} bytes over max_payload ${maxPayload}`), { code: 'max_payload' });
            sock.write(Buffer.concat([Buffer.from(`PUB ${subject} ${data.length}${CRLF}`), data, Buffer.from(CRLF)]));
        },
        subscribe(subject, fn) {
            const sid = nextSid++;
            subs.set(sid, { subject, fn });
            if (ready && sock) sock.write(`SUB ${subject} ${sid}${CRLF}`);
            return async () => {
                subs.delete(sid);
                if (ready && sock) sock.write(`UNSUB ${sid}${CRLF}`);
            };
        },
        async close() {
            stopped = true;
            if (timer) { clearTimeout(timer); timer = null; }
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
