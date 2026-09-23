'use strict';
/**
 * Realtime gateway (SSE), inside this service until ADR-005 decides where Realtime lives.
 *
 *   GET /realtime/stream?topics=live.stream.*,network.notification.*
 *
 * Every message is `id: <seq>` + `data: {"seq":n,"event":<envelope>}`. Visibility decides who sees
 * an event:
 *   public    anyone subscribed to a matching topic (signed-out visitors too, unless disabled)
 *   subject   only the user whose subject id is the event's actor.id or its subject.id (subject
 *             type user); a guessed topic yields nothing for anyone else
 *   internal  service principals (token with events.event.read) only, never a browser
 * Developer-app events (app.<project_key>.*, events.app.publish) are never streamed here, to anyone.
 *
 * Resume: `Last-Event-ID` (or ?last_event_id=) is a seq. Missed events are replayed first; when the
 * cursor is older than retention an `event: gap` message comes first, so the client knows to
 * refetch state. Heartbeat comment every 25 s.
 */
const { http } = require('openvibe-contracts');
const topics = require('./topics');
const { rowToEnvelope } = require('./store');

const ORIGIN_RE = /^https:\/\/(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)*openvibe\.[a-z]{2,63}$/;
const MAX_BUFFERED = 1024 * 1024; // a client this far behind is dropped; it resumes with Last-Event-ID

function createRealtime({ store, auth, config, log = console }) {
    const opts = config.realtime;
    const conns = new Set();
    let heartbeat = null;

    function originAllowed(origin) {
        return typeof origin === 'string' && (ORIGIN_RE.test(origin) || opts.extraOrigins.includes(origin));
    }

    /** CORS for browser EventSource({ withCredentials: true }) from https://*.openvibe.* only. */
    function cors(req, res, next) {
        const origin = req.headers.origin;
        res.setHeader('Vary', 'Origin');
        if (origin && originAllowed(origin)) {
            res.setHeader('Access-Control-Allow-Origin', origin);
            res.setHeader('Access-Control-Allow-Credentials', 'true');
            if (req.method === 'OPTIONS') {
                res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
                res.setHeader('Access-Control-Allow-Headers', 'Authorization, Last-Event-ID, Cache-Control');
                res.setHeader('Access-Control-Max-Age', '600');
                return res.status(204).end();
            }
        } else if (req.method === 'OPTIONS') {
            return res.status(204).end();
        }
        return next();
    }

    function visibleTo(viewer, row) {
        // Developer-app events (and anything sandbox) never reach realtime: apps pull or subscribe.
        if (row.project_id || (row.env && row.env !== 'production')) return false;
        if (row.visibility === 'public') return true;
        if (viewer.kind === 'service') return true;
        if (row.visibility !== 'subject' || viewer.kind !== 'user' || !viewer.subjectId) return false;
        let actor = null;
        try { actor = JSON.parse(row.actor); } catch { actor = null; }
        if (actor && actor.type === 'user' && actor.id === viewer.subjectId) return true;
        return row.subject_type === 'user' && row.subject_id === viewer.subjectId;
    }

    function write(conn, chunk) {
        if (conn.closed) return;
        conn.res.write(chunk);
        if (conn.res.writableLength > MAX_BUFFERED) {
            log.warn('[realtime] dropping a slow client');
            close(conn);
        }
    }

    function sendEvent(conn, row) {
        conn.lastSeq = row.seq;
        write(conn, `id: ${row.seq}\ndata: ${JSON.stringify({ seq: row.seq, event: rowToEnvelope(row) })}\n\n`);
    }

    function sendGap(conn, gap) {
        write(conn, `event: gap\ndata: ${JSON.stringify(gap)}\n\n`);
    }

    function close(conn) {
        if (conn.closed) return;
        conn.closed = true;
        conns.delete(conn);
        try { conn.res.end(); } catch { /* already gone */ }
    }

    function parseTopics(raw) {
        const list = String(raw || '').split(',').map(s => s.trim()).filter(Boolean);
        return [...new Set(list)];
    }

    function handler(req, res) {
        const ctx = req.ov;
        const wanted = parseTopics(req.query.topics);
        if (!wanted.length) return http.sendProblem(res, 400, 'realtime.bad_request', { detail: 'topics=<pattern>[,<pattern>] is required', ctx });
        if (wanted.length > opts.maxTopics) {
            return http.sendProblem(res, 400, 'realtime.too_many_topics', { detail: `at most ${opts.maxTopics} topics per connection`, ctx });
        }
        const bad = wanted.find(t => !topics.isValidPattern(t));
        if (bad) return http.sendProblem(res, 400, 'realtime.bad_topic', { detail: `invalid topic pattern ${bad}`, ctx });

        const viewer = auth.realtimeViewer(req);
        if (viewer.error) return http.sendProblem(res, viewer.error.status, viewer.error.code, { detail: viewer.error.detail, ctx });
        if (viewer.kind === 'anonymous' && !opts.allowAnonymous) {
            return http.sendProblem(res, 401, 'token.missing', { detail: 'sign in to open a realtime stream', ctx });
        }
        if (conns.size >= opts.maxConnections) {
            res.setHeader('Retry-After', '10');
            return http.sendProblem(res, 503, 'realtime.over_capacity', { detail: 'too many realtime connections, retry shortly', ctx });
        }

        const rawLast = req.headers['last-event-id'] ?? req.query.last_event_id;
        const lastId = rawLast != null && /^\d{1,15}$/.test(String(rawLast).trim()) ? Number(String(rawLast).trim()) : null;

        res.status(200);
        res.setHeader('Content-Type', 'text/event-stream; charset=utf-8');
        res.setHeader('Cache-Control', 'no-cache, no-transform');
        res.setHeader('Connection', 'keep-alive');
        res.setHeader('X-Accel-Buffering', 'no');
        res.flushHeaders();
        req.socket.setTimeout(0);
        req.socket.setNoDelay(true);

        const conn = { res, viewer, patterns: wanted, lastSeq: lastId ?? store.lastSeq(), closed: false };
        write(conn, `retry: 3000\n: connected ${viewer.kind}\n\n`);

        // Catch up synchronously (better-sqlite3 is synchronous, so no event can be published in
        // between), then join the live fan-out.
        if (lastId != null) {
            const latest = store.lastSeq();
            const oldest = store.oldestSeq();
            if (lastId > latest) {
                sendGap(conn, { reason: 'cursor_ahead', from_seq: latest + 1, to_seq: lastId, latest_seq: latest });
                conn.lastSeq = latest;
            } else {
                let cursor = lastId;
                if (lastId < oldest - 1) {
                    sendGap(conn, { reason: 'retention', from_seq: lastId + 1, to_seq: oldest - 1, latest_seq: latest });
                    cursor = oldest - 1;
                }
                const { rows, cursor: scanned } = store.scan(cursor, {
                    patterns: wanted, limit: opts.replayMax, scanMax: opts.replayMax * 50, accept: row => visibleTo(viewer, row),
                });
                for (const row of rows) sendEvent(conn, row);
                if (scanned < latest) {
                    // Replay stopped at its limit: everything after `scanned` is reported as a gap.
                    sendGap(conn, { reason: 'replay_limit', from_seq: scanned + 1, to_seq: latest, latest_seq: latest });
                }
                conn.lastSeq = latest;
            }
        }

        conns.add(conn);
        res.on('close', () => close(conn));
        return undefined;
    }

    /** Called after a publish commits, with the newly stored rows in seq order. */
    function publish(rows) {
        if (!conns.size) return;
        for (const row of rows) {
            for (const conn of conns) {
                if (conn.closed || row.seq <= conn.lastSeq) continue;
                if (!conn.patterns.some(p => topics.matches(p, row.event_type))) continue;
                if (!visibleTo(conn.viewer, row)) continue;
                sendEvent(conn, row);
            }
        }
    }

    function start() {
        heartbeat = setInterval(() => {
            for (const conn of conns) write(conn, ': hb\n\n');
        }, opts.heartbeatMs);
        heartbeat.unref?.();
    }

    function stop() {
        clearInterval(heartbeat);
        for (const conn of [...conns]) close(conn);
    }

    return { cors, handler, publish, start, stop, count: () => conns.size, visibleTo, originAllowed };
}

module.exports = { createRealtime };
