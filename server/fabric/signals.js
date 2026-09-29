'use strict';
/**
 * Per-carrier rolling signals and a circuit breaker (ADR-042 decision 5). This is a local copy of the pattern in
 * OpenVibe.Media's server/placement/signals.js — same idea (an EWMA of signal latency, a breaker on consecutive
 * errors, half-open after a pause, closed on the next success) — kept here so Events never requires another
 * repository at runtime. In-memory only: a restart re-learns from the next calls. `now` is injectable for tests.
 *
 *   record(key, ok, ms)   one observation
 *   healthy(key)          false while the breaker is open (the planner drops the carrier with a reason)
 *   ewma(key)             the current EWMA latency in ms (0 before any sample)
 */
const EWMA_ALPHA = 0.3;
const OPEN_AFTER = 5;          // consecutive failures that open the breaker
const HALF_OPEN_AFTER_MS = 60000;

function createSignals({ alpha = EWMA_ALPHA, openAfter = OPEN_AFTER, halfOpenMs = HALF_OPEN_AFTER_MS, now = () => Date.now() } = {}) {
    const map = new Map();   // key -> { ewmaMs, streak, openAt, halfOpenAt, trial }

    function get(key) {
        let s = map.get(key);
        if (!s) { s = { ewmaMs: 0, streak: 0, openAt: null, halfOpenAt: null, trial: false }; map.set(key, s); }
        return s;
    }

    function record(key, ok, ms = 0) {
        const s = get(key);
        if (ok) {
            if (ms > 0) s.ewmaMs = s.ewmaMs ? alpha * ms + (1 - alpha) * s.ewmaMs : ms;
            s.streak = 0;
            s.openAt = null; s.halfOpenAt = null; s.trial = false;
            return;
        }
        s.streak += 1;
        if (s.trial) { s.openAt = now(); s.halfOpenAt = null; s.trial = false; return; }   // a failed trial re-opens at once
        if (!s.openAt && s.streak >= openAfter) { s.openAt = now(); s.halfOpenAt = null; }
    }

    /** closed (healthy) | open (skipped) | half-open (one trial allowed). */
    function state(key) {
        const s = map.get(key);
        if (!s || !s.openAt) return 'closed';
        if (s.trial) return 'half-open';
        if (s.halfOpenAt && now() >= s.halfOpenAt) return 'half-open';
        if (now() - s.openAt < halfOpenMs) return 'open';
        s.halfOpenAt = now();
        return 'half-open';
    }

    function healthy(key) { return state(key) !== 'open'; }
    function ewma(key) { const s = map.get(key); return s ? Math.round(s.ewmaMs * 1000) / 1000 : 0; }
    function snapshot() { const out = {}; for (const k of map.keys()) out[k] = { state: state(k), ewma_ms: ewma(k) }; return out; }
    function reset() { map.clear(); }

    return { record, state, healthy, ewma, snapshot, reset };
}

module.exports = { createSignals, EWMA_ALPHA, OPEN_AFTER, HALF_OPEN_AFTER_MS };
