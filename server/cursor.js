'use strict';
/**
 * Opaque cursors (ADR-042 decision 7): a cursor names a position in the hot store — the global seq — together with
 * the retention epoch that position belongs to. The wire form is `c1.<epoch>.<base64url(seq)>`; it is opaque to
 * clients, which hand it back as `after=` or `Last-Event-ID` and never parse it.
 *
 * decode() never throws on user input: anything malformed, non-canonical or of another version is null, and the
 * caller answers with its own refusal or gap shape. encode() is strict — it is only ever called with a seq the
 * store just handed out.
 */
const VERSION = 'c1';
const MAX_LENGTH = 200;   // the contract caps a cursor at 200 characters

/** The cursor for `seq` in `epoch`. The epoch is a small integer, so the string stays short. */
function encode(seq, epoch = 0) {
    if (!Number.isSafeInteger(seq) || seq < 0) throw new TypeError('seq must be a safe integer >= 0');
    if (!Number.isInteger(epoch) || epoch < 0) throw new TypeError('epoch must be an integer >= 0');
    return `${VERSION}.${epoch}.${Buffer.from(String(seq), 'utf8').toString('base64url')}`;
}

/** { seq, epoch } for a cursor, or null for anything that is not one. Never throws. */
function decode(input) {
    if (typeof input !== 'string' || input.length === 0 || input.length > MAX_LENGTH) return null;
    const parts = input.split('.');
    if (parts.length !== 3 || parts[0] !== VERSION) return null;
    if (!/^\d{1,15}$/.test(parts[1]) || !/^[A-Za-z0-9_-]+$/.test(parts[2])) return null;
    const epoch = Number(parts[1]);
    let seq;
    try { seq = Number(Buffer.from(parts[2], 'base64url').toString('utf8')); } catch { return null; }
    if (!Number.isSafeInteger(seq) || seq < 0) return null;
    // Re-encoding rejects non-canonical base64 and leading zeros, so one position has exactly one cursor string.
    if (encode(seq, epoch) !== input) return null;
    return { seq, epoch };
}

module.exports = { encode, decode, VERSION };
