'use strict';
/**
 * Topic patterns. A topic is an event type (`media.vod.ready`); a pattern is a dot-separated list of
 * literal segments and `*`, where `*` stands for ONE OR MORE whole segments:
 *
 *   media.vod.ready   exactly that type
 *   media.vod.*       media.vod.ready, media.vod.clip.cut, ...
 *   *.created         every type that ends in .created
 *   media.*.ready     media.vod.ready, media.a.b.ready
 *   *                 everything
 */

const PATTERN_RE = /^(\*|[a-z0-9_]+)(\.(\*|[a-z0-9_]+))*$/;
const MAX_LEN = 200;
const cache = new Map();

function isValidPattern(p) {
    return typeof p === 'string' && p.length > 0 && p.length <= MAX_LEN && PATTERN_RE.test(p) && !p.includes('*.*');
}

function compile(pattern) {
    let re = cache.get(pattern);
    if (re) return re;
    if (!isValidPattern(pattern)) throw new TypeError(`invalid topic pattern "${pattern}"`);
    const body = pattern.split('.').map(seg => (seg === '*' ? '[a-z0-9_]+(?:\\.[a-z0-9_]+)*' : seg)).join('\\.');
    re = new RegExp(`^${body}$`);
    if (cache.size > 5000) cache.clear();
    cache.set(pattern, re);
    return re;
}

function matches(pattern, eventType) {
    return compile(pattern).test(eventType);
}

/**
 * A SQLite GLOB that selects a superset of what the pattern matches (GLOB `*` also spans dots),
 * so the SQL narrows the scan and matches() makes the exact decision. Valid patterns only contain
 * [a-z0-9_.*], none of which GLOB treats specially except `*`, so the pattern is its own GLOB.
 */
function toGlob(pattern) {
    if (!isValidPattern(pattern)) throw new TypeError(`invalid topic pattern "${pattern}"`);
    return pattern;
}

module.exports = { isValidPattern, compile, matches, toGlob };
