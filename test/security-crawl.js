'use strict';
/**
 * The crawler behind the security suites (roadmap WS-R task 5): lists every route of the booted
 * Events app (walking Express's router stack, so a route added later is crawled without anyone
 * listing it), fills route parameters with seeded values, and requests each path as several
 * callers, reporting any response whose body or headers carry a value that caller must never see.
 * The realtime stream is read for a moment and then closed. Not a test itself (no .test.js).
 */

/** The path an Express 4 layer is mounted at ('' for app-level middleware), or null when it is a pattern. */
function mountPath(layer) {
    if (!layer.regexp || layer.regexp.fast_slash) return '';
    let src = layer.regexp.source.replace(/^\^/, '').replace(/\\\/\?\(\?=\\\/\|\$\)$/i, '').replace(/\/\?\(\?=\/\|\$\)$/i, '');
    let i = 0;
    src = src.replace(/\(\?:\(\[\^\\\/\]\+\?\)\)/g, () => `:${(layer.keys[i++] || {}).name || 'param'}`);
    src = src.replace(/\\\//g, '/').replace(/\\\./g, '.').replace(/\\-/g, '-');
    return /[\\^$()|[\]*+?]/.test(src) ? null : src;
}

/** Every route of the Express app behind `server` (an http.Server): [{ path, methods }]. */
function listRoutes(server) {
    const app = server.listeners('request').find((fn) => fn && fn._router);
    if (!app) throw new Error('no Express app on this server');
    const out = [];
    const walk = (stack, prefix) => {
        for (const layer of stack) {
            if (layer.route) {
                const methods = Object.keys(layer.route.methods).filter((m) => layer.route.methods[m]);
                for (const p of [].concat(layer.route.path)) if (typeof p === 'string') out.push({ path: prefix + p, methods });
            } else if (layer.handle && Array.isArray(layer.handle.stack)) {
                const mp = mountPath(layer);
                if (mp !== null) walk(layer.handle.stack, prefix + mp);
            } else {
                const mp = mountPath(layer);
                if (mp) out.push({ path: prefix + mp, methods: ['_all'] });
            }
        }
    };
    walk(app._router.stack, '');
    const seen = new Set();
    return out.filter((r) => { const k = `${r.methods.join(',')} ${r.path}`; if (seen.has(k)) return false; seen.add(k); return true; });
}

/** Concrete paths for a template: candidate i of every parameter, for each i (no cross product). */
function expand(template, values) {
    const names = [];
    const t = `/${template.replace(/^\/+/, '')}`.replace(/\*/g, 'x').replace(/:([A-Za-z0-9_]+)\??(\([^)]*\))?/g, (m, n) => { names.push(n); return `:${n}`; });
    if (!names.length) return [t];
    const lists = names.map((n) => values(n));
    const width = Math.max(...lists.map((l) => l.length));
    const paths = new Set();
    for (let i = 0; i < width; i++) {
        let j = 0;
        paths.add(t.replace(/:([A-Za-z0-9_]+)/g, () => { const l = lists[j++]; return encodeURIComponent(String(l[Math.min(i, l.length - 1)])); }));
    }
    return [...paths];
}

/** Every path for `method`: each route expanded, also with each of `queries` appended, plus `extra`. */
function pathsFor(routes, values, { method = 'get', queries = [], extra = [] } = {}) {
    const paths = new Set();
    for (const r of routes) {
        if (!r.methods.includes(method) && !r.methods.includes('_all')) continue;
        for (const p of expand(r.path, values)) {
            paths.add(p);
            for (const q of queries) if (!p.includes('?')) paths.add(`${p}?${q}`);
        }
    }
    for (const p of extra) paths.add(p);
    return [...paths];
}

/** The forms of a secret worth looking for: as is, URL-encoded, base64 and base64url. */
function forms(value) {
    const v = String(value);
    const out = new Set([v, encodeURIComponent(v)]);
    if (v.length >= 12) {
        out.add(Buffer.from(v).toString('base64').replace(/=+$/, ''));
        out.add(Buffer.from(v).toString('base64url'));
    }
    return [...out];
}

/** Which of `needles` ({ label: value }) a response ({ text, headers }) carries: [{ label, where }]. */
function leaks(res, needles) {
    const found = [];
    const h = res.headers;
    const headerText = [...(h && typeof h.entries === 'function' ? h.entries() : Object.entries(h || {}))].map(([k, v]) => `${k}: ${v}`).join('\n');
    for (const [label, value] of Object.entries(needles)) {
        if (!value) continue;
        for (const f of forms(value)) {
            if (res.text && res.text.includes(f)) { found.push({ label, where: 'body' }); break; }
            if (headerText.includes(f)) { found.push({ label, where: 'headers' }); break; }
        }
    }
    return found;
}

/** One request; a streaming answer (SSE) is read for `streamMs` and then cut. */
async function hit(base, method, p, { headers = {}, body, streamMs = 250 } = {}) {
    const ac = new AbortController();
    const h = { ...headers };
    let b;
    if (body !== undefined) { if (typeof body === 'string') b = body; else b = JSON.stringify(body); h['content-type'] = h['content-type'] || 'application/json'; }
    const res = await fetch(base + p, { method, headers: h, body: b, redirect: 'manual', signal: ac.signal });
    let text = '';
    if (String(res.headers.get('content-type') || '').includes('text/event-stream')) {
        const reader = res.body.getReader();
        const dec = new TextDecoder();
        const stop = setTimeout(() => ac.abort(), streamMs);
        try { for (;;) { const { done, value } = await reader.read(); if (done) break; text += dec.decode(value, { stream: true }); } } catch { /* cut */ }
        clearTimeout(stop);
    } else {
        text = await res.text();
    }
    return { status: res.status, headers: res.headers, text };
}

/**
 * Requests every path as every caller ({ who: headers }). `needlesFor(who)` names what that caller
 * must never see. Resolves { found, answered, statuses, byPath }.
 */
async function crawl(base, paths, people, needlesFor, { method = 'GET', body, concurrency = 8 } = {}) {
    const found = [];
    const statuses = {};
    const byPath = {};
    let answered = 0;
    for (const [who, headers] of Object.entries(people)) {
        const needles = needlesFor(who);
        const queue = [...paths];
        await Promise.all(Array.from({ length: concurrency }, async () => {
            while (queue.length) {
                const p = queue.shift();
                let r;
                try { r = await hit(base, method, p, { headers, body }); } catch { r = { status: 0, text: '', headers: {} }; }
                if (r.status) answered++;
                const cls = r.status ? `${String(r.status)[0]}xx` : 'none';
                statuses[cls] = (statuses[cls] || 0) + 1;
                byPath[`${who} ${method} ${p}`] = r.status;
                for (const l of leaks(r, needles)) found.push(`${who}: ${method} ${p} → ${r.status} carries ${l.label} in its ${l.where}`);
            }
        }));
    }
    return { found, answered, statuses, byPath };
}

module.exports = { mountPath, listRoutes, expand, pathsFor, forms, leaks, hit, crawl };
