'use strict';
// Every page of openvibe.events gives the shared navbar the site's own page list (the owner, 2026-11-12): the home,
// /updates, /terms, /privacy and /dmca all send Events / GitHub, and the GitHub row is named for where it goes.
const assert = require('assert');
const { boot, suite } = require('./helpers');
const { NAV_LINKS } = require('../server/home');

function initLinks(html) {
    const at = html.indexOf('OpenVibeNavbar.init');
    if (at < 0) return null;
    const start = html.indexOf('links', at);
    const open = html.indexOf('[', start);
    let depth = 0, q = null, esc = false;
    for (let i = open; i < html.length; i += 1) {
        const c = html[i];
        if (q) { if (esc) esc = false; else if (c === '\\') esc = true; else if (c === q) q = null; continue; }
        if (c === '"' || c === "'") { q = c; continue; }
        if (c === '[') depth += 1; else if (c === ']') { depth -= 1; if (!depth) return JSON.parse(html.slice(open, i + 1)); }
    }
    return null;
}

const t = suite('page-links');

t('every page sends Events\' own list, and every row is labelled for where it goes', async () => {
    const h = await boot();
    try {
        const own = NAV_LINKS.map(({ label, href }) => ({ label, href }));
        assert.strictEqual(own[0].label, 'Events', 'the home row carries the site name');
        for (const path of ['/', '/updates', '/terms', '/privacy', '/dmca']) {
            const r = await fetch(h.base + path);
            assert.strictEqual(r.status, 200, `${path} answered ${r.status}`);
            const links = (initLinks(await r.text()) || []).map(({ label, href }) => ({ label, href }));
            assert.deepStrictEqual(links, own, `${path} sends Events' list`);
        }
        assert.ok(/GitHub/.test(own[1].label) && own[1].href.startsWith('https://github.com/'), 'the off-site guide is named GitHub');
    } finally { await h.stop(); }
});

t.run();
