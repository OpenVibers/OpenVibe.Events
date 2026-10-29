'use strict';
// The product home (plan T7: openvibe.events is the API and product origin): it renders server/home.js for
// every caller by default — browser, crawler, or a plain curl with no Accept — with the public event list,
// one h1, a canonical and JSON-LD; the text/plain API index is kept for an explicit text/plain (or ?format=text).
// It also serves the pinned OpenVibe Frame at /shared, answers robots.txt, sitemap.xml, llms.txt and
// llms-full.txt, and names its own origin everywhere. A parse-only check of the nginx vhost too.
const assert = require('assert');

function assertSitemapLastmods(xml, tag = 'url') {
    const entries = [...xml.matchAll(new RegExp(`<${tag}>([\\s\\S]*?)</${tag}>`, 'g'))];
    for (const [, body] of entries) {
        const match = /<lastmod>(\d{4}-\d{2}-\d{2})<\/lastmod>/.exec(body);
        assert.ok(match, body);
        assert.strictEqual(new Date(match[1]).toISOString().slice(0, 10), match[1]);
    }
    return entries;
}

/** The readable text of a page: scripts, styles and tags removed (what a crawler or an AI reader gets). */
const readable = (html) => html.replace(/<script[\s\S]*?<\/script>/gi, ' ').replace(/<style[\s\S]*?<\/style>/gi, ' ')
    .replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();

const fs = require('fs');
const path = require('path');
const nodeHttp = require('http');
const { boot, suite } = require('./helpers');
const { load } = require('../server/config');
const { HOME_CSP, renderHome } = require('../server/home');

const SITE = 'https://openvibe.events';

// GET / for a caller that explicitly asks for text: the API index, byte for byte, as it was before the home.
const INDEX = [
    'OpenVibe.Events: durable events, subscriptions, signed delivery, dead letters and replay.',
    '',
    'POST /api/v1/events              publish (service token, events.event.publish; app token, events.app.publish)',
    'GET  /api/v1/events              pull with a cursor (events.event.read; app token, events.app.read)',
    '     /api/v1/subscriptions       webhook subscriptions (events.subscription.manage; app token, events.app.subscribe)',
    '     /api/v1/deliveries          DLQ inspect and replay (events.delivery.admin)',
    'GET  /realtime/stream?topics=... server-sent events for browsers',
    'GET  /api/health, /api/ready, /release.json, /limits.json',
    '',
    'Source: https://github.com/OpenVibers/OpenVibe.Events',
    '',
].join('\n');

const t = suite('home');
let h;

/** A raw GET with exactly the headers given: node:http sends no Accept unless the caller sets one. */
function raw(base, p, headers = {}) {
    return new Promise((resolve, reject) => {
        const url = new URL(base + p);
        nodeHttp.get({ hostname: url.hostname, port: url.port, path: url.pathname + url.search, headers }, (res) => {
            let body = '';
            res.setEncoding('utf8');
            res.on('data', (c) => { body += c; });
            res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
        }).on('error', reject);
    });
}

t('in production the API origin and the product origin are the same host', () => {
    const production = load({ NODE_ENV: 'production' });
    assert.strictEqual(production.baseUrl, SITE, 'the API origin defaults to openvibe.events');
    assert.strictEqual(production.siteUrl, SITE, 'the product origin is the same origin');
});

t('boot with openvibe.events as both the API and the product origin', async () => {
    h = await boot({ env: { BASE_URL: SITE } });   // siteUrl follows baseUrl
});

t('GET / as a browser: the home page, its CSP and its cache policy', async () => {
    const r = await raw(h.base, '/', { Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' });
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/html/);
    assert.strictEqual(r.headers['content-security-policy'], HOME_CSP);
    assert.strictEqual(r.headers['cache-control'], 'private, max-age=300');
    assert.strictEqual(r.headers.vary, 'Accept');
    assert.strictEqual((r.body.match(/<h1\b/g) || []).length, 1, 'exactly one h1');
    // The page's own rules: the body meets the edges (no browser margin), each curl sample takes a full row.
    // body margins are gone, the page clips the hero aura's spill, and the curl samples get one full-width column.\n    assert.match(r.body, /<style>body\{margin:0\}[\s\S]{0,200}\.page \.sc-code\{grid-template-columns:minmax\(0,1fr\)\}/);\n    assert.match(r.body, /\.page\{overflow-x:clip\}/);
    assert.ok(r.body.includes('Events your services'), 'the hero text');
    assert.ok(r.body.includes(`<link rel="canonical" href="${SITE}/">`), 'the canonical product origin');
    assert.ok(r.body.includes(`The API is at ${SITE}`), 'the hero note names the API origin');
    assert.ok(r.body.includes(`${SITE}/api/v1/events`), 'the API examples use the API origin');
    assert.ok(!r.body.includes('events.openvibe.network'), 'the home names no other origin');
    assert.strictEqual((r.body.match(/<link rel="canonical"/g) || []).length, 1, 'one canonical');
    assert.match(r.body, /<html lang="en">/);
});

t('GET / for a crawler or a plain curl: the server-rendered HTML, with real readable text', async () => {
    // `*/*` and no Accept at all are what curl, fetch() and many crawlers send. Both must get the page, not the
    // text API index: otherwise the HTML that reaches a crawler has no title, h1, canonical or event list.
    for (const headers of [{}, { Accept: '*/*' }, { Accept: 'application/json' }]) {
        const r = await raw(h.base, '/', headers);
        assert.strictEqual(r.status, 200, JSON.stringify(headers));
        assert.match(r.headers['content-type'], /^text\/html/, JSON.stringify(headers));
        assert.ok(readable(r.body).length > 2000, `${readable(r.body).length} readable chars for ${JSON.stringify(headers)}`);
        assert.strictEqual((r.body.match(/<h1\b/g) || []).length, 1, JSON.stringify(headers));
        assert.ok(r.body.includes(`<link rel="canonical" href="${SITE}/">`), JSON.stringify(headers));
        // One JSON-LD script per page: every node lives in the page's @graph.
        const scripts = [...r.body.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map((m) => JSON.parse(m[1]));
        assert.strictEqual(scripts.length, 1, 'the page carries exactly one JSON-LD script');
        const ld = scripts.flatMap((x) => (Array.isArray(x['@graph']) ? x['@graph'] : [x]));
        assert.ok(ld.some((x) => x['@type'] === 'WebSite'), 'the WebSite node');
    }
});

t('the home CSP lets Cloudflare Web Analytics load and report (it injects its beacon on this zone)', () => {
    assert.match(HOME_CSP, /script-src [^;]*https:\/\/static\.cloudflareinsights\.com/);
    assert.match(HOME_CSP, /connect-src [^;]*https:\/\/cloudflareinsights\.com/);
});

t('a browser gets a page for a path nothing serves (lang, title, icon); an API client the problem document', async () => {
    const page = await raw(h.base, '/no-such-page', { Accept: 'text/html,*/*;q=0.8' });
    assert.strictEqual(page.status, 404);
    assert.match(page.headers['content-type'], /^text\/html/);
    assert.ok(page.body.includes('<html lang="en">') && page.body.includes('<title>Not found') && page.body.includes('rel="icon"'), page.body.slice(0, 300));
    const api = await raw(h.base, '/no-such-page', { Accept: 'application/json' });
    assert.strictEqual(api.status, 404);
    assert.match(api.headers['content-type'], /problem\+json|json/);
    const apiPath = await raw(h.base, '/api/v1/nothing', { Accept: 'text/html' });
    assert.match(apiPath.headers['content-type'], /json/, 'an /api/ path answers JSON whatever it accepts');
});

t('/favicon.ico is the app icon (SVG), cached a day', async () => {
    const r = await raw(h.base, '/favicon.ico', {});
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^image\/svg\+xml/);
    assert.ok(r.body.startsWith('<svg'));
    assert.strictEqual(r.headers['cache-control'], 'public, max-age=86400');
});

t('the home links the pinned Frame stylesheet, and /shared serves it', async () => {
    const r = await raw(h.base, '/', { Accept: 'text/html' });
    const href = /<link rel="stylesheet" href="([^"]+)"/.exec(r.body);
    assert.ok(href && href[1].startsWith('/shared/showcase.css?v='), `showcase.css link, got ${href && href[1]}`);
    const sheet = await raw(h.base, href[1]);
    assert.strictEqual(sheet.status, 200);
    assert.match(sheet.headers['content-type'], /^text\/css/);
    assert.ok(sheet.body.includes('.ov-'), 'the showcase stylesheet');
});

t('GET / for a caller that asks for text: the API index, byte for byte', async () => {
    // Only an explicit text/plain (header or `?format=text`) gets the API index now: the HTML page is the default.
    for (const [p, headers] of [['/', { Accept: 'text/plain' }], ['/?format=text', {}], ['/?format=text', { Accept: '*/*' }]]) {
        const r = await raw(h.base, p, headers);
        assert.strictEqual(r.status, 200, `${p} ${JSON.stringify(headers)}`);
        assert.match(r.headers['content-type'], /^text\/plain/, `${p} ${JSON.stringify(headers)}`);
        assert.strictEqual(r.headers.vary, 'Accept', `${p} ${JSON.stringify(headers)}`);
        assert.strictEqual(r.headers['cache-control'], 'private, no-store', `${p} ${JSON.stringify(headers)}`);
        assert.strictEqual(r.body, INDEX, `${p} ${JSON.stringify(headers)}`);
    }
});

t('the server-rendered event list: title, real <time datetime>, place, description, category links and JSON-LD', () => {
    const last = Date.parse('2026-10-29T09:41:00Z');
    const stats = {
        day: 4, week: 4, types: 1, services: [{ ns: 'live', n: 3 }, { ns: 'blog', n: 1 }], subscriptions: 0, consumers: 0,
        delivered: 0, dead: 0, measuredAt: last,
        public: [
            { type: 'live.stream.started', n: 3, last },
            { type: 'blog.post.published', n: 1, last: 0 },   // no arrival recorded: no startDate may be invented
        ],
    };
    const page = renderHome({ siteUrl: SITE, apiUrl: SITE, stats });
    assert.ok(readable(page).length > 2000, `${readable(page).length} readable chars`);
    assert.strictEqual((page.match(/<h1\b/g) || []).length, 1);
    assert.strictEqual((page.match(/<link rel="canonical"/g) || []).length, 1);
    const card = /<li class="hm-event"[\s\S]*?<\/li>/.exec(page)[0];
    assert.match(card, /A channel went live on OpenVibe\.Live/, 'the event title');
    assert.match(card, /<time datetime="2026-10-29T09:41:00\.000Z">/, 'a real <time datetime>');
    assert.match(card, /Online, from <a href="https:\/\/openvibe\.live">OpenVibe\.Live<\/a>/, 'the place and publisher');
    assert.match(card, /Published by OpenVibe\.Live as live\.stream\.started/, 'the short description');
    assert.match(card, /3 this week/, 'the count');
    assert.match(card, /Category: <a href="https:\/\/openvibe\.live">live<\/a>/, 'a category link');
    assert.match(page, /<nav class="hm-cats"[^>]*><a href="https:\/\/openvibe\.live">OpenVibe\.Live<\/a>/, 'the category nav');
    assert.match(page, /<code>live\.stream\.started<\/code>/, 'the exact event type');

    // One JSON-LD script per page: every node lives in the page's @graph.
    const scripts = [...page.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map((m) => JSON.parse(m[1]));
    assert.strictEqual(scripts.length, 1, 'the page carries exactly one JSON-LD script');
    const ld = scripts.flatMap((x) => (Array.isArray(x['@graph']) ? x['@graph'] : [x]));
    assert.ok(ld.some((x) => x['@type'] === 'WebSite'), 'the WebSite node');
    const list = ld.find((x) => x['@type'] === 'ItemList');
    assert.ok(list, 'an ItemList of the events');
    assert.strictEqual(list.numberOfItems, 2);
    assert.strictEqual(list.itemListElement[0].item['@type'], 'Event');
    assert.strictEqual(list.itemListElement[0].item.startDate, '2026-10-29T09:41:00.000Z');
    assert.ok(!('startDate' in list.itemListElement[1].item), 'no date invented when none was recorded');

    // A stats failure renders the shell, the h1, the description and a clear empty state — never a blank page.
    const shell = renderHome({ siteUrl: SITE, apiUrl: SITE, stats: null });
    assert.match(shell, /The live bus figures are unavailable right now/);
    assert.strictEqual((shell.match(/<h1\b/g) || []).length, 1);
    assert.ok(shell.includes(`<link rel="canonical" href="${SITE}/">`));
});

t('robots.txt welcomes the home and keeps the API out, and names the sitemap', async () => {
    const r = await raw(h.base, '/robots.txt');
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/plain/);
    assert.ok(r.body.includes('Allow: /'));
    assert.ok(r.body.includes('Disallow: /api/'));
    assert.ok(r.body.includes('Disallow: /realtime/'));
    assert.ok(r.body.includes(`Sitemap: ${SITE}/sitemap.xml`));
});

t('sitemap.xml lists the product home and its update log', async () => {
    const r = await raw(h.base, '/sitemap.xml');
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /xml/);
    assert.deepStrictEqual([...r.body.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]), [`${SITE}/`, `${SITE}/updates`]);
    assert.strictEqual(assertSitemapLastmods(r.body).length, 2);
});

t('/updates: what shipped on this site, the shared update log, with one heading', async () => {
    const r = await raw(h.base, '/updates', { accept: 'text/html' });
    assert.strictEqual(r.status, 200);
    assert.ok(r.body.includes('<h1>What shipped on OpenVibe.Events</h1>') && r.body.includes('data-ov-shipped="log" data-service="events"'));
    assert.strictEqual((r.body.match(/<h1[\s>]/g) || []).length, 1);
    assert.match(r.body, /<script src="\/shared\/shipped\.js\?v=[0-9a-f]+" defer><\/script>/);
    assert.ok(r.body.includes(`<link rel="canonical" href="${SITE}/updates">`));
    assert.ok(String(r.headers['content-security-policy']).includes('https://openvibe.network'), 'the log reads the network changelog');
});

t('llms.txt is a plain-text map that starts with the site name', async () => {
    const r = await raw(h.base, '/llms.txt');
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/plain/);
    assert.ok(r.body.startsWith('# OpenVibe.Events'), r.body.slice(0, 40));
    assert.ok(r.body.includes("> The OpenVibe network's event bus"), 'the home summary');
    assert.ok(r.body.includes('https://github.com/OpenVibers/OpenVibe.Events#readme'), 'the guide');
    assert.ok(r.body.includes(`The API and the realtime gateway are at ${SITE}`), 'the discovery names the API origin');
});

t('llms-full.txt is the same map in full text, starting with the site name', async () => {
    const r = await raw(h.base, '/llms-full.txt');
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/plain/);
    assert.ok(r.body.startsWith('# OpenVibe.Events'), r.body.slice(0, 40));
    assert.ok(r.body.includes("> The OpenVibe network's event bus"), 'the home summary');
    assert.ok(r.body.includes('## Start here'), 'the /llms.txt sections as pages');
    assert.ok(r.body.includes('### Guide'), 'each page with its full text');
});

t('deploy/nginx/openvibe.events.conf is the API and product origin, with no redirect to the old host', () => {
    const conf = fs.readFileSync(path.join(__dirname, '..', 'deploy', 'nginx', 'openvibe.events.conf'), 'utf8');
    assert.match(conf, /server_name openvibe\.events;/);
    assert.match(conf, /server_name www\.openvibe\.events;/);
    assert.ok(conf.includes('/etc/letsencrypt/live/openvibe.events/fullchain.pem'));
    assert.ok(conf.includes('/etc/letsencrypt/live/openvibe.events/privkey.pem'));
    assert.ok(!conf.includes('$proxy_add_x_forwarded_for'), 'the client address is never appended to');
    // The product surface (plan expansion II) and everything the API host proxies (plan T7).
    for (const loc of ['location = / {', 'location ^~ /shared/ {', 'location = /robots.txt {', 'location = /sitemap.xml {', 'location = /llms.txt {', 'location = /llms-full.txt {',
        'location = /realtime/stream {', 'location = /api/health', 'location = /api/ready', 'location = /release.json', 'location = /limits.json', 'location = /release-metrics']) {
        assert.ok(conf.includes(loc), `${loc} is proxied`);
    }
    assert.ok(conf.includes('location ~ ^/api/v1/(events|subscriptions|checkpoints)(/|$)'), 'the token-guarded API block');
    assert.ok(conf.includes('location /api/ {'), 'the /api/ refusal block');
    assert.ok(conf.includes('proxy_buffering off;'), 'SSE is not buffered');
    assert.ok(conf.includes('proxy_read_timeout 1h;'), 'SSE keeps its long read timeout');
    const refusals = [conf.indexOf('location ~* ^/metrics(/|$) { return 404; }'), conf.indexOf('location ~* ^/internal(/|$) { return 404; }')];
    assert.ok(refusals.every((i) => i >= 0), 'metrics and /internal/ are refused');
    assert.ok(!conf.includes('events.openvibe.network$request_uri'), 'no catch-all redirect to the old API host');
    // The catch-all hands GET/HEAD to the app (its /favicon.ico and its own 404 page with lang), never other methods.
    const catchAll = conf.slice(conf.lastIndexOf('    location / {'));
    assert.match(catchAll, /limit_except GET HEAD \{ deny all; \}/, 'the catch-all only passes GET and HEAD');
    assert.match(catchAll, /proxy_pass http:\/\/127\.0\.0\.1:4300;/, 'and lets the app answer (favicon, 404 page)');
    assert.ok(!/location \/ \{ return 404; \}/.test(conf), 'nginx\'s bare 404 page (no lang) is gone');
    // Open tabs' release reports are POSTs: their own location, POST only, small bodies.
    assert.match(conf, /location = \/release-metrics \{ limit_except POST \{ deny all; \} client_max_body_size 4k; proxy_pass http:\/\/127\.0\.0\.1:4300;/);
    assert.ok(!fs.existsSync(path.join(__dirname, '..', 'deploy', 'nginx', 'events.openvibe.network.conf')), 'the retired events.openvibe.network vhost is gone (plan T7)');
    assert.ok(conf.includes('return 301 https://openvibe.events$request_uri;'), ':80 and www reach the apex');
});

t('stop', async () => { await h.stop(); });

t.run();
