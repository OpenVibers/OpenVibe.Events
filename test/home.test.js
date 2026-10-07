'use strict';
// The product home (plan expansion II): openvibe.events renders server/home.js for a browser and keeps
// the text/plain API index for curl and API clients, serves the pinned OpenVibe Frame at /shared, and
// answers robots.txt, sitemap.xml and llms.txt. A parse-only check of the new nginx vhost too.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const nodeHttp = require('http');
const { boot, suite } = require('./helpers');
const { HOME_CSP } = require('../server/home');

const SITE = 'https://openvibe.events';

// GET / without an Accept a browser would send: the API index, byte for byte, as it was before the home.
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

t('boot with the product domain configured', async () => { h = await boot({ env: { EVENTS_SITE_URL: SITE } }); });

t('GET / as a browser: the home page, its CSP and its cache policy', async () => {
    const r = await raw(h.base, '/', { Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' });
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/html/);
    assert.strictEqual(r.headers['content-security-policy'], HOME_CSP);
    assert.strictEqual(r.headers['cache-control'], 'private, max-age=300');
    assert.strictEqual(r.headers.vary, 'Accept');
    assert.strictEqual((r.body.match(/<h1\b/g) || []).length, 1, 'exactly one h1');
    assert.ok(r.body.includes('Events your services'), 'the hero text');
    assert.ok(r.body.includes(`<link rel="canonical" href="${SITE}/">`), 'the canonical product origin');
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

t('GET / for curl and API clients: the text/plain index, byte for byte', async () => {
    // `*/*`, no Accept at all, and a JSON client: all three get the API index.
    for (const headers of [{ Accept: '*/*' }, {}, { Accept: 'application/json' }]) {
        const r = await raw(h.base, '/', headers);
        assert.strictEqual(r.status, 200, JSON.stringify(headers));
        assert.match(r.headers['content-type'], /^text\/plain/, JSON.stringify(headers));
        assert.strictEqual(r.headers.vary, 'Accept', JSON.stringify(headers));
        assert.strictEqual(r.body, INDEX, JSON.stringify(headers));
    }
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

t('sitemap.xml lists the product home only', async () => {
    const r = await raw(h.base, '/sitemap.xml');
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /xml/);
    assert.deepStrictEqual([...r.body.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1]), [`${SITE}/`]);
});

t('llms.txt is a plain-text map that starts with the site name', async () => {
    const r = await raw(h.base, '/llms.txt');
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/plain/);
    assert.ok(r.body.startsWith('# OpenVibe.Events'), r.body.slice(0, 40));
    assert.ok(r.body.includes('> Durable events for the OpenVibe network'), 'the home summary');
    assert.ok(r.body.includes('https://github.com/OpenVibers/OpenVibe.Events#readme'), 'the guide');
});

t('deploy/nginx/openvibe.events.conf proxies the product surface and nothing else', () => {
    const conf = fs.readFileSync(path.join(__dirname, '..', 'deploy', 'nginx', 'openvibe.events.conf'), 'utf8');
    assert.match(conf, /server_name openvibe\.events;/);
    assert.match(conf, /server_name www\.openvibe\.events;/);
    assert.ok(conf.includes('/etc/letsencrypt/live/openvibe.events/fullchain.pem'));
    assert.ok(conf.includes('/etc/letsencrypt/live/openvibe.events/privkey.pem'));
    assert.ok(!conf.includes('$proxy_add_x_forwarded_for'), 'the client address is never appended to');
    for (const loc of ['location = / {', 'location ^~ /shared/ {', 'location = /robots.txt {', 'location = /sitemap.xml {', 'location = /llms.txt {']) {
        assert.ok(conf.includes(loc), `${loc} is proxied`);
    }
    const refusals = [conf.indexOf('location = /metrics { return 404; }'), conf.indexOf('location /internal/ { return 404; }')];
    assert.ok(refusals.every((i) => i >= 0), 'metrics and /internal/ are refused');
    const catchAll = conf.indexOf('location / { return 301 https://events.openvibe.network$request_uri; }');
    assert.ok(catchAll >= 0, 'everything else goes to the API host');
    assert.ok(Math.max(...refusals) < catchAll, 'the refusals come before the catch-all');
    assert.ok(conf.includes('return 301 https://openvibe.events$request_uri;'), ':80 and www reach the apex');
});

t('stop', async () => { await h.stop(); });

t.run();
