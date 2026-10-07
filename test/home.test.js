'use strict';
// The product home (plan T7: openvibe.events is the API and product origin): it renders server/home.js
// for a browser and keeps the text/plain API index for curl and API clients, serves the pinned OpenVibe
// Frame at /shared, answers robots.txt, sitemap.xml and llms.txt, and names its own origin everywhere.
// A parse-only check of the nginx vhost too.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const nodeHttp = require('http');
const { boot, suite } = require('./helpers');
const { load } = require('../server/config');
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
    assert.ok(r.body.includes('Events your services'), 'the hero text');
    assert.ok(r.body.includes(`<link rel="canonical" href="${SITE}/">`), 'the canonical product origin');
    assert.ok(r.body.includes(`The API is at ${SITE}`), 'the hero note names the API origin');
    assert.ok(r.body.includes(`${SITE}/api/v1/events`), 'the API examples use the API origin');
    assert.ok(!r.body.includes('events.openvibe.network'), 'the home names no other origin');
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
    assert.ok(r.body.includes(`The API and the realtime gateway are at ${SITE}`), 'the discovery names the API origin');
});

t('deploy/nginx/openvibe.events.conf is the API and product origin, with no redirect to the old host', () => {
    const conf = fs.readFileSync(path.join(__dirname, '..', 'deploy', 'nginx', 'openvibe.events.conf'), 'utf8');
    assert.match(conf, /server_name openvibe\.events;/);
    assert.match(conf, /server_name www\.openvibe\.events;/);
    assert.ok(conf.includes('/etc/letsencrypt/live/openvibe.events/fullchain.pem'));
    assert.ok(conf.includes('/etc/letsencrypt/live/openvibe.events/privkey.pem'));
    assert.ok(!conf.includes('$proxy_add_x_forwarded_for'), 'the client address is never appended to');
    // The product surface (plan expansion II) and everything the API host proxies (plan T7).
    for (const loc of ['location = / {', 'location ^~ /shared/ {', 'location = /robots.txt {', 'location = /sitemap.xml {', 'location = /llms.txt {',
        'location = /realtime/stream {', 'location = /api/health', 'location = /api/ready', 'location = /release.json', 'location = /limits.json']) {
        assert.ok(conf.includes(loc), `${loc} is proxied`);
    }
    assert.ok(conf.includes('location ~ ^/api/v1/(events|subscriptions|checkpoints)(/|$)'), 'the token-guarded API block');
    assert.ok(conf.includes('location /api/ {'), 'the /api/ refusal block');
    assert.ok(conf.includes('proxy_buffering off;'), 'SSE is not buffered');
    assert.ok(conf.includes('proxy_read_timeout 1h;'), 'SSE keeps its long read timeout');
    const refusals = [conf.indexOf('location = /metrics { return 404; }'), conf.indexOf('location /internal/ { return 404; }')];
    assert.ok(refusals.every((i) => i >= 0), 'metrics and /internal/ are refused');
    assert.ok(!conf.includes('events.openvibe.network$request_uri'), 'no catch-all redirect to the old API host');
    const old = fs.readFileSync(path.join(__dirname, '..', 'deploy', 'nginx', 'events.openvibe.network.conf'), 'utf8')
        .split('\n').filter((l) => !/^\s*#/.test(l)).join('\n');
    assert.ok(!old.includes('proxy_pass'), 'events.openvibe.network serves nothing itself');
    assert.strictEqual((old.match(/return 308 https:\/\/openvibe\.events\$request_uri;/g) || []).length, 2, 'HTTP and HTTPS answer 308 to openvibe.events');
    assert.ok(conf.includes('return 301 https://openvibe.events$request_uri;'), ':80 and www reach the apex');
});

t('stop', async () => { await h.stop(); });

t.run();
