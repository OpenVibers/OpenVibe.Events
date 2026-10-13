'use strict';
// IndexNow (openvibe-shared/indexnow): the key file engines fetch is served at /<INDEXNOW_KEY>.txt only
// when a key is configured; unset, the path is not mounted (404) and nothing is ever sent. The client is
// created once at boot from INDEXNOW_KEY (server/index.js).
const assert = require('assert');
const nodeHttp = require('http');
const { boot, suite } = require('./helpers');

const KEY = require('crypto').randomBytes(16).toString('hex');   // 32 hex characters: the 8–128 IndexNow wants
const SITE = 'https://openvibe.events';

const t = suite('indexnow');
let without, withKey;

/** A raw GET with no Accept a browser would send. */
function raw(base, p) {
    return new Promise((resolve, reject) => {
        const url = new URL(base + p);
        nodeHttp.get({ hostname: url.hostname, port: url.port, path: url.pathname + url.search }, (res) => {
            let body = '';
            res.setEncoding('utf8');
            res.on('data', (c) => { body += c; });
            res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
        }).on('error', reject);
    });
}

t('boot without INDEXNOW_KEY', async () => { without = await boot({ env: { BASE_URL: SITE } }); });
t('boot with INDEXNOW_KEY', async () => { withKey = await boot({ env: { BASE_URL: SITE, INDEXNOW_KEY: KEY } }); });

t('with no key configured the key file is 404', async () => {
    const r = await raw(without.base, `/${KEY}.txt`);
    assert.strictEqual(r.status, 404, 'no /<key>.txt without INDEXNOW_KEY');
});

t('with a key configured the key file returns the key and nothing else', async () => {
    const r = await raw(withKey.base, `/${KEY}.txt`);
    assert.strictEqual(r.status, 200);
    assert.match(r.headers['content-type'], /^text\/plain/);
    assert.strictEqual(r.body, KEY);
    const other = await raw(withKey.base, '/another-key.txt');
    assert.strictEqual(other.status, 404, 'only the configured key file is served');
});

t('stop', async () => { await without.stop(); await withKey.stop(); });

t.run();
