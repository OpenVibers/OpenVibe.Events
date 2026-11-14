'use strict';
/**
 * The shared navbar's token (the owner, 11-14: the menu's Alerts said "Your notifications are on your account" on
 * Events): the access cookie is HTTP-only (httpOnlyAccess), so the navbar asks /auth/navbar-token, same-origin and
 * never cached, and the page tells it where (tokenUrl). Signed out is 401; another site is refused.
 */
const assert = require('node:assert/strict');
const { boot, request, userToken, suite } = require('./helpers');

const t = suite('navbar-token');
let h;

t('the service is up', async () => { h = await boot(); });

t('the signed-in person gets their token, same-origin and never cached', async () => {
    const at = userToken();
    const r = await request(h.base, 'GET', '/auth/navbar-token', {
        headers: { Cookie: `ov_token=${at}`, 'Sec-Fetch-Site': 'same-origin' },
    });
    assert.equal(r.status, 200);
    assert.deepEqual(r.body, { token: at });
    assert.match(r.headers.get('cache-control') || '', /no-store/, 'never cached');
});

t('signed out is 401', async () => {
    const r = await request(h.base, 'GET', '/auth/navbar-token', { headers: { 'Sec-Fetch-Site': 'same-origin' } });
    assert.equal(r.status, 401);
});

t('another site is refused', async () => {
    const at = userToken();
    const r = await request(h.base, 'GET', '/auth/navbar-token', {
        headers: { Cookie: `ov_token=${at}`, 'Sec-Fetch-Site': 'cross-site' },
    });
    assert.equal(r.status, 403);
});

t('the page tells the navbar where to ask', async () => {
    const r = await request(h.base, 'GET', '/', { headers: { Accept: 'text/html' } });
    assert.equal(r.status, 200);
    assert.ok(r.text.includes('"tokenUrl":"/auth/navbar-token"'), r.text.slice(0, 400));
});

t('the service stops', async () => { await h.stop(); });

t.run();
