'use strict';
const assert = require('node:assert/strict');
const express = require('express');
const { webSso } = require('../server/web-sso');
const config = require('../server/config').load({ NODE_ENV: 'production', BASE_URL: 'https://openvibe.events', OV_OAUTH_CLIENT_ID: 'events', OV_OAUTH_CLIENT_SECRET: 'test-only-secret' });
(async () => {
    const grants = [];
    const crypto = require('node:crypto');
    const { privateKey, publicKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
    const jwk = { ...publicKey.export({ format: 'jwk' }), kid: 'sso-test', alg: 'RS256', use: 'sig' };
    let challenge;
    const app = express();
    app.use('/auth', webSso({ config, log: { warn() {}, error() {} }, fetchImpl: async (_url, options) => {
        if (_url.endsWith('/api/.well-known/jwks')) return new Response(JSON.stringify({ keys: [jwk] }));
        const grant = JSON.parse(options.body);
        grants.push(grant);
        if (grant.code === 'issued') {
            assert.equal(crypto.createHash('sha256').update(grant.code_verifier).digest('base64url'), challenge);
            const token = require('jsonwebtoken').sign({ sub: '12', type: 'person', username: 'sso-test' }, privateKey,
                { algorithm: 'RS256', keyid: jwk.kid, issuer: 'https://openvibe.network', audience: 'openvibe.network', expiresIn: '1h' });
            return new Response(JSON.stringify({ access_token: token, refresh_token: 'test-refresh' }));
        }
        return new Response(JSON.stringify({ error: 'invalid_grant' }), { status: 400 });
    } }));
    const server = app.listen(0, '127.0.0.1');
    await new Promise(resolve => server.once('listening', resolve));
    const get = (path, cookie) => fetch(`http://127.0.0.1:${server.address().port}${path}`, { redirect: 'manual', headers: cookie ? { Cookie: cookie } : {} });
    const cookies = r => r.headers.getSetCookie().map(c => c.split(';')[0]).join('; ');
    try {
        const start = await get('/auth/login?silent=1&next=%2F%3Ffrom%3Dnav%23details');
        assert.equal(start.status, 302);
        const u = new URL(start.headers.get('location'));
        assert.equal(u.origin, 'https://openvibe.network');
        assert.equal(u.searchParams.get('client_id'), 'events');
        assert.equal(u.searchParams.get('redirect_uri'), 'https://openvibe.events/auth/callback');
        assert.equal(u.searchParams.get('prompt'), 'none');
        assert.equal(u.searchParams.get('code_challenge_method'), 'S256');
        assert.ok(u.searchParams.get('code_challenge'));
        assert.ok(start.headers.getSetCookie().every(c => !/Domain=/i.test(c)), 'flow cookies are host-only');
        const guest = await get('/auth/callback?error=login_required&state='+u.searchParams.get('state'), cookies(start));
        assert.equal(guest.status, 302);
        assert.equal(guest.headers.get('location'), '/?from=nav&sso=none#details');
        assert.ok(!cookies(guest).includes('ov_token='));
        const mismatch = await get('/auth/callback?code=unissued&state=wrong', cookies(start));
        assert.ok(!cookies(mismatch).includes('ov_token='));
        assert.equal(grants.length, 0, 'a mismatched state never reaches the token endpoint');
        const me = await get('/auth/me');
        assert.deepEqual(await me.json(), { user: null });
        assert.equal(me.headers.get('cache-control'), 'no-store');
        const health = await get('/auth/health');
        assert.deepEqual(await health.json(), { ok: true, client: 'ok' });
        assert.equal(grants[0].client_id, 'events');
        const external = await get('/auth/login?silent=1&next=https%3A%2F%2Fevil.test');
        const back = await get('/auth/callback?error=login_required', cookies(external));
        assert.equal(back.headers.get('location'), '/?sso=none');
        challenge = u.searchParams.get('code_challenge');
        const signedIn = await get('/auth/callback?code=issued&state='+u.searchParams.get('state'), cookies(start));
        assert.equal(signedIn.status, 302);
        assert.equal(signedIn.headers.get('location'), '/?from=nav#details');
        const access = signedIn.headers.getSetCookie().find(c => c.startsWith('ov_token='));
        assert.ok(access && /HttpOnly/i.test(access) && /Secure/i.test(access) && /SameSite=Lax/i.test(access));
        assert.ok(!/Domain=/i.test(access));
        const person = await get('/auth/me', cookies(signedIn));
        assert.equal((await person.json()).user.username, 'sso-test');

        console.log('web SSO: SDK silent login, PKCE, state, guest return, host-only cookies and no-store passed');
    } finally { await new Promise(resolve => server.close(resolve)); }
})().catch(err => { console.error(err); process.exitCode = 1; });
