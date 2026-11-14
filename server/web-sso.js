'use strict';
// Browser sessions use the shared SDK. API/service-token authorization remains in its own middleware.
const express = require('express');
const { createSsoClient } = require('openvibe-sdk/sso');
function webSso({ config, fetchImpl = globalThis.fetch, log = console }) {
    const client = createSsoClient({
        site: 'events', siteName: 'OpenVibe.Events', baseUrl: config.baseUrl,
        clientId: config.oauth.clientId, clientSecret: config.oauth.clientSecret,
        redirectUri: `${config.baseUrl}/auth/callback`, scope: 'profile theme',
        networkUrl: config.networkUrl, networkInternalUrl: config.networkInternalUrl,
        issuer: config.issuer, publicKey: config.networkPublicKey, audience: ['openvibe.network'],
        secureCookies: config.isProduction, httpOnlyAccess: true, fetch: fetchImpl, log,
    });
    const router = express.Router();
    router.use((_req, res, next) => { res.set('Cache-Control', 'no-store'); next(); });
    // The shared navbar's Dock and Alerts read the person's Network places with their token. This site's access
    // token is HTTP-only (httpOnlyAccess: no script can read it), so the navbar asks for it here: same-origin only,
    // never cached. Without it the menu's Alerts said "Your notifications are on your account" (the owner, 11-14).
    router.get('/navbar-token', client.optionalAuth(), (req, res) => {
        res.set({ 'Cache-Control': 'private, no-store', Vary: 'Cookie' });
        const site = String(req.get('sec-fetch-site') || '');
        if (site && site !== 'same-origin') return res.status(403).json({ error: 'Same-origin only', code: 'events.navbar_token.cross_site' });
        if (!req.user || !req.token) return res.status(401).json({ error: 'Not signed in', code: 'events.navbar_token.signed_out' });
        return res.json({ token: req.token });
    });
    router.use(client.router(express));
    return router;
}
module.exports = { webSso };
