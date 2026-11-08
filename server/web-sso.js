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
    router.use(client.router(express));
    return router;
}
module.exports = { webSso };
