'use strict';
/**
 * Crawl and machine-readability artifacts for openvibe.events, the product domain, built from
 * openvibe-shared/seo the same way every other OpenVibe site builds them:
 *
 *   GET /robots.txt    welcomes search and AI crawlers to the home page, keeps the API and the
 *                      realtime gateway out of the index, and always names the sitemap
 *   GET /sitemap.xml   the product home only (openvibe.events is the API origin too, plan T7)
 *   GET /llms.txt      what OpenVibe.Events does today, in plain language: the README's Purpose,
 *                      Publishing, Subscriptions and delivery, Developer apps and Realtime sections
 *
 * Read-only and viewer-independent: built from the config (the product site URL and the API origin).
 */
const express = require('express');
const cache = require('openvibe-shared/cache-policy');
const seo = require('openvibe-shared/seo');
const { DESCRIPTION } = require('./home');

const SOURCE = 'https://github.com/OpenVibers/OpenVibe.Events';
const GUIDE = `${SOURCE}#readme`;

/** The README's Purpose, Publishing, Subscriptions and delivery, Developer apps and Realtime in plain sentences. */
function details({ siteUrl, apiUrl }) {
    return [
        'OpenVibe.Events is the durable event backbone of the OpenVibe network. Every authoritative service writes its domain mutation and its outbox event in one transaction, a relay publishes them here, consumers keep inbox/idempotency receipts, and browsers get authorized, resumable projections over the realtime stream; an event exists if and only if the change it describes committed.',
        '',
        `Publishing: POST ${apiUrl}/api/v1/events takes one event envelope or up to a hundred at a time, stored atomically and given one global order. A service may only publish the event types its own namespace owns, a repeat of an event id is answered 200 with duplicate: true and is never stored twice, and a producer can redact what it published: the named events become tombstones that every read path serves from then on.`,
        '',
        `Subscriptions and delivery: a subscription is a topic pattern such as media.vod.* and an endpoint (http on 127.0.0.1 or an OpenVibe host). Each delivery is an HMAC-signed POST (X-OpenVibe-Signature-V2, over the timestamp and the raw body, inside a five-minute window) and answers 2xx once the event is safely stored; anything else is retried with backoff and, after the last attempt, waits in a dead-letter queue that operators replay. Consumers can also pull in order instead, with an opaque cursor that crosses the retention tiers without a gap.`,
        '',
        'Developer apps: an app signs in through OpenVibe.Network (client credentials, audience openvibe.events) and publishes, reads and subscribes on the same routes under the events.app.* capabilities, scoped to its own project and environment. Sandbox events never reach production, app webhook endpoints must be public HTTPS addresses, and per-project quotas are enforced here.',
        '',
        `Realtime: browsers follow topics over server-sent events at ${apiUrl}/realtime/stream with a one-use ticket from OpenVibe.Network; a reconnect resumes from its last cursor, or is told about the gap when the position is too old to replay. Subject-visible events reach only the person they are about, and internal events never reach a browser.`,
        '',
        `The guide is ${GUIDE}. The API and the realtime gateway are at ${apiUrl}; the product home is ${siteUrl}/.`,
    ].join('\n');
}

function createDiscoveryRoutes({ config }) {
    const router = express.Router();
    const abs = (p) => seo.absolute(p, config.siteUrl);

    router.get('/robots.txt', (_req, res) => res.type('text/plain').set('Cache-Control', cache.htmlHeaders())
        .send(seo.robotsTxt({ sitemaps: [abs('/sitemap.xml')], disallow: ['/api/', '/realtime/'] })));

    router.get('/sitemap.xml', (_req, res) => res.type('application/xml').set('Cache-Control', cache.htmlHeaders())
        .send(seo.sitemapXml([{ loc: abs('/') }])));

    router.get('/llms.txt', (_req, res) => res.type('text/plain').set('Cache-Control', cache.htmlHeaders())
        .send(seo.llmsTxt({
            name: 'OpenVibe.Events',
            summary: DESCRIPTION,
            details: details({ siteUrl: config.siteUrl, apiUrl: config.baseUrl }),
            sections: [
                { title: 'Start here', links: [
                    { title: 'OpenVibe.Events home', url: abs('/') },
                    { title: 'Guide', url: GUIDE, note: 'the README: publishing, subscriptions, developer apps and realtime' },
                    { title: 'Source code', url: SOURCE },
                ] },
                { title: 'Machine-readable', links: [
                    { title: 'Sitemap', url: abs('/sitemap.xml') },
                    { title: 'robots.txt', url: abs('/robots.txt') },
                    { title: 'API and realtime', url: config.baseUrl, note: 'the JSON API and the realtime gateway' },
                ] },
            ],
        })));

    return router;
}

module.exports = { createDiscoveryRoutes };
