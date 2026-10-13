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
 *   GET /llms-full.txt the same public pages in full text (openvibe-shared/seo.llmsFull): the
 *                      /llms.txt header, then every page's full text, capped at 512 KiB
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

const FULL_MAX_BYTES = 512 * 1024;

/**
 * The public pages this domain publishes, grouped the way /llms.txt lists them. One entry is one
 * /llms.txt link (title, url, optional note) and one /llms-full.txt page (title, url, full text).
 */
function catalog({ siteUrl, apiUrl }) {
    const home = seo.absolute('/', siteUrl);
    return [
        { title: 'Start here', links: [
            { title: 'OpenVibe.Events home', url: home, text: DESCRIPTION },
            { title: 'Guide', url: GUIDE, note: 'the README: publishing, subscriptions, developer apps and realtime', text: details({ siteUrl, apiUrl }) },
            { title: 'Source code', url: SOURCE, text: 'The OpenVibe.Events source on GitHub: the durable event store, the publish, read and subscription API, the delivery worker, the retention tiers and the realtime gateway.' },
        ] },
        { title: 'Machine-readable', links: [
            { title: 'Sitemap', url: seo.absolute('/sitemap.xml', siteUrl), text: `The product home only: ${home}` },
            { title: 'robots.txt', url: seo.absolute('/robots.txt', siteUrl), text: 'The crawler rules: the product home is welcome, the API and the realtime gateway are kept out of the index.' },
            { title: 'API and realtime', url: apiUrl, note: 'the JSON API and the realtime gateway', text: `${apiUrl}/api/v1 is the token-guarded JSON API; ${apiUrl}/realtime/stream is the browser Server-Sent Events gateway.` },
        ] },
    ];
}

function createDiscoveryRoutes({ config, releaseLastmod }) {
    const router = express.Router();
    const abs = (p) => seo.absolute(p, config.siteUrl);

    router.get('/robots.txt', (_req, res) => res.type('text/plain').set('Cache-Control', cache.htmlHeaders())
        .send(seo.robotsTxt({ sitemaps: [abs('/sitemap.xml')], disallow: ['/api/', '/realtime/'] })));

    router.get('/sitemap.xml', (_req, res) => res.type('application/xml').set('Cache-Control', cache.htmlHeaders())
        .send(seo.sitemapXml([{ loc: abs('/'), lastmod: releaseLastmod }])));

    const pages = catalog({ siteUrl: config.siteUrl, apiUrl: config.baseUrl });

    router.get('/llms.txt', (_req, res) => res.type('text/plain').set('Cache-Control', cache.htmlHeaders())
        .send(seo.llmsTxt({
            name: 'OpenVibe.Events',
            summary: DESCRIPTION,
            details: details({ siteUrl: config.siteUrl, apiUrl: config.baseUrl }),
            sections: pages.map((s) => ({ title: s.title, links: s.links.map((l) => ({ title: l.title, url: l.url, ...(l.note ? { note: l.note } : {}) })) })),
        })));

    // /llms-full.txt: the same pages as /llms.txt, each with its full plain text (openvibe-shared/seo.llmsFull).
    router.get('/llms-full.txt', (_req, res) => res.type('text/plain').set('Cache-Control', cache.htmlHeaders())
        .send(seo.llmsFull({
            site: 'OpenVibe.Events',
            summary: DESCRIPTION,
            base: config.siteUrl,
            maxBytes: FULL_MAX_BYTES,
            sections: pages.map((s) => ({ title: s.title, pages: s.links.map((l) => ({ title: l.title, url: l.url, text: l.text })) })),
        })));

    return router;
}

module.exports = { createDiscoveryRoutes };
