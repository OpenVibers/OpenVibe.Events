'use strict';

/**
 * The product home of OpenVibe.Events (openvibe.events, plan T7: the API origin is the product origin too).
 * The OpenVibe Frame (openvibe-shared/shell) around openvibe-shared/showcase sections
 * for developers. Every claim restates the README (Publishing, Subscriptions and delivery, Developer apps,
 * Realtime) and the code; nothing here reads data.
 *
 *   renderHome({ siteUrl, apiUrl })   the whole document (siteUrl: the canonical product origin; apiUrl: the API origin)
 *   HOME_CSP                          its Content-Security-Policy (the Frame's scripts and calls to the Network)
 */
const shell = require('openvibe-shared/shell');
const showcase = require('openvibe-shared/showcase');
const ovServe = require('openvibe-shared/serve');
const appIcon = require('openvibe-shared/app-icon');

const SITE_NAME = 'OpenVibe.Events';
const NETWORK_URL = 'https://openvibe.network';
const SOURCE = 'https://github.com/OpenVibers/OpenVibe.Events';
const DESCRIPTION = 'Durable events for the OpenVibe network: publish once, and every subscriber gets it signed, retried and replayable. '
    + 'Webhooks, pull with a cursor, dead letters, replay and realtime streams for browsers.';

const HOME_CSP = [
    "default-src 'self'",
    "script-src 'self' 'unsafe-inline' https://openvibe.network",
    "style-src 'self' 'unsafe-inline' https://openvibe.network https://fonts.googleapis.com https://cdnjs.cloudflare.com",
    "font-src 'self' data: https://fonts.gstatic.com https://cdnjs.cloudflare.com",
    "img-src 'self' data: https:",
    "connect-src 'self' https://openvibe.network https://openvibe.events",
    "frame-src 'self' https://openvibe.network",
    "frame-ancestors 'none'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self' https://openvibe.network",
].join('; ');

function sections({ apiUrl }) {
    const publish = [
        `curl -X POST ${apiUrl}/api/v1/events \\`,
        '  -H "Authorization: Bearer $TOKEN" -H \'Content-Type: application/json\' \\',
        '  -d \'{ "event_id": "evt_01JAB2C3D4E5F6G7H8J9K0MNPQ", "event_type": "app.<project_key>.order.created",',
        '        "version": 1, "source": "app-<app id>", "actor": { "type": "app", "id": "app_<ULID>" },',
        '        "timestamp": "2026-10-07T12:00:00Z", "subject": { "type": "order", "id": "42" },',
        '        "payload": { "total": 3 } }\'',
    ].join('\n');
    const subscribe = [
        `curl -X POST ${apiUrl}/api/v1/subscriptions \\`,
        '  -H "Authorization: Bearer $TOKEN" -H \'Content-Type: application/json\' \\',
        '  -d \'{ "topic_pattern": "app.<project_key>.*", "endpoint": "https://hooks.example.com/openvibe" }\'',
        '# → { "id": "sub_…", "secret": "whsec_…" }  the secret is shown once',
    ].join('\n');
    const pull = [
        `curl "${apiUrl}/api/v1/events?topic=app.<project_key>.*&limit=100" -H "Authorization: Bearer $TOKEN"`,
        '# then the same with &after=<next_cursor> from the previous page',
    ].join('\n');
    return showcase.hero({
        eyebrow: 'OpenVibe.Events · alpha',
        title: 'Events your services', accent: 'can count on.',
        lede: 'The event backbone of the OpenVibe network. Publish an event once and every subscriber gets it: stored durably, signed, '
            + 'retried with backoff, and replayable after a failure. Live, Media, Chat, Games, Network and OpenRe.Stream publish to it in production.',
        actions: [{ label: 'Read the guide', href: `${SOURCE}#readme`, primary: true }, { label: 'See an example', href: '#examples' }],
        note: `Alpha. The API is at ${apiUrl}; developer apps sign in through OpenVibe.Network.`,
    }) + showcase.features({
        title: 'What you get',
        items: [
            { icon: 'ov:upload', title: 'Publish once', text: 'One event or up to a hundred at a time, stored atomically and numbered in order. Sending the same event id twice never stores it twice.' },
            { icon: 'ov:bell', title: 'Signed webhooks', text: 'Subscribe with a topic pattern such as media.vod.*. Every delivery is HMAC-signed with its timestamp and retried with backoff.' },
            { icon: 'ov:history', title: 'Dead letters and replay', text: 'Deliveries that keep failing wait in a dead-letter queue. Replay them, or catch a new subscription up from a point in its history.' },
            { icon: 'ov:download', title: 'Pull with a cursor', text: 'Read events in order with an opaque cursor when you would rather pull than receive webhooks.' },
            { icon: 'ov:live', title: 'Realtime for browsers', text: 'Server-sent events, authorized per topic with a one-use ticket from OpenVibe.Network, and resumable after a reconnect.' },
            { icon: 'ov:code', title: 'Developer apps', text: 'An app publishes under its own project\'s names and keeps sandbox and production apart: sandbox events never reach production.' },
        ],
    }) + showcase.code({
        id: 'examples',
        title: 'Publish, subscribe, pull',
        lede: 'With an app token from OpenVibe.Network (client credentials, audience openvibe.events).',
        samples: [
            { label: 'Publish', lang: 'bash', code: publish },
            { label: 'Subscribe', lang: 'bash', code: subscribe },
            { label: 'Pull', lang: 'bash', code: pull },
        ],
    }) + showcase.steps({
        title: 'Receive a webhook safely',
        items: [
            { title: 'Keep the secret', text: 'The subscription\'s secret is shown once, when you create it or rotate it.' },
            { title: 'Check the signature', text: 'Verify X-OpenVibe-Signature-V2 over the timestamp and the raw body, and refuse anything older than five minutes.' },
            { title: 'Answer quickly', text: 'Reply 2xx once the event is safely stored; anything else is retried, then dead-lettered.' },
            { title: 'Expect repeats', text: 'A retry can bring the same event again: keep its event id and handle it once.' },
        ],
    });
}

function renderHome({ siteUrl, apiUrl }) {
    const nav = {
        service: 'events',
        apiBase: NETWORK_URL,
        links: [{ label: 'Guide', href: `${SOURCE}#readme` }],
        history: { type: 'page', title: SITE_NAME },
    };
    const footer = { service: 'events', variant: 'full', mount: '#ov-footer', brandName: SITE_NAME };
    return shell.page({
        name: SITE_NAME, service: 'events', lang: 'en',
        title: `${SITE_NAME}: durable events, signed webhooks and replay`,
        siteName: SITE_NAME,
        description: DESCRIPTION,
        summary: DESCRIPTION,
        canonical: `${siteUrl}/`,
        robots: 'index, follow',
        navbar: nav, footer, home: '/', navLinks: [{ label: 'Guide', href: `${SOURCE}#readme` }],
        head: [
            appIcon.headTags({ site: 'events' }),
            `<link rel="stylesheet" href="${ovServe.url(showcase.STYLESHEET)}">`,
        ].join('\n'),
        body: `<div id="navbar-mount"></div>
<main id="main" class="page">
${sections({ apiUrl })}
</main>`,
    });
}

module.exports = { renderHome, HOME_CSP, DESCRIPTION, sections };
