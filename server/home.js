'use strict';

/**
 * The product home of OpenVibe.Events (openvibe.events, plan T7: the API origin is the product origin too).
 * The OpenVibe Frame (openvibe-shared/shell) around openvibe-shared/showcase sections
 * for developers. Every claim restates the README (Publishing, Subscriptions and delivery, Developer apps,
 * Realtime) and the code. The bus at work comes from server/home-stats.js (counts and names only, never a payload):
 * the numbers, who published this week and the public event types with when each last arrived; each section is left
 * out when there is nothing to show.
 *
 *   renderHome({ siteUrl, apiUrl, stats })   the whole document (siteUrl: the canonical product origin; apiUrl: the API
 *                                            origin; stats: home-stats read(), or null)
 *   HOME_CSP                          its Content-Security-Policy (the Frame's scripts and calls to the Network)
 */
const shell = require('openvibe-shared/shell');
const frame = require('openvibe-shared/frame');
const showcase = require('openvibe-shared/showcase');
const ovServe = require('openvibe-shared/serve');
const appIcon = require('openvibe-shared/app-icon');
const seo = require('openvibe-shared/seo');

const SITE_NAME = 'OpenVibe.Events';
const NETWORK_URL = 'https://openvibe.network';
const SOURCE = 'https://github.com/OpenVibers/OpenVibe.Events';
const DESCRIPTION = 'The OpenVibe network\'s event bus: publish once and every subscriber gets it signed, retried and replayable, '
    + 'by webhook, pull or realtime stream.';

const HOME_CSP = [
    "default-src 'self'",
    // Cloudflare Web Analytics: Cloudflare injects its beacon at the edge on this zone; script-src loads it and connect-src
    // is where it reports (performance timing only, no cookies).
    "script-src 'self' 'unsafe-inline' https://openvibe.network https://static.cloudflareinsights.com",
    "style-src 'self' 'unsafe-inline' https://openvibe.network https://fonts.googleapis.com https://cdnjs.cloudflare.com",
    "font-src 'self' data: https://fonts.gstatic.com https://cdnjs.cloudflare.com",
    "img-src 'self' data: https:",
    "connect-src 'self' https://openvibe.network https://openvibe.events https://cloudflareinsights.com",
    "frame-src 'self' https://openvibe.network",
    "frame-ancestors 'none'",
    "object-src 'none'",
    "base-uri 'self'",
    "form-action 'self' https://openvibe.network",
].join('; ');

const esc = showcase.esc;
const fmt = (n) => Number(n).toLocaleString('en-US');
// Where each publishing service lives (the first segment of its event types). One not listed is shown by name only.
const SERVICES = {
    live: ['OpenVibe.Live', 'https://openvibe.live'], search: ['OpenVibe.Search', 'https://search.openvibe.network'], sources: ['OpenVibe.Sources', null],
    reviews: ['OpenVibe.Reviews', 'https://openvibe.reviews'], ai: ['OpenVibe AI', 'https://ai.openvibe.services'], work: ['OpenVibe.Work', 'https://openvibe.work'],
    trade: ['OpenVibe.Trade', 'https://openvibe.trade'], media: ['OpenVibe.Media', 'https://openvibe.media'], deals: ['OpenVibe.Deals', 'https://openvibe.deals'],
    community: ['OpenVibe.Community', 'https://openvibe.community'], help: ['OpenVibe.Help', 'https://openvibe.help'], chat: ['OpenVibe.Chat', 'https://openvibe.chat'],
    rent: ['OpenVibe.Rent', 'https://openvibe.rent'], host: ['OpenVibe.Host', 'https://openvibe.host'], network: ['OpenVibe.Network', 'https://openvibe.network'],
    news: ['OpenVibe.News', 'https://openvibe.news'], coupons: ['OpenVibe.Coupons', 'https://openvibe.coupons'], games: ['OpenVibe.Games', 'https://openvibe.games'],
    tools: ['OpenVibe.Tools', 'https://openvibe.tools'], wiki: ['OpenVibe.Wiki', 'https://openvibe.wiki'], blog: ['OpenVibe.Blog', 'https://openvibe.blog'],
    quest: ['OpenVibe.Quest', 'https://openvibe.quest'], inventory: ['OpenVibe.Inventory', 'https://inventory.openvibe.network'],
    services: ['OpenVibe.Services', 'https://openvibe.services'], events: ['OpenVibe.Events', 'https://openvibe.events'], bot: ['OpenVibe.Bot', 'https://openvibe.bot'],
    actor: ['OpenVibe.Actor', 'https://openvibe.actor'], watch: ['OpenVibe.Watch', 'https://openvibe.watch'], codes: ['OpenVibe.Codes', 'https://openvibe.codes'],
    space: ['OpenVibe.Space', 'https://openvibe.space'], run: ['OpenVibe.Run', 'https://openvibe.run'], zone: ['OpenVibe.Zone', 'https://openvibe.zone'],
    food: ['OpenVibe.Food', 'https://openvibe.food'], download: ['OpenVibe.Download', 'https://openvibe.download'], mediahub: ['OpenVibe.Video', 'https://openvibe.video'],
    openre: ['OpenRestream', 'https://openre.stream'], play: ['OpenVibe Play', 'https://play.openvibe.network'], billing: ['OpenVibe.Billing', null],
    app: ['Developer apps', null],
};
// Plain words for the public event types people meet most; any other type is shown by its name alone.
const TYPE_WORDS = {
    'live.stream.started': 'A channel went live on OpenVibe.Live', 'live.stream.ended': 'A stream ended on OpenVibe.Live',
    'blog.post.published': 'A post was published on OpenVibe.Blog', 'wiki.page.published': 'A page was published on OpenVibe.Wiki',
    'chat.message.created': 'A message in a public chat room', 'community.paste.created': 'A paste was shared on OpenVibe.Community',
    'community.paste.updated': 'A paste was edited on OpenVibe.Community', 'community.thread.created': 'A new forum thread on OpenVibe.Community',
    'coupons.coupon.created': 'A coupon was added on OpenVibe.Coupons', 'deals.offer.created': 'A deal was added on OpenVibe.Deals',
    'deals.offer.updated': 'A deal changed on OpenVibe.Deals', 'games.world.published': 'A creator world was published on OpenVibe.Games',
    'services.app.published': 'A developer app was published', 'reviews.signal.added': 'A review signal was added on OpenVibe.Reviews',
    'trade.observation.created': 'A market observation on OpenVibe.Trade',
};

function ago(ms, now) {
    const s = Math.max(0, Math.round((now - ms) / 1000));
    if (s < 60) return 'just now';
    if (s < 3600) return `${Math.round(s / 60)} min ago`;
    if (s < 86400) return `${Math.round(s / 3600)} h ago`;
    const d = Math.round(s / 86400);
    return `${d} day${d === 1 ? '' : 's'} ago`;
}
const serviceName = (ns) => (SERVICES[ns] || [ns])[0];
const serviceLink = (ns) => { const [name, url] = SERVICES[ns] || [ns, null]; return url ? `<a href="${esc(url)}">${esc(name)}</a>` : esc(name); };

/** The numbers, who published and the public types (each left out when empty). */
function activity(stats) {
    if (!stats) return '';
    const now = stats.measuredAt || Date.now();
    const tiles = [
        [stats.day, 'events in the last 24 hours'], [stats.week, 'events this week'], [stats.services.length, 'services published this week'],
        [stats.types, 'event types this week'], [stats.subscriptions, `active subscriptions${stats.consumers ? ` from ${fmt(stats.consumers)} consumers` : ''}`],
        [stats.delivered, 'deliveries made in the last 30 days'],
    ].filter(([n]) => n > 0);
    const statRow = tiles.length ? `<section class="sc-sec hm-stats-sec" aria-label="The bus right now"><ul class="hm-stats">${tiles.map(([n, label]) => `<li><b>${esc(fmt(n))}</b><span>${esc(label)}</span></li>`).join('')}</ul>${stats.dead === 0 && stats.delivered ? '<p class="hm-note">No deliveries are waiting in the dead-letter queue.</p>' : ''}</section>` : '';
    const top = stats.services.slice(0, 16);
    const max = top.length ? top[0].n : 1;
    const who = top.length ? `<section class="sc-sec" aria-labelledby="h-who"><h2 id="h-who">Who published this week</h2>
<p class="sc-lede">Events by the service that published them over the last seven days. Every one was stored before any subscriber saw it.</p>
<ul class="hm-bars">${top.map((r) => `<li><span class="hm-bar-name">${serviceLink(r.ns)}</span><span class="hm-bar" aria-hidden="true"><i style="width:${Math.max(2, Math.round((r.n / max) * 100))}%"></i></span><span class="hm-bar-n">${esc(fmt(r.n))}</span></li>`).join('')}</ul>
${stats.services.length > top.length ? `<p class="hm-note">And ${esc(stats.services.length - top.length)} more.</p>` : ''}</section>` : '';
    const types = stats.public.length ? `<section class="sc-sec" aria-labelledby="h-types"><h2 id="h-types">Public events this week</h2>
<p class="sc-lede">The event types anyone may read, with how many arrived in the last seven days. Subscribe to a pattern such as live.stream.* to get them as signed webhooks.</p>
<div class="sc-table-wrap" tabindex="0" role="region" aria-labelledby="h-types"><table class="sc-table hm-table"><thead><tr><th scope="col">Event type</th><th scope="col">What happened</th><th scope="col">This week</th><th scope="col">Last one</th></tr></thead><tbody>
${stats.public.map((r) => `<tr><th scope="row"><code>${esc(r.type)}</code></th><td>${esc(TYPE_WORDS[r.type] || `From ${serviceName(r.type.split('.')[0])}`)}</td><td>${esc(fmt(r.n))}</td><td>${esc(ago(r.last, now))}</td></tr>`).join('')}
</tbody></table></div></section>` : '';
    return { statRow, who, types };
}

/** Questions people ask first, answered from the README and the code. */
const FAQ = [
    ['What is OpenVibe.Events?', 'The event bus of the OpenVibe network. A service publishes an event once; Events stores it, then delivers it to every subscriber as a signed webhook, keeps it for pulls with a cursor, and streams public ones to browsers.'],
    ['Who can publish?', 'OpenVibe services with the events.event.publish capability, and developer apps under their own project\'s names (app.<project_key>.*) with an app token from OpenVibe.Network.'],
    ['How do I know a webhook really came from Events?', 'Every delivery is signed over its timestamp and raw body (X-OpenVibe-Signature-V2). Check it with verifyDeliveryV2 from openvibe-sdk/events and refuse anything more than five minutes old.'],
    ['What if my endpoint is down?', 'The delivery is retried with backoff. One that keeps failing waits in the dead-letter queue, where it can be replayed, and a new subscription can catch up from a point in history.'],
    ['How long are events kept?', 'Thirty days in the hot store for delivery, pulls and replay, then a year in the replay tier for pulls and stream resumes.'],
    ['Can a browser listen?', 'Yes. Server-sent events at /realtime/stream, authorized per topic with a one-use ticket from OpenVibe.Network, and resumable after a reconnect.'],
    ['Are sandbox and production separate?', 'Yes. A developer app\'s sandbox events never reach production apps or subscriptions, and are pruned after seven days.'],
];

function sections({ apiUrl, stats = null }) {
    const live = activity(stats) || { statRow: '', who: '', types: '' };
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
    const publishers = stats && stats.services.filter((r) => r.ns !== 'app').length;
    return showcase.hero({
        eyebrow: 'OpenVibe.Events · part of OpenVibe Services · alpha',
        title: 'Events your services', accent: 'can count on.',
        lede: 'The event backbone of the OpenVibe network. Publish an event once and every subscriber gets it: stored durably, signed, '
            + `retried with backoff, and replayable after a failure.${publishers ? ` ${publishers} OpenVibe services published to it this week.` : ''}`,
        actions: [{ label: 'Read the guide', href: `${SOURCE}#readme`, primary: true }, { label: 'See an example', href: '#examples' }],
        note: `Alpha. The API is at ${apiUrl}; developer apps sign in through OpenVibe.Network.`,
    }) + '<section class="sc-sec sc-part-of"><p class="sc-note">Part of <a href="https://openvibe.services/">OpenVibe Services</a>: the building blocks every OpenVibe product runs on, with one console, one account and one SDK.</p></section>' + live.statRow + live.who + live.types + showcase.features({
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
    }) + `<section class="sc-sec" id="faq" aria-labelledby="h-faq"><h2 id="h-faq">Questions</h2>
<div class="hm-faq">${FAQ.map(([q, a]) => `<div class="hm-qa"><h3>${esc(q)}</h3><p>${esc(a)}</p></div>`).join('')}</div></section>`;
}

// The page's own few rules (it has no stylesheet beyond the showcase's): no browser margin around the body, so the Frame's
// footer meets the edges; each curl sample on its own full-width row (three columns cut every command off); the four
// webhook steps two by two instead of three and one alone, each title level with its number.
// The data sections' rules follow: the stat tiles, the per-service bars, the public types table and the questions, in theme tokens (accent-light is drawn for dark themes, so text in it is mixed toward the text colour).
const HOME_STYLE = '<style>body{margin:0}.page .sc-code{grid-template-columns:minmax(0,1fr)}.page .sc-steps{grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr))}.page .sc-steps li{align-items:start;align-content:start}.page .sc-steps b{line-height:30px}'
    + '.page{--hm-accent-text:color-mix(in srgb,var(--accent-light,#60a5fa) 45%,var(--text-primary,#e6edf7))}.sc-eyebrow{color:var(--hm-accent-text)}.sc-table thead th{color:var(--text-secondary,#96a7c2)}'
    + '.hm-stats-sec{margin-top:40px}.hm-stats{list-style:none;margin:0;padding:0;display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(min(130px,100%),1fr))}'
    + '.hm-stats li{display:grid;gap:2px;padding:14px 16px;border-radius:14px;border:1px solid var(--border,rgba(255,255,255,.08));background:var(--bg-secondary,#101828)}'
    + '.hm-stats b{font-size:28px;line-height:1.1;letter-spacing:-.02em}.hm-stats span{font-size:13.5px;color:var(--text-secondary,#96a7c2)}'
    + '.hm-note{margin:10px 0 0;font-size:14px;color:var(--text-secondary,#96a7c2)}'
    + '.hm-bars{list-style:none;margin:0;padding:0;display:grid;gap:8px 28px;grid-template-columns:repeat(auto-fill,minmax(min(100%,460px),1fr))}'
    + '.hm-bars li{display:grid;grid-template-columns:minmax(0,11em) minmax(0,1fr) auto;align-items:center;gap:12px;font-size:14.5px}'
    + '.hm-bar-name{overflow-wrap:anywhere}.hm-bar-name a{color:var(--hm-accent-text);text-decoration:none}.hm-bar-name a:hover{text-decoration:underline}.hm-bar{height:10px;border-radius:999px;background:var(--bg-tertiary,#172238);overflow:hidden}'
    + '.hm-bar i{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,var(--accent,#3b82f6),var(--accent-light,#60a5fa))}.hm-bar-n{font-variant-numeric:tabular-nums;color:var(--text-secondary,#96a7c2);text-align:right;min-width:4.5em}'
    + '.hm-table code{font-size:13px;overflow-wrap:anywhere}.hm-table td:nth-child(3),.hm-table td:nth-child(4){white-space:nowrap;font-variant-numeric:tabular-nums}'
    + '.hm-faq{display:grid;gap:12px 28px;grid-template-columns:repeat(auto-fill,minmax(min(380px,100%),1fr))}.hm-qa{padding:4px 0 14px;border-bottom:1px solid var(--border,rgba(255,255,255,.08))}'
    + '.hm-qa h3{margin:0 0 6px;font-size:16px}.hm-qa p{margin:0;font-size:14.5px;line-height:1.6;color:var(--text-secondary,#96a7c2)}'
    + '@media (max-width:520px){.page .sc-code pre{white-space:pre-wrap;overflow-wrap:anywhere}.hm-bars li{grid-template-columns:minmax(0,10.5em) minmax(0,1fr) auto;font-size:13.5px;gap:8px}.hm-bar-n{min-width:3.5em}'
    // On a phone each public type is a small card: the type across the top, what happened below it, the count and the last one on the right.
    + '.hm-table thead{display:none}.hm-table,.hm-table tbody{display:block}.hm-table tr{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:2px 12px;padding:10px 14px;border-top:1px solid var(--border,rgba(255,255,255,.08))}'
    + '.hm-table th,.hm-table td{display:block;padding:0;border:0}.hm-table tbody th{grid-column:1/-1;grid-row:1;width:auto}.hm-table td:nth-child(2){grid-column:1;grid-row:2/span 2;font-size:13.5px;color:var(--text-secondary,#96a7c2)}'
    + '.hm-table td:nth-child(3){grid-column:2;grid-row:2;text-align:right}.hm-table td:nth-child(4){grid-column:2;grid-row:3;text-align:right;font-size:13px;color:var(--text-secondary,#96a7c2)}}'
    + ''
    + '@media (max-width:420px){.sc-hero,.sc-sec,.sc-cta{padding-left:16px;padding-right:16px}}</style>';

const FOOTER = { service: 'events', variant: 'full', mount: '#ov-footer', brandName: SITE_NAME, updates: '/updates' };

/** /updates: what shipped on this site, the network's shared update log (openvibe-shared/frame updatesBody). */
function renderUpdates({ siteUrl }) {
    const links = [{ label: 'Guide', href: `${SOURCE}#readme` }];
    return shell.page({
        name: SITE_NAME, service: 'events', lang: 'en',
        title: `What shipped on ${SITE_NAME}`,
        siteName: SITE_NAME,
        description: `Every change deployed to ${SITE_NAME}, newest first.`,
        canonical: `${siteUrl}/updates`,
        robots: 'index, follow',
        navbar: { service: 'events', apiBase: NETWORK_URL, links, history: { type: 'page', title: SITE_NAME } },
        footer: FOOTER, home: '/', navLinks: links,
        head: appIcon.headTags({ site: 'events' }),
        body: `<div id="navbar-mount"></div>
<main id="main" class="page">
${frame.updatesBody({ service: 'events', siteName: SITE_NAME })}
</main>
<script src="${ovServe.url('shipped.js')}" defer></script>`,
    });
}

function renderHome({ siteUrl, apiUrl, stats = null }) {
    const nav = {
        service: 'events',
        apiBase: NETWORK_URL,
        links: [{ label: 'Guide', href: `${SOURCE}#readme` }],
        history: { type: 'page', title: SITE_NAME },
    };
    const footer = FOOTER;
    return shell.page({
        name: SITE_NAME, service: 'events', lang: 'en',
        title: `${SITE_NAME}: durable events, signed webhooks and replay`,
        siteName: SITE_NAME,
        description: DESCRIPTION,
        summary: DESCRIPTION,
        canonical: `${siteUrl}/`,
        robots: 'index, follow',
        jsonLd: [
            seo.jsonLd.website({ name: SITE_NAME, url: siteUrl, description: DESCRIPTION }),
            seo.jsonLd.softwareApp({ name: SITE_NAME, url: `${siteUrl}/`, description: DESCRIPTION, category: 'DeveloperApplication', keywords: ['event bus', 'webhooks', 'server-sent events', 'replay', 'dead-letter queue'] }),
            seo.jsonLd.faq(FAQ.map(([q, a]) => ({ q, a }))),
        ],
        navbar: nav, footer, home: '/', navLinks: [{ label: 'Guide', href: `${SOURCE}#readme` }],
        head: [
            appIcon.headTags({ site: 'events' }),
            `<link rel="stylesheet" href="${ovServe.url(showcase.STYLESHEET)}">`,
            HOME_STYLE,
        ].join('\n'),
        body: `<div id="navbar-mount"></div>
<main id="main" class="page">
${sections({ apiUrl, stats })}
</main>`,
    });
}

/** The page a browser gets for a path nothing serves: a real document (language, title, the app icon), not JSON. */
function renderNotFound() {
    return ['<!doctype html>', '<html lang="en">', '<head>', '<meta charset="utf-8">', '<meta name="viewport" content="width=device-width, initial-scale=1">',
        `<title>Not found · ${SITE_NAME}</title>`, '<meta name="robots" content="noindex">', appIcon.headTags({ site: 'events' }), '</head>',
        '<body style="font-family:system-ui,sans-serif;margin:3rem auto;max-width:36rem;padding:0 1rem;line-height:1.5">',
        '<h1>Not found</h1>', `<p>Nothing is here. <a href="/">${SITE_NAME}</a> · <a href="${NETWORK_URL}">OpenVibe</a></p>`, '</body>', '</html>'].join('\n');
}

module.exports = { renderHome, renderUpdates, renderNotFound, HOME_CSP, DESCRIPTION, sections };
