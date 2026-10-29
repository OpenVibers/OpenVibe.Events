'use strict';
// The home page's live sections (server/home-stats.js, server/home.js): counts and names read from Events' own tables,
// the public events with when each last arrived, never a payload, internal types never named, a clear empty state on
// an empty or failed bus, and a FAQPage that holds exactly the questions the page shows.
const assert = require('assert');
const { boot, request, serviceToken, envelope, suite } = require('./helpers');
const { createHomeStats } = require('../server/home-stats');

const t = suite('home-stats');
let h;
const html = async () => (await request(h.base, 'GET', '/', { headers: { Accept: 'text/html' } })).text;
const section = (page, id) => (new RegExp(`<section class="sc-sec"[^>]*aria-labelledby="${id}"[\\s\\S]*?</section>`).exec(page) || [''])[0];

t('boot', async () => { h = await boot(); });

t('an empty bus: the counts are left out, the public event list says so, the rest of the page stays', async () => {
    const page = await html();
    assert.ok(!page.includes('aria-labelledby="h-who"'));
    assert.ok(!page.includes('class="hm-stats"'), 'no tiles of zeros');
    assert.match(page, /id="h-events">Public events this week<\/h2><p class="sc-lede">No public events have arrived this week\./);
    assert.match(page, /Events your services/);
    assert.match(page, /id="h-faq">Questions</);
});

t('published events show as counts by service, public types and the latest public events, never a payload', async () => {
    const live = serviceToken('live', ['events.event.publish']);
    const blog = serviceToken('blog', ['events.event.publish']);
    const post = (token, body) => request(h.base, 'POST', '/api/v1/events', { token, body });
    for (const ev of [
        envelope('live', { event_type: 'live.stream.started', visibility: 'public', payload: { title: 'SECRET-TITLE' } }),
        envelope('live', { event_type: 'live.stream.started', visibility: 'public' }),
        envelope('live', { event_type: 'live.internal.tick', visibility: 'internal' }),
    ]) assert.ok([200, 201, 202].includes((await post(live, ev)).status));
    assert.ok([200, 201, 202].includes((await post(blog, envelope('blog', { event_type: 'blog.post.published', visibility: 'public', subject: { type: 'post', id: 'p1', revision: 1 } }))).status));

    // A fresh reader (the app's own keeps its answer five minutes).
    const stats = await createHomeStats({ store: h.store, log: { warn() {} } }).read();
    assert.strictEqual(stats.week, 4);
    assert.deepStrictEqual(stats.services, [{ ns: 'live', n: 3 }, { ns: 'blog', n: 1 }]);
    assert.deepStrictEqual(stats.public.map((r) => [r.type, r.n]), [['live.stream.started', 2], ['blog.post.published', 1]]);
    assert.strictEqual(JSON.stringify(stats).includes('SECRET-TITLE'), false, 'no payload leaves the reader');
    assert.strictEqual(JSON.stringify(stats.public).includes('live.internal.tick'), false, 'an internal type is never named');

    const { renderHome } = require('../server/home');
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats });
    const events = section(page, 'h-events');
    assert.match(section(page, 'h-who'), /<a href="https:\/\/openvibe\.live">OpenVibe\.Live<\/a>/);
    assert.match(events, /A channel went live on OpenVibe\.Live/);
    assert.match(events, /<time datetime="\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z">/);
    assert.match(events, /Online, from <a href="https:\/\/openvibe\.live">OpenVibe\.Live<\/a> · 2 this week/);
    assert.match(events, /<code>live\.stream\.started<\/code>/);
    assert.match(events, /A post was published on OpenVibe\.Blog/);
    assert.match(events, /<code>blog\.post\.published<\/code>/);
    assert.ok(!page.includes('SECRET-TITLE'));
    assert.ok(!page.includes('live.internal.tick'));
    assert.match(page, /<b>4<\/b><span>events this week<\/span>/);
    assert.match(page, / 2 OpenVibe services published to it this week\./);
    const ld = [...page.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map((m) => JSON.parse(m[1]));
    const list = ld.find((x) => x['@type'] === 'ItemList');
    assert.ok(list, 'the public events are an ItemList');
    assert.strictEqual(list.numberOfItems, 2);
    assert.ok(list.itemListElement.every((x) => x.item['@type'] === 'Event' && x.item.startDate));
    assert.ok(!JSON.stringify(list).includes('SECRET-TITLE'));
});

t('JSON-LD: WebSite, the app, and a FAQPage with exactly the questions shown', async () => {
    const page = await html();
    const ld = [...page.matchAll(/<script type="application\/ld\+json">([\s\S]*?)<\/script>/g)].map((m) => JSON.parse(m[1]));
    for (const type of ['WebSite', 'WebApplication', 'FAQPage']) assert.ok(ld.some((x) => x['@type'] === type), type);
    const faq = (/<section class="sc-sec" id="faq"[\s\S]*?<\/section>/.exec(page) || [''])[0];
    const shown = [...faq.matchAll(/<h3>([^<]+)<\/h3>/g)].map((m) => m[1].replace(/&#39;/g, "'"));
    assert.ok(shown.length >= 6);
    assert.deepStrictEqual(ld.find((x) => x['@type'] === 'FAQPage').mainEntity.map((q) => q.name), shown);
    const description = /<meta name="description" content="([^"]*)"/.exec(page)[1];
    assert.ok(description.length <= 160, `description ${description.length} chars`);
});

t('a stalled store resolves to no stats within the 1.5 s bound, so the page falls back to its empty state', async () => {
    const pending = () => new Promise(() => {});
    const stalled = {
        db: { prepare: () => ({ get: pending, all: pending }) },
        deliveryCounts: pending,
    };
    const started = Date.now();
    const stats = await createHomeStats({ store: stalled, log: { warn() {} } }).read();
    const elapsed = Date.now() - started;
    assert.strictEqual(stats, null);
    assert.ok(elapsed >= 1400 && elapsed < 2500, `resolved in ${elapsed} ms`);
});

t('stop', async () => { await h.stop(); });

t.run();
