'use strict';
// The home on a phone (the owner's short-pages rule, 2026-10-30): the blocks of the same kind are one pick block,
// a row of chips picks one of them, and the parts a reader rarely needs are folds. Radio inputs and :has(), no
// script for the chips; every block stays in the page for crawlers; a wider screen shows them all and no chips.
const assert = require('assert');
const { renderHome } = require('../server/home');

const last = Date.parse('2026-10-29T09:41:00Z');
const STATS = {
    day: 4, week: 4, types: 2, services: [{ ns: 'live', n: 3 }, { ns: 'blog', n: 1 }], subscriptions: 0, consumers: 0,
    delivered: 0, dead: 0, measuredAt: last,
    public: [
        { type: 'live.stream.started', n: 3, last },
        { type: 'blog.post.published', n: 1, last: 0 },
    ],
};

const t = (name, fn) => { try { fn(); console.log(`ok   ${name}`); } catch (e) { console.error(`FAIL ${name}: ${e.message}`); process.exitCode = 1; } };

t('the public events are one list: no chip row, every card in the page', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    assert.ok(!page.includes('pick-events'), 'the event pick block is gone');
    const cards = [...page.matchAll(/<li class="hm-event" id="event-([a-z0-9-]+)" data-cat="(\d+)">/g)];
    assert.strictEqual(cards.length, 2, `every event card is in the page (${cards.length})`);
});

t('what you get is one list too: no chip row, every card in the page', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    assert.ok(!page.includes('pick-features'), 'the feature pick block is gone');
    assert.strictEqual((page.match(/class="sc-card"/g) || []).length, 6, 'every card is in the page');
});

t('nothing hides an event, a feature, a bar or a fold on a phone', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    assert.ok(!page.includes('.pick-row{display:none}'), 'no chip row is styled');
    assert.ok(!page.includes('.pick-pane{display:none}'), 'no block is hidden by a pick rule');
    assert.ok(!page.includes('.pick-events .hm-events>li{display:none}'), 'no event card is hidden by a category');
    assert.ok(!page.includes('nth-child(n+11)'), 'no bar is hidden');
});

t('the parts a reader rarely needs are folds that start open on every screen', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    for (const id of ['who', 'howto', 'faq']) {
        assert.ok(page.includes(`<details class="hm-fold hm-fold-block" id="${id}" open>`), `${id} is a fold, open in the page`);
        assert.ok(!page.includes(`document.getElementById("${id}")`), `${id} is never closed by script`);
    }
    assert.match(page, /<h2 id="h-who">Who published this week<\/h2>/);
    assert.match(page, /<h2 id="h-faq">Questions<\/h2>/);
    assert.match(page, /What is OpenVibe\.Events\?/, 'the questions are still in the page');
});
