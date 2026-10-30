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

t('the public events are one pick block: a chip per category, the first picked, every card in the page', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    const block = page.slice(page.indexOf('<div class="pick pick-events"'), page.indexOf('</section>', page.indexOf('<div class="pick pick-events"')));
    assert.ok(block.startsWith('<div class="pick pick-events"'), 'the pick block is on the home');
    assert.match(block, /<div class="pick-row" role="radiogroup" aria-label="Public events this week">/);
    // One chip per category, in the order the categories appear.
    const chips = [...block.matchAll(/<input type="radio" name="pick-events" id="pk-events-(\d+)" value="\1"( checked)?><label for="pk-events-\1">([^<]+)<\/label>/g)];
    assert.strictEqual(chips.length, 2, `a chip per category (${chips.length})`);
    assert.deepStrictEqual(chips.map((m) => m[1]), ['0', '1'], 'the chips are in order');
    assert.deepStrictEqual(chips.filter((m) => m[2]).map((m) => m[1]), ['0'], 'the first category is picked');
    assert.ok(chips[0][3].includes('OpenVibe.Live'), `the chip names the category (${chips[0][3]})`);
    // Every card stays in the page: the crawler still reads them all.
    const cards = [...block.matchAll(/<li class="hm-event" id="event-([a-z0-9-]+)" data-cat="(\d+)">/g)];
    assert.strictEqual(cards.length, 2, `every event card is in the page (${cards.length})`);
    assert.deepStrictEqual(cards.map((m) => m[2]), ['0', '1'], 'each card knows its category');
});

t('what you get is one pick block too: six chips, the first picked, every card in the page', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    const block = page.slice(page.indexOf('<div class="pick" id="pick-features">'), page.indexOf('</section>', page.indexOf('<div class="pick" id="pick-features">')));
    const chips = [...block.matchAll(/<input type="radio" name="pick-features" id="pk-features-(\d+)" value="\1"( checked)?><label for="pk-features-\1">([^<]+)<\/label>/g)];
    assert.strictEqual(chips.length, 6, `a chip per feature (${chips.length})`);
    assert.deepStrictEqual(chips.filter((m) => m[2]).map((m) => m[1]), ['0'], 'the first feature is picked');
    assert.strictEqual((block.match(/class="sc-card"/g) || []).length, 6, 'every card is in the page');
    // The CSS that shows the picked block, one rule per block.
    for (let i = 0; i < 6; i++) {
        assert.ok(page.includes(`.pick:has(#pk-features-${i}:checked) #pp-features-${i}{display:block}`), `feature ${i} shows when its chip is picked`);
    }
});

t('the CSS guard: the chips are hidden and the blocks all show until 640 px and where :has() is understood', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    assert.ok(page.includes('.pick-row{display:none}'), 'the chips are hidden by default');
    assert.ok(page.includes('@supports selector(:has(*)){@media (max-width:640px){'), 'the phone rules are under :has() and 640 px');
    assert.ok(page.includes('.pick-pane{display:none}'), 'the blocks are hidden on a phone');
    assert.match(page, /\.pick-row label\{[^}]*min-height:44px/, 'a chip is a 44 px target');
    assert.ok(page.includes('.pick-events .hm-events>li{display:none}'), 'only the picked category shows on a phone');
    assert.ok(page.includes('.pick-events:has(#pk-events-0:checked) .hm-events>li[data-cat="0"]{display:block}'), 'the picked category shows');
});

t('the parts a reader rarely needs are folds, open in the page and closed on a phone', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    for (const id of ['who', 'howto', 'faq']) {
        assert.ok(page.includes(`<details class="hm-fold hm-fold-block" id="${id}" open>`), `${id} is a fold, open in the page`);
        assert.ok(page.includes(`document.getElementById("${id}")`), `${id} has the script that closes it on a phone`);
    }
    // The content of the folds stays in the page.
    assert.match(page, /<h2 id="h-who">Who published this week<\/h2>/);
    assert.match(page, /<h2 id="h-faq">Questions<\/h2>/);
    assert.match(page, /What is OpenVibe\.Events\?/, 'the questions are still in the page');
});
