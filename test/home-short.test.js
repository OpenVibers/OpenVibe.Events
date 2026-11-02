'use strict';
// The home on a phone: the blocks of the same kind are all shown, and a row of chips jumps to one (the owner,
// 2026-10-31: never hide content to shorten); the parts a reader rarely needs are folds. A wider screen shows no chips.
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

t('the public events: every card shown, a chip per category that jumps to its first card', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    const block = page.slice(page.indexOf('<div class="pick pick-events"'), page.indexOf('</section>', page.indexOf('<div class="pick pick-events"')));
    assert.ok(block.startsWith('<div class="pick pick-events"'), 'the block is on the home');
    assert.match(block, /<nav class="pick-row" aria-label="Public events this week">/);
    const chips = [...block.matchAll(/<a class="pick-chip" href="#(event-[a-z0-9-]+)">([^<]+)<\/a>/g)];
    assert.strictEqual(chips.length, 2, `a chip per category (${chips.length})`);
    assert.ok(chips[0][2].includes('OpenVibe.Live'), `the chip names the category (${chips[0][2]})`);
    for (const [, id] of chips) assert.ok(block.includes(`<li class="hm-event" id="${id}"`), `${id} is a card in the page`);
    assert.ok(!/type="radio"/.test(page), 'no chip is a radio: nothing is picked');
    const cards = [...block.matchAll(/<li class="hm-event" id="event-([a-z0-9-]+)" data-cat="(\d+)">/g)];
    assert.strictEqual(cards.length, 2, `every event card is in the page (${cards.length})`);
});

t('what you get: six cards shown, a chip per card that jumps to it', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    const block = page.slice(page.indexOf('<div class="pick" id="pick-features">'), page.indexOf('</section>', page.indexOf('<div class="pick" id="pick-features">')));
    const chips = [...block.matchAll(/<a class="pick-chip" href="#(pp-features-\d+)">([^<]+)<\/a>/g)];
    assert.strictEqual(chips.length, 6, `a chip per feature (${chips.length})`);
    for (const [, id] of chips) assert.ok(block.includes(`<div class="pick-pane" id="${id}">`), `${id} is in the page`);
    assert.strictEqual((block.match(/class="sc-card"/g) || []).length, 6, 'every card is in the page');
});

t('the CSS: no chips where there is room, link chips on a phone, and nothing hidden', () => {
    const page = renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats: STATS });
    assert.ok(page.includes('.pick-row{display:none}'), 'no chips where there is room');
    assert.match(page, /\.pick-chip\{[^}]*min-height:44px/, 'a chip is a 44 px target');
    assert.ok(!page.includes('.pick-pane{display:none}') && !page.includes('.hm-events>li{display:none}'), 'no block or card is hidden');
    assert.ok(!/nth-child\(n\+11\)\{display:none\}/.test(page), 'no list is cut short on a phone');
    assert.ok(!page.includes(':checked'), 'no picker rules');
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
