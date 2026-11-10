'use strict';
// The home's picture is the bus's own week: who published, how much, and how many subscriptions received it (the
// owner, 2026-11-09: no more "generic circled" homes). Real counts only: with fewer than two publishers there is no
// picture, and the subscriptions line is there only when there are subscriptions.
const assert = require('assert');
const { renderHome } = require('../server/home');

const at = Date.parse('2026-11-09T09:41:00Z');
const base = { day: 10, week: 24958, types: 4, subscriptions: 61, consumers: 14, delivered: 100, dead: 0, measuredAt: at, public: [{ type: 'live.stream.started', n: 3, last: at }] };
const render = (stats) => String(renderHome({ siteUrl: 'https://openvibe.events', apiUrl: 'https://openvibe.events', stats }));
const t = (name, fn) => { try { fn(); console.log(`ok   ${name}`); } catch (e) { console.error(`FAIL ${name}: ${e.message}`); process.exitCode = 1; } };

t('the picture is the week\'s publishers, the bus and the subscriptions, from the counts given', () => {
    const page = render({ ...base, services: [{ ns: 'live', n: 12480 }, { ns: 'chat', n: 9301 }, { ns: 'network', n: 2110 }, { ns: 'wiki', n: 801 }, { ns: 'blog', n: 190 }, { ns: 'app', n: 5000 }] });
    const scene = (page.match(/<figure class="ev-scene"[\s\S]*?<\/figure>/) || [''])[0];
    assert.ok(scene, 'the picture is in the hero');
    const rows = Array.from(scene.matchAll(/<li><span>([^<]+)<\/span><i style="--w:(\d+)%"><\/i><b>([^<]+)<\/b><\/li>/g)).map((m) => [m[1], Number(m[2]), m[3]]);
    assert.deepStrictEqual(rows, [['Live', 100, '12,480'], ['Chat', 75, '9,301'], ['Network', 17, '2,110'], ['Wiki', 6, '801']], 'the four that published most, each bar its share of the largest');
    assert.match(scene, /and 1 more service</, 'the rest are counted (developer apps are not a service)');
    assert.match(scene, /<p class="ev-scene-bus"><b>The bus<\/b><span>24,958 events this week<\/span><\/p>/);
    assert.match(scene, /<p class="ev-scene-subs"><b>61<\/b> subscriptions receive them, signed and retried<\/p>/);
    assert.ok(!/sc-orbit/.test(page), 'the generic orbit is gone');
    assert.match(page, /@media \(prefers-reduced-motion:reduce\)\{\.ev-scene-pubs i\{animation:none\}\.ev-scene-flow i\{display:none\}\}/, 'still under reduced motion');
});

t('no subscriptions: that line is left out, not shown as zero', () => {
    const page = render({ ...base, subscriptions: 0, services: [{ ns: 'live', n: 3 }, { ns: 'blog', n: 1 }] });
    assert.ok(page.includes('class="ev-scene"'));
    assert.ok(!page.includes('<p class="ev-scene-subs">'), 'no line about subscriptions');
});

t('fewer than two publishers: no picture at all', () => {
    assert.ok(!render({ ...base, services: [{ ns: 'live', n: 3 }] }).includes('class="ev-scene"'));
    assert.ok(!render({ ...base, services: [] }).includes('class="ev-scene"'));
});
