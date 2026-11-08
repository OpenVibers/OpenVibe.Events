'use strict';
// The delivery counts by status (store.deliveryCounts) walk every delivery, and the metrics, readiness, home and status
// API asked for them every few seconds (414k calls in 35 days on production, 289 minutes of database time). Callers at
// the same moment share one query; with countsCacheMs (production: 60 s) its answer is kept; fresh: true counts again;
// without it every call counts, so a test reads its own writes.
const assert = require('assert');
const { boot, suite } = require('./helpers');
const { createStore } = require('../server/store');

const t = suite('delivery-counts');
let h;

t('boot', async () => { h = await boot(); });

t('one query for callers at the same moment, kept for countsCacheMs, fresh on demand', async () => {
    let queries = 0;
    const db = new Proxy(h.db, {
        get(target, key) {
            if (key === 'prepare') {
                return (sql) => {
                    const st = target.prepare(sql);
                    if (!/FILTER \(WHERE status/.test(sql)) return st;
                    const counted = (read) => async (...a) => { queries += 1; return read(...a); };
                    return { ...st, all: counted(st.all), get: counted(st.get) };
                };
            }
            const v = target[key];
            return typeof v === 'function' ? v.bind(target) : v;
        },
    });
    const kept = createStore(db, { clock: h.clock, countsCacheMs: 60_000 });
    const [a, b, c] = await Promise.all([kept.deliveryCounts(), kept.deliveryCounts(), kept.deliveryCounts()]);
    assert.strictEqual(queries, 1, 'three callers at once share one query');
    assert.deepStrictEqual(a, b);
    assert.deepStrictEqual(b, c);
    a.dead = 999;
    assert.notStrictEqual((await kept.deliveryCounts()).dead, 999, 'each caller gets its own copy');
    assert.strictEqual(queries, 1, 'within the window: kept');
    await kept.deliveryCounts({ fresh: true });
    assert.strictEqual(queries, 2, 'fresh: counted again');
    h.clock.advance(59_000);
    assert.strictEqual((await kept.deliveryCounts()).pending, 0, 'still inside the 60 s window');
    assert.strictEqual(queries, 2, 'within the 60 s window: kept');
    h.clock.advance(1_000);
    await kept.deliveryCounts();
    assert.strictEqual(queries, 3, 'past the 60 s window: counted again');
    const every = createStore(db, { clock: h.clock });
    await every.deliveryCounts();
    await every.deliveryCounts();
    assert.strictEqual(queries, 5, 'without countsCacheMs every call counts');
    assert.deepStrictEqual(Object.keys(await every.deliveryCounts()).sort(), ['dead', 'delivered', 'failed', 'pending']);
});

t('the setting: off outside production unless EVENTS_COUNTS_CACHE_MS is set', async () => {
    const { load: loadConfig } = require('../server/config');
    const base = { DATABASE_URL: 'postgres://x/y', OV_OAUTH_CLIENT_SECRET: 'x', EVENTS_SECRETS: 'x'.repeat(32) };
    assert.strictEqual(loadConfig({ ...base, NODE_ENV: 'test' }).countsCacheMs, 0);
    assert.strictEqual(loadConfig({ ...base, NODE_ENV: 'test', EVENTS_COUNTS_CACHE_MS: '5000' }).countsCacheMs, 5000);
    assert.strictEqual(loadConfig({ ...base, NODE_ENV: 'production' }).countsCacheMs, 60_000, 'production keeps the counts 60 s');
});

t('close', async () => { await h.close(); });

t.run();
