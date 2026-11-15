'use strict';
/**
 * Retention never holds up a start. On 2026-11-15 the first prune ran before the server listened, its sandbox delete
 * took 14 s on 3.2 million rows, and a deploy's new release missed its 30 s readiness window and was rolled back. Now
 * the server answers while the first prune is still running, the sandbox delete has an index of its own (migration
 * 0013) and the archive batch walks idx_events_received.
 *
 *   node test/prune-after-listen.test.js
 */
const assert = require('assert');
const { testDb } = require('./db');
const { boot, request } = require('./helpers');

(async () => {
    const d = await testDb();
    try {
        const idx = await d.db.prepare("SELECT indexdef FROM pg_indexes WHERE tablename = 'events' AND indexname = 'idx_events_sandbox_received'").get();
        assert.ok(idx, 'the sandbox retention index exists after the migrations');
        assert.match(idx.indexdef, /\(received_at\) WHERE \(env = 'sandbox'::text\)/);

        // A prune whose deletes wait until the test lets them go: the server must answer meanwhile.
        let release;
        const gate = new Promise((resolve) => { release = resolve; });
        let held = 0;
        const db = new Proxy(d.db, {
            get(t, k) {
                if (k === 'prepare') {
                    return (sql) => {
                        const st = t.prepare(sql);
                        if (!/^DELETE FROM events WHERE/.test(sql)) return st;
                        return {
                            run: async (...a) => { held++; await gate; return st.run(...a); },
                            get: (...a) => st.get(...a), all: (...a) => st.all(...a),
                        };
                    };
                }
                const v = t[k];
                return typeof v === 'function' ? v.bind(t) : v;
            },
        });
        const h = await boot({ worker: 'on', db });
        try {
            assert.strictEqual(held, 1, 'the first prune started and is waiting on its delete');
            const r = await request(h.base, 'GET', '/api/health');
            assert.strictEqual(r.status, 200, 'the server answers while the first prune runs');
            release();
            await h.firstPrune;
        } finally {
            release();
            await h.stop();
        }
    } finally {
        await d.close();
    }
    console.log('prune after listen: the server answers first; the sandbox delete has its index');
})().catch((err) => { console.error(err); process.exit(1); });
