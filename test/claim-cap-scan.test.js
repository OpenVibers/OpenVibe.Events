'use strict';
/**
 * A claim pass stops when the subscription's inflight cap is reached.
 *
 * The old loop asked for the next delivery once more after the last slot was taken. That lookup could never
 * succeed (its cap filter is false for every row) yet the planner walked every pending row of the subscription
 * to prove it. On production on 2026-11-08 it was half of the 111,300 claim lookups and 442 M index tuples a
 * day (pg_stat_statements: 357 s of DB time, ~794 index tuples per lookup). This test proves the pass does not
 * make that second lookup, and what the lookup costs when the cap is reached (EXPLAIN ANALYZE on PGlite).
 *
 *   node test/claim-cap-scan.test.js
 */
const assert = require('assert');
const { boot, request, serviceToken, envelope, subscriber, suite } = require('./helpers');

const t = suite('claim-cap-scan');
let h;

t('boot', async () => { h = await boot(); });

t('a full subscription is not looked up again; the lookup it used to make walks the whole backlog', async () => {
    const stub = await subscriber(() => 204);
    const sub = await request(h.base, 'POST', '/api/v1/subscriptions', {
        token: serviceToken('wide', ['events.subscription.manage']),
        body: { topic_pattern: 'live.cap.*', endpoint: stub.url },
    });
    assert.strictEqual(sub.status, 201, sub.text);
    const subscriptionId = sub.body.id;
    for (let i = 0; i < 60; i++) {
        const r = await request(h.base, 'POST', '/api/v1/events', {
            token: serviceToken('live', ['events.event.publish']),
            body: envelope('live', { event_type: 'live.cap.x' }),
        });
        assert.ok(r.status === 201 || r.status === 200, r.text);
    }

    // Capture the per-subscription claim lookup (`next` in store.js) the pass prepares.
    const lookups = [];
    const prepare = h.db.prepare.bind(h.db);
    h.db.prepare = (text) => {
        const q = prepare(text);
        if (/FROM deliveries d JOIN subscriptions s/.test(text) && /ORDER BY d.priority, d.seq LIMIT 1/.test(text)) lookups.push(q.source);
        return q;
    };
    const now = h.clock.now();
    let claimed;
    try {
        claimed = (await h.store.claimDeliveries(now, 1, { owner: 'worker-a', leaseUntil: now + 60_000 })).filter((d) => d.subscription_id === subscriptionId);
    } finally {
        h.db.prepare = prepare;
    }
    assert.strictEqual(claimed.length, 1, 'max_inflight 1: one delivery');
    assert.strictEqual(lookups.length, 1, 'one lookup — the claim itself, not a second one to discover the cap');

    // What the second lookup cost: with the cap reached, every one of the other 59 pending rows is filtered out.
    await h.db.query('SET enable_seqscan = off');
    const plan = (await h.db.many(`EXPLAIN (ANALYZE, BUFFERS) ${lookups[0]}`, [subscriptionId, now, now, now, now]))
        .map((r) => Object.values(r)[0]).join('\n');
    assert.match(plan, /idx_deliveries_claim/, 'the claim index is the plan');
    // With the cap reached the cap filter removes every row the index returned: the query had to walk them all.
    const removed = Math.max(0, ...[...plan.matchAll(/Rows Removed by (?:Join )?Filter: (\d+)/g)].map((m) => Number(m[1])));
    assert.ok(removed >= 50, `the cap-reached lookup walks the whole pending backlog (removed ${removed})`);
    await stub.close();
});

t('close', async () => { await h.close(); });

t.run();
