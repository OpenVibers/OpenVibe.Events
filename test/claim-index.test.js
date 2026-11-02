'use strict';
/**
 * The claim walks a subscription's pending deliveries in (priority, seq) order and stops at the first claimable one
 * (migration 0011, idx_deliveries_claim). Without it, a claim read and sorted every pending row of the subscription:
 * 265 ms each with 74,000 pending for Search on 2026-11-02.
 *
 *   node test/claim-index.test.js
 */
const assert = require('assert');
const { testDb } = require('./db');

(async () => {
    const d = await testDb();
    try {
        const idx = await d.db.prepare("SELECT indexdef FROM pg_indexes WHERE tablename = 'deliveries' AND indexname = 'idx_deliveries_claim'").get();
        assert.ok(idx, 'the claim index exists after the migrations');
        assert.match(idx.indexdef, /\(subscription_id, priority, seq\)/);
        assert.match(idx.indexdef, /WHERE \(status = ANY \(ARRAY\['pending'::text, 'failed'::text\]\)\)/);
    } finally {
        await d.close();
    }
    console.log('claim index: all checks passed');
})().catch((err) => { console.error(err); process.exit(1); });
