-- phase: expand
-- The claim walks a subscription's pending deliveries in (priority, seq) order and takes the first claimable one
-- (store.js claimDeliveries / next). With only (status, next_attempt_at) and (subscription_id, status, seq) to use, it
-- read every pending row of the subscription and sorted them: 265 ms a claim with 74,000 pending for Search on
-- 2026-11-02 (one PostgreSQL core at 100%, ~190 deliveries a minute). This index lets the claim stop at the first
-- row: 0.4 ms. Created CONCURRENTLY on production the same day; IF NOT EXISTS makes this a no-op there.
CREATE INDEX IF NOT EXISTS idx_deliveries_claim ON deliveries (subscription_id, priority, seq) WHERE status IN ('pending', 'failed');
