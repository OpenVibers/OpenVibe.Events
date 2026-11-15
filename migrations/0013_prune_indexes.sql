-- phase: expand
-- Retention (store.js prune) deletes old sandbox events with `env = 'sandbox' AND received_at < ?`. With only
-- idx_events_received to use, PostgreSQL walked a month of production events to find the handful from the sandbox:
-- 14 s on 2026-11-15 with 3.2 million rows (7 of them sandbox), run before the server listened, so a deploy's new
-- release missed its 30 s readiness window and was rolled back. This partial index holds only sandbox rows. Created
-- CONCURRENTLY on production the same day; IF NOT EXISTS makes this a no-op there.
CREATE INDEX IF NOT EXISTS idx_events_sandbox_received ON events (received_at) WHERE env = 'sandbox';
