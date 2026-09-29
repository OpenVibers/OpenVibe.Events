-- phase: expand
-- ADR-042 decisions 1 and 4 (plan T7): the delivery policy is the semantic vocabulary for how an event type is
-- carried; delivery_policies holds one row per event-type pattern (the same matcher subscriptions use). The carrier
-- class (TOPIC/QUEUE/STREAM) is derived, never stored and never in an envelope. ordering_key already exists (0002);
-- a partial index makes "the head of this key" cheap. max_inflight lets a subscription keep several keys in flight
-- (default 1 = today: one delivery in flight per subscription); never two of one key.
CREATE TABLE delivery_policies (
    pattern    text COLLATE "C" PRIMARY KEY,
    policy     jsonb NOT NULL,
    revision   integer NOT NULL DEFAULT 1,
    updated_at bigint NOT NULL,
    updated_by text COLLATE "C"
);
ALTER TABLE subscriptions ADD COLUMN max_inflight integer NOT NULL DEFAULT 1 CHECK (max_inflight BETWEEN 1 AND 16);
CREATE INDEX idx_deliveries_ordering ON deliveries(subscription_id, ordering_key, seq)
    WHERE ordering_key IS NOT NULL AND status IN ('pending', 'failed');
