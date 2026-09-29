-- phase: expand
-- ADR-042 decision 3: a delivery is claimed with a database lease, so more than one worker can run without sending the
-- same delivery twice. lease_owner/lease_until mark the one in flight (at most one per subscription: the claim locks the
-- subscription row, FOR UPDATE SKIP LOCKED); an expired lease is claimable again (at least once). ordering_key and
-- carrier are filled by the planner (ADR-042 decisions 4-5); until then they stay NULL.
ALTER TABLE deliveries ADD COLUMN lease_owner  text COLLATE "C";
ALTER TABLE deliveries ADD COLUMN lease_until  bigint;
ALTER TABLE deliveries ADD COLUMN ordering_key text COLLATE "C";
ALTER TABLE deliveries ADD COLUMN carrier      text COLLATE "C";
CREATE INDEX idx_deliveries_lease ON deliveries(subscription_id, lease_until) WHERE lease_until IS NOT NULL;
