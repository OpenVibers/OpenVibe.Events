-- phase: expand
-- ADR-042 decision 5 (plan T7): the planner records a (carrier class, ordering key) decision in delivery_placements
-- only when it CHANGES, so a past delivery can be explained from history (GET /api/v1/placement). It is advisory
-- history, pruned with the events retention; deliveries themselves carry the chosen carrier in deliveries.carrier
-- (0002). ordering_key is nullable: a policy without ordering still gets a decision (per class, no key).
CREATE TABLE delivery_placements (
    id            bigserial PRIMARY KEY,
    carrier_class text NOT NULL,
    ordering_key  text,
    carrier       text NOT NULL,
    result        jsonb NOT NULL,
    decided_at    bigint NOT NULL
);
CREATE INDEX idx_delivery_placements_lookup ON delivery_placements(carrier_class, ordering_key, decided_at DESC);
