-- phase: expand
-- GitHub webhook ingress (AL5: a ticket follows its pull request). One row per GitHub delivery GUID, holding the
-- event ids it stored, so a redelivered webhook answers them and stores no second event. The normalized github.*
-- events live in the hot store like any other event; this table is only the ingress's idempotency record. One row
-- per delivery (not per event) because a delivery can name several pull requests and they must be claimed together.
-- Nothing here holds a secret.
CREATE TABLE IF NOT EXISTS github_deliveries (
    delivery_id text COLLATE "C" PRIMARY KEY,               -- X-GitHub-Delivery
    event_ids   jsonb NOT NULL,                             -- ["evt_…", …] stored for it
    event_types jsonb NOT NULL,                             -- ["github.check.completed", …] in the same order
    received_at timestamptz NOT NULL DEFAULT now()
);
