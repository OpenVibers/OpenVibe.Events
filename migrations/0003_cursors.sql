-- phase: expand
-- ADR-042 decision 7: opaque cursors beside the global seq. A cursor names a position in the hot store — the global
-- seq — together with the retention epoch the position belongs to. The epoch changes only when the hot store is
-- truncated or re-imported (store.bumpEpoch), so a cursor from another epoch is answered with the existing gap shape
-- instead of silently restarting a consumer. Checkpoints keep the numeric position in `cursor` and gain the epoch and
-- carrier of the position they name.
CREATE TABLE store_epoch (
    name  text COLLATE "C" PRIMARY KEY,
    value bigint NOT NULL
);
INSERT INTO store_epoch (name, value) VALUES ('events', 0) ON CONFLICT DO NOTHING;

ALTER TABLE consumer_checkpoints ADD COLUMN epoch bigint NOT NULL DEFAULT 0;
ALTER TABLE consumer_checkpoints ADD COLUMN carrier text COLLATE "C";
