-- phase: expand
-- ADR-042 decision 8: three tiers. hot = `events` (pruned at retention.hot, EVENTS_RETENTION_DAYS); replay =
-- `events_archive` in the same database, pruned at retention.replay (EVENTS_REPLAY_RETENTION_DAYS); archive = monthly
-- NDJSON objects outside the database (scripts/events-archive.js, operator only). The prune MOVES a hot event here in
-- the transaction that deletes it, so a position is always in exactly one tier. No delivery rows follow it.
--
-- A row keeps, as columns, what the read path filters and orders on: identity, topic, the cursor position (seq in the
-- epoch it was moved in: the same pair server/cursor.js encodes), owner and visibility, the redaction markers and
-- received_at. Everything else of the event row (actor, payload, trace, …) is one gzip'd JSON object in `body`
-- (server/archive.js), restored to the hot row's exact column values on read. Additive only: nothing in `events` changes.
CREATE TABLE events_archive (
    id           text COLLATE "C" PRIMARY KEY,
    seq          bigint NOT NULL,
    epoch        bigint NOT NULL,
    event_type   text COLLATE "C" NOT NULL,
    source       text COLLATE "C" NOT NULL,
    subject_type text COLLATE "C" NOT NULL,
    subject_id   text COLLATE "C" NOT NULL,
    visibility   text COLLATE "C" NOT NULL,
    project_id   text COLLATE "C",
    env          text COLLATE "C" NOT NULL DEFAULT 'production',
    received_at  bigint NOT NULL,
    redacted_at  bigint,
    redacts      bigint NOT NULL DEFAULT 0,
    body         bytea NOT NULL,
    archived_at  bigint NOT NULL,
    -- An operator restore (scripts/events-archive.js restore) keeps the rows it brings back past retention.replay
    -- until this time (ms); NULL for rows the prune moved.
    hold_until   bigint
);
-- Pull and scan: seq order within the current epoch (also the one position per epoch: a re-imported store whose seq
-- collides with an archived row of the same epoch makes the move fail instead of silently losing the hot row).
CREATE UNIQUE INDEX idx_events_archive_position ON events_archive(epoch, seq);
CREATE INDEX idx_events_archive_type_seq ON events_archive(event_type, seq);
-- Replay pruning and the monthly export.
CREATE INDEX idx_events_archive_received ON events_archive(received_at);
-- Redaction by subject reaches archived rows too.
CREATE INDEX idx_events_archive_subject ON events_archive(source, subject_type, subject_id);
