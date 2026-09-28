-- phase: expand
-- OpenVibe.Events on PostgreSQL (ADR-035, roadmap WS-X2): the tables as they were on SQLite (converted by openvibe-sdk
-- tools/asyncify/sqlite-schema-to-pg: text COLLATE "C" compares like SQLite, integers are bigint, identities keep their ids),
-- then the openvibe-publishing stores and the openvibe-sdk inbox and outbox. Generated once on 2026-09-28; never edited after it runs.

CREATE TABLE sequences (
    name  text COLLATE "C" PRIMARY KEY,
    value bigint NOT NULL
);
CREATE TABLE events (
    id               text COLLATE "C" PRIMARY KEY,
    seq              bigint NOT NULL UNIQUE,
    event_type       text COLLATE "C" NOT NULL,
    version          bigint NOT NULL,
    source           text COLLATE "C" NOT NULL,
    actor            text COLLATE "C" NOT NULL,
    subject_type     text COLLATE "C" NOT NULL,
    subject_id       text COLLATE "C" NOT NULL,
    subject_revision bigint,
    trace_id         text COLLATE "C" NOT NULL,
    priority         text COLLATE "C" NOT NULL,
    visibility       text COLLATE "C" NOT NULL,
    occurred_at      text COLLATE "C" NOT NULL,
    received_at      bigint NOT NULL,
    payload          text COLLATE "C" NOT NULL,
    hops             bigint NOT NULL DEFAULT 1,
    publisher        text COLLATE "C" NOT NULL,
    request_id       text COLLATE "C"
);
CREATE INDEX idx_events_trace ON events(trace_id);
CREATE INDEX idx_events_received ON events(received_at);
CREATE INDEX idx_events_type_seq ON events(event_type, seq);

CREATE TABLE subscriptions (
    id            text COLLATE "C" PRIMARY KEY,
    consumer      text COLLATE "C" NOT NULL,
    topic_pattern text COLLATE "C" NOT NULL,
    endpoint      text COLLATE "C" NOT NULL,
    secret        text COLLATE "C" NOT NULL,
    enabled       bigint NOT NULL DEFAULT 1,
    retry_policy  text COLLATE "C",
    created_at    bigint NOT NULL,
    updated_at    bigint NOT NULL
);
CREATE INDEX idx_subscriptions_consumer ON subscriptions(consumer);

CREATE TABLE deliveries (
    event_id        text COLLATE "C" NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    subscription_id text COLLATE "C" NOT NULL REFERENCES subscriptions(id) ON DELETE CASCADE,
    seq             bigint NOT NULL,
    priority        bigint NOT NULL,
    attempt         bigint NOT NULL DEFAULT 0,
    status          text COLLATE "C" NOT NULL CHECK (status IN ('pending', 'delivered', 'failed', 'dead')),
    next_attempt_at bigint,
    last_error      text COLLATE "C",
    last_status     bigint,
    delivered_at    bigint,
    created_at      bigint NOT NULL,
    updated_at      bigint NOT NULL,
    PRIMARY KEY (event_id, subscription_id)
);
CREATE INDEX idx_deliveries_due ON deliveries(status, next_attempt_at);
CREATE INDEX idx_deliveries_sub ON deliveries(subscription_id, status, seq);

CREATE TABLE consumer_checkpoints (
    consumer      text COLLATE "C" NOT NULL,
    topic_pattern text COLLATE "C" NOT NULL,
    cursor        bigint NOT NULL,
    updated_at    bigint NOT NULL,
    PRIMARY KEY (consumer, topic_pattern)
);

-- Developer apps whose access Network revoked (network.app.revoked -> scope 'app') or whose
-- events.app.subscribe grant it withdrew (scope 'events.app.subscribe'). An app token issued at or
-- before revoked_at (ms, the revocation's own timestamp) is refused for that scope.
CREATE TABLE app_revocations (
    consumer   text COLLATE "C" NOT NULL,
    scope      text COLLATE "C" NOT NULL,
    revoked_at bigint NOT NULL,
    PRIMARY KEY (consumer, scope)
);

-- Publish receipts: remember accepted event ids for longer than the events themselves, so a
-- producer that re-publishes an old id after retention is still answered "duplicate".
CREATE TABLE idempotency_receipts (
    consumer     text COLLATE "C" NOT NULL,
    event_id     text COLLATE "C" NOT NULL,
    processed_at bigint NOT NULL,
    PRIMARY KEY (consumer, event_id)
);

-- Columns and indexes added after the first release (server/store.js ADDED_COLUMNS: developer apps, ADR-014, and redaction).
ALTER TABLE events ADD COLUMN project_id text COLLATE "C";
ALTER TABLE events ADD COLUMN env text COLLATE "C" NOT NULL DEFAULT 'production';
ALTER TABLE events ADD COLUMN size_bytes bigint NOT NULL DEFAULT 0;
ALTER TABLE events ADD COLUMN redacted_at bigint;
ALTER TABLE events ADD COLUMN redacted_by text COLLATE "C";
ALTER TABLE events ADD COLUMN redacts bigint NOT NULL DEFAULT 0;
ALTER TABLE subscriptions ADD COLUMN project_id text COLLATE "C";
ALTER TABLE subscriptions ADD COLUMN env text COLLATE "C" NOT NULL DEFAULT 'production';
ALTER TABLE subscriptions ADD COLUMN previous_secret text COLLATE "C";
ALTER TABLE subscriptions ADD COLUMN previous_secret_until bigint;
CREATE INDEX idx_events_project ON events(project_id, env, received_at) WHERE project_id IS NOT NULL;
CREATE INDEX idx_subscriptions_project ON subscriptions(project_id, env) WHERE project_id IS NOT NULL;
CREATE INDEX idx_events_subject ON events(source, subject_type, subject_id);

-- The global event sequence (an import replaces the row with SQLite's).
INSERT INTO sequences (name, value) VALUES ('events', 0) ON CONFLICT DO NOTHING;

-- server/usage.js: per-app usage windows (was created by the module).
CREATE TABLE app_usage (
    project_id   text COLLATE "C" NOT NULL,
    env          text COLLATE "C" NOT NULL,
    capability   text COLLATE "C" NOT NULL,
    unit         text COLLATE "C" NOT NULL,
    window_start bigint NOT NULL,
    quantity     bigint NOT NULL DEFAULT 0,
    errors       bigint NOT NULL DEFAULT 0,
    error_codes  text COLLATE "C" NOT NULL DEFAULT '{}',
    samples      text COLLATE "C" NOT NULL DEFAULT '[]',
    revision     bigint NOT NULL DEFAULT 1,
    event_id     text COLLATE "C",
    emitted_at   bigint,
    PRIMARY KEY (project_id, env, capability, unit, window_start)
);
CREATE INDEX idx_app_usage_open ON app_usage(emitted_at, window_start);

-- Redaction rewrites payloads in place and PostgreSQL has no secure_delete: reclaim the replaced row versions soon.
ALTER TABLE events SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_vacuum_threshold = 50);
