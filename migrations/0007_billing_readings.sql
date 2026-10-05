-- phase: expand
-- Plan T5 step 14 (govern universal): the outbox of platform.usage-sample@1 readings Events posts to OpenVibe.Billing
-- (POST /api/v1/usage, capability billing.usage.record; server/billing.js). One row per (billable project, metric,
-- closed UTC hour), made once by one aggregate over `events` and `deliveries` after the hour closed; nothing is
-- written on the publish or delivery path. `id` is the reading's id and idempotency_key (deterministic: re-running a
-- period inserts nothing, ON CONFLICT DO NOTHING). A reading is never edited after it is created (the trigger below
-- refuses it); only the sending columns move: pending -> sent (2xx, or Billing's replay of the key) or refused (Billing
-- refused the reading itself; never retried). lease_until keeps a row claimed by one sender while it is posted.
-- Additive only: two new tables, nothing existing changes.
CREATE TABLE billing_readings (
    id              text COLLATE "C" PRIMARY KEY,
    project_id      text COLLATE "C" NOT NULL,
    metric          text COLLATE "C" NOT NULL,
    period_start    bigint NOT NULL,
    reading         jsonb NOT NULL,
    state           text COLLATE "C" NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'sent', 'refused')),
    attempts        bigint NOT NULL DEFAULT 0,
    last_error      text COLLATE "C",
    next_attempt_at bigint NOT NULL,
    lease_until     bigint,
    created_at      bigint NOT NULL,
    sent_at         bigint,
    UNIQUE (project_id, metric, period_start)
);
CREATE INDEX idx_billing_readings_due ON billing_readings(next_attempt_at) WHERE state = 'pending';
CREATE FUNCTION billing_readings_frozen() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.project_id IS DISTINCT FROM OLD.project_id OR NEW.metric IS DISTINCT FROM OLD.metric
        OR NEW.period_start IS DISTINCT FROM OLD.period_start OR NEW.reading IS DISTINCT FROM OLD.reading
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'billing_readings: reading % is never edited', OLD.id;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER billing_readings_frozen BEFORE UPDATE ON billing_readings FOR EACH ROW EXECUTE FUNCTION billing_readings_frozen();
-- The closed hours already aggregated, per metric (an hour with no billable use leaves no reading, but is marked here).
CREATE TABLE billing_periods (
    metric        text COLLATE "C" NOT NULL,
    period_start  bigint NOT NULL,
    readings      bigint NOT NULL,
    aggregated_at bigint NOT NULL,
    PRIMARY KEY (metric, period_start)
);
