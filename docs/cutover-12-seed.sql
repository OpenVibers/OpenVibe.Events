-- PostgreSQL seed for the 0006_events_archive cutover rehearsal (docs/cutover-12.md). Loaded after migrations
-- 0001-0005, before 0006: the shape production has today. Times are relative to now() (ms), so the rehearsal's prune
-- with the defaults (EVENTS_RETENTION_DAYS=30, EVENTS_REPLAY_RETENTION_DAYS=365) sees:
--   seq 1  production, 400 days old   -> moved to replay, then dropped by the replay prune (past 365 days)
--   seq 2  production,  60 days old   -> moved to replay (kept), its two deliveries go with the hot row
--   seq 3  production,  45 days old, tombstoned (redacted) -> moved to replay, still a tombstone
--   seq 4  sandbox,     45 days old   -> deleted (sandbox never enters replay)
--   seq 5  production,   1 day old    -> stays hot
-- The epoch stays 0 and the sequence counter at 5, as a live store's would.
INSERT INTO events (id, seq, event_type, version, source, actor, subject_type, subject_id, subject_revision, trace_id, priority,
    visibility, occurred_at, received_at, payload, hops, publisher, request_id, project_id, env, size_bytes, redacted_at, redacted_by, redacts)
SELECT v.id, v.seq, v.event_type, 1, v.source, '{"type":"service","id":"svc:live"}', 'stream', v.subject_id, NULL,
    'trc_rehearsal_' || v.seq, 'important', 'public', '2026-01-01T00:00:00.000Z',
    (extract(epoch FROM now()) * 1000)::bigint - v.days::bigint * 86400000, v.payload, 1, v.publisher, NULL, v.project_id, v.env,
    length(v.payload), v.redacted_at, v.redacted_by, 0
FROM (VALUES
    ('evt_rehearsal_0001', 1, 'live.stream.started', 'live', 'str_1', '{"title":"oldest"}', 'svc:live', NULL, 'production', 400, NULL::bigint, NULL),
    ('evt_rehearsal_0002', 2, 'live.stream.started', 'live', 'str_2', '{"title":"replay ✓"}', 'svc:live', NULL, 'production', 60, NULL, NULL),
    ('evt_rehearsal_0003', 3, 'live.stream.started', 'live', 'str_3', '{"redacted":true,"redacted_at":1,"redacted_by":"evt_rehearsal_0005"}', 'svc:live', NULL, 'production', 45, 1, 'evt_rehearsal_0005'),
    ('evt_rehearsal_0004', 4, 'app.prj_rehearsal.tick', 'app', 'str_4', '{"n":4}', 'app:prj_rehearsal', 'prj_rehearsal', 'sandbox', 45, NULL, NULL),
    ('evt_rehearsal_0005', 5, 'live.stream.started', 'live', 'str_5', '{"title":"hot"}', 'svc:live', NULL, 'production', 1, NULL, NULL)
) AS v(id, seq, event_type, source, subject_id, payload, publisher, project_id, env, days, redacted_at, redacted_by);

UPDATE sequences SET value = 5 WHERE name = 'events';

INSERT INTO subscriptions (id, consumer, topic_pattern, endpoint, secret, enabled, created_at, updated_at)
VALUES ('sub_rehearsal', 'search', 'live.stream.*', 'http://127.0.0.1:9/hook', 'rehearsal-secret-rehearsal-secret', 1, 0, 0);
INSERT INTO deliveries (event_id, subscription_id, seq, priority, status, delivered_at, created_at, updated_at)
VALUES ('evt_rehearsal_0002', 'sub_rehearsal', 2, 1, 'delivered', 0, 0, 0),
       ('evt_rehearsal_0005', 'sub_rehearsal', 5, 1, 'pending', NULL, 0, 0);
