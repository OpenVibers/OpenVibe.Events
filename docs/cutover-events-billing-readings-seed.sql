-- PostgreSQL seed for the 0007_billing_readings cutover rehearsal (docs/cutover-events-billing-readings.md). Loaded after
-- migrations 0001-0006, before 0007: an app project's closed hour as production stores it. All rows sit one minute into the
-- UTC hour that started two hours ago (closed, past the 5-minute grace):
--   seq 1  prj_01JAB2C3D4E5F6G7H8J9K0MNPQ production, 100 bytes     -> 1 operation (publish)
--   seq 2  prj_01JAB2C3D4E5F6G7H8J9K0MNPQ production, 70000 bytes   -> 2 operations (publish) + 2 (its delivered webhook)
--   seq 3  the same project in sandbox                               -> not billed
--   seq 4  first-party (live), no project, delivered to Search       -> not billed
-- Expected: one reading for the project, quantity 5.
INSERT INTO events (id, seq, event_type, version, source, actor, subject_type, subject_id, subject_revision, trace_id, priority,
    visibility, occurred_at, received_at, payload, hops, publisher, request_id, project_id, env, size_bytes, redacted_at, redacted_by, redacts)
SELECT v.id, v.seq, v.event_type, 1, v.source, '{"type":"app","id":"app_rehearsal"}', 'order', 'o_' || v.seq, NULL,
    'trc_billing_' || v.seq, 'normal', 'public', '2026-10-04T00:00:00.000Z',
    (floor(extract(epoch FROM now()) * 1000 / 3600000)::bigint - 2) * 3600000 + 60000, '{"n":1}', 1, v.publisher, NULL,
    v.project_id, v.env, v.size_bytes, NULL, NULL, 0
FROM (VALUES
    ('evt_billing_0001', 1, 'app.rehearsal.order.created', 'app', 'app:app_rehearsal', 'prj_01JAB2C3D4E5F6G7H8J9K0MNPQ', 'production', 100),
    ('evt_billing_0002', 2, 'app.rehearsal.order.created', 'app', 'app:app_rehearsal', 'prj_01JAB2C3D4E5F6G7H8J9K0MNPQ', 'production', 70000),
    ('evt_billing_0003', 3, 'app.rehearsal.order.created', 'app', 'app:app_rehearsal', 'prj_01JAB2C3D4E5F6G7H8J9K0MNPQ', 'sandbox', 100),
    ('evt_billing_0004', 4, 'live.stream.started', 'live', 'svc:live', NULL, 'production', 100)
) AS v(id, seq, event_type, source, publisher, project_id, env, size_bytes);

UPDATE sequences SET value = 4 WHERE name = 'events';

INSERT INTO subscriptions (id, consumer, topic_pattern, endpoint, secret, enabled, created_at, updated_at, project_id, env) VALUES
    ('sub_billing_app', 'app:app_rehearsal', 'app.rehearsal.*', 'https://hooks.example.com/h', 'rehearsal-secret-rehearsal-secret', 1, 0, 0, 'prj_01JAB2C3D4E5F6G7H8J9K0MNPQ', 'production'),
    ('sub_billing_search', 'search', 'live.stream.*', 'http://127.0.0.1:9/hook', 'rehearsal-secret-rehearsal-secret', 1, 0, 0, NULL, 'production');
INSERT INTO deliveries (event_id, subscription_id, seq, priority, status, delivered_at, created_at, updated_at)
SELECT d.event_id, d.subscription_id, d.seq, 1, 'delivered', e.received_at + 1000, e.received_at, e.received_at
FROM (VALUES ('evt_billing_0002', 'sub_billing_app', 2), ('evt_billing_0004', 'sub_billing_search', 4)) AS d(event_id, subscription_id, seq)
JOIN events e ON e.id = d.event_id;
