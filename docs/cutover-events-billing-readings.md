# Cutover: queue-operation readings to OpenVibe.Billing (`0007_billing_readings`, plan T5 step 14)

One additive migration (`-- phase: expand`): two new tables, `billing_readings` (the outbox of `platform.usage-sample@1`
readings) and `billing_periods` (the closed hours already aggregated), plus a trigger that refuses any edit of a stored
reading. Nothing in `events`, `deliveries` or any other table changes; no backfill runs. The code that fills them is off
by default (`EVENTS_BILLING_INTERVAL_MS=0`): after the deploy the tables stay empty until an operator turns it on. No money
moves here: Billing stores the readings and rates them in its own sweep. It also bumps `openvibe-contracts` from v0.81.0
to v0.94.2 (`platform.usage-sample@1`).

## What is metered

| | |
|---|---|
| metric | `queue-operation-64kb` (the rate card's `metric`; the reading's `resource` and `unit`) |
| who | a developer project in **production**: the event's project for a publish, the app subscription's project for a delivery. First-party traffic (no project) and sandbox are not billed. |
| period | one UTC hour `[H, H+1h)`: publishes by `received_at`, webhook deliveries by `delivered_at` (status `delivered`; failed and dead deliveries are not billed). Closed at `H + 1h + 5 min`. |
| quantity | each publish and each delivered webhook counts `ceil(size_bytes / 65536)` operations, at least 1 |
| reading | `id` = `idempotency_key` = `events:queue-operation-64kb:<prj_…>:<H as ISO>`, `service` `events`, `project` the project, `provider` `local`, `operation` `events.queue`, `at` = `H`, `source` `openvibe.events` |

One aggregate query over the closed hour (server/billing.js) inserts one row per project with `ON CONFLICT DO NOTHING`,
in the transaction that marks the hour in `billing_periods`; re-running an hour inserts nothing, and a stored reading is
never edited, so every retry posts the same body and Billing's dedupe on `idempotency_key` answers a replay with 200.
Nothing is written on the publish or delivery path.

## Environment (`/etc/openvibe/events.env`)

| variable | default | meaning |
|---|---|---|
| `EVENTS_BILLING_INTERVAL_MS` | `0` | how often the loop aggregates closed hours and posts pending readings; `0` = off (nothing aggregated or sent) |
| `OV_BILLING_INTERNAL_URL` | unset | Billing's base URL (`http://127.0.0.1:4600`); unset = readings are aggregated and stay queued, nothing is sent |
| `OV_OAUTH_CLIENT_ID` | `events` | Events' Network client (client_credentials) |
| `OV_OAUTH_CLIENT_SECRET` | unset | its secret; unset = nothing is sent (as without the URL) |
| `OV_BILLING_AUDIENCE` | `openvibe.billing` | the token audience |
| `OV_BILLING_TIMEOUT_MS` | `10000` | per request (token and post) |
| `OV_NETWORK_INTERNAL_URL` | `http://127.0.0.1:4000` | already set: where the token comes from |

Answers: 2xx (201 stored, 200 replay) → `sent`. Any other 4xx (400, 409 `billing.usage_key_reused`, 422
`billing.invalid_input`, …) → `refused` with `last_error`, never retried. 401, 403 (the grant), 404, 408, 425, 429, 5xx,
a network error or a token failure → stays `pending` with backoff (30 s doubling, at most 1 h) and the rest of the batch
waits for the next tick. One loop per process; two processes are safe (rows are claimed `FOR UPDATE SKIP LOCKED` with a
2-minute lease). After an outage the loop catches up at most 7 days of hours (well inside `EVENTS_RETENTION_DAYS`).

## Network grant (OpenVibe.Network, its own PR)

Events' service client needs `billing.usage.record` on audience `openvibe.billing`. In
`server/identity/principals.js` `DEFAULT_GRANTS`, next to the Tools row:

```js
    // Plan T5 step 14: Events posts one queue-operation reading per production project and closed hour to Billing (POST /api/v1/usage).
    ['events', 'billing.usage.record', 'openvibe.billing', []],
```

The `events` client must also exist as a confidential client with a secret (the value of `OV_OAUTH_CLIENT_SECRET`).
Without the grant every token request for the audience fails; each tick logs
`[billing] reading events:… not sent, retried in … s: token: …` and the readings wait, pending, in `billing_readings`.

## Order

1. Health: `ov access run openvibe-ovh health events` answers ready. Backup: today's `ov_events` dump exists (OpenVibe.Host
   docs/backups.md); the migration only adds tables.
2. Deploy the release (pipeline, `ovhost deploy events`). On start it applies `0007_billing_readings.sql` as the owner role
   (`DATABASE_DIRECT_URL`). `billing_readings` and `billing_periods` stay empty: the loop is off.
3. Merge and deploy the Network grant above; give the `events` client its secret.
4. Turn it on: add `EVENTS_BILLING_INTERVAL_MS=300000`, `OV_BILLING_INTERNAL_URL=http://127.0.0.1:4600`,
   `OV_OAUTH_CLIENT_SECRET=…` to `/etc/openvibe/events.env`, restart (`ovhost deploy events --restart`). The first tick
   aggregates the newest closed hour only (no back-billing), then every hour as it closes.

## Verification

- `SELECT id, name, phase FROM ov_migrations WHERE id = '0007'` → `0007 | billing_readings | expand`; both tables empty
  while the loop is off.
- After step 4: `SELECT state, COUNT(*) FROM billing_readings GROUP BY state` shows `sent` growing and `pending` back to 0
  after a tick; `refused` rows carry Billing's answer in `last_error`; `pending` rows with `last_error` `token: …` mean the
  grant or the secret is missing. `SELECT MAX(period_start) FROM billing_periods` is the last closed hour.
- In Billing: `GET /api/v1/usage?service=events` (`billing.ledger.admin`) lists one reading per project and hour.

```rehearse
seed: docs/cutover-events-billing-readings-seed.sql
node -e "const a=require('assert');const {load}=require('./server/config');const {openDb}=require('./server/store');const {createBillingReadings,HOUR_MS}=require('./server/billing');const quiet={log(){},warn(){},error(){}};(async()=>{const c=load();const db=await openDb(c,{log:quiet});const b=createBillingReadings({db,config:c.billing,log:quiet});a.strictEqual(b.enabled,false);a.strictEqual(b.sending,false);const n=async(t)=>(await db.prepare('SELECT COUNT(*)::int AS n FROM '+t).get()).n;a.strictEqual(await n('billing_readings'),0);a.strictEqual(await n('events'),4);const at=(await db.prepare('SELECT received_at AS t FROM events WHERE seq = 1').get()).t;const H=Math.floor(Number(at)/HOUR_MS)*HOUR_MS;const r=await b.aggregate(H);a.deepStrictEqual([r.readings,r.created],[1,1]);const [row]=await db.prepare('SELECT * FROM billing_readings').all();a.deepStrictEqual([row.project_id,row.reading.quantity,row.state],['prj_01JAB2C3D4E5F6G7H8J9K0MNPQ',5,'pending']);a.strictEqual((await b.aggregate(H)).created,0);a.strictEqual(await n('billing_readings'),1);a.strictEqual((await b.send()).sent,0);await db.close();console.log('rehearsal ok '+row.id)})().catch(e=>{console.error(e);process.exit(1)})"
```

## Rollback

- **Stop sending, no rollback:** set `EVENTS_BILLING_INTERVAL_MS=0` (or remove `OV_BILLING_INTERNAL_URL`) and restart.
  Pending readings stay in the table and are posted when it is turned on again; Billing dedupes on `idempotency_key`.
- **Code only:** roll back to the previous release. It does not know the two tables and never reads them; the migration
  stays applied (an applied id the old runner does not have is not an error).
- **Remove the tables** (a contract step, by hand as the owner, after a code rollback, only if they must go):
  `DROP TABLE billing_readings, billing_periods; DROP FUNCTION billing_readings_frozen(); DELETE FROM ov_migrations WHERE id = '0007';`.
  Readings not yet posted are lost; Billing keeps the ones it stored.
