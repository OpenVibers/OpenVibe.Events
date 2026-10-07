# Cutover: the replay tier (`0006_events_archive`, ADR-042 decision 8)

One additive migration (`-- phase: expand`): a new table `events_archive` and its four indexes. Nothing in `events`,
`deliveries` or any other table changes; no backfill runs at migration time. The release that ships it starts moving
hot events past `EVENTS_RETENTION_DAYS` (30) into `events_archive` at its hourly prune instead of deleting them, and
reads span both tables. `EVENTS_REPLAY_RETENTION_DAYS` defaults to 365; `0` is the old behaviour exactly.

## Before

1. Health: `ov access run openvibe-ovh health events` answers ` ready `.
2. Backup: the host's nightly `ov_events` dump is from today, or take one (`pg_dump -Fc ov_events`, OpenVibe.Host
   docs/backups.md). The migration only adds a table, so the backup is for the release, not for the migration.
3. Size the first prune: `SELECT COUNT(*) FROM events WHERE received_at < (extract(epoch FROM now()) * 1000)::bigint - 30 * 86400000;`
   is 0 or close to it on a store that prunes hourly today; whatever it is, the prune moves 500 rows per transaction and
   at most 100 000 per run, the next run continues.

## Cutover

1. Deploy the release (the pipeline's `ovhost deploy events`). On start the service applies `0006_events_archive.sql`
   as the owner role (`DATABASE_DIRECT_URL`): `CREATE TABLE events_archive …` plus its indexes. On an empty table this
   takes milliseconds and holds no lock on `events`.
2. `/etc/openvibe/events.env`: nothing to add for the default (365 days of replay). To keep today's behaviour, set
   `EVENTS_REPLAY_RETENTION_DAYS=0` and restart.
3. Check: `curl -s https://openvibe.events/limits.json` lists `replay_retention_days`; after the first prune
   (start + up to `EVENTS_PRUNE_INTERVAL_MS`) the journal (`ov access run openvibe-ovh journal openvibe-events.service 200`)
   shows `[retention] pruned N hot events (N moved to replay), …` when anything was due, and
   `node scripts/events-archive.js status` (on the host, with the service env) lists the replay rows per month.

The archive tier (`scripts/events-archive.js export|restore --month YYYY-MM`) is an operator job, never part of this
cutover; see README "Retention: three tiers".

## Rollback

- Release only: roll back to the previous release. It never reads `events_archive`; the table and the rows moved into
  it stay, unread, and the old release deletes hot events past retention as before. Rolling forward again finds them
  in place (cursors into them keep working, the epoch is unchanged).
- The table too (only if the release is abandoned): after rolling back, `DROP TABLE events_archive;` as the owner and
  delete its row from the migrations ledger, `DELETE FROM ov_migrations WHERE id = '0006';`
  (the ledger table of openvibe-sdk/db). The events in it are lost; restore them from the backup if they matter.

## Rehearsal

The seed is PostgreSQL ([cutover-12-seed.sql](cutover-12-seed.sql), loaded after 0001–0005, before 0006): five hot
events at 400, 60, 45 (a tombstone), 45 (sandbox) and 1 day(s) old, a subscription and two deliveries. The commands
apply 0006 through the service's own `openDb` (a second run applies nothing), run the tiered prune with the
defaults, and check the result: the 400-day event is gone from both tiers, the 60- and 45-day production events are in
the replay tier (the tombstone still a tombstone, the delivery gone with the hot row), the sandbox event is deleted, the
1-day event stays hot with its pending delivery, one scan from the start spans both tiers in seq order, a by-id read
finds the archived event, and a second prune moves nothing.

```rehearse
seed: docs/cutover-12-seed.sql
node scripts/events-archive.js status
node -e "const a=require('assert');const {load}=require('./server/config');const {openDb,createStore}=require('./server/store');(async()=>{const db=await openDb(load(),{log:{log(){},warn(){}}});const s=createStore(db);const r=await s.prune({retentionDays:30,replayRetentionDays:365});console.log(JSON.stringify(r));a.strictEqual(r.archived,3);a.strictEqual(r.replay,1);const n=async(t)=>(await db.prepare('SELECT COUNT(*)::int AS n FROM '+t).get()).n;a.strictEqual(await n('events'),1);a.strictEqual(await n('events_archive'),2);a.strictEqual(await n('deliveries'),1);const {rows}=await s.scan(0);a.deepStrictEqual(rows.map(x=>x.seq),[2,3,5]);a.strictEqual(rows[1].redacted_by,'evt_rehearsal_0005');const e=await s.getEvent('evt_rehearsal_0002');a.strictEqual(JSON.parse(e.payload).title,'replay ✓');a.strictEqual(await s.getEvent('evt_rehearsal_0001'),null);a.strictEqual((await s.prune({retentionDays:30,replayRetentionDays:365})).archived,0);await db.close();console.log('rehearsal ok')})().catch(e=>{console.error(e);process.exit(1)})"
node scripts/events-archive.js status
```
