# OpenVibe.Events

> Durable events, subscriptions, delivery, retry, dead letters and replay for the network.

**Status:** alpha (roadmap Wave 3), deployed on `openvibe-ovh` (unit `openvibe-events`, 127.0.0.1:4300, public at `https://openvibe.events`). Live (`live.stream.*`), Media, Chat, Games, Network and OpenRestream publish to it in production, and every consumer webhook requires signature v2. Media's completion events reach Live through Events (`/internal/media-events`; Live keeps the older `/internal/media-webhook` during the transition), and the shared notification bell can follow `network.notification.created` over the realtime stream with a Network ticket.  
**Domain:** `openvibe.events` is the API and product origin (plan T7): the JSON API, the realtime gateway (ADR-005 put Realtime inside Events; `realtime.openvibe.network` has no runtime of its own), the home page and its `robots.txt`/`sitemap.xml`/`llms.txt`. The old `events.openvibe.network` address was retired on 2026-10-10.  
**Plan:** OpenVibe End-to-End Realignment & Implementation Plan, revision 3 (20 Sep 2026), §6 and §6.3.  
**License:** AGPL-3.0 (same as every OpenVibe service).

## Purpose

The durable event backbone the network does not have yet: every authoritative service writes domain mutations and outbox events in one transaction, a relay publishes them here, consumers use inbox/idempotency receipts, and browsers get authorized, resumable projections through the Realtime delivery plane.

## Running it

```bash
npm install
cp .env.example .env
npm run dev            # http://127.0.0.1:4300
npm test               # every test/*.test.js, temp databases, no network needed
```

Node 22 in production (`fnm exec --using=22.22.1 npm test`). Production: `/opt/openvibe.events`, env `/etc/openvibe/events.env`, unit [deploy/systemd/openvibe-events.service](deploy/systemd/openvibe-events.service), store PostgreSQL (`ov_events` on the host's data role), nginx [deploy/nginx/openvibe.events.conf](deploy/nginx/openvibe.events.conf) (the public vhost, on the API and product origin: it exposes the home and its discovery files, `/realtime/stream`, health, and the token-guarded `/api/v1/events`, `/api/v1/subscriptions` and `/api/v1/checkpoints` for developer apps; `/api/v1/deliveries` stays host-local; services on the host call `127.0.0.1:4300`).

`GET /api/health` is liveness. `GET /api/ready` (openvibe-shared/ready) is 200 only when every required check passes — `db` (a real query), `network_jwks` (the Network signing key has loaded: openvibe-sdk/auth `createNetworkKeys`, retried every 30 s while Network boots, refreshed every 15 minutes, refetched at once for a token naming an unknown kid, and the last good keys kept through a Network outage) and `delivery_worker` (when `EVENTS_WORKER` is on) — and 503 otherwise, with `failed: [...]`. The optional `dlq` check fails once more than `EVENTS_DLQ_DEGRADED_AT` (default 100) deliveries are dead: the service stays ready (200) and reports `status: "degraded"`, `degraded: ["dlq"]`. Each check carries `status`, `required`, `latency_ms`, `checked_at` and, for `dlq`, `detail: { depth, threshold }`; `latest_seq`, `deliveries`, `worker` and `realtime_connections` are still in the body. **Shape change (Track O):** `checks` used to be booleans (`{ db, worker, key }`); they are now objects keyed `db`, `network_jwks`, `delivery_worker`, `dlq`.

`GET /metrics` (Prometheus text, direct loopback callers only — a request carrying `X-Forwarded-For` gets 404, and nginx refuses the path): HTTP golden signals by route template (`http_requests_total`, `http_request_duration_seconds`, `http_requests_in_flight`; SSE sessions excluded), process metrics, `release_info`, and `events_deliveries{status}`, `events_dlq_depth`, `events_latest_seq`, `events_realtime_connections`, `events_delivery_latency_seconds{priority}` (acceptance → successful delivery, retries included), `events_delivery_attempts_total{outcome}`. `GET /release.json` is the deployed release (ADR-016, `release.mount` from openvibe-shared 1.5.0, with the manifest 1.1.0 fields under openvibe-contracts 0.32.0); its `POST /release-metrics` would count open tabs' update reports as `release_client_updates_total`, but Events has no pages and nginx does not proxy that path. `GET /limits.json` lists the developer limits enforced here (per project and environment), read from the running config; [openvibe.services/docs/limits](https://openvibe.services/docs/limits) renders it.

## Auth

Services call with an OpenVibe.Network client-credentials token (`POST /oauth/token`, `audience=openvibe.events`), verified offline against the Network JWKS (openvibe-sdk/auth `verifyServiceToken` with the pinned openvibe-contracts rules; people's tokens with `verifyUserToken`, which refuses service principals and typed tokens). Each route checks one capability:

| Capability | Routes |
|---|---|
| `events.event.publish` | `POST /api/v1/events` |
| `events.subscription.manage` | `/api/v1/subscriptions…` (own subscriptions only) |
| `events.event.read` | `GET /api/v1/events`, `GET /api/v1/events/:id`, `/api/v1/checkpoints`, realtime as a service |
| `events.delivery.admin` | `GET /api/v1/deliveries`, `POST /api/v1/deliveries/replay` |
| `events.resource.read` | `GET /api/v1/resources`, `GET /api/v1/resources/:ovrn` (the [resource index](#resource-index)) |

These four are `internal` in `openvibe-contracts` (never granted to developer apps). Developer apps use the three `public` capabilities in [Developer apps](#developer-apps) instead, on the same routes. `events.resource.read` is `first-party` (ADR-048), `active` in `openvibe-contracts` since v0.110.0 — the guard is the same service-token check, and the same 401/403, every other route here gets. `server/auth.js` grants with the contracts rule (exact id or a `family.*` grant) and hands the decision to `capabilities.check()` for every id the installed contracts know (the pinned v0.110.0 knows all eight).

Sandbox tokens (`env: sandbox`, developer apps only) are accepted only on the developer-app routes; every other route answers `401 token.sandbox_refused`. App tokens are never judged on a first-party capability: an app token on an operator route is a `403`.

Errors are RFC 9457 problem+json (`openvibe-contracts` `http.problem`) with a stable `code`.

### Per-actor limits

Every capability route also limits each principal (`svc:live`, `app:app_…`) by requests, after its guard and before any work (`server/actor-limits.js`, openvibe-sdk/limits, roadmap WS-R task 4). Past a limit: `429` problem+json `rate_limited` with `Retry-After`, one `[limits]` log line and `events_rate_limited_total{limit,window}`. Counters are per process; `EVENTS_LIMITS=off` turns them all off (a rollback lever).

| Route | Per principal |
|---|---|
| Single reads: `GET /api/v1/events/:id`, `GET /api/v1/checkpoints`, subscription and delivery lists | `EVENTS_LIMITS_MINUTE` / `EVENTS_LIMITS_HOUR` (120 a minute, 3000 an hour) |
| `GET /api/v1/events` (pull), `PUT /api/v1/checkpoints` | 600 / 20000 (a consumer pages through a backlog) |
| `POST /api/v1/events`, a service | 600 / 20000 (outboxes batch and retry a 429); **Network is never counted**: it carries revocations, cutoffs and deletions |
| `POST /api/v1/events`, a developer app | 60 / 1200, besides the project's event quota |
| Subscription create, enable, disable | 30 / 300 |
| `POST /api/v1/subscriptions/:id/rotate-secret` | 10 / 100 |
| `POST /api/v1/deliveries/replay` | 6 / 60 |

Never limited: `/api/health`, `/api/ready`, `/release.json`, `/limits.json`, `/metrics` and the realtime stream (capped by `REALTIME_MAX_CONNECTIONS` and `REALTIME_MAX_TOPICS`). `test/actor-limits.test.js`.

## Capabilities

Implemented here (the service manifest's `capabilities`): the four internal ones in the table above
(`events.event.publish`, `events.event.read`, `events.subscription.manage`, `events.delivery.admin`),
the three public developer-app ones (`events.app.publish`, `events.app.read`, `events.app.subscribe`,
[Developer apps](#developer-apps)) and the first-party `events.resource.read` (ADR-048,
[Resource index](#resource-index); `active` in `openvibe-contracts` since v0.110.0). Events
calls no other service with a grant: it only loads the Network signing key (JWKS) and makes the signed
deliveries its subscriptions ask for. It produces one event of its own, `events.usage.recorded` (a
project's hourly publishing and delivery rollup, for Network; never visible to apps).

## Publishing

`POST /api/v1/events` takes one `events.event-envelope@1` envelope, or `{ "events": [ … ] }` (up to 100, stored atomically).

- `source` must be the calling service (`svc:live` publishes `source: "live"`), and `event_type` must start with a prefix that source owns (`live.*`, `media.*`, `network.*`, `community.*`, `chat.*`, `openre.*`, `billing.*`, `tips.*`, `vip.*`, `ai.*`, `games.*`, `tools.*`, `codes.*`, `host.*`, `bot.*`, `watch.*`, `run.*`, `space.*`, and the publication products (`wiki.*`, `blog.*`, `news.*`, `reviews.*`, `deals.*`, `coupons.*`, `trade.*`, `sources.*`, `search.*`); `EVENTS_SOURCE_PREFIXES` overrides). A network-wide prefix may be published by any first-party service: `provider.*` carries storage provider health/capacity telemetry (`provider.health.degraded`, `provider.capacity.warning`), so whichever service observes a provider can report it; `EVENTS_SHARED_PREFIXES` overrides.
- Idempotent on `event_id`: a repeat answers `200 { event_id, seq, duplicate: true }` and is never stored twice (even after retention, for `EVENTS_RECEIPT_RETENTION_DAYS`).
- Missing `trace_id` is taken from the request's `traceparent`; `priority` defaults to `important`, `visibility` to `internal`.
- Loop guard: an event whose trace already carries 8 hops (the cross-service chain depth, or the same source/type/subject repeating in the trace) is refused with `409 events.loop_detected`. The chain depth of a first-party publish counts first-party events only (developer-app events in the same trace never count toward it), so an app cannot poison a trace it has seen.
- Each accepted event gets a global `seq` and is committed with one delivery row per matching subscription before anything is sent.

### Redaction

A producer takes back what it published (a deleted chat message) with a directive in the payload of any event it publishes, normally its own `*.deleted` event ([server/redaction.js](server/redaction.js)):

```json
{ "event_type": "chat.message.deleted", "source": "chat", "visibility": "public",
  "subject": { "type": "chat_message", "id": "123" },
  "payload": { "message_ids": [123], "redacts": { "subject_type": "chat_message", "subject_ids": ["123"] } } }
```

- `redacts.event_ids` (up to 1000 `evt_…`) names events directly; `subject_type` + `subject_ids` (up to 1000) names every stored event of the same source about those subjects.
- In the transaction that stores the directive, each target becomes a **tombstone**: `payload` is replaced by `{ "redacted": true, "redacted_at": "<ISO>", "redacted_by": "<the redacting event_id>" }` and `actor` by the producer itself (`{ "type": "service", "id": "<source>" }`, or the app). `event_id`, `seq`, `event_type`, `subject`, `timestamp` and `visibility` stay, so sequences have no holes. On PostgreSQL the replaced row version is a dead tuple until autovacuum reclaims it (the `events` table vacuums after 1% of its rows change, so within minutes); until then, and in backups taken before the redaction, the old bytes still exist on the host. No read path serves them.
- Every read path serves the tombstone from then on: pull, `GET /api/v1/events/:id`, SSE replay (anonymous, signed in or service), queued deliveries and DLQ replays. First-party consumers get the tombstone at the original seq and then the deletion event itself; they should check `payload.redacted`.
- Authority is the publish rule: only the owning source (for an app, its own project and environment) can redact. Naming another source's event by id refuses the whole publish with `403 events.redaction_not_allowed`; a subject match never reaches another source. A malformed directive is `422 events.invalid_redaction`. Events carrying a directive are never redacted themselves. No capability beyond `events.event.publish` is needed.
- Redaction applies to what is stored when the directive arrives; it does not block later events about the same subject.
- Events stored before a producer published deletions: the one-time `scripts/redact-backfill.js` (chat only) ran on 2026-09-24 and was retired with the move to PostgreSQL (it is in git history).

## Subscriptions and delivery

```http
POST /api/v1/subscriptions
{ "topic_pattern": "media.vod.*", "endpoint": "http://127.0.0.1:3000/internal/events", "retry_policy": { "max_attempts": 8 } }
→ 201 { "id": "sub_…", "secret": "whsec_…", … }     # the secret is shown once
```

Topic patterns are dot-separated segments where `*` stands for one or more whole segments (`media.vod.*`, `*.created`, `media.*.ready`, `*`). Endpoints must be `http(s)` on `127.0.0.1` or `openvibe.<tld>`/its subdomains (`EVENTS_ENDPOINT_HOSTS`); redirects are never followed. A caller may supply its own `secret` (a string of 32..256 characters; the same bound on `POST /api/v1/subscriptions/:id/rotate-secret`), otherwise Events generates a `whsec_…` one; either way it is returned once, on creation, and the consumer needs it to verify the v2 signature. The consumer is the calling service, and re-creating a subscription it already holds for the same `topic_pattern` and `endpoint` answers `409 events.subscription_exists` with the existing `subscription_id`, so an ensure-at-boot script re-runs safely.

Each delivery is `POST <endpoint>` with body `{ "event": <envelope>, "seq": n }` and headers `X-OpenVibe-Event-Id`, `X-OpenVibe-Event-Type`, `X-OpenVibe-Seq`, `X-OpenVibe-Subscription-Id`, `X-OpenVibe-Delivery-Attempt`, `X-OpenVibe-Timestamp: <unix seconds>`, `X-OpenVibe-Signature-V2: t=<that timestamp>,v2=<HMAC-SHA256 of "<t>.<raw body>">` and `traceparent` (the event's trace). During secret rotation, the v2 header also carries a second `v2=` signature made with the previous secret. Any 2xx is delivered. Anything else is retried after 1 s, 5 s, 30 s, 2 min, 10 min, 1 h (then hourly) up to 8 attempts, after which the delivery is `dead`. Priority classes go first (`critical`, `important`, `low`, then seq); one delivery per subscription is in flight at a time and at most 20 overall. Delivery is at least once and not strictly ordered; consumers dedupe with an inbox and order with `subject.revision` (`createPgOrderedInbox` below does both).

**Replay window (signature v2).** Events sends only the v2 signature. It signs the timestamp together with the raw body, and every attempt, retries included, is signed afresh with the time it is sent. Consumers check it with `verifyDeliveryV2(raw, headers, secret, { toleranceSec = 300, now })` (here) or openvibe-sdk ≥ 0.4.0 `parseDelivery(raw, headers, secret, { requireV2: true })` and reject anything more than 300 s from their clock either way. Reject a missing, wrong or stale v2 signature; do not fall back to v1.

Operators: `GET /api/v1/deliveries?status=dead` is the dead-letter queue; `POST /api/v1/deliveries/replay { subscription_id, event_ids: [...] }` or `{ subscription_id, from_seq }` requeues retained events (that is also how a new subscription catches up on history).

Pull consumers: start with `GET /api/v1/events?topic=media.vod.*&limit=100`, then request `GET /api/v1/events?topic=media.vod.*&after=<next_cursor>&limit=100`, using the previous response's opaque `next_cursor` ([ADR-042](https://github.com/OpenVibers/OpenVibe.Contracts/blob/main/docs/adr/ADR-042-events-fabric.md) decision 7). The response carries `{ events: [{ seq, cursor, event }], next_cursor, latest_cursor }` (`latest_cursor` is the head as an opaque cursor, for a consumer that starts at "now" without history), plus `gap: { from_seq, to_seq }` when the position is older than both the hot store and the replay tier (below) or from another retention epoch. Use `next_cursor` even when no events match, since it advances past scanned events. A position is only ever an opaque cursor: `after_seq` is refused with 400 (never read as "from the start"), and the per-event `seq` is informational. `PUT /api/v1/checkpoints { topic, cursor, carrier? }` stores a consumer's cursor here if it has nowhere better (a cursor string; a number is refused); `GET /api/v1/checkpoints?topic=` and the PUT answer `{ consumer, topic, cursor, carrier, updated_at }`, with `cursor` as stored (null when none is).

### Retention: three tiers

[ADR-042](https://github.com/OpenVibers/OpenVibe.Contracts/blob/main/docs/adr/ADR-042-events-fabric.md) decision 8. An event lives in one tier at a time:

| Tier | Where | Kept | Read by |
|---|---|---|---|
| hot | `events` (with its deliveries) | `EVENTS_RETENTION_DAYS` (30) | everything: delivery, pulls, SSE, by id, replay to subscriptions |
| replay | `events_archive`, same database: the filter columns plus a gzip'd body, no delivery rows | `EVENTS_REPLAY_RETENTION_DAYS` (365) from receipt; `0` = no replay tier | pulls, SSE resume and `GET /api/v1/events/:id`, with the same cursor |
| archive | one NDJSON object per month (gzip, with a manifest), outside the database | until an operator deletes it | nothing online; an operator restores a month into replay |

The hourly prune (`EVENTS_PRUNE_INTERVAL_MS`) moves each hot event past `EVENTS_RETENTION_DAYS` into `events_archive` in the transaction that deletes it: 500 rows at a time, locked `FOR UPDATE SKIP LOCKED`, inserted idempotently on `event_id`, so two Events processes pruning at once each move different rows and no event is moved twice. Its deliveries go with the hot row (operator replay to a subscription, `POST /api/v1/deliveries/replay`, reaches hot events only). Replay rows past `EVENTS_REPLAY_RETENTION_DAYS` are then deleted. A pull or SSE resume runs from a cursor in the replay tier into the hot store without a `gap`; `gap` (the same shape) still means the position is older than both tiers or from another epoch. Redaction tombstones replay rows exactly as hot ones, the sandbox and environment rules read them the same way, and a re-publish of an archived `event_id` is still `duplicate: true` with its original `seq` and `cursor`. Sandbox events never enter replay: they are deleted at `EVENTS_APP_SANDBOX_RETENTION_DAYS`, as before; publish receipts keep `EVENTS_RECEIPT_RETENTION_DAYS`.

`EVENTS_REPLAY_RETENTION_DAYS=0` keeps the old behaviour exactly: events past `EVENTS_RETENTION_DAYS` are deleted, and the next prune also empties `events_archive`, because no rule would prune the rows left there otherwise. `GET /limits.json` publishes the window as `replay_retention_days`.

The archive tier is an operator job, never run automatically (`DATABASE_URL` as for the service):

```sh
node scripts/events-archive.js status                                  # replay rows per month, oldest hot event
node scripts/events-archive.js export  --month 2026-01 [--older-than-days 90]
node scripts/events-archive.js restore --month 2026-01 [--hold-days 30]
```

`export` writes the closed month's replay rows older than the cutoff to `<YYYY>/events-<YYYY-MM>.ndjson.gz` (one row per line: every column of the hot row, payload and actor as their stored JSON text, plus `epoch` and `archived_at`) and `<YYYY>/events-<YYYY-MM>.json` (`count`, `sha256`). It deletes those rows from `events_archive` only after reading the object back and matching both, and only rows unchanged since it read them (a row redacted meanwhile stays; rerun). Re-running, or exporting the rest of a month later, merges into the month's object. `restore` checks the object against its manifest, then inserts it back into `events_archive`, idempotent on `event_id`, held for `--hold-days` so the replay prune does not delete it at once. A redaction published after a month was exported does not reach its object, so restore only what you need. Export and restore of one month are serialised by a PostgreSQL advisory lock. Storage (`scripts/archive-storage.js`): a local directory, `EVENTS_ARCHIVE_DIR` (default `data/archive`), or an S3-compatible bucket when `EVENTS_ARCHIVE_S3_BUCKET` is set (`EVENTS_ARCHIVE_S3_ENDPOINT`, `_REGION`, `_ACCESS_KEY_ID`, `_SECRET_ACCESS_KEY`, `_PREFIX`; path-style, SigV4 signed with node:crypto and sent with node's `fetch`, no dependency). Each object is built in memory.

## Resource index

`GET /api/v1/resources` is this authority's resource index (ADR-048 section 3; capability `events.resource.read`): the surface OpenVibe.Services fans out over and merges, so it can list every resource of the network without owning any service's rows. It pages `common.resource-summary@1` for the resources Events owns — today its subscriptions (`sub_<ULID>`, kind `events.subscription`, read from the `subscriptions` table) — as `common.resource-list-result@1`; `GET /api/v1/resources/:ovrn` reads one by its OVRN (`common.resource-summary@1`).

- Query: `?project=prj_…&kind=events.subscription&cursor=…&limit=…`. `?project=` is the tenancy boundary: only that project's subscriptions answer, never another project's and never a project-less one. Without it the first-party caller sees everything. `?kind=` picks a kind (an unknown one is an empty page, not an error); `cursor` is the opaque cursor from the previous page's `next_cursor`; `limit` defaults to 100 and is at most 1000. Pages are ordered by `(kind, id)`.
- Summary: `id`, `kind`, `service: "events"`, `project_id` (when the subscription is a project's), `owner` (when the consumer is a `usr_` subject), `name` (the topic pattern), `state` (`active` | `disabled`, from `enabled`), `created_at`. Every summary and every page is validated against the released `common.resource-*` schemas (`test/resource-index.test.js`).
- OVRN: present exactly when `openvibe-contracts`' `contracts.resources.nameOf` composes one — `ovrn:events:<prj_…>:subscription/sub_…` for a project-scoped subscription. A project-less subscription is a first-party one (a service consumer with no project): it has no project segment, so no name, and only the unscoped caller sees it at all. An event is not a resource (`evt_` is what `common.resource-name@1` refuses by design) and a project's queue is a derived carrier class, not a stored row, so `events.queue` joins the index only when Events stores a queue row.
- Errors: `400 resources.bad_query` for a value the index cannot honour (a project that is not a `prj_` id, a `limit` outside 1-1000, a cursor it did not issue) and `404 resources.unknown_resource` for an OVRN that names nothing it composes, both problem+json. Answers carry `Cache-Control: private, max-age=60`. The capability is `first-party`: a developer app's token is a `403`, as on every first-party route, and the two routes are not per-actor limited — they are one fan-out call each, not a browser's traffic.

## The fabric: carriers, the planner and explain

How an event type is carried is its delivery policy (`events.delivery-policy@1`, [ADR-042](https://github.com/OpenVibers/OpenVibe.Contracts/blob/main/docs/adr/ADR-042-events-fabric.md) decision 1). The carrier class — TOPIC (fan-out, no ack), QUEUE (competing consumers, ack) or STREAM (retained, ordered per key) — is a derived, internal label, never in the envelope. PostgreSQL stays the record: every event and every delivery is committed before any carrier sees it, and the worker's lease is the only thing that makes a send happen. A publish commits, then the chosen carrier is signalled; a lost or duplicated signal changes latency, never the outcome.

Carriers (`server/fabric/carriers`; `EVENTS_CARRIERS`, default `pg-v1,valkey-v1,nats-v1,nats-js-v1,cloud-v1`):

- `pg-v1` — the poll. Always present, every class; the worker's 500 ms tick and its in-process `kick()` carry delivery. It is the floor every delivery falls back to.
- `valkey-v1` — push, only when `VALKEY_URL` is set. **QUEUE:** after the commit, each new delivery is XADDed to a Valkey stream (`openvibe-sdk/queue`, consumer group `workers`); each Events process claims just that one row (`store.claimDelivery`, the same key-head and inflight-cap rules as the poll) and hands it to the worker's normal send — the item is acked either way, so a lost item only falls to the poll. **TOPIC:** the stored envelopes are PUBLISHed on a Valkey channel tagged with this process's id, and every process re-emits envelopes from other processes to its own SSE clients (deduped by event id), so an event published on one process reaches a browser connected to another. Health is an EWMA plus a breaker; while a breaker is open the planner excludes the carrier with a reason and `pg-v1` delivers.
- `nats-v1` — optional NATS Core, TOPIC only, only when `NATS_URL` is set (`nats://[user:pass@]host:4222`, loopback or the private network; no TLS; subjects under `NATS_SUBJECT_PREFIX`, default `ov.events.`). After the commit the event ids (never the envelopes) are published on `<prefix>fabric.topic`; every process loads those from other processes from PostgreSQL and re-emits the committed rows to its own SSE clients (an id that is not committed is ignored; deduped by event id; visibility applied by the receiving gateway). Core NATS keeps nothing: a lost message reaches a remote browser on its next resume (`Last-Event-ID` replays from PostgreSQL). While it is not connected, the broker refuses its publish or subscribe (a permissions violation; the handshake probes the subject, a refused connection is re-opened every 30 s) or its breaker is open, the planner excludes it with that reason and `valkey-v1` or `pg-v1` carries TOPIC; `EVENTS_CARRIERS_DISABLED=nats-v1` stops it connecting at all. During a rolling deploy, old processes drop `{ids}` messages and new ones drop old `{rows}` messages, so cross-process TOPIC fan-out falls back to resume-from-PostgreSQL until every process runs the new version. Tests: `eval "$(scripts/test-nats.sh up)"` (a broker with JetStream) sets `OV_TEST_NATS_URL` for `test/fabric-nats.test.js` and `test/fabric-jetstream.test.js`.
- `nats-js-v1` — optional JetStream, STREAM only, when `NATS_URL` is set and `NATS_JETSTREAM` is not `off` (nats-server 2.10+ with `-js`; `deploy/nats/nats-server.conf`). Spoken as JSON over the same dependency-free client (`server/fabric/nats-core.js`, request/reply), no client library. After the commit each new STREAM delivery's `{ event_id, subscription_id }` (never content) is published, with the stream's ack awaited, on `<prefix>fabric.stream.<key>` of the stream `NATS_STREAM` (default `OV_EVENTS_STREAM`, file storage, kept 24 h; `<key>` is a subject-safe hash of the ordering key). Every process running the delivery worker has its own durable push consumer (new messages only, explicit ack, one unacked message at a time) and, in stream order, claims exactly that row (`store.claimDelivery`: the key-head lease, `max_inflight` and due rules of the poll), sends it through the worker, then acks — whatever the claim answered. A message whose claim yields (another process or the poll has it, not its key's head yet, done, or never committed) leaves the row to the poll; a process that dies before its ack gets the message again after `ack_wait` and the claim, finding it delivered, sends nothing. Order per key is the store's, never the broker's. While it is not connected, the broker refuses it, the stream cannot be ensured (re-checked after every reconnect) or its breaker is open, the planner excludes it with that reason and `pg-v1` carries STREAM. The planner asks `events:durable` + `events:ordered` for STREAM, so only `nats-js-v1` and `pg-v1` can carry it; `rc-nats-js-v1` is cheaper than `rc-pg-v1`, so JetStream is preferred while healthy. Rollback: `EVENTS_CARRIERS_DISABLED=nats-js-v1` (or `NATS_JETSTREAM=off`) — it never connects and new STREAM deliveries go to `pg-v1`; rows already placed on it are delivered by the poll either way.
- `cloud-v1` — optional SQS-compatible cloud queue, QUEUE only, only when `CLOUD_QUEUE_URL` is set (an `http(s)` endpoint; SigV4 with `CLOUD_QUEUE_REGION`, `CLOUD_QUEUE_ACCESS_KEY`, `CLOUD_QUEUE_SECRET_KEY` — credentials may be omitted for a local endpoint — and an optional path prefix `CLOUD_QUEUE_PREFIX`; signed with node:crypto, sent with node's fetch, no dependency). After the commit each new delivery's `{ event_id, subscription_id }` (never content) is POSTed as an SQS `SendMessage`; nothing is consumed here, so the endpoint's consumer claims the row and this process's poll recovers what the signal lost. While the breaker is open on consecutive signal errors the planner excludes it with reason `breaker open (cloud-v1)` and `pg-v1`/`valkey-v1` carries QUEUE; `EVENTS_CARRIERS_DISABLED=cloud-v1` (or no `CLOUD_QUEUE_URL`) leaves it out. Its offer names `rc-cloud-queue-v1`, which is not in `server/fabric/rate-cards.json` until the owner sets its price: `openvibe-sdk/placement` prices an offer without a card at Infinity and never assumes a paid provider free, so `pg-v1`/`valkey-v1` keep carrying QUEUE until that card is published.

The planner (`server/fabric/planner.js`) calls `openvibe-sdk/placement` `plan()` per (carrier class, ordering key) with one `resource-offer@1` per adapter and the rate cards in `server/fabric/rate-cards.json`, caching decisions with hysteresis and recording a change in `delivery_placements`. A key's backlog stays on one carrier until it drains (a placement is pinned per (subscription, key), not per time window). An adapter named in `EVENTS_CARRIERS_DISABLED` is ineligible at once with reason `disabled by configuration` — the ADR's rollback; an unknown id in `EVENTS_CARRIERS` refuses to start.

Explain (operators only, `events.delivery.admin`, like `/api/v1/deliveries`): `GET /api/v1/placement?event_type=<t>[&key=<k>]` answers the carrier class, the resolved policy, the planner's `platform.placement-result@1` (reasons, and per candidate its eligibility / `excluded_because` / cost / latency) and the last recorded decision; `GET /api/v1/placement/deliveries/:event_id/:subscription_id` answers a delivery's carrier and the decision in force when it was created.

`delivery_semantics: at_most_once` makes a failed attempt final (`dead`, never retried); the default stays `at_least_once`.

## Developer apps

Roadmap Wave 20, ADR-014. A developer app gets a token from OpenVibe.Network (`POST /oauth/token`, `grant_type=client_credentials`, `audience=openvibe.events`) with `sub: app:app_<ULID>`, `project_id: prj_<ULID>`, `env: sandbox|production`. It uses the same routes as services, with these capabilities (all `public`, openvibe-contracts ≥ 0.28.0):

| Capability | Routes | Scope |
|---|---|---|
| `events.app.publish` | `POST /api/v1/events` | event types `app.<project_key>.<name>[.<more>]` only |
| `events.app.read` | `GET /api/v1/events`, `GET /api/v1/events/:id`, `/api/v1/checkpoints` | own project's events in the token's env, plus first-party `public` events |
| `events.app.subscribe` | `/api/v1/subscriptions…` (the app's own) | same as read; public https endpoints only |

**Names.** `project_key` is `p` followed by the project's ULID in lowercase: `prj_01JAB2C3D4E5F6G7H8J9K0MNPQ` → `p01jab2c3d4e5f6g7h8j9k0mnpq`. An app event's `source` is `app-` followed by the app's ULID in lowercase: `app:app_01JAB…` → `app-01jab…`. Both fit the existing `events.event-envelope@1` patterns, so the envelope contract did not change. `actor` is `{ "type": "app", "id": "app_<ULID>" }`, or the user in the token's `on_behalf_of`. First-party services can never publish `app.*` (the source names `app` and `app-*` are reserved and refused in `EVENTS_SOURCE_PREFIXES`).

```bash
EVENTS=https://openvibe.events
PK=p$(echo "${PRJ#prj_}" | tr 'A-Z' 'a-z')              # project_key
SRC=app-$(echo "${APP#app_}" | tr 'A-Z' 'a-z')         # source
curl -s -X POST "$EVENTS/api/v1/events" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
  "event_id": "evt_01JAB2C3D4E5F6G7H8J9K0MNPQ", "event_type": "app.'$PK'.order.created", "version": 1,
  "source": "'$SRC'", "actor": { "type": "app", "id": "'$APP'" }, "timestamp": "2026-09-23T12:00:00Z",
  "subject": { "type": "order", "id": "42" }, "payload": { "total": 3 } }'
curl -s "$EVENTS/api/v1/events?topic=app.$PK.*" -H "Authorization: Bearer $TOKEN"  # first page
curl -s "$EVENTS/api/v1/events?topic=app.$PK.*&after=$CURSOR" -H "Authorization: Bearer $TOKEN"  # CURSOR = previous next_cursor
curl -s -X POST "$EVENTS/api/v1/subscriptions" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{ "topic_pattern": "app.'$PK'.*", "endpoint": "https://hooks.example.com/openvibe" }'   # → secret, shown once
```

**Topic scope.** Every app pattern must start with a literal segment; an `app.*` pattern must name the app's own `project_key` (`app.<project_key>.*`); `*`, `*.created`, `app.*` and another project's key are `403 events.topic_not_allowed`. A first-party pattern (`live.*`) is allowed and yields only that namespace's `public` events.

**Sandbox.** Events store each app event's `project_id` and `env`. Apps see and receive only events of their own environment: sandbox events never reach production apps or production subscriptions, and production events never reach sandbox ones. First-party readers and subscribers never see sandbox events, and see production app events only through a pattern that starts with `app.`. Realtime (SSE) never streams app events. Sandbox events are pruned after `EVENTS_APP_SANDBOX_RETENTION_DAYS` (7).

**Webhook endpoints (SSRF guard, [server/egress.js](server/egress.js)).** An app subscription's endpoint must be `https`, without credentials, and its hostname must resolve only to public unicast addresses (loopback, RFC 1918, link-local and cloud metadata, CGNAT, multicast, documentation and benchmarking ranges, unique-local IPv6, and v4-in-v6 forms of any of those are refused). This is checked when the subscription is created and again on every delivery attempt, inside the connection's own DNS lookup, so the socket goes to the address that was checked. A refused address is a permanent failure (dead at once; replayable). Redirects are never followed (a `3xx` is a failed attempt). Deliveries carry the same `X-OpenVibe-Timestamp` and `X-OpenVibe-Signature-V2` headers as first-party ones.

**Quotas** (per project and environment, enforced here, `429 events.quota_exceeded` with `quota` = `publish_rate` (plus `Retry-After: 60`), `retained_bytes` or `subscriptions`):

| | production | sandbox |
|---|---|---|
| events per minute | `EVENTS_APP_PUBLISH_PER_MINUTE` = 120 | `EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE` = 30 |
| retained bytes (payload + actor + type + subject id of stored events) | `EVENTS_APP_RETAINED_BYTES` = 50 MiB | `EVENTS_APP_SANDBOX_RETAINED_BYTES` = 5 MiB |
| subscriptions | `EVENTS_APP_MAX_SUBSCRIPTIONS` = 20 | `EVENTS_APP_SANDBOX_MAX_SUBSCRIPTIONS` = 5 |

Quotas recorded in Network (`dev_quotas`) are not read yet: that needs `network.project.read`, which is still planned. Until then these defaults apply to every project.

**Revocation.** When Network's event relay delivers `network.app.revoked` (sent for every app of an archived project too), Events disables that app's subscriptions in the same transaction and refuses its tokens issued before the revocation (`401 token.revoked`). `network.grant.changed` that withdraws `events.app.subscribe` disables the app's subscriptions and refuses creating or re-enabling one with a token issued before the change. Without the relay, revoked apps' tokens still end within their 5-minute lifetime, but existing subscriptions keep delivering.

`EVENTS_APPS=off` turns the developer-app paths off (app tokens are then judged like any other token and refused).

**Usage** ([server/usage.js](server/usage.js), roadmap WS-N task 4). Each project's use is counted per environment and UTC hour in the transaction that does the work: `events.app.publish` in `events` (events stored; a repeated `event_id` is not counted) with every refused publish request as an error (its problem code, status, trace id and, when one event was refused, its `event_id`), and `events.app.subscribe` in `deliveries` (webhook attempts to the project's subscriptions; one without a 2xx is an error, `events.delivery.http_<status>`, `events.delivery.timeout` or `events.delivery.failed`, with the event id and its trace id). A minute after an hour closes, each rollup is stored once as an `events.usage.recorded` event (source `events`, subject the project, visibility `internal`, payload `common.usage-recorded@1` from openvibe-contracts 0.63.0) in the transaction that marks it sent, so Events is its own outbox; first-party subscribers get it like any event, apps never see it. OpenVibe.Network subscribes to it for the project dashboard on openvibe.services. `EVENTS_USAGE=off` counts nothing; `EVENTS_USAGE_FLUSH_MS` (300000) is how often closed hours are looked for. Sent rollups are kept 7 days in `app_usage`.

**Billing readings** ([server/billing.js](server/billing.js), plan T5 step 14; [docs/cutover-events-billing-readings.md](docs/cutover-events-billing-readings.md)). Each closed UTC hour, one aggregate query turns a production project's queue operations (each publish and each delivered webhook counts `ceil(bytes / 65536)`, at least 1) into one `platform.usage-sample@1` reading, metric `queue-operation-64kb`, `idempotency_key` `events:queue-operation-64kb:<project>:<hour>`, stored once in `billing_readings` and never edited, then posted to Billing (`billing.usage.record`, `POST /api/v1/usage`) with Events' Network service token. Off by default: `EVENTS_BILLING_INTERVAL_MS` (0) turns the loop on, `OV_BILLING_INTERNAL_URL` and `OV_OAUTH_CLIENT_SECRET` let it send (unset: readings stay queued). Sandbox and first-party traffic are not billed.

## Client library

`require('openvibe-events')` (package `main` is [lib/client.js](lib/client.js); services can depend on this repo by tarball):

```js
const events = require('openvibe-events');
const tokenClient = contracts.serviceAuth.createTokenClient({ tokenUrl: `${NETWORK}/oauth/token`, clientId, clientSecret, audience: 'openvibe.events' });

// Producer: transactional outbox in the service's own PostgreSQL (openvibe-sdk/db; the table from events.outboxSchema()
// goes in one of the service's migrations)
const outbox = events.createPgOutbox(db, { events: events.createPublisher({ eventsUrl: 'http://127.0.0.1:4300', tokenClient }) });
await db.tx(async (t) => {
    await t.query('UPDATE vods SET status = $1 WHERE id = $2', ['ready', id]);
    await outbox.enqueue(t, { event_type: 'media.vod.ready', source: 'media', actor, subject: { type: 'vod', id, revision }, payload });
});                       // the event exists if and only if the change committed
outbox.start();           // relay: publishes pending rows, marks them sent, backs off on failure

// Consumer: signed webhook (v2: signature plus a 300 s replay window) + exactly-once effects in the consumer's own database
const inbox = events.createPgInbox(db);                         // the table from events.inboxSchema()
app.post('/internal/events', express.json({ verify: (req, _res, buf) => { req.rawBody = buf; } }), async (req, res) => {
    if (!events.verifyDeliveryV2(req.rawBody, req.headers, SECRET)) return res.sendStatus(401);
    await inbox.once('live', req.body.event.event_id, async (t) => { /* writes on t */ });
    res.sendStatus(204);
});
```

`enqueue(t, …)` takes the transaction handle, so it cannot run outside one. `once()` records the receipt and runs the handler
in one transaction, so a crash before commit leaves nothing behind and the redelivery runs it again, and a crash after
commit makes the redelivery a no-op. The outbox and inbox are openvibe-sdk's PostgreSQL kits (ADR-042): this package
re-exports them and keeps no SQLite copies.

`createPgOrderedInbox(db)` adds a per-key head (the table from `events.inboxHeadsSchema()`, beside the inbox's): `apply(consumer, event, fn)`
records the receipt, moves the head of the subject's key (`events.subjectKey(event)`: `["<type>","<id>"]`, or its `sha256:` past 200
characters, never truncated) to `subject.revision` only if that is higher, and runs
`fn(t)`, all in one transaction. It answers `{ duplicate: true }`, `{ stale: true, head }` (an equal or older revision, for
example a dead delivery replayed after its successor: the receipt is kept and `fn` does not run, so answer 2xx) or
`{ stale: false, result }`. `{ key, revision }` overrides both; an event without them is only deduplicated. Concurrent
applies on one key serialise on its head row, so effects commit in increasing revisions.

## Realtime (SSE)

```js
let lastEventId = null;
async function connect() {
    try {
        // Network issues a fresh one-use ticket for each stream, including reconnects.
        const response = await fetch('https://openvibe.network/api/v1/realtime/ticket', {
            method: 'POST', headers: { Authorization: `Bearer ${networkJwt}` },
        });
        if (!response.ok) throw new Error(`ticket request failed: ${response.status}`);
        const { ticket, stream_url, topics } = await response.json();
        const url = new URL(stream_url);
        url.searchParams.set('topics', topics.join(','));
        url.searchParams.set('ticket', ticket);
        if (lastEventId != null) url.searchParams.set('last_event_id', lastEventId);
        const stream = new EventSource(url);
        stream.onmessage = (m) => { lastEventId = m.lastEventId; const { seq, event } = JSON.parse(m.data); };
        stream.addEventListener('gap', (m) => { /* events were missed: refetch state */ });
        stream.onerror = () => { stream.close(); setTimeout(connect, 1000); };
    } catch (err) {
        setTimeout(connect, 1000);
    }
}
void connect();
```

- Auth: a **realtime ticket** (`?ticket=`), the Network `ov_token` cookie or a Bearer user JWT (the `ov_token` cookie is scoped to its issuing host, and an EventSource cannot set an Authorization header, so a client on another site uses a ticket); a service token with `events.event.read`; or nobody (public events only, `REALTIME_ALLOW_ANONYMOUS`). An expired cookie degrades to anonymous; a bad Bearer is a 401.
- Realtime tickets (ADR-005 amendment 2): a page on any OpenVibe site cannot count on a cookie of openvibe.events (third-party there), and an EventSource cannot send a header. So it asks Network for a ticket (`POST https://openvibe.network/api/v1/realtime/ticket`, answering `network.realtime-ticket-result@1`) and opens `/realtime/stream?topics=network.notification.*&ticket=<ticket>` without credentials. The ticket is an RS256 JWT signed with Network's key (`identity.realtime-ticket-claims@1`): `iss <OV_NETWORK_ISSUER>/realtime`, `sub <usr_>`, `aud [openvibe.events]`, `typ` and `purpose` `realtime`, a lifetime of at most 300 s (Network mints 120 s) and `jti rtk_…`.
  - Events accepts each ticket once, never as Bearer or cookie, and never logs it. The refusals are 401: `ticket.invalid`, `ticket.expired` and `ticket.used`.
  - A reconnect asks for a new ticket and resumes with `last_event_id` (the cursor from the last message's SSE `id`).
  - Conversely, a user JWT that carries `typ` or `purpose` (a ticket, a FedCM assertion) is not a session here.
- A person's topic: `network.notification.created` (Network's outbox; visibility `subject`, subject the recipient). `topics=network.notification.*` streams a person's own notifications and nobody else's. There is no `user:<id>` topic: it is not a valid pattern (400 `realtime.bad_topic`), and subject visibility already does its job.
- Visibility: `public` events go to anyone subscribed to the topic; `subject` events only to the user whose subject id (`usr_…`) is the event's `actor.id` or its user `subject.id`; `internal` events never reach a browser. A guessed topic yields nothing.
- Resume: the SSE `id` is the cursor (ADR-042 decision 7), so the browser's automatic `Last-Event-ID` (or `?last_event_id=`) replays what was missed. Only a cursor is a position: anything else, a bare number included, is ignored and the stream starts at the head. A cursor older than both the hot store and the replay tier — or from another retention epoch (`reason: "epoch"`) — first gets `event: gap` (`{ reason, from_seq, to_seq }`), as does a replay that hits `REALTIME_REPLAY_MAX`.
- Public replay window: browsers (signed out or signed in) are replayed `public` events received in the last `REALTIME_PUBLIC_REPLAY_SECONDS` (300; 0 = none), enough to ride out a reconnect. Older public events are not replayed: the stream opens with `event: gap` (`reason: "public_window"`, `window_seconds`), and the client refetches state from the owning service. `subject` events addressed to the viewer, and service viewers, keep the whole retention. This keeps the stream from paging through a month of public history, chat lines included; nothing in the network needs more (no browser surface replays public events today).
- Redacted events are replayed as their tombstones ([Redaction](#redaction)).
- Heartbeat comment every 25 s; at most 20 topics per connection and `REALTIME_MAX_CONNECTIONS` (2000) overall; CORS with credentials for `https://*.openvibe.*` only.

## Owns

- `events`, `events_archive` (the replay tier), `subscriptions`, `deliveries`, `consumer_checkpoints`, `idempotency_receipts`, `app_revocations`, `app_usage`, `billing_readings`, `billing_periods` (PostgreSQL `ov_events`, ADR-035; the realtime fan-out is in-process, one Events process)
- developer-app event scope, sandbox separation and per-project Events quotas (ADR-014)
- canonical event envelope (event_id, trace_id, type, version, source, actor, subject + revision, payload)
- priority classes `critical|important|low`, loop guards, backpressure, DLQ and replay
- Realtime: SSE topics, `Last-Event-ID`/cursor resume, gap detection (WS and presence are not built yet)

## Does not own

- media bytes or game/media transport packets
- product business rules

## Depends on

- OpenVibe.Contracts (`events.event-envelope@1`, service tokens, problem details, ids)
- OpenVibe.Network (token signing key; user subject ids for topic authorization)

## Acceptance (must be true before "done")

- event persists before consumer delivery (`test/delivery.test.js`)
- kill a consumer mid-processing; replay creates exactly one effect (`test/outbox-inbox.test.js`)
- browser reconnect resumes from a cursor or reports a gap (`test/realtime.test.js`)
- a hot-pruned event stays readable through replay, a cursor across the tier boundary reports no false gap, two concurrent prunes move each event once, replay pruning answers `gap`, export → restore round-trips byte for byte, and `EVENTS_REPLAY_RETENTION_DAYS=0` behaves as before the tiers (`test/archive.test.js`)
- a guessed private topic yields no data (`test/realtime.test.js`)
- a developer app cannot publish, read or subscribe outside its project, sandbox never meets production, app webhooks reach public addresses only, quotas hold (`test/apps.test.js`)
- a project's publishing and deliveries are counted per hour, refusals and failed attempts as errors, and each closed hour is sent once as `events.usage.recorded`, never visible to apps (`test/usage.test.js`)
- each closed hour's queue operations per production project become one valid `platform.usage-sample@1` reading, rounded per 64 KiB, created once and never edited; the sender marks it sent on a 2xx, refuses it on a 4xx about the reading, keeps it pending on a 5xx, and two senders never post it twice; off by default (`test/billing.test.js`)

## Bootstrap / extraction source

Replaces the best-effort internal POSTs/webhooks between Live, Media, Network and Community. First migrations: notifications, then Media ready/failed, then stream lifecycle.

## Security

Reporting a vulnerability: [SECURITY.md](SECURITY.md). The rules the code keeps:

- **Auth.** Every API route needs a Network client-credentials token for audience `openvibe.events`,
  verified offline against the Network JWKS, holding that route's capability. App tokens are never
  judged on a first-party capability; sandbox tokens are accepted only on developer-app routes.
  Realtime accepts a single-use Network ticket, the `ov_token` cookie, a Bearer user JWT or a service
  token with `events.event.read`; a ticket is never a session and is never logged.
- **Private data.** `internal` events never reach a browser; `subject` events reach only that person;
  a guessed topic yields nothing; redacted events replay as tombstones. Developer apps see only their
  own project and environment.
- **Egress.** First-party subscription endpoints must be `http(s)` on `127.0.0.1` or an OpenVibe host
  (`EVENTS_ENDPOINT_HOSTS`); app webhooks must be `https` and resolve only to public addresses,
  checked at creation and inside every delivery's DNS lookup ([server/egress.js](server/egress.js));
  redirects are never followed.
- **Integrity.** Deliveries are signed per subscription with the v2 signature only
  (`X-OpenVibe-Signature-V2`; the v1 body-only header is gone); a subscription secret is shown once.
- **Exposure.** `/api/v1/deliveries` stays host-local and `/metrics` answers direct loopback callers
  only; nginx logs `/realtime/stream` without its query string
  (`deploy/nginx/log_format_events_noquery.conf`), so tickets never reach the access log.

## Deploy

Production deploys with `sudo ovhost deploy events` on the host (strategy `git-checkout`: fetch,
fast-forward `/opt/openvibe.events`, install on a lockfile change, restart, wait for `/api/ready`).
The unit is `openvibe-events.service` on `127.0.0.1:4300`, the env file `/etc/openvibe/events.env`. The database is
`ov_events` on the host's data role (`sudo /opt/openvibe.host/roles/data/add-service.sh events` writes its settings); the
release migrates it at boot. nginx serves `openvibe.events`, the API and product origin, from
[deploy/nginx/openvibe.events.conf](deploy/nginx/openvibe.events.conf) (the old `events.openvibe.network` address was retired
on 2026-10-10). ovhost treats open
realtime connections as a report-only drain, so `--wait-idle` waits for them.
Rollback: ovhost puts the previous sha back by itself when `/api/ready` does not answer 2xx after the
restart; afterwards `sudo ovhost rollback events --to <sha>`. Migrations only add tables and columns.
`EVENTS_LIMITS=off` turns the per-actor limits off without a deploy.

## Launch rule

This repository does not make the product real, and the domain keeps its placeholder page on
[OpenVibers/OpenVibe.Sites](https://github.com/OpenVibers/OpenVibe.Sites) until all of the
following exist here (plan §12.12):

1. an owning runtime with health/readiness endpoints and observability;
2. canonical identity/auth integration (OpenVibe.Network subjects, scoped service principals);
3. server-rendered or static public routes that are useful without JavaScript;
4. real persistence and end-to-end workflows;
5. capability and event registration against `OpenVibe.Contracts`;
6. a migration/seed strategy, a security/threat review, and sitemap/robots/feed behaviour;
7. acceptance tests proving the advertised functionality.

The launch release removes the domain from `OpenVibe.Sites/sites.json`, switches routing and
registers maturity in the ecosystem registry atomically. A placeholder is never counted as an
implemented service.

---

Part of the [OpenVibe network](https://openvibe.network). Built in the open by [OpenVibers](https://github.com/OpenVibers).

<!-- versions:start -->
- openvibe-contracts: v0.132.0
- openvibe-sdk: v0.43.0
- openvibe-shared: v3.0.1
<!-- versions:end -->
