# OpenVibe.Events

> Durable events, subscriptions, delivery, retry, dead letters and replay for the network.

**Status:** alpha (roadmap Wave 3). Runs and is tested; no service publishes to it in production yet.  
**Domain:** `events.openvibe.network` (the realtime gateway is served here too until ADR-005 decides where `realtime.openvibe.network` lives)  
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

Node 22 in production (`fnm exec --using=22.22.1 npm test`). Production: `/opt/openvibe.events`, env `/etc/openvibe/events.env`, unit [deploy/systemd/openvibe-events.service](deploy/systemd/openvibe-events.service), store `/var/lib/openvibe-events/events.db`, nginx [deploy/nginx/events.openvibe.network.conf](deploy/nginx/events.openvibe.network.conf) (public vhost exposes `/realtime/stream`, health, and the token-guarded `/api/v1/events`, `/api/v1/subscriptions` and `/api/v1/checkpoints` for developer apps; `/api/v1/deliveries` stays host-local; services on the host call `127.0.0.1:4300`).

`GET /api/health` is liveness. `GET /api/ready` (openvibe-shared/ready) is 200 only when every required check passes — `db` (a real query), `network_jwks` (the Network signing key has loaded; it retries every 30 s while Network boots) and `delivery_worker` (when `EVENTS_WORKER` is on) — and 503 otherwise, with `failed: [...]`. The optional `dlq` check fails once more than `EVENTS_DLQ_DEGRADED_AT` (default 100) deliveries are dead: the service stays ready (200) and reports `status: "degraded"`, `degraded: ["dlq"]`. Each check carries `status`, `required`, `latency_ms`, `checked_at` and, for `dlq`, `detail: { depth, threshold }`; `latest_seq`, `deliveries`, `worker` and `realtime_connections` are still in the body. **Shape change (Track O):** `checks` used to be booleans (`{ db, worker, key }`); they are now objects keyed `db`, `network_jwks`, `delivery_worker`, `dlq`.

`GET /metrics` (Prometheus text, direct loopback callers only — a request carrying `X-Forwarded-For` gets 404, and nginx refuses the path): HTTP golden signals by route template (`http_requests_total`, `http_request_duration_seconds`, `http_requests_in_flight`; SSE sessions excluded), process metrics, `release_info`, and `events_deliveries{status}`, `events_dlq_depth`, `events_latest_seq`, `events_realtime_connections`, `events_delivery_latency_seconds{priority}` (acceptance → successful delivery, retries included), `events_delivery_attempts_total{outcome}`. `GET /release.json` is the deployed release (ADR-016).

## Auth

Services call with an OpenVibe.Network client-credentials token (`POST /oauth/token`, `audience=openvibe.events`), verified offline against the Network JWKS. Each route checks one capability:

| Capability | Routes |
|---|---|
| `events.event.publish` | `POST /api/v1/events` |
| `events.subscription.manage` | `/api/v1/subscriptions…` (own subscriptions only) |
| `events.event.read` | `GET /api/v1/events`, `GET /api/v1/events/:id`, `/api/v1/checkpoints`, realtime as a service |
| `events.delivery.admin` | `GET /api/v1/deliveries`, `POST /api/v1/deliveries/replay` |

These four are `internal` in `openvibe-contracts` (never granted to developer apps). Developer apps use the three `public` capabilities in [Developer apps](#developer-apps) instead, on the same routes. `server/auth.js` grants with the contracts rule (exact id or a `family.*` grant) and hands the decision to `capabilities.check()` for every id the installed contracts know (v0.28.0 knows all seven).

Sandbox tokens (`env: sandbox`, developer apps only) are accepted only on the developer-app routes; every other route answers `401 token.sandbox_refused`. App tokens are never judged on a first-party capability: an app token on an operator route is a `403`.

Errors are RFC 9457 problem+json (`openvibe-contracts` `http.problem`) with a stable `code`.

## Publishing

`POST /api/v1/events` takes one `events.event-envelope@1` envelope, or `{ "events": [ … ] }` (up to 100, stored atomically).

- `source` must be the calling service (`svc:live` publishes `source: "live"`), and `event_type` must start with a prefix that source owns (`live.*`, `media.*`, `network.*`, `community.*`, `chat.*`, `openre.*`, `billing.*`, `tips.*`, `vip.*`, `ai.*`, `games.*`, `tools.*`, `codes.*`, `host.*`; `EVENTS_SOURCE_PREFIXES` overrides).
- Idempotent on `event_id`: a repeat answers `200 { event_id, seq, duplicate: true }` and is never stored twice (even after retention, for `EVENTS_RECEIPT_RETENTION_DAYS`).
- Missing `trace_id` is taken from the request's `traceparent`; `priority` defaults to `important`, `visibility` to `internal`.
- Loop guard: an event whose trace already carries 8 hops (the cross-service chain depth, or the same source/type/subject repeating in the trace) is refused with `409 events.loop_detected`. The chain depth of a first-party publish counts first-party events only (developer-app events in the same trace never count toward it), so an app cannot poison a trace it has seen.
- Each accepted event gets a global `seq` and is committed with one delivery row per matching subscription before anything is sent.

## Subscriptions and delivery

```http
POST /api/v1/subscriptions
{ "topic_pattern": "media.vod.*", "endpoint": "http://127.0.0.1:3000/internal/events", "retry_policy": { "max_attempts": 8 } }
→ 201 { "id": "sub_…", "secret": "whsec_…", … }     # the secret is shown once
```

Topic patterns are dot-separated segments where `*` stands for one or more whole segments (`media.vod.*`, `*.created`, `media.*.ready`, `*`). Endpoints must be `http(s)` on `127.0.0.1` or `openvibe.<tld>`/its subdomains (`EVENTS_ENDPOINT_HOSTS`); redirects are never followed.

Each delivery is `POST <endpoint>` with body `{ "event": <envelope>, "seq": n }` and headers `X-OpenVibe-Event-Id`, `X-OpenVibe-Event-Type`, `X-OpenVibe-Seq`, `X-OpenVibe-Subscription-Id`, `X-OpenVibe-Delivery-Attempt`, `X-OpenVibe-Signature: sha256=<HMAC-SHA256 of the raw body with the subscription secret>`, `X-OpenVibe-Timestamp: <unix seconds>`, `X-OpenVibe-Signature-V2: t=<that timestamp>,v2=<HMAC-SHA256 of "<t>.<raw body>">` and `traceparent` (the event's trace). Any 2xx is delivered. Anything else is retried after 1 s, 5 s, 30 s, 2 min, 10 min, 1 h (then hourly) up to 8 attempts, after which the delivery is `dead`. Priority classes go first (`critical`, `important`, `low`, then seq); one delivery per subscription is in flight at a time and at most 20 overall. Delivery is at least once and not strictly ordered; consumers dedupe with an inbox and order with `subject.revision`.

**Replay window (signature v2).** v1 signs only the body, so a captured delivery verifies forever (the inbox's `event_id` dedupe is all that stops a replay). v2 signs the timestamp together with the body, and every attempt, retries included, is signed afresh with the time it is sent. Consumers check it with `verifyDeliveryV2(raw, headers, secret, { toleranceSec = 300, now })` (here) or openvibe-sdk ≥ 0.4.0 `parseDelivery(raw, headers, secret, { requireV2: true })` and reject anything more than 300 s from their clock either way. A v2 header that is present but wrong or stale is a failure: never fall back to v1 then. Rollout: Events sends both headers first, then each consumer requires v2; v1 stays on the wire until every consumer does. See [docs/replay-window-rollout.md](docs/replay-window-rollout.md).

Operators: `GET /api/v1/deliveries?status=dead` is the dead-letter queue; `POST /api/v1/deliveries/replay { subscription_id, event_ids: [...] }` or `{ subscription_id, from_seq }` requeues retained events (that is also how a new subscription catches up on history).

Pull consumers: `GET /api/v1/events?topic=media.vod.*&after_seq=<cursor>&limit=100` returns `{ events: [{ seq, event }], next_after_seq, latest_seq }`, plus `gap: { from_seq, to_seq }` when the cursor is older than retention. `PUT /api/v1/checkpoints { topic, cursor }` stores a consumer's cursor here if it has nowhere better.

## Developer apps

Roadmap Wave 20, ADR-014. A developer app gets a token from OpenVibe.Network (`POST /oauth/token`, `grant_type=client_credentials`, `audience=openvibe.events`) with `sub: app:app_<ULID>`, `project_id: prj_<ULID>`, `env: sandbox|production`. It uses the same routes as services, with these capabilities (all `public`, openvibe-contracts ≥ 0.28.0):

| Capability | Routes | Scope |
|---|---|---|
| `events.app.publish` | `POST /api/v1/events` | event types `app.<project_key>.<name>[.<more>]` only |
| `events.app.read` | `GET /api/v1/events`, `GET /api/v1/events/:id`, `/api/v1/checkpoints` | own project's events in the token's env, plus first-party `public` events |
| `events.app.subscribe` | `/api/v1/subscriptions…` (the app's own) | same as read; public https endpoints only |

**Names.** `project_key` is `p` followed by the project's ULID in lowercase: `prj_01JAB2C3D4E5F6G7H8J9K0MNPQ` → `p01jab2c3d4e5f6g7h8j9k0mnpq`. An app event's `source` is `app-` followed by the app's ULID in lowercase: `app:app_01JAB…` → `app-01jab…`. Both fit the existing `events.event-envelope@1` patterns, so the envelope contract did not change. `actor` is `{ "type": "app", "id": "app_<ULID>" }`, or the user in the token's `on_behalf_of`. First-party services can never publish `app.*` (the source names `app` and `app-*` are reserved and refused in `EVENTS_SOURCE_PREFIXES`).

```bash
EVENTS=https://events.openvibe.network
PK=p$(echo "${PRJ#prj_}" | tr 'A-Z' 'a-z')              # project_key
SRC=app-$(echo "${APP#app_}" | tr 'A-Z' 'a-z')         # source
curl -s -X POST "$EVENTS/api/v1/events" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
  "event_id": "evt_01JAB2C3D4E5F6G7H8J9K0MNPQ", "event_type": "app.'$PK'.order.created", "version": 1,
  "source": "'$SRC'", "actor": { "type": "app", "id": "'$APP'" }, "timestamp": "2026-09-23T12:00:00Z",
  "subject": { "type": "order", "id": "42" }, "payload": { "total": 3 } }'
curl -s "$EVENTS/api/v1/events?topic=app.$PK.*&after_seq=0" -H "Authorization: Bearer $TOKEN"
curl -s -X POST "$EVENTS/api/v1/subscriptions" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{ "topic_pattern": "app.'$PK'.*", "endpoint": "https://hooks.example.com/openvibe" }'   # → secret, shown once
```

**Topic scope.** Every app pattern must start with a literal segment; an `app.*` pattern must name the app's own `project_key` (`app.<project_key>.*`); `*`, `*.created`, `app.*` and another project's key are `403 events.topic_not_allowed`. A first-party pattern (`live.*`) is allowed and yields only that namespace's `public` events.

**Sandbox.** Events store each app event's `project_id` and `env`. Apps see and receive only events of their own environment: sandbox events never reach production apps or production subscriptions, and production events never reach sandbox ones. First-party readers and subscribers never see sandbox events, and see production app events only through a pattern that starts with `app.`. Realtime (SSE) never streams app events. Sandbox events are pruned after `EVENTS_APP_SANDBOX_RETENTION_DAYS` (7).

**Webhook endpoints (SSRF guard, [server/egress.js](server/egress.js)).** An app subscription's endpoint must be `https`, without credentials, and its hostname must resolve only to public unicast addresses (loopback, RFC 1918, link-local and cloud metadata, CGNAT, multicast, documentation and benchmarking ranges, unique-local IPv6, and v4-in-v6 forms of any of those are refused). This is checked when the subscription is created and again on every delivery attempt, inside the connection's own DNS lookup, so the socket goes to the address that was checked. A refused address is a permanent failure (dead at once; replayable). Redirects are never followed (a `3xx` is a failed attempt). Deliveries are signed exactly like first-party ones (`X-OpenVibe-Signature`, and `X-OpenVibe-Timestamp` with `X-OpenVibe-Signature-V2`).

**Quotas** (per project and environment, enforced here, `429 events.quota_exceeded` with `quota` = `publish_rate` (plus `Retry-After: 60`), `retained_bytes` or `subscriptions`):

| | production | sandbox |
|---|---|---|
| events per minute | `EVENTS_APP_PUBLISH_PER_MINUTE` = 120 | `EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE` = 30 |
| retained bytes (payload + actor + type + subject id of stored events) | `EVENTS_APP_RETAINED_BYTES` = 50 MiB | `EVENTS_APP_SANDBOX_RETAINED_BYTES` = 5 MiB |
| subscriptions | `EVENTS_APP_MAX_SUBSCRIPTIONS` = 20 | `EVENTS_APP_SANDBOX_MAX_SUBSCRIPTIONS` = 5 |

Quotas recorded in Network (`dev_quotas`) are not read yet: that needs `network.project.read`, which is still planned. Until then these defaults apply to every project.

**Revocation.** When Network's event relay delivers `network.app.revoked` (sent for every app of an archived project too), Events disables that app's subscriptions in the same transaction and refuses its tokens issued before the revocation (`401 token.revoked`). `network.grant.changed` that withdraws `events.app.subscribe` disables the app's subscriptions and refuses creating or re-enabling one with a token issued before the change. Without the relay, revoked apps' tokens still end within their 5-minute lifetime, but existing subscriptions keep delivering.

`EVENTS_APPS=off` turns the developer-app paths off (app tokens are then judged like any other token and refused).

## Client library

`require('openvibe-events')` (package `main` is [lib/client.js](lib/client.js); services can depend on this repo by tarball):

```js
const events = require('openvibe-events');
const tokenClient = contracts.serviceAuth.createTokenClient({ tokenUrl: `${NETWORK}/oauth/token`, clientId, clientSecret, audience: 'openvibe.events' });

// Producer: transactional outbox in the service's own better-sqlite3 database
const outbox = events.createOutbox(db, { publisher: events.createPublisher({ eventsUrl: 'http://127.0.0.1:4300', tokenClient }) });
outbox.ensureSchema();
db.transaction(() => {
    db.prepare('UPDATE vods SET status = ? WHERE id = ?').run('ready', id);
    outbox.enqueue({ event_type: 'media.vod.ready', source: 'media', actor, subject: { type: 'vod', id, revision }, payload });
})();                     // the event exists if and only if the change committed
outbox.start();           // relay: publishes pending rows, marks them sent, backs off on failure

// Consumer: signed webhook (v2: signature plus a 300 s replay window) + exactly-once effects in the consumer's own database
const inbox = events.createInbox(db);
inbox.ensureSchema();
app.post('/internal/events', express.json({ verify: (req, _res, buf) => { req.rawBody = buf; } }), (req, res) => {
    if (!events.verifyDeliveryV2(req.rawBody, req.headers, SECRET)) return res.sendStatus(401);
    inbox.once('live', req.body.event.event_id, () => { /* synchronous db writes */ });
    res.sendStatus(204);
});
```

`enqueue()` refuses to run outside a transaction. `once()` records the receipt and runs the handler in one SQLite transaction, so a crash before commit leaves nothing behind and the redelivery runs it again, and a crash after commit makes the redelivery a no-op.

## Realtime (SSE)

```js
const es = new EventSource('https://events.openvibe.network/realtime/stream?topics=live.stream.*,network.notification.*', { withCredentials: true });
es.onmessage = (m) => { const { seq, event } = JSON.parse(m.data); };
es.addEventListener('gap', (m) => { /* events were missed: refetch state */ });
```

- Auth: the Network `ov_token` cookie or a Bearer user JWT; a service token with `events.event.read`; or nobody (public events only, `REALTIME_ALLOW_ANONYMOUS`). An expired cookie degrades to anonymous; a bad Bearer is a 401.
- Visibility: `public` events go to anyone subscribed to the topic; `subject` events only to the user whose subject id (`usr_…`) is the event's `actor.id` or its user `subject.id`; `internal` events never reach a browser. A guessed topic yields nothing.
- Resume: the SSE `id` is the seq, so the browser's automatic `Last-Event-ID` (or `?last_event_id=`) replays what was missed. A cursor older than retention first gets `event: gap` (`{ reason, from_seq, to_seq }`), as does a replay that hits `REALTIME_REPLAY_MAX`.
- Heartbeat comment every 25 s; at most 20 topics per connection and `REALTIME_MAX_CONNECTIONS` (2000) overall; CORS with credentials for `https://*.openvibe.*` only.

## Owns

- `events`, `subscriptions`, `deliveries`, `consumer_checkpoints`, `idempotency_receipts`, `app_revocations` (SQLite today; the plan's PostgreSQL + Redis fanout is a later step)
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
- a guessed private topic yields no data (`test/realtime.test.js`)
- a developer app cannot publish, read or subscribe outside its project, sandbox never meets production, app webhooks reach public addresses only, quotas hold (`test/apps.test.js`)

## Bootstrap / extraction source

Replaces the best-effort internal POSTs/webhooks between Live, Media, Network and Community. First migrations: notifications, then Media ready/failed, then stream lifecycle.

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
