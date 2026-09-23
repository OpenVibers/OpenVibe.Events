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

Node 22 in production (`fnm exec --using=22.22.1 npm test`). Production: `/opt/openvibe.events`, env `/etc/openvibe/events.env`, unit [deploy/systemd/openvibe-events.service](deploy/systemd/openvibe-events.service), store `/var/lib/openvibe-events/events.db`, nginx [deploy/nginx/events.openvibe.network.conf](deploy/nginx/events.openvibe.network.conf) (public vhost exposes `/realtime/stream` and health only; services on the host call `127.0.0.1:4300`).

`GET /api/health` is liveness. `GET /api/ready` is 200 only when the database answers, the delivery worker runs and the Network signing key has loaded (it retries every 30 s while Network boots).

## Auth

Services call with an OpenVibe.Network client-credentials token (`POST /oauth/token`, `audience=openvibe.events`), verified offline against the Network JWKS. Each route checks one capability:

| Capability | Routes |
|---|---|
| `events.event.publish` | `POST /api/v1/events` |
| `events.subscription.manage` | `/api/v1/subscriptions…` (own subscriptions only) |
| `events.event.read` | `GET /api/v1/events`, `GET /api/v1/events/:id`, `/api/v1/checkpoints`, realtime as a service |
| `events.delivery.admin` | `GET /api/v1/deliveries`, `POST /api/v1/deliveries/replay` |

These ids are not in `openvibe-contracts` yet; their manifests are proposed in [docs/capabilities-proposal/](docs/capabilities-proposal/) (with the service manifest). Until the contracts release defines them, `server/auth.js` grants them with the contracts rule (exact id or a `family.*` grant) and hands the decision to `capabilities.check()` as soon as contracts know the id.

Errors are RFC 9457 problem+json (`openvibe-contracts` `http.problem`) with a stable `code`.

## Publishing

`POST /api/v1/events` takes one `events.event-envelope@1` envelope, or `{ "events": [ … ] }` (up to 100, stored atomically).

- `source` must be the calling service (`svc:live` publishes `source: "live"`), and `event_type` must start with a prefix that source owns (`live.*`, `media.*`, `network.*`, `community.*`, `chat.*`, `openre.*`, `billing.*`, `tips.*`, `vip.*`, `ai.*`, `games.*`, `tools.*`, `codes.*`, `host.*`; `EVENTS_SOURCE_PREFIXES` overrides).
- Idempotent on `event_id`: a repeat answers `200 { event_id, seq, duplicate: true }` and is never stored twice (even after retention, for `EVENTS_RECEIPT_RETENTION_DAYS`).
- Missing `trace_id` is taken from the request's `traceparent`; `priority` defaults to `important`, `visibility` to `internal`.
- Loop guard: an event whose trace already carries 8 hops (the cross-service chain depth, or the same source/type/subject repeating in the trace) is refused with `409 events.loop_detected`.
- Each accepted event gets a global `seq` and is committed with one delivery row per matching subscription before anything is sent.

## Subscriptions and delivery

```http
POST /api/v1/subscriptions
{ "topic_pattern": "media.vod.*", "endpoint": "http://127.0.0.1:3000/internal/events", "retry_policy": { "max_attempts": 8 } }
→ 201 { "id": "sub_…", "secret": "whsec_…", … }     # the secret is shown once
```

Topic patterns are dot-separated segments where `*` stands for one or more whole segments (`media.vod.*`, `*.created`, `media.*.ready`, `*`). Endpoints must be `http(s)` on `127.0.0.1` or `openvibe.<tld>`/its subdomains (`EVENTS_ENDPOINT_HOSTS`); redirects are never followed.

Each delivery is `POST <endpoint>` with body `{ "event": <envelope>, "seq": n }` and headers `X-OpenVibe-Event-Id`, `X-OpenVibe-Event-Type`, `X-OpenVibe-Seq`, `X-OpenVibe-Subscription-Id`, `X-OpenVibe-Delivery-Attempt`, `X-OpenVibe-Signature: sha256=<HMAC-SHA256 of the raw body with the subscription secret>` and `traceparent` (the event's trace). Any 2xx is delivered. Anything else is retried after 1 s, 5 s, 30 s, 2 min, 10 min, 1 h (then hourly) up to 8 attempts, after which the delivery is `dead`. Priority classes go first (`critical`, `important`, `low`, then seq); one delivery per subscription is in flight at a time and at most 20 overall. Delivery is at least once and not strictly ordered; consumers dedupe with an inbox and order with `subject.revision`.

Operators: `GET /api/v1/deliveries?status=dead` is the dead-letter queue; `POST /api/v1/deliveries/replay { subscription_id, event_ids: [...] }` or `{ subscription_id, from_seq }` requeues retained events (that is also how a new subscription catches up on history).

Pull consumers: `GET /api/v1/events?topic=media.vod.*&after_seq=<cursor>&limit=100` returns `{ events: [{ seq, event }], next_after_seq, latest_seq }`, plus `gap: { from_seq, to_seq }` when the cursor is older than retention. `PUT /api/v1/checkpoints { topic, cursor }` stores a consumer's cursor here if it has nowhere better.

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

// Consumer: signed webhook + exactly-once effects in the consumer's own database
const inbox = events.createInbox(db);
inbox.ensureSchema();
app.post('/internal/events', express.json({ verify: (req, _res, buf) => { req.rawBody = buf; } }), (req, res) => {
    if (!events.verifyDelivery(req.rawBody, req.get('X-OpenVibe-Signature'), SECRET)) return res.sendStatus(401);
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

- `events`, `subscriptions`, `deliveries`, `consumer_checkpoints`, `idempotency_receipts` (SQLite today; the plan's PostgreSQL + Redis fanout is a later step)
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
