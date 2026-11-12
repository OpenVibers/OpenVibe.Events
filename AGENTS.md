# AGENTS.md — OpenVibe.Events

## Project overview

The durable event backbone of the network: durable events, subscriptions, signed delivery, retry, dead
letters and replay, plus the Realtime SSE plane (ADR-005 put Realtime inside Events). Every authoritative
service writes its domain change and its outbox row in one transaction; a relay publishes here; consumers
use inbox/idempotency receipts; browsers get authorized, resumable projections. Node.js/Express,
PostgreSQL (`ov_events`), CommonJS, **no build step**.

- Service id `events`, port `4300`, origin `https://openvibe.events` (API and product origin), unit
  `openvibe-events.service`, release root `/opt/openvibe.events`. The old `events.openvibe.network` was
  retired on 2026-10-10.
- `package.json` `main` is [lib/client.js](lib/client.js) — the client library services import — while
  `npm start` runs [server/index.js](server/index.js).

## What it owns

- Tables: `events`, `events_archive` (the replay tier), `subscriptions`, `deliveries`,
  `consumer_checkpoints`, `idempotency_receipts`, `app_revocations`, `app_usage`, `billing_readings`,
  `billing_periods`, `telemetry_samples`; the realtime fan-out is in-process (one Events process).
- The canonical event envelope (event_id, trace_id, type, version, source, actor, subject + revision,
  payload), visibility (`public|internal|subject`), priority classes `critical|important|low`, loop
  guards, backpressure, the DLQ and replay.
- Developer-app event scope, sandbox separation and per-project quotas (ADR-014).
- Realtime: SSE topics, `Last-Event-ID`/cursor resume and gap detection (WebSocket and presence are not
  built).
- The resource index (`/api/v1/resources`), the fabric (carriers, planner, explain), the home page and its
  stats, and the developer limits (`/limits.json`).
- Retention: three tiers (hot window, archive/replay, then deletion), checked at boot and in the config.

Not owned: the domain events themselves (each service), identity (Network), the consumers' inboxes, the
service that decides what is private (Events enforces visibility, it does not invent it).

## Who calls it, and whom it calls

- Callers: every service with a Network client-credentials token audience `openvibe.events`; developer
  apps; browsers on the realtime stream with a ticket/cookie/Bearer; services reading the resource index.
- Depends on: **OpenVibe.Contracts** (`events.event-envelope@1`, service tokens, problem details, ids)
  and **OpenVibe.Network** (the JWKS signing key and user subject ids for topic authorization). No
  product service is required for delivery.

## Repository map

- **Entry:** [server/index.js](server/index.js) — store, keys, config, worker and realtime; graceful stop.
- **App:** [server/app.js](server/app.js) — `/auth`, `/shared`, the `/api` routers, realtime, health/
  readiness, metrics, home, discovery, legal and release files.
- **API:** [server/api/](server/api/) — `publish.js`, `subscriptions.js`, `read.js`, `policies.js`,
  `placement.js`, `resources.js`, `resource-control.js`.
- **Fabric:** [server/fabric/](server/fabric/) — `planner.js`, `signals.js`, `policy.js`, `nats-core.js`,
  `rate-cards.json`.
- **Delivery/store:** [server/egress.js](server/egress.js), the store/worker modules, `server/realtime.js`,
  [server/home.js](server/home.js), [server/home-stats.js](server/home-stats.js),
  [server/discovery.js](server/discovery.js), [server/openapi.js](server/openapi.js).
- **Client library:** [lib/client.js](lib/client.js) (also shipped to services through the SDK's events
  client); [server/endpoints.js](server/endpoints.js), [server/limits.js](server/limits.js).
- **Migrations:** [migrations/](migrations/) (events, subscriptions, deliveries, checkpoints, receipts,
  app usage/billing, telemetry).
- **Deploy:** [deploy/systemd/openvibe-events.service](deploy/systemd/openvibe-events.service),
  [deploy/nginx/openvibe.events.conf](deploy/nginx/openvibe.events.conf).

## Commands

```bash
npm ci
cp .env.example .env
npm run dev                # http://127.0.0.1:4300
npm start                  # node server/index.js
npm test                   # test/run.js: every file in its own process, temp databases
npm run test:pg            # same on PostgreSQL (EVENTS_TEST_STORE=pg)
npm test -- delivery       # filter by file name; --strict makes a skip fail
ov test                    # the harness's sharded run of what the change reaches
```

Node 22 in production (`fnm exec --using=22.22.1 npm test`).

## Database and migrations

`ov_events` on the host's data role (ADR-035); temp/PGlite databases in tests. Async through
`openvibe-sdk/db`; migrations `NNNN_*.sql` applied at boot; never edit an applied one, add a new number.
The event and archive tables are append-only; a delivery is claimed/updated, an event is not rewritten
except for a redaction tombstone. The delivery worker is the only writer of `deliveries` state; a replay
never mutates the original event.

## Auth and capabilities

- Services use a Network client-credentials token (audience `openvibe.events`), verified offline against
  the JWKS with `openvibe-sdk/auth` `verifyServiceToken` and the pinned contracts rules. People use
  `verifyUserToken`, which refuses service principals and typed tokens.
- Capabilities: `events.event.publish`, `events.subscription.manage` (own subscriptions only),
  `events.event.read` (+ checkpoints/realtime), `events.delivery.admin`, `events.resource.read`
  (`first-party`). Developer apps get the public app capabilities on the same routes; a sandbox token is
  accepted **only** on developer-app routes (`401 token.sandbox_refused` elsewhere); an app token is never
  judged on a first-party capability.
- Realtime accepts a **single-use Network ticket**, the `ov_token` cookie, a Bearer user JWT or a service
  token with `events.event.read`; a ticket is never a session and is never logged.
- `/api/v1/deliveries` stays host-local; `/metrics` answers direct loopback callers only; nginx refuses
  both paths publicly.

## Frontend loading model

The only page is the server-rendered home (`server/home.js`): the bus at work from this service's own
tables (counts and names only — never a payload, actor, subject id or principal), with a clear empty state
and a 1.5 s bound; `/` is HTML by default, and the text API index answers `Accept: text/plain` or
`?format=text`. Discovery (`robots.txt`, `sitemap.xml`, `llms.txt`) and the JSON-LD `ItemList`/`FAQPage`
come from the same data. There is no SPA, bundler or transpiler; `openvibe-shared` supplies the Frame and
`/shared/*` from the pinned node_modules. The realtime stream is SSE at `/realtime/stream`.

## Deploy

`sudo ovhost deploy events` (git-checkout: fetch, fast-forward `/opt/openvibe.events`, install on a
lockfile change, restart, wait for `/api/ready`). Unit `openvibe-events.service` on
`127.0.0.1:4300`, env `/etc/openvibe/events.env`; nginx
[deploy/nginx/openvibe.events.conf](deploy/nginx/openvibe.events.conf) exposes the home/discovery,
`/realtime/stream`, health and the token-guarded developer API; `/api/v1/deliveries` stays host-local.
ovhost treats open realtime connections as a report-only drain (`--wait-idle` waits for them). Rollback
`sudo ovhost rollback events --to <sha>`; migrations only add tables and columns. `EVENTS_LIMITS=off`
turns the per-actor limits off without a deploy.

## Tests

`npm test` runs each `test/*.test.js` in its own process with temp databases and in-process Network keys;
`npm run test:pg` runs the same on PostgreSQL. The suite covers publish/envelope/validation, visibility and
redaction, subscriptions and endpoint egress rules, signed v2 delivery and the replay window, retries/
backoff, DLQ and replay, checkpoints/cursors, developer apps and sandbox separation, realtime resume/gap,
the resource index and the home stats bounds. `ov test` runs the same.

## Conventions

- CommonJS, 4-space indent, single quotes, semicolons; factories with injectable config/store/clock/fetch/
  log; `openvibe-contracts` shapes for the envelope, errors and ids.
- Errors are RFC 9457 problem+json with stable codes.
- Priority is data (`critical|important|low`), not a new queue; retries use exponential backoff; a
  delivery attempt is idempotent by delivery id.
- Sign with **v2 only** (`X-OpenVibe-Signature-V2`); a subscription secret is shown once.

## Pitfalls

- **`internal` events never reach a browser; `subject` events reach only that person.** A guessed topic
  returns nothing; do not "helpfully" widen a topic.
- **Redacted events replay as tombstones**, never as the original payload.
- **Endpoints are allow-listed:** first-party subscriptions must be `http(s)` on `127.0.0.1` or an
  OpenVibe host (`EVENTS_ENDPOINT_HOSTS`); app webhooks must be public https, checked at creation and at
  every delivery's DNS lookup; **redirects are never followed**.
- **A sandbox token is refused everywhere but the developer-app routes**; an app token is never judged on
  a first-party capability.
- **The delivery worker is the only mutator of delivery state**; a replay is a new delivery, not an edit.
- **`/api/v1/deliveries` is host-local and `/metrics` is loopback-only.** Do not add them to the vhost.
- **The home page leaves only counts and names.** No payload, actor, subject id or principal in the stats.
- **Don't wait on the database for the shell:** the stats read has a 1.5 s bound and a clear empty state;
  a slow read must not take the page down.
- **One Events process owns the realtime fan-out.** A second process needs a shared fan-out design first.

## Documentation

- [README.md](README.md) — purpose, run, auth/capabilities, publishing/redaction, subscriptions/delivery,
  retention, resource index, fabric, developer apps, client library, realtime, security, deploy.
- [docs/](docs/) — delivery, subscriptions, realtime and operations notes.
- `lib/client.js` is the reference client; `openvibe-sdk/events` is the supported one.
