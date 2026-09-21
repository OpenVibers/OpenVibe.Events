# OpenVibe.Events

> Durable events, subscriptions, delivery, retry, dead letters and replay for the network.

**Status:** placeholder — planning only, no runnable code yet.  
**Domain:** `events.openvibe.network (+ realtime.openvibe.network runtime)`  
**Plan:** OpenVibe End-to-End Realignment & Implementation Plan, revision 3 (20 Sep 2026), §6 and §6.3.  
**License:** AGPL-3.0 (same as every OpenVibe service).

## Purpose

The durable event backbone the network does not have yet: every authoritative service writes domain mutations and outbox events in one transaction, a relay publishes them here, consumers use inbox/idempotency receipts, and browsers get authorized, resumable projections through the Realtime delivery plane.

## Owns

- `events`, `subscriptions`, `outbox_deliveries`, `consumer_checkpoints`, `idempotency_receipts` (PostgreSQL first; Redis only for fanout/leases)
- canonical event envelope (event_id, trace_id, type, version, source, actor, subject + revision, payload)
- priority classes `critical|important|low`, loop guards, backpressure, DLQ and replay
- Realtime: WS/SSE topics, `Last-Event-ID`/cursor resume, gap detection, presence (ephemeral)

## Does not own

- media bytes or game/media transport packets
- product business rules

## Planned surfaces

- publish/subscribe/replay APIs; signed webhook deliveries
- operator DLQ inspect/replay and queue diagnostics
- `realtime.openvibe.network` gateway

## Data (authority tables / families)

- see above

## Capabilities and events

- `events.publish`, `events.subscribe`, `events.replay`, `realtime.subscribe`

Events: `all `*.created|updated|deleted` families from every service`

## Depends on

- OpenVibe.Contracts
- OpenVibe.Network (topic authorization per subject)

## Acceptance (must be true before "done")

- event persists before consumer delivery
- kill a consumer mid-processing; replay creates exactly one effect
- browser reconnect resumes from a cursor or reports a gap
- a guessed private topic yields no data

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
