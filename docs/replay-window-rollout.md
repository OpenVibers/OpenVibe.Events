# Replay window for webhook deliveries (signature v2): rollout

Status (2026-09-23): OpenVibe.Events signs v2 in code (not deployed). openvibe-sdk 0.4.0 is committed but **not tagged**. Contracts and ADR-004 describe v2. Consumers have **not** been changed: each change below has to wait for the `v0.4.0` SDK tag.

## What changes on the wire

Every delivery attempt now carries three signature headers, all keyed with the subscription secret:

| Header | Value |
|---|---|
| `X-OpenVibe-Signature` (v1, unchanged) | `sha256=<hex HMAC-SHA256 of the raw body>` |
| `X-OpenVibe-Timestamp` | `<unix seconds>`, the time this attempt was sent |
| `X-OpenVibe-Signature-V2` | `t=<that timestamp>,v2=<hex HMAC-SHA256 of "<t>.<raw body>">` |

- The worker signs each attempt when it sends it (`server/worker.js`). Retries and operator replays from the DLQ therefore always carry a fresh timestamp. A delivery that waited in the queue, or backed off for an hour, is still inside the window when it arrives.
- A consumer accepts v2 only when `t` is within **±300 s** of its own clock. It compares signatures in constant time, and it refuses a v2 header that is present but wrong or stale. It never falls back to v1 in that case. With `requireV2` it also refuses a delivery with no v2 header, which blocks replaying a captured delivery with the v2 header stripped off.
- The helpers:
  - Events `lib/client.js`: `signV2(raw, secret, ts)` and `verifyDeliveryV2(raw, headers, secret, { toleranceSec = 300, now })`.
  - openvibe-sdk 0.4.0: `verifyDeliveryV2`, `parseDelivery(raw, headers, secret, { requireV2, toleranceSec, now })`, and `signDeliveryV2` / `signDeliveryHeaders` for tests.
- Every host must keep its clock synced with NTP. Events and all the consumers listed here run on the same host today, so they share one clock. Developer-app endpoints run on their own clocks.

## Deploy order

1. **Review and tag openvibe-sdk `v0.4.0`** (OpenVibe.SDK). Nothing depends on it yet.
2. **Deploy OpenVibe.Events.** It keeps sending v1 and adds the two v2 headers, which current consumers ignore. Then check that one real delivery carries `X-OpenVibe-Timestamp` and `X-OpenVibe-Signature-V2`: look at a consumer's access log, or use a throwaway subscription to a local endpoint. Also compare `date +%s` on the host with a trusted clock. Rollback is to redeploy the previous release. That is safe only while no consumer requires v2.
3. **Codes docs** (already committed) can go out any time after step 1. The page now points readers to `openvibe-sdk` 0.4.0.
4. **Consumers, one at a time**, each after step 2 is confirmed. Each one pins `openvibe-sdk` to `v0.4.0` (Search has no SDK), switches to v2 as shown below, updates its tests, runs `npm test` on Node 22, and deploys. After each deploy, watch that service for `401 *.bad_signature` and for its deliveries in `GET /api/v1/deliveries?status=failed&subscription_id=…`. The suggested order goes from least to most consequential:
   1. **Live** (`/internal/openre-events`): inert until `OPENRE_URL` / `OPENRE_EVENTS_SECRET` are set.
   2. **Search**: index updates, rebuildable.
   3. **News**, then **Deals**, then **Trade**: source-driven imports, idempotent.
   4. **Reviews**.
   5. **Tips**, then **VIP**: settlement and entitlements from Billing events. Do these last, when the rest has run clean for a day.
   6. **Examples** (`webhook-consumer`): a reference app, not deployed. Update it in the same week so developers copy the v2 form.
5. **Optional, after every consumer requires v2:** stop sending v1 from the Events worker. `verifyDelivery` stays exported in the SDK and in `lib/client.js`.

**If a consumer rejects deliveries** (bad secret, clock skew), Events keeps retrying for about 2 h 13 min (1 s, 5 s, 30 s, 2 min, 10 min, 1 h, 1 h) before the delivery goes to the dead-letter queue. Meanwhile, roll the consumer back to its previous release. Then requeue its dead deliveries with `POST /api/v1/deliveries/replay { subscription_id, event_ids }` or `{ subscription_id, from_seq }`. Replays are signed afresh, and the consumer's inbox dedupes on `event_id`.

## Consumers that verify Events deliveries

I searched `~/OpenVibers/*/{server,lib,scripts,examples}` and `~/orca/workspaces/OpenVibe.Live/seadragon` (excluding `node_modules`) for `verifyDelivery`, `parseDelivery`, `x-openvibe-signature`, `createHmac` and `timingSafeEqual`.

| Service | Route | Verifier today | SDK pin | Secret (env) | Change |
|---|---|---|---|---|---|
| Live (seadragon) | `POST /internal/openre-events` | SDK `parseDelivery` (`server/openre/mirror.js:141`) | v0.2.2 | `OPENRE_EVENTS_SECRET` (one) | add `{ requireV2: true }` |
| Search | `POST /internal/events` | hand-rolled HMAC (`server/api/webhook.js:27-38, 98`) | none | `SEARCH_EVENTS_SECRET` (list) | hand-rolled v2 check (no SDK dependency) |
| News | `POST /internal/events` | SDK `verifyDelivery` (`server/http/webhook.js:35-36`) | v0.2.2 | `NEWS_EVENTS_SECRET` (list) | `verifyDeliveryV2(raw, req.headers, s)` |
| Deals | `POST /internal/events` | SDK `verifyDelivery` (`server/http/internal.js:28-29`) | v0.2.2 | `DEALS_EVENTS_SECRET` (list) | `verifyDeliveryV2(raw, req.headers, s)` |
| Trade | `POST /internal/events` | SDK `parseDelivery` (`server/events/webhook.js:25`) | v0.2.2 | `TRADE_EVENTS_WEBHOOK_SECRET` (one) | add `{ requireV2: true }` |
| Reviews | `POST /internal/events` | SDK `parseDelivery` per secret (`server/http/consumer.js:48`) | v0.2.2 | `REVIEWS_EVENTS_SECRET` (list) | add `{ requireV2: true }` |
| Tips | `POST /internal/events` | SDK `parseDelivery` per secret (`server/events/consumer.js:51`) | v0.2.2 | `TIPS_EVENTS_SECRET` (list) | add `{ requireV2: true }` |
| VIP | `POST /internal/events` | SDK `parseDelivery` per secret (`server/events/consumer.js:80`) | v0.2.2 | `VIP_EVENTS_SECRET` (list) | add `{ requireV2: true }` |
| Examples `webhook-consumer` (reference) | `POST /webhooks/openvibe` | SDK `parseDelivery` per secret (`examples/webhook-consumer/server.js:71`) | v0.3.1 | `OV_WEBHOOK_SECRET`, `OV_WEBHOOK_SECRET_PREVIOUS` | add `{ requireV2: true }` and fix one test |

These do **not** receive Events deliveries, so they need no change:
- **Producers only:** Wiki, Blog, Chat, Billing, Media, Host, Coupons, Games, Network and OpenRe.Stream. Network's `resend-webhook.js` is Resend's Svix webhook, and Media's `X-OVMedia-Signature` is Media's own webhook to Live.
- **No Events code:** Community, AI and Tools.
- **Not code:** Publishing (a library) and Realtime (closed, part of Events).
- **Codes:** `server/domain/webhooks.js` is a signature *tester*, not a receiver. Its docs text is updated. The tester still checks and generates v1 only. A follow-up after the SDK tag can make it inspect `X-OpenVibe-Signature-V2` and include the v2 headers in the sample.

### How the changes below were checked

Each diff was applied to a scratch copy of the repo with the local openvibe-sdk 0.4.0 installed in its `node_modules`, then tested on Node 22.22.1:
- **Full suites:** Deals, News, Search, Tips, Reviews, Trade, VIP and Examples all pass.
- **Live:** only `test/openre-switch.test.js` was run. It passes.
- **SDK bump alone, code unchanged:** every suite still passes, except the Examples smoke test. That test blanks only `X-OpenVibe-Signature` and expects a 401. With 0.4.0 the fresh v2 header from the mock is accepted, which is correct: a valid v2 is never overridden by v1. The Examples diff fixes the test.

### Common to the SDK consumers

In `package.json`, change the pin and refresh the lockfile with `npm install`:

```diff
-    "openvibe-sdk": "https://codeload.github.com/OpenVibers/OpenVibe.SDK/tar.gz/refs/tags/v0.2.2",
+    "openvibe-sdk": "https://codeload.github.com/OpenVibers/OpenVibe.SDK/tar.gz/refs/tags/v0.4.0",
```

Examples pins v0.3.1 in two places: the root `package.json` and `examples/webhook-consumer/package.json`.

The 0.2.2 → 0.4.0 jump includes 0.3.0's behaviour changes. None of these services uses the APIs involved: they import only `openvibe-sdk/core`, `/auth` and `/events`, and none calls `iterate({ onPage })`, `responseType: 'response'` or the SDK's authorize-URL builders. Their suites pass on 0.4.0.

Tests that post signed deliveries must send v2. Replace `signDelivery(raw, secret)` with the SDK's `...signDeliveryHeaders(raw, secret)`, which returns all three headers. A negative test that only swaps `X-OpenVibe-Signature` for a bad value no longer fails when a valid v2 is present, so it must also break or drop the v2 header.

### Live (`~/orca/workspaces/OpenVibe.Live/seadragon`)

```diff
--- a/server/openre/mirror.js
+++ b/server/openre/mirror.js
@@ -138,7 +138,7 @@
     const secret = process.env.OPENRE_EVENTS_SECRET || '';
     if (!secret) return res.status(503).json({ error: 'OPENRE_EVENTS_SECRET is not set' });
     const { parseDelivery } = require('openvibe-sdk/events');
-    const delivery = parseDelivery(req.rawBody, req.headers, secret);
+    const delivery = parseDelivery(req.rawBody, req.headers, secret, { requireV2: true });
     if (!delivery) return res.status(401).json({ error: 'bad signature' });
     // Signed but unusable (no event_id): acknowledge so it is not redelivered forever.
     if (!delivery.event || !delivery.event.event_id) return res.status(204).end();
--- a/test/openre-switch.test.js
+++ b/test/openre-switch.test.js
@@ -51,14 +51,14 @@
 });
 
 function signed(event, secret) {
-    const { signDelivery } = require('openvibe-sdk/events');
+    const { signDeliveryHeaders } = require('openvibe-sdk/events');
     const raw = Buffer.from(JSON.stringify({ event, seq: 1 }));
-    return { raw, sig: signDelivery(raw, secret) };
+    return { raw, headers: signDeliveryHeaders(raw, secret) };
 }
 
-async function deliver(base, event, { secret = 'whsec_test', sig } = {}) {
+async function deliver(base, event, { secret = 'whsec_test', headers } = {}) {
     const s = signed(event, secret);
-    const res = await fetch(`${base}/internal/openre-events`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-OpenVibe-Signature': sig || s.sig }, body: s.raw });
+    const res = await fetch(`${base}/internal/openre-events`, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(headers || s.headers) }, body: s.raw });
     return res.status;
 }
 
@@ -162,7 +162,7 @@
     assert.strictEqual(openreCalls.filter(c => c.url.endsWith('/keys/rotate')).pop().subject, SUBJECT);
 
     // ── Mirror ────────────────────────────────────────────────
-    assert.strictEqual(await deliver(base, sessionEvent('started', SES(3), 2), { sig: 'sha256=00' }), 401, 'bad signature');
+    assert.strictEqual(await deliver(base, sessionEvent('started', SES(3), 2), { headers: { 'X-OpenVibe-Signature': 'sha256=00', 'X-OpenVibe-Timestamp': '1', 'X-OpenVibe-Signature-V2': 't=1,v2=00' } }), 401, 'bad signature');
     assert.strictEqual(await deliver(base, sessionEvent('started', SES(3), 2, { mirror_to_live: false })), 204);
     assert.strictEqual(d.prepare('SELECT COUNT(*) AS n FROM streams WHERE is_live = 1').get().n, 0, 'no consent, no mirror');
 
```

### Search (no SDK dependency: hand-rolled, same rules as `verifyDeliveryV2`)

Search deliberately has no `openvibe-sdk` dependency, so it gets its own v2 check. The alternative is adding `openvibe-sdk` v0.4.0 and calling `verifyDeliveryV2(raw, req.headers, s)` for each secret. Either choice gives the same behaviour.

```diff
--- a/server/api/webhook.js
+++ b/server/api/webhook.js
@@ -28,6 +28,41 @@
     return 'sha256=' + crypto.createHmac('sha256', String(secret)).update(raw).digest('hex');
 }
 
+/** X-OpenVibe-Signature-V2 value: t=<unix seconds>,v2=<hex HMAC-SHA256 of "<t>.<raw body>">. */
+function signV2(raw, secret, ts = Math.floor(Date.now() / 1000)) {
+    return `t=${ts},v2=` + crypto.createHmac('sha256', String(secret)).update(`${ts}.`).update(raw).digest('hex');
+}
+
+const V2_TOLERANCE_SEC = 300;
+
+/**
+ * Constant-time check of X-OpenVibe-Signature-V2 against the raw body with any of `secrets`, and
+ * of its timestamp (±300 s of now; X-OpenVibe-Timestamp, when sent, must be the same t). Same
+ * rules as openvibe-sdk 0.4.0 verifyDeliveryV2(). v1 is never consulted.
+ */
+function verifySignatureV2(raw, headerV2, headerTs, secrets, now = Date.now()) {
+    if (!raw || typeof headerV2 !== 'string') return false;
+    let t = null;
+    const given = [];
+    for (const part of headerV2.split(',')) {
+        const i = part.indexOf('=');
+        if (i < 0) return false;
+        const k = part.slice(0, i).trim();
+        const v = part.slice(i + 1).trim();
+        if (k === 't') {
+            if (t !== null || !/^\d{1,12}$/.test(v)) return false;
+            t = Number(v);
+        } else if (k === 'v2') given.push(Buffer.from(v));
+    }
+    if (t === null || !given.length) return false;
+    if (headerTs !== undefined && String(headerTs).trim() !== String(t)) return false;
+    if (Math.abs(now / 1000 - t) > V2_TOLERANCE_SEC) return false;
+    return secrets.some((s) => {
+        const expected = Buffer.from(signV2(raw, s, t).split(',v2=')[1]);
+        return given.some((g) => g.length === expected.length && crypto.timingSafeEqual(g, expected));
+    });
+}
+
 function verifySignature(raw, header, secrets) {
     if (!raw || typeof header !== 'string') return false;
     const given = Buffer.from(header.trim());
@@ -95,8 +130,8 @@
             const secrets = config.events.webhookSecrets;
             if (!secrets.length) return http.sendProblem(res, 503, 'search.webhook_disabled', { detail: 'SEARCH_EVENTS_SECRET is not set', ctx });
             const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
-            if (!verifySignature(raw, req.get('x-openvibe-signature'), secrets)) {
-                return http.sendProblem(res, 401, 'search.bad_signature', { detail: 'X-OpenVibe-Signature does not verify', ctx });
+            if (!verifySignatureV2(raw, req.get('x-openvibe-signature-v2'), req.get('x-openvibe-timestamp'), secrets)) {
+                return http.sendProblem(res, 401, 'search.bad_signature', { detail: 'X-OpenVibe-Signature-V2 does not verify or is outside the replay window', ctx });
             }
             let body;
             try { body = JSON.parse(raw.toString('utf8')); } catch { body = null; }
@@ -131,4 +166,4 @@
     return router;
 }
 
-module.exports = { webhookRouter, createInbox, documentFromEvent, verifySignature, sign, CONSUMER };
+module.exports = { webhookRouter, createInbox, documentFromEvent, verifySignature, sign, verifySignatureV2, signV2, CONSUMER };
--- a/test/helpers.js
+++ b/test/helpers.js
@@ -11,7 +11,7 @@
 const { serviceAuth, ids } = require('openvibe-contracts');
 const { load } = require('../server/config');
 const { start } = require('../server/index');
-const { sign } = require('../server/api/webhook');
+const { sign, signV2 } = require('../server/api/webhook');
 
 const ISSUER = 'https://openvibe.network';
 const WEBHOOK_SECRET = 'whsec_test_' + 'x'.repeat(40);
@@ -116,10 +116,11 @@
 /** POST a signed delivery to /internal/events. */
 async function deliver(base, event, { secret = WEBHOOK_SECRET, seq = 1, badSignature = false } = {}) {
     const raw = JSON.stringify({ event, seq });
-    const signature = badSignature ? sign(raw, 'wrong-secret-' + 'y'.repeat(32)) : sign(raw, secret);
+    const key = badSignature ? 'wrong-secret-' + 'y'.repeat(32) : secret;
+    const ts = Math.floor(Date.now() / 1000);
     return request(base, 'POST', '/internal/events', {
         raw,
-        headers: { 'Content-Type': 'application/json', 'X-OpenVibe-Signature': signature, 'X-OpenVibe-Event-Id': event.event_id },
+        headers: { 'Content-Type': 'application/json', 'X-OpenVibe-Signature': sign(raw, key), 'X-OpenVibe-Timestamp': String(ts), 'X-OpenVibe-Signature-V2': signV2(raw, key, ts), 'X-OpenVibe-Event-Id': event.event_id },
     });
 }
 
```

### News

The News test helper signs by hand, so it also computes v2 by hand.

```diff
--- a/server/http/webhook.js
+++ b/server/http/webhook.js
@@ -16,7 +16,7 @@
  */
 const express = require('express');
 const { http } = require('openvibe-contracts');
-const { verifyDelivery, createInbox } = require('openvibe-sdk/events');
+const { verifyDeliveryV2, createInbox } = require('openvibe-sdk/events');
 
 const CONSUMER = 'news-sources';
 const EVT_RE = /^evt_[0-9A-HJKMNP-TV-Z]{26}$/;
@@ -32,8 +32,8 @@
         const secrets = config.events.webhookSecrets;
         if (!secrets.length) return http.sendProblem(res, 503, 'news.webhook_disabled', { detail: 'NEWS_EVENTS_SECRET is not set', ctx });
         const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
-        const sig = req.get('x-openvibe-signature');
-        if (!secrets.some((s) => verifyDelivery(raw, sig, s))) return http.sendProblem(res, 401, 'news.bad_signature', { detail: 'X-OpenVibe-Signature does not verify', ctx });
+        // v2 only: signature over "<t>.<raw body>" and t within ±300 s (a replayed or v1-only delivery fails).
+        if (!secrets.some((s) => verifyDeliveryV2(raw, req.headers, s))) return http.sendProblem(res, 401, 'news.bad_signature', { detail: 'X-OpenVibe-Signature-V2 does not verify or is outside the replay window', ctx });
         let body;
         try { body = JSON.parse(raw.toString('utf8')); } catch { body = null; }
         const event = body && body.event;
--- a/test/helpers/mocks.js
+++ b/test/helpers/mocks.js
@@ -193,7 +193,9 @@
     const envelope = { event_id: `evt_${ids.ulid(Date.now())}`, version: 1, timestamp: new Date().toISOString(), visibility: 'internal', actor: { type: 'service', id: 'sources' }, source: 'sources', payload: {}, ...event };
     const body = JSON.stringify({ event: envelope, seq });
     const sig = `sha256=${crypto.createHmac('sha256', secret).update(body).digest('hex')}`;
-    return { body, envelope, headers: { 'content-type': 'application/json', 'x-openvibe-signature': sig, 'x-openvibe-delivery-attempt': String(attempt), 'x-openvibe-seq': String(seq) } };
+    const ts = Math.floor(Date.now() / 1000);
+    const v2 = `t=${ts},v2=${crypto.createHmac('sha256', secret).update(`${ts}.${body}`).digest('hex')}`;
+    return { body, envelope, headers: { 'content-type': 'application/json', 'x-openvibe-signature': sig, 'x-openvibe-timestamp': String(ts), 'x-openvibe-signature-v2': v2, 'x-openvibe-delivery-attempt': String(attempt), 'x-openvibe-seq': String(seq) } };
 }
 
 module.exports = { startNetwork, startSources, startAi, delivery, listen };
```

### Deals

```diff
--- a/server/http/internal.js
+++ b/server/http/internal.js
@@ -11,7 +11,7 @@
  */
 const express = require('express');
 const { http } = require('openvibe-contracts');
-const { verifyDelivery, createInbox } = require('openvibe-sdk/events');
+const { verifyDeliveryV2, createInbox } = require('openvibe-sdk/events');
 
 const CONSUMER = 'deals-sources';
 const TYPES = new Set(['sources.item.created', 'sources.item.updated', 'sources.item.removed']);
@@ -25,8 +25,8 @@
         const secrets = config.events.webhookSecrets;
         if (!secrets.length) return http.sendProblem(res, 503, 'deals.webhook_disabled', { detail: 'DEALS_EVENTS_SECRET is not set', ctx: req.ov });
         const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
-        const sig = req.get('x-openvibe-signature');
-        if (!secrets.some((s) => verifyDelivery(raw, sig, s))) return http.sendProblem(res, 401, 'deals.bad_signature', { detail: 'X-OpenVibe-Signature does not verify', ctx: req.ov });
+        // v2 only: signature over "<t>.<raw body>" and t within ±300 s (a replayed or v1-only delivery fails).
+        if (!secrets.some((s) => verifyDeliveryV2(raw, req.headers, s))) return http.sendProblem(res, 401, 'deals.bad_signature', { detail: 'X-OpenVibe-Signature-V2 does not verify or is outside the replay window', ctx: req.ov });
         let body;
         try { body = JSON.parse(raw.toString('utf8')); } catch { body = null; }
         const event = body && body.event;
--- a/test/import.test.js
+++ b/test/import.test.js
@@ -6,7 +6,7 @@
 const assert = require('assert');
 const { boot, check, done, robotsOf, HOUR } = require('./helpers/boot');
 const { sourceItem } = require('./helpers/mocks');
-const { signDelivery } = require('openvibe-sdk/events');
+const { signDelivery, signDeliveryHeaders } = require('openvibe-sdk/events');
 
 (async () => {
     const t = await boot();
@@ -127,15 +127,17 @@
 
     await check('the signed event wake-up: bad signature 401, deals items scheduled once, others ignored', async () => {
         const env = (id, category) => ({ event: { event_id: id, event_type: 'sources.item.updated', source: 'sources', version: 1, timestamp: new Date().toISOString(), actor: { type: 'service', id: 'sources' }, subject: { type: 'item', id: 'itm_x' }, visibility: 'internal', payload: { category } }, seq: 1 });
-        const post = (body, sig) => t.get('/internal/events', { body, headers: { 'content-type': 'application/json', 'x-openvibe-signature': sig } });
+        const post = (body, sig) => t.get('/internal/events', { body, headers: { 'content-type': 'application/json', ...(typeof sig === 'string' ? { 'x-openvibe-signature': sig } : sig) } });
         const good = JSON.stringify(env('evt_01K5AAAAAAAAAAAAAAAAAAAAA1', 'deals'));
         assert.strictEqual((await post(good, 'sha256=00')).status, 401);
-        const r1 = await post(good, signDelivery(good, 'whsec-test'));
+        assert.strictEqual((await post(good, signDelivery(good, 'whsec-test'))).status, 401, 'v1 only: refused');
+        assert.strictEqual((await post(good, signDeliveryHeaders(good, 'whsec-test', { now: Date.now() - 301000 }))).status, 401, 'stale v2: refused');
+        const r1 = await post(good, signDeliveryHeaders(good, 'whsec-test'));
         assert.deepStrictEqual([r1.status, r1.json().outcome, r1.json().duplicate], [200, 'import_scheduled', false]);
-        const r2 = await post(good, signDelivery(good, 'whsec-test'));
+        const r2 = await post(good, signDeliveryHeaders(good, 'whsec-test'));
         assert.strictEqual(r2.json().duplicate, true);
         const news = JSON.stringify(env('evt_01K5AAAAAAAAAAAAAAAAAAAAA2', 'news'));
-        assert.strictEqual((await post(news, signDelivery(news, 'whsec-test'))).json().outcome, 'ignored');
+        assert.strictEqual((await post(news, signDeliveryHeaders(news, 'whsec-test'))).json().outcome, 'ignored');
     });
 
     await check('Sources down: the pull fails honestly (readiness says so), keeps its cursor, invents nothing', async () => {
```

### Trade

```diff
--- a/server/events/webhook.js
+++ b/server/events/webhook.js
@@ -22,7 +22,7 @@
     router.post('/internal/events', express.raw({ type: '*/*', limit: '1mb' }), function receiveEventsDelivery(req, res) {
         if (!config.events.webhookSecret) return http.sendProblem(res, 404, 'route.not_found', { detail: 'Not found', ctx: req.ov });
         const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.from('');
-        const delivery = parseDelivery(raw, req.headers, config.events.webhookSecret);
+        const delivery = parseDelivery(raw, req.headers, config.events.webhookSecret, { requireV2: true });
         if (!delivery) return http.sendProblem(res, 401, 'webhook.signature_invalid', { detail: 'The delivery signature does not verify', ctx: req.ov });
         const e = delivery.event;
         if (typeof e.event_id !== 'string' || typeof e.event_type !== 'string') return http.sendProblem(res, 400, 'webhook.malformed', { detail: 'Not an event envelope', ctx: req.ov });
--- a/test/sync.test.js
+++ b/test/sync.test.js
@@ -6,7 +6,7 @@
  * wakes the sync and is idempotent per event id. Deterministic resolution.
  */
 const assert = require('assert');
-const { signDelivery } = require('openvibe-sdk/events');
+const { signDelivery, signDeliveryHeaders } = require('openvibe-sdk/events');
 const { boot, check, done } = require('./helpers/boot');
 const { mapItem } = require('../server/domain/mapping');
 
@@ -119,12 +119,14 @@
         const body = JSON.stringify({ event: { event_id: 'evt_01JABCDEFGHJKMNPQRSTVWXYZ0', event_type: 'sources.item.created', payload: {} }, seq: 7 });
         const bad = await t.get('/internal/events', { method: 'POST', body, headers: { 'content-type': 'application/json', 'x-openvibe-signature': 'sha256=00' } });
         assert.strictEqual(bad.status, 401);
-        const ok = await t.get('/internal/events', { method: 'POST', body, headers: { 'content-type': 'application/json', 'x-openvibe-signature': signDelivery(body, 'hook-secret') } });
+        const v1only = await t.get('/internal/events', { method: 'POST', body, headers: { 'content-type': 'application/json', 'x-openvibe-signature': signDelivery(body, 'hook-secret') } });
+        assert.strictEqual(v1only.status, 401, 'v1 only: refused (requireV2)');
+        const ok = await t.get('/internal/events', { method: 'POST', body, headers: { 'content-type': 'application/json', ...signDeliveryHeaders(body, 'hook-secret') } });
         assert.strictEqual(ok.status, 200);
         assert.strictEqual(ok.json().sync, true);
         await t.ctx.sync.run();
         assert.strictEqual((await t.get('/i/AAPL.json')).json().documents.length, 1);
-        const again = await t.get('/internal/events', { method: 'POST', body, headers: { 'content-type': 'application/json', 'x-openvibe-signature': signDelivery(body, 'hook-secret') } });
+        const again = await t.get('/internal/events', { method: 'POST', body, headers: { 'content-type': 'application/json', ...signDeliveryHeaders(body, 'hook-secret') } });
         assert.strictEqual(again.json().duplicate, true);
         assert.strictEqual(again.json().sync, false);
     });
```

### Reviews

```diff
--- a/server/http/consumer.js
+++ b/server/http/consumer.js
@@ -45,7 +45,7 @@
         if (!secrets.length) return http.sendProblem(res, 503, 'reviews.webhook_disabled', { detail: 'REVIEWS_EVENTS_SECRET is not set', ctx: req.ov });
         const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
         let delivery = null;
-        for (const s of secrets) { delivery = parseDelivery(raw, req.headers, s); if (delivery) break; }
+        for (const s of secrets) { delivery = parseDelivery(raw, req.headers, s, { requireV2: true }); if (delivery) break; }
         if (!delivery) return http.sendProblem(res, 401, 'reviews.bad_signature', { detail: 'X-OpenVibe-Signature does not verify', ctx: req.ov });
         const event = delivery.event;
         if (!event || typeof event.event_id !== 'string' || !/^evt_[0-9A-HJKMNP-TV-Z]{26}$/.test(event.event_id)) {
--- a/test/helpers.js
+++ b/test/helpers.js
@@ -10,7 +10,7 @@
 const os = require('os');
 const path = require('path');
 const { serviceAuth, ids } = require('openvibe-contracts');
-const { signDelivery } = require('openvibe-sdk/events');
+const { signDeliveryHeaders } = require('openvibe-sdk/events');
 const { load } = require('../server/config');
 const { start } = require('../server/index');
 
@@ -212,7 +212,7 @@
 /** A signed delivery from OpenVibe.Events to POST /internal/events. */
 async function deliver(h, envelope, { secret = WEBHOOK_SECRET } = {}) {
     const raw = JSON.stringify({ event: envelope, seq: 1 });
-    return req(h, 'POST', '/internal/events', { body: raw, headers: { 'X-OpenVibe-Signature': signDelivery(raw, secret) } });
+    return req(h, 'POST', '/internal/events', { body: raw, headers: signDeliveryHeaders(raw, secret) });
 }
 
 function sourcesEvent(type, item, extra = {}) {
```

### Tips

```diff
--- a/server/events/consumer.js
+++ b/server/events/consumer.js
@@ -48,7 +48,7 @@
         if (!secrets.length) return http.sendProblem(res, 503, 'tips.webhook_disabled', { detail: 'TIPS_EVENTS_SECRET is not set', ctx: req.ov });
         const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
         let delivery = null;
-        for (const s of secrets) { delivery = parseDelivery(raw, req.headers, s); if (delivery) break; }
+        for (const s of secrets) { delivery = parseDelivery(raw, req.headers, s, { requireV2: true }); if (delivery) break; }
         if (!delivery) return http.sendProblem(res, 401, 'tips.bad_signature', { detail: 'X-OpenVibe-Signature does not verify', ctx: req.ov });
         const event = delivery.event;
         if (!event || typeof event.event_id !== 'string' || !/^evt_[0-9A-HJKMNP-TV-Z]{26}$/.test(event.event_id)) {
--- a/test/helpers/app.js
+++ b/test/helpers/app.js
@@ -13,7 +13,7 @@
 const path = require('path');
 const http = require('http');
 const crypto = require('crypto');
-const { signDelivery } = require('openvibe-sdk/events');
+const { signDeliveryHeaders } = require('openvibe-sdk/events');
 const { startNetwork, startBilling, startEvents, startLive } = require('./stubs');
 
 const EVENTS_SECRET = 'e'.repeat(48);
@@ -75,7 +75,7 @@
         const raw = JSON.stringify({ event, seq });
         const res = await fetch(`${base}/internal/events`, {
             method: 'POST', body: raw,
-            headers: { 'Content-Type': 'application/json', 'X-OpenVibe-Signature': signDelivery(raw, secret), 'X-OpenVibe-Seq': String(seq) },
+            headers: { 'Content-Type': 'application/json', ...signDeliveryHeaders(raw, secret), 'X-OpenVibe-Seq': String(seq) },
         });
         return { status: res.status, json: await res.json().catch(() => null) };
     }
--- a/test/settlement.test.js
+++ b/test/settlement.test.js
@@ -239,8 +239,8 @@
         const bad = await t.deliver(ev, { secret: 'x'.repeat(48) });
         assert.strictEqual(bad.status, 401);
         const raw = JSON.stringify({ event: ev, seq: 1 });
-        const { signDelivery } = require('openvibe-sdk/events');
-        const proxied = await fetch(`${t.base}/internal/events`, { method: 'POST', body: raw, headers: { 'Content-Type': 'application/json', 'X-OpenVibe-Signature': signDelivery(raw, 'e'.repeat(48)), 'X-Forwarded-For': '203.0.113.9' } });
+        const { signDeliveryHeaders } = require('openvibe-sdk/events');
+        const proxied = await fetch(`${t.base}/internal/events`, { method: 'POST', body: raw, headers: { 'Content-Type': 'application/json', ...signDeliveryHeaders(raw, 'e'.repeat(48)), 'X-Forwarded-For': '203.0.113.9' } });
         assert.strictEqual(proxied.status, 404);
         const forged = await t.deliver({ ...ev, source: 'live' });
         assert.strictEqual(forged.json.outcome, 'ignored:source');
```

### VIP

```diff
--- a/server/events/consumer.js
+++ b/server/events/consumer.js
@@ -77,7 +77,7 @@
         if (!secrets.length) return http.sendProblem(res, 503, 'vip.webhook_disabled', { detail: 'VIP_EVENTS_SECRET is not set', ctx: req.ov });
         const raw = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0);
         let delivery = null;
-        for (const s of secrets) { delivery = parseDelivery(raw, req.headers, s); if (delivery) break; }
+        for (const s of secrets) { delivery = parseDelivery(raw, req.headers, s, { requireV2: true }); if (delivery) break; }
         if (!delivery) return http.sendProblem(res, 401, 'vip.bad_signature', { detail: 'X-OpenVibe-Signature does not verify', ctx: req.ov });
         const event = delivery.event;
         if (!event || typeof event.event_id !== 'string' || !/^evt_[0-9A-HJKMNP-TV-Z]{26}$/.test(event.event_id)) {
--- a/test/helpers/app.js
+++ b/test/helpers/app.js
@@ -12,7 +12,7 @@
 const os = require('os');
 const path = require('path');
 const http = require('http');
-const { signDelivery } = require('openvibe-sdk/events');
+const { signDeliveryHeaders } = require('openvibe-sdk/events');
 const { startNetwork, startBilling } = require('./stubs');
 
 const EVENTS_SECRET = 'e'.repeat(48);
@@ -68,7 +68,7 @@
         const raw = JSON.stringify({ event, seq });
         const res = await fetch(`${base}/internal/events`, {
             method: 'POST', body: raw,
-            headers: { 'Content-Type': 'application/json', 'X-OpenVibe-Signature': signDelivery(raw, secret), 'X-OpenVibe-Seq': String(seq) },
+            headers: { 'Content-Type': 'application/json', ...signDeliveryHeaders(raw, secret), 'X-OpenVibe-Seq': String(seq) },
         });
         return { status: res.status, json: await res.json().catch(() => null) };
     }
--- a/test/entitlements.test.js
+++ b/test/entitlements.test.js
@@ -5,7 +5,7 @@
  * valid_until (+ grace); ordering and duplicates are safe. Injected clock throughout.
  */
 const assert = require('assert');
-const { signDelivery } = require('openvibe-sdk/events');
+const { signDeliveryHeaders } = require('openvibe-sdk/events');
 const { boot, harness } = require('./helpers/app');
 const { DAY } = require('./helpers/stubs');
 
@@ -56,7 +56,7 @@
         assert.strictEqual((await t.deliver(forged)).json.outcome, 'ignored:source');
         assert.strictEqual(t.domain.entitlements.getRow(m.subject, creator.subject), null);
         const raw = JSON.stringify({ event: ent, seq: 1 });
-        const res = await fetch(`${t.base}/internal/events`, { method: 'POST', body: raw, headers: { 'X-OpenVibe-Signature': signDelivery(raw + ' ', t.EVENTS_SECRET) } });
+        const res = await fetch(`${t.base}/internal/events`, { method: 'POST', body: raw, headers: signDeliveryHeaders(raw + ' ', t.EVENTS_SECRET) });
         assert.strictEqual(res.status, 401);
     });
 
```

### Examples (`examples/webhook-consumer`)

Also update `examples/webhook-consumer/README.md`. Its "Signature scheme note" (line 67) says the HMAC has no timestamp. Replace it with the v2 description: the signature covers `"<t>.<raw body>"`, deliveries are refused outside ±300 s, and `requireV2` is on. The inbox stays mandatory, because delivery is at least once. Line 4 and the root README's table row name only `X-OpenVibe-Signature`, so add v2 there too.

```diff
--- a/examples/webhook-consumer/server.js
+++ b/examples/webhook-consumer/server.js
@@ -68,7 +68,7 @@
 
     function verify(raw, headers) {
         for (const secret of config.secrets) {
-            const d = parseDelivery(raw, headers, secret);
+            const d = parseDelivery(raw, headers, secret, { requireV2: true });
             if (d) return d;
         }
         return null;
--- a/examples/webhook-consumer/test/smoke.test.js
+++ b/examples/webhook-consumer/test/smoke.test.js
@@ -106,7 +106,9 @@
     const forgedBody = last.body.replaceAll(e2.event_id, e3.event_id);
     assert.equal((await replay(last, { 'X-OpenVibe-Signature': signDelivery(forgedBody, 'whsec_not_the_secret'), 'X-OpenVibe-Event-Id': e3.event_id }, forgedBody)).status, 401);
     assert.equal((await replay(last, { 'X-OpenVibe-Event-Id': e3.event_id }, forgedBody)).status, 401, 'changed bytes, old signature');
-    assert.equal((await replay(last, { 'X-OpenVibe-Signature': '' })).status, 401);
+    assert.equal((await replay(last, { 'X-OpenVibe-Signature': '', 'X-OpenVibe-Signature-V2': '' })).status, 401);
+    const { 'X-OpenVibe-Signature-V2': _v2, ...v1only } = last.headers;
+    assert.equal((await realFetch(local, { method: 'POST', headers: v1only, body: last.body })).status, 401, 'v1 only (v2 stripped): refused');
     // Header and signed body disagree on the event id: refused.
     assert.equal((await replay(last, { 'X-OpenVibe-Event-Id': e3.event_id })).status, 400);
     assert.deepEqual(handled, [e1.event_id, e2.event_id]);
```
