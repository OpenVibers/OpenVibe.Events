'use strict';
// Track O: GET /metrics (loopback only, route templates, Events gauges and delivery histogram) and a
// truthful /api/ready (required db/key/worker, optional DLQ → degraded but still ready).
const assert = require('assert');
const nodeHttp = require('http');
const { boot, request, serviceToken, envelope, subscriber, suite } = require('./helpers');

const t = suite('observability');
const live = serviceToken('live', ['events.event.publish']);
const media = serviceToken('media', ['events.subscription.manage']);
const reader = serviceToken('games', ['events.event.read']);
let h;

function get(base, p, headers = {}) {
    return new Promise((resolve, reject) => nodeHttp.get(base + p, { headers }, (res) => {
        let b = ''; res.on('data', (d) => { b += d; }); res.on('end', () => resolve({ status: res.statusCode, body: b }));
    }).on('error', reject));
}

t('boot with the worker running', async () => { h = await boot({ worker: 'on', env: { EVENTS_DLQ_DEGRADED_AT: '0', EVENTS_MAX_ATTEMPTS: '1' } }); });

t('/api/ready: every check reports status, latency and checked_at; worker is required', async () => {
    const r = await request(h.base, 'GET', '/api/ready');
    assert.strictEqual(r.status, 200, r.text);
    assert.strictEqual(r.body.status, 'ready');
    assert.strictEqual(r.body.service, 'events');
    assert.deepStrictEqual(Object.keys(r.body.checks), ['db', 'network_jwks', 'delivery_worker', 'dlq']);
    for (const [name, c] of Object.entries(r.body.checks)) {
        assert.strictEqual(c.status, 'ok', name);
        assert.strictEqual(typeof c.latency_ms, 'number');
        assert.ok(Date.parse(c.checked_at));
    }
    assert.strictEqual(r.body.checks.dlq.required, false);
    assert.deepStrictEqual(r.body.checks.dlq.detail, { depth: 0, threshold: 0 });
    assert.ok('latest_seq' in r.body && 'deliveries' in r.body && 'realtime_connections' in r.body);
    assert.strictEqual(r.body.worker.running, true);
});

t('deliveries feed the latency histogram and attempt counter; a dead one degrades /api/ready', async () => {
    const ok = await subscriber(() => 204);
    const bad = await subscriber(() => 500);
    await request(h.base, 'POST', '/api/v1/subscriptions', { token: media, body: { topic_pattern: 'live.stream.*', endpoint: ok.url } });
    await request(h.base, 'POST', '/api/v1/subscriptions', { token: serviceToken('games', ['events.subscription.manage']), body: { topic_pattern: 'live.stream.*', endpoint: bad.url } });
    const r = await request(h.base, 'POST', '/api/v1/events', { token: live, body: envelope() });
    assert.strictEqual(r.status, 201, r.text);
    await h.worker.drain();
    assert.strictEqual(ok.calls.length, 1);
    const ready = await request(h.base, 'GET', '/api/ready');
    assert.strictEqual(ready.status, 200, 'a DLQ over the threshold degrades, it does not make the service unready');
    assert.strictEqual(ready.body.ready, true);
    assert.strictEqual(ready.body.status, 'degraded');
    assert.deepStrictEqual(ready.body.degraded, ['dlq']);
    assert.strictEqual(ready.body.checks.dlq.detail.depth, 1);
    await ok.close(); await bad.close();
});

t('/metrics: loopback only, templates not ids, Events gauges', async () => {
    const id = h.db.prepare('SELECT id FROM events ORDER BY seq LIMIT 1').get().id;
    await request(h.base, 'GET', `/api/v1/events/${id}`, { token: reader });
    await request(h.base, 'GET', '/api/v1/events?topic=live.*&after_seq=0', { token: reader });
    await request(h.base, 'GET', '/no/such/12345');
    let m = await get(h.base, '/metrics', { 'X-Forwarded-For': '203.0.113.7' });
    assert.strictEqual(m.status, 404, 'through a proxy: not found');
    m = await get(h.base, '/metrics');
    assert.strictEqual(m.status, 200);
    const text = m.body;
    assert.ok(text.includes('route="/api/v1/events/:id"'), 'route template for the single-event read');
    assert.ok(!text.includes(id), 'no event id in any label');
    assert.ok(!/route="[^"]*12345/.test(text), 'unmatched paths are not labels');
    assert.ok(/http_requests_total\{method="GET",route="unmatched",status_class="4xx"\} 1/.test(text));
    assert.ok(/events_deliveries\{status="delivered"\} 1\n/.test(text));
    assert.ok(/events_deliveries\{status="dead"\} 1\n/.test(text));
    assert.ok(/\nevents_dlq_depth 1\n/.test(text));
    assert.ok(/\nevents_latest_seq 1\n/.test(text));
    assert.ok(/\nevents_realtime_connections 0\n/.test(text));
    assert.ok(/events_delivery_latency_seconds_count\{priority="important"\} 1\n/.test(text), text.split('\n').filter(l => l.includes('latency')).join('\n'));
    assert.ok(/events_delivery_attempts_total\{outcome="delivered"\} 1\n/.test(text));
    assert.ok(/events_delivery_attempts_total\{outcome="dead"\} 1\n/.test(text));
    assert.ok(/release_info\{service="events",release="[0-9a-f]{7,12}"\} 1/.test(text));
    assert.ok(!text.includes('realtime/stream'), 'SSE sessions are not HTTP request samples');
});

t('release.json', async () => {
    const r = await request(h.base, 'GET', '/release.json');
    assert.strictEqual(r.status, 200);
    assert.strictEqual(r.body.service, 'events');
});

t('a stopped worker makes the service unready', async () => {
    await h.worker.stop();
    const r = await request(h.base, 'GET', '/api/ready');
    assert.strictEqual(r.status, 503);
    assert.strictEqual(r.body.ready, false);
    assert.deepStrictEqual(r.body.failed, ['delivery_worker']);
    assert.strictEqual(r.body.checks.delivery_worker.error, 'delivery worker is not running');
});

t('a closed database makes the service unready', async () => {
    h.db.close();
    const r = await request(h.base, 'GET', '/api/ready');
    assert.strictEqual(r.status, 503);
    assert.ok(r.body.failed.includes('db'));
    assert.strictEqual(r.body.latest_seq, null);
    // close() closes the db again; tolerate it.
    try { await h.close(); } catch { /* already closed */ }
});

t.run();
