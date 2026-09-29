'use strict';
/**
 * pg-v1 — the poll (ADR-042 decision 2). Always present, every carrier class. A delivery is committed to
 * `deliveries` before any carrier sees it; the worker's own tick (and its in-process kick()) carry it, so signal()
 * does nothing. It is the floor a delivery falls back to when another carrier is unhealthy or ineligible.
 *
 * The offer prices the poll at its interval: half the interval is the expected extra latency beside a push carrier,
 * which is what keeps valkey-v1 ahead whenever it is healthy.
 */
function createPgCarrier({ intervalMs = 500, now = () => Date.now() } = {}) {
    const latency = Math.max(1, Math.round(intervalMs / 2));
    return {
        id: 'pg-v1',
        classes: new Set(['TOPIC', 'QUEUE', 'STREAM']),
        async start() { /* nothing to consume: the worker poll carries it */ },
        async stop() {},
        signal() { /* the poll and the local kick() are the signal */ },
        healthy: () => true,
        offer() {
            return {
                offer_id: 'pg-v1',
                kind: 'provider',
                provider: 'postgres',
                adapter: 'pg-v1',
                region: 'cell',
                trust: 'first-party',
                capabilities: ['events:gateway', 'events:durable', 'events:ordered'],
                latency_ms: { publish_p95: latency, ack_p95: latency },
                health: { status: 'up', checked_at: new Date(now()).toISOString() },
                pricing: { model: 'per-operation', unit: 'delivery-operation', rate_card: 'rc-pg-v1' },
                updated_at: new Date(now()).toISOString(),
            };
        },
    };
}

module.exports = { createPgCarrier };
