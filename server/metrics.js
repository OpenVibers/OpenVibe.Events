'use strict';
/**
 * Prometheus metrics (roadmap Track O): the openvibe-shared registry that app.js instruments with
 * HTTP golden signals, plus the Events domain gauges read at scrape time and the worker's delivery
 * hooks. Served on GET /metrics to direct loopback callers only.
 *
 *   events_deliveries{status}                     deliveries by status (pending, failed, delivered, dead)
 *   events_dlq_depth                              dead deliveries waiting for replay
 *   events_latest_seq                             last sequence number assigned
 *   events_realtime_connections                   open SSE connections
 *   events_delivery_latency_seconds{priority}     acceptance → successful delivery (retries included)
 *   events_delivery_attempts_total{outcome}       delivered | retry | dead
 */
const { createRegistry } = require('openvibe-shared/metrics');

// Delivery latency spans retries with backoff up to an hour, so the buckets reach well past 10 s.
const LATENCY_BUCKETS = [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 1800, 3600, 21600];

function createMetrics({ store }) {
    const registry = createRegistry();
    let realtime = null;

    registry.gauge({
        name: 'events_deliveries', help: 'Deliveries by status', labelNames: ['status'],
        collect: () => Object.entries(store.deliveryCounts()).map(([status, value]) => ({ labels: { status }, value })),
    });
    registry.gauge({ name: 'events_dlq_depth', help: 'Dead deliveries (the DLQ) waiting for replay', collect: () => store.deliveryCounts().dead });
    registry.gauge({ name: 'events_latest_seq', help: 'Last sequence number assigned to an event', collect: () => store.lastSeq() });
    registry.gauge({ name: 'events_realtime_connections', help: 'Open realtime (SSE) connections', collect: () => (realtime ? realtime.count() : undefined) });
    const latency = registry.histogram({ name: 'events_delivery_latency_seconds', help: 'Time from acceptance to successful delivery, retries included', labelNames: ['priority'], buckets: LATENCY_BUCKETS });
    const attempts = registry.counter({ name: 'events_delivery_attempts_total', help: 'Delivery attempts by outcome', labelNames: ['outcome'] });

    return {
        registry,
        observe: {
            delivered: (seconds, row) => latency.observe({ priority: row && row.priority }, seconds),
            attempt: (outcome) => attempts.inc({ outcome }),
        },
        bind(parts) { realtime = parts.realtime || realtime; },
    };
}

module.exports = { createMetrics };
