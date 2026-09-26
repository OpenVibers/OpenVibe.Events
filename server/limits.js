'use strict';
/**
 * GET /limits.json (roadmap WS-N task 7): the limits a developer project meets here, read from the
 * running configuration, so OpenVibe.Codes' limits page (openvibe.codes/docs/limits) shows what is
 * enforced and never restates it. Public: nothing in it is per project or secret.
 *
 *   limits[]  id, label, capability (the grant it bounds; null for the anonymous realtime stream),
 *             unit (count | bytes | per_minute | days), production and sandbox (the project's
 *             environment; null where config turns the limit off with 0), exceeded (what the
 *             caller gets past it)
 */
function limitsOf(config) {
    const q = config.apps.quotas;
    // A quota of 0 is off here (store.js); the page reads 0 as "none allowed", so off is null.
    const off = (v) => (v === 0 ? null : v);
    const both = (v) => ({ production: v, sandbox: v });
    return {
        service: 'events',
        scope: 'per project and environment (ADR-014)',
        limits: [
            { id: 'publish_per_minute', label: 'Events published per minute', capability: 'events.app.publish', unit: 'per_minute',
                production: off(q.production.publishPerMinute), sandbox: off(q.sandbox.publishPerMinute), exceeded: '429 events.quota_exceeded' },
            { id: 'retained_bytes', label: 'Event bytes kept', capability: 'events.app.publish', unit: 'bytes',
                production: off(q.production.retainedBytes), sandbox: off(q.sandbox.retainedBytes), exceeded: '429 events.quota_exceeded' },
            { id: 'retention_days', label: 'Days an event is kept', capability: 'events.app.read', unit: 'days',
                production: config.retentionDays, sandbox: config.apps.sandboxRetentionDays, exceeded: 'pruned' },
            { id: 'subscriptions', label: 'Webhook subscriptions', capability: 'events.app.subscribe', unit: 'count',
                production: off(q.production.maxSubscriptions), sandbox: off(q.sandbox.maxSubscriptions), exceeded: '429 events.quota_exceeded' },
            { id: 'payload_bytes', label: 'Payload size of one event', capability: 'events.app.publish', unit: 'bytes',
                ...both(config.maxPayloadBytes), exceeded: '413 events.payload_too_large' },
            { id: 'batch', label: 'Events in one request', capability: 'events.app.publish', unit: 'count',
                ...both(config.maxBatch), exceeded: '413 events.batch_too_large' },
            { id: 'realtime_topics', label: 'Topics on one realtime connection', capability: null, unit: 'count',
                ...both(config.realtime.maxTopics), exceeded: '400 realtime.too_many_topics' },
        ],
    };
}

function mountLimits(app, config) {
    const body = limitsOf(config);
    app.get('/limits.json', (_req, res) => {
        res.set('Cache-Control', 'public, max-age=300').set('Access-Control-Allow-Origin', '*').json(body);
    });
}

module.exports = { limitsOf, mountLimits };
