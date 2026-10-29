'use strict';
/**
 * OpenAPI 3.1 for OpenVibe.Events' JSON API, from the route table below.
 *
 * ROUTES is the table: every entry is one method on one path under /api/v1. server/app.js mounts
 * exactly these routers, and test/openapi.test.js walks the mounted stack and fails when the table
 * and the app differ in either direction — so the document cannot advertise an address the service
 * does not answer.
 *
 *   GET /openapi.json            this document (public, Cache-Control 5 minutes)
 *   GET /api/v1/openapi.json     308 to /openapi.json
 *
 * /api/health and /api/ready are operational probes, not API surface. The authority resource index
 * (/api/v1/resources, /api/v1/resources/control) is OpenVibe.Services' service-to-service view of the
 * subscriptions this service owns; it is not part of the public API and stays out.
 *
 * Request fields are what server/api/*.js reads out of the body and query string; response properties
 * are the serializers' output (store.js, read.js, policies.js). Nothing is inferred.
 */
const pkg = require('../package.json');

const MOUNT = '/api/v1';
const SITE = 'https://openvibe.events';

const ref = (name) => ({ $ref: `#/components/schemas/${name}` });
const str = (extra = {}) => ({ type: 'string', ...extra });
const nullable = (type, extra = {}) => ({ type: [type, 'null'], ...extra });
const arrayOf = (items) => ({ type: 'array', items });
const integer = (extra = {}) => ({ type: 'integer', ...extra });

const problem = {
    description: 'A problem (RFC 9457, application/problem+json): type, title, status and a stable code',
    content: {
        'application/problem+json': {
            schema: {
                type: 'object',
                required: ['type', 'title', 'status'],
                properties: { type: str(), title: str(), status: integer(), code: str(), detail: str(), request_id: str() },
            },
        },
    },
};
const errors = (codes = [400, 401, 403, 404, 409, 413, 422, 429]) => ({
    ...Object.fromEntries(codes.map((code) => [code, problem])),
    default: problem,
});

const path = (name, schema, description) => ({ name, in: 'path', required: true, schema, ...(description && { description }) });
const query = (name, schema, description) => ({ name, in: 'query', required: false, schema, ...(description && { description }) });

const ANONYMOUS = [{}];
const SERVICE = [{ bearer: [] }];
const APP = [{ app: [] }];
const ADMIN = [{ bearer: [] }];

const json = (description, schema) => ({ description, ...(schema ? { content: { 'application/json': { schema } } } : {}) });
const body = (schema, required = true) => ({ required, content: { 'application/json': { schema } } });

const topicPattern = str({ pattern: '^[a-z0-9_]+(\\.[a-z0-9_]+)*(\\.[*])?$', description: 'Dot-separated segments of [a-z0-9_] or *' });
const subscription = ref('Subscription');
const checkpoint = ref('Checkpoint');
const eventItem = { type: 'object', required: ['cursor', 'event'], properties: { cursor: str({ description: 'Opaque cursor' }), event: ref('EventEnvelope') } };

const PATHS = {};
function route(method, p, operation) {
    (PATHS[p] = PATHS[p] || {})[method] = operation;
}

// ── Publish and pull ───────────────────────────────────────────────────────────────────────────
route('post', '/events', {
    operationId: 'publishEvents', summary: 'Publish one event or a batch', tags: ['events'], security: [...SERVICE, ...APP],
    description: 'events.event.publish (a first-party service token, audience openvibe.events) or events.app.publish (a developer app token). A repeat event_id is answered with its stored position and never stored twice. `source` must be the calling service (or the app\'s own source) and `event_type` must start with a prefix it owns. A payload carrying `redacts` tombstones the named earlier events of the same source.',
    requestBody: body({ oneOf: [
        ref('EventEnvelope'),
        { type: 'object', required: ['events'], properties: { events: { type: 'array', minItems: 1, maxItems: 100, items: ref('EventEnvelope') } } },
    ] }),
    responses: {
        201: json('Stored (single: the receipt; batch: one result per event, in order)', { oneOf: [
            ref('PublishReceipt'),
            { type: 'object', required: ['results'], properties: { results: arrayOf(ref('PublishReceipt')) } },
        ] }),
        200: json('Every event was already stored (batch form: { results })'),
        ...errors([400, 401, 403, 413, 422, 429, 503]),
    },
});
route('get', '/events', {
    operationId: 'pullEvents', summary: 'Pull events by topic pattern from a cursor', tags: ['events'], security: [...SERVICE, ...APP],
    description: 'events.event.read (service) or events.app.read (app): a page of retained events queued after `after`. `latest_cursor` is the head; a cursor from another retention epoch answers `gap`. `after_seq` is refused (400).',
    parameters: [
        query('topic', str({ description: '1 to 20 comma-separated patterns; default *' })),
        query('after', str({ description: 'Opaque cursor: a page\'s next_cursor, or latest_cursor for the head' })),
        query('limit', integer({ minimum: 1, maximum: 1000, default: 100 })),
    ],
    responses: { 200: json('A page and the head', { type: 'object', required: ['events', 'next_cursor', 'latest_cursor'], properties: {
        events: arrayOf(eventItem), next_cursor: str(), latest_cursor: str(),
        gap: { type: 'object', properties: { from_seq: integer(), to_seq: integer() } },
    } }), ...errors([400, 401, 403, 429]) },
});
route('get', '/events/{id}', {
    operationId: 'getEvent', summary: 'One event (hot store or replay tier)', tags: ['events'], security: [...SERVICE, ...APP],
    parameters: [path('id', str({ pattern: '^evt_[0-9A-HJKMNP-TV-Z]{26}$' }), 'The event id')],
    responses: { 200: json('The event and its position', eventItem), ...errors([401, 403, 404, 429]) },
});

// ── Checkpoints ───────────────────────────────────────────────────────────────────────────────
route('get', '/checkpoints', {
    operationId: 'getCheckpoint', summary: 'The caller\'s stored cursor for a topic pattern', tags: ['checkpoints'], security: [...SERVICE, ...APP],
    parameters: [query('topic', topicPattern, 'Required')],
    responses: { 200: json('The checkpoint (cursor null when none is stored)', checkpoint), ...errors([400, 401, 403, 429]) },
});
route('put', '/checkpoints', {
    operationId: 'putCheckpoint', summary: 'Store the caller\'s cursor for a topic pattern', tags: ['checkpoints'], security: [...SERVICE, ...APP],
    requestBody: body({ type: 'object', required: ['topic', 'cursor'], properties: {
        topic: topicPattern, cursor: str({ description: 'An opaque cursor (a page\'s next_cursor), never a number' }), carrier: nullable('string'),
    } }),
    responses: { 200: json('The stored checkpoint', checkpoint), ...errors([400, 401, 403, 429]) },
});

// ── Subscriptions ─────────────────────────────────────────────────────────────────────────────
route('get', '/subscriptions', {
    operationId: 'listSubscriptions', summary: 'The caller\'s webhook subscriptions (no secrets)', tags: ['subscriptions'], security: [...SERVICE, ...APP],
    responses: { 200: json('Subscriptions', { type: 'object', required: ['subscriptions'], properties: { subscriptions: arrayOf(subscription) } }), ...errors([401, 403, 429]) },
});
route('post', '/subscriptions', {
    operationId: 'createSubscription', summary: 'Subscribe an endpoint to a topic pattern', tags: ['subscriptions'], security: [...SERVICE, ...APP],
    description: 'events.subscription.manage (service) or events.app.subscribe (app). The endpoint must be http(s) on the allow-list; an app\'s endpoint must be https and resolve only to public addresses. The signing secret is shown once.',
    requestBody: body({ type: 'object', required: ['topic_pattern', 'endpoint'], properties: {
        topic_pattern: topicPattern, endpoint: str({ format: 'uri' }),
        secret: str({ minLength: 32, maxLength: 256, description: 'Optional; a whsec_ secret is generated when absent' }),
        retry_policy: { type: 'object', properties: { max_attempts: integer({ minimum: 1, maximum: 20 }), backoff_ms: arrayOf(integer({ minimum: 0, maximum: 86400000 })) } },
    } }),
    responses: { 201: json('The subscription and its secret', { allOf: [subscription, { type: 'object', properties: { secret: str() } }] }), ...errors([400, 401, 403, 409, 422, 429]) },
});
route('get', '/subscriptions/{id}', {
    operationId: 'getSubscription', summary: 'One of the caller\'s subscriptions', tags: ['subscriptions'], security: [...SERVICE, ...APP],
    parameters: [path('id', str({ pattern: '^sub_[0-9A-HJKMNP-TV-Z]{26}$' }))],
    responses: { 200: json('The subscription', subscription), ...errors([401, 403, 404, 429]) },
});
route('post', '/subscriptions/{id}/rotate-secret', {
    operationId: 'rotateSubscriptionSecret', summary: 'Rotate a subscription\'s signing secret', tags: ['subscriptions'], security: [...SERVICE, ...APP],
    description: 'The old secret keeps signing next to the new one for overlap_s (default one day, at most seven).',
    parameters: [path('id', str({ pattern: '^sub_[0-9A-HJKMNP-TV-Z]{26}$' }))],
    requestBody: body({ type: 'object', properties: {
        secret: str({ minLength: 32, maxLength: 256 }), overlap_s: integer({ minimum: 0, maximum: 604800 }),
    } }, false),
    responses: { 200: json('The subscription, its new secret and when the old one stops', { allOf: [subscription, { type: 'object', properties: { secret: str(), previous_secret_valid_until: str({ format: 'date-time' }) } }] }), ...errors([400, 401, 403, 404, 422, 429]) },
});
for (const [action, enabled] of [['disable', false], ['enable', true]]) {
    route('post', `/subscriptions/{id}/${action}`, {
        operationId: `${action}Subscription`, summary: `${enabled ? 'Enable' : 'Disable'} a subscription`, tags: ['subscriptions'], security: [...SERVICE, ...APP],
        parameters: [path('id', str({ pattern: '^sub_[0-9A-HJKMNP-TV-Z]{26}$' }))],
        responses: { 200: json('The subscription', subscription), ...errors([401, 403, 404, 429]) },
    });
}

// ── Deliveries (operators) ────────────────────────────────────────────────────────────────────
route('get', '/deliveries', {
    operationId: 'listDeliveries', summary: 'Delivery queue and dead letters (staff)', tags: ['deliveries'], security: ADMIN,
    description: 'events.delivery.admin.',
    parameters: [
        query('status', { type: 'string', enum: ['pending', 'delivered', 'failed', 'dead'] }),
        query('subscription_id', str()), query('after_seq', integer({ minimum: 0 })), query('limit', integer({ minimum: 1, maximum: 1000, default: 100 })),
    ],
    responses: { 200: json('Deliveries and the counts by status', { type: 'object', properties: {
        deliveries: arrayOf(ref('Delivery')), counts: { type: 'object', additionalProperties: integer() },
    } }), ...errors([400, 401, 403, 429]) },
});
route('post', '/deliveries/replay', {
    operationId: 'replayDeliveries', summary: 'Requeue a subscription\'s retained events (staff)', tags: ['deliveries'], security: ADMIN,
    description: 'events.delivery.admin. Pass exactly one of from_seq or event_ids.',
    requestBody: body({ type: 'object', required: ['subscription_id'], properties: {
        subscription_id: str({ pattern: '^sub_' }), from_seq: integer({ minimum: 0 }), event_ids: { type: 'array', items: str(), maxItems: 1000 },
    } }),
    responses: { 200: json('What was queued', { type: 'object', properties: { subscription_id: str(), queued: integer() } }), ...errors([400, 401, 403, 404, 429]) },
});


// ── Delivery policies and placement (operators) ──────────────────────────────────────────────
route('get', '/delivery-policies', {
    operationId: 'listDeliveryPolicies', summary: 'Delivery policies by topic pattern (staff)', tags: ['delivery-policies'], security: ADMIN,
    description: 'events.delivery.admin.',
    responses: { 200: json('Policies', { type: 'object', required: ['policies'], properties: { policies: arrayOf(ref('DeliveryPolicy')) } }), ...errors([401, 403, 429]) },
});
route('get', '/delivery-policies/resolve', {
    operationId: 'resolveDeliveryPolicy', summary: 'The policy that governs an event type (staff)', tags: ['delivery-policies'], security: ADMIN,
    description: 'events.delivery.admin.',
    parameters: [query('event_type', str({ maxLength: 200 }), 'Required')],
    responses: { 200: json('The resolved policy', { type: 'object', properties: { event_type: str(), pattern: str(), revision: integer(), policy: { type: 'object' }, carrier_class: str() } }), ...errors([400, 401, 403, 429]) },
});
route('put', '/delivery-policies/{pattern}', {
    operationId: 'putDeliveryPolicy', summary: 'Create or replace a delivery policy (staff)', tags: ['delivery-policies'], security: ADMIN,
    description: 'events.delivery.admin. The body is events.delivery-policy@1.',
    parameters: [path('pattern', topicPattern)],
    requestBody: body({ type: 'object' }),
    responses: { 200: json('The stored policy', ref('DeliveryPolicy')), ...errors([400, 401, 403, 422, 429]) },
});
route('delete', '/delivery-policies/{pattern}', {
    operationId: 'deleteDeliveryPolicy', summary: 'Remove a delivery policy (staff)', tags: ['delivery-policies'], security: ADMIN,
    description: 'events.delivery.admin.',
    parameters: [path('pattern', topicPattern)],
    responses: { 204: { description: 'Removed' }, ...errors([401, 403, 404, 422, 429]) },
});
route('get', '/placement', {
    operationId: 'explainPlacement', summary: 'Explain the carrier placement for an event type (staff)', tags: ['placement'], security: ADMIN,
    description: 'events.delivery.admin. platform.placement-result@1 plus the last decision that changed.',
    parameters: [query('event_type', str({ maxLength: 200 }), 'Required'), query('key', str({ maxLength: 200 }))],
    responses: { 200: json('The resolved policy and placement', { type: 'object', properties: {
        event_type: str(), policy: { type: 'object' }, pattern: str(), carrier_class: str(), ordering_key: str(),
        placement: { type: 'object' }, last_decision: nullable('object'),
    } }), ...errors([400, 401, 403, 429]) },
});
route('get', '/placement/deliveries/{event_id}/{subscription_id}', {
    operationId: 'getDeliveryPlacement', summary: 'The placement in force for one delivery (staff)', tags: ['placement'], security: ADMIN,
    description: 'events.delivery.admin.',
    parameters: [path('event_id', str({ pattern: '^evt_' })), path('subscription_id', str({ pattern: '^sub_' }))],
    responses: { 200: json('The carrier and the decision', { type: 'object', properties: { carrier: nullable('string'), decision: nullable('object') } }), ...errors([401, 403, 404, 429]) },
});

function openapi() {
    return {
        openapi: '3.1.0',
        info: {
            title: 'OpenVibe.Events API',
            version: pkg.version,
            description: pkg.description,
            license: { name: 'AGPL-3.0-only', identifier: 'AGPL-3.0-only' },
            contact: { name: 'OpenVibers', url: 'https://github.com/OpenVibers/OpenVibe.Events', email: 'contact@openvibe.network' },
        },
        servers: [{ url: SITE }],
        tags: ['events', 'checkpoints', 'subscriptions', 'deliveries', 'delivery-policies', 'placement'].map((name) => ({ name })),
        components: {
            securitySchemes: {
                bearer: { type: 'http', scheme: 'bearer', bearerFormat: 'JWT', description: 'A Network service token for audience openvibe.events carrying the route\'s capability (events.event.publish, events.event.read, events.subscription.manage, events.delivery.admin)' },
                app: { type: 'http', scheme: 'bearer', bearerFormat: 'JWT', description: 'A developer app token holding events.app.publish, events.app.read or events.app.subscribe; it acts only for its own project and environment' },
            },
            schemas: {
                EventEnvelope: { type: 'object', required: ['event_id', 'event_type', 'version', 'source', 'actor', 'timestamp', 'subject', 'payload'], properties: {
                    event_id: str({ pattern: '^evt_[0-9A-HJKMNP-TV-Z]{26}$' }), trace_id: str({ pattern: '^[0-9a-f]{32}$' }),
                    event_type: str({ pattern: '^[a-z][a-z0-9_]*(\\.[a-z0-9_]+){2,}$' }), version: integer({ minimum: 1 }),
                    source: str({ pattern: '^[a-z][a-z0-9-]{1,39}$' }),
                    actor: ref('SubjectRef'), on_behalf_of: ref('SubjectRef'), timestamp: str({ format: 'date-time' }),
                    priority: { type: 'string', enum: ['critical', 'important', 'low'], default: 'important' },
                    visibility: { type: 'string', enum: ['public', 'subject', 'internal'], default: 'internal' },
                    subject: ref('SubjectRef'), payload: { type: 'object' },
                } },
                SubjectRef: { type: 'object', required: ['type', 'id'], properties: {
                    type: str(), id: str(), revision: integer(),
                }, additionalProperties: false },
                PublishReceipt: { type: 'object', required: ['event_id', 'duplicate'], properties: {
                    event_id: str(), duplicate: { type: 'boolean' }, cursor: str({ description: 'Opaque position; absent when a duplicate was pruned by retention' }),
                    pruned: { type: 'boolean' },
                } },
                Subscription: { type: 'object', required: ['id', 'consumer', 'topic_pattern', 'endpoint', 'enabled'], properties: {
                    id: str({ pattern: '^sub_' }), consumer: str(), topic_pattern: str(), endpoint: str({ format: 'uri' }),
                    enabled: { type: 'boolean' }, retry_policy: nullable('object'), created_at: str({ format: 'date-time' }), updated_at: str({ format: 'date-time' }),
                    project_id: str({ pattern: '^prj_' }), env: { type: 'string', enum: ['production', 'sandbox'] }, secret: str({ description: 'Only in the response that creates or rotates it' }),
                } },
                Checkpoint: { type: 'object', required: ['consumer', 'topic', 'cursor'], properties: {
                    consumer: str(), topic: str(), cursor: nullable('string'), carrier: nullable('string'), updated_at: nullable('string', { format: 'date-time' }),
                } },
                DeliveryPolicy: { type: 'object', properties: {
                    pattern: str(), policy: { type: 'object' }, revision: integer(), updated_at: str({ format: 'date-time' }),
                    updated_by: nullable('string'), carrier_class: str(),
                } },
                Delivery: { type: 'object', properties: {
                    event_id: str(), subscription_id: str(), seq: integer(), status: { type: 'string', enum: ['pending', 'delivered', 'failed', 'dead'] },
                    attempt: integer(), next_attempt_at: nullable('string', { format: 'date-time' }), last_status: nullable('integer'),
                    last_error: nullable('string'), delivered_at: nullable('string', { format: 'date-time' }),
                } },
            },
        },
        paths: { ...Object.fromEntries(Object.entries(PATHS).sort(([a], [b]) => a.localeCompare(b)).map(([p, item]) => [MOUNT + p, item])) },
    };
}

/** The documented routes as "METHOD /api/v1/path": what the app must mount. */
function routes() {
    return Object.entries(PATHS).flatMap(([p, item]) => Object.keys(item).map((method) => `${method.toUpperCase()} ${MOUNT}${p}`)).sort();
}

/** Serve GET /openapi.json here; GET /api/v1/openapi.json redirects to it (308). */
function mount(app, document = openapi()) {
    const send = (_req, res) => {
        res.set('Cache-Control', 'public, max-age=300');
        res.type('application/json; charset=utf-8').send(JSON.stringify(document));
    };
    app.get('/openapi.json', send);
    app.get('/api/v1/openapi.json', (_req, res) => res.redirect(308, '/openapi.json'));
}

module.exports = { openapi, routes, mount, MOUNT };
