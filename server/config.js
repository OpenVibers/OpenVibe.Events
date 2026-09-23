'use strict';
/**
 * OpenVibe.Events configuration. Everything comes from the environment (.env in development,
 * /etc/openvibe/events.env in production); see .env.example for the documented list.
 *
 * load(env) is pure so tests can build a config without touching process.env.
 */

const int = (v, d) => {
    const n = parseInt(v, 10);
    return Number.isFinite(n) ? n : d;
};
const list = (v, d) => (v == null || v === '' ? d : String(v).split(',').map(s => s.trim()).filter(Boolean));

/**
 * Which sources may publish which event types. The first segment of every prefix must equal the
 * source, so a service can only ever publish into its own namespace (svc:live -> live.*).
 */
const DEFAULT_SOURCES = ['live', 'media', 'network', 'community', 'chat', 'openre', 'billing', 'tips',
    'vip', 'ai', 'games', 'tools', 'codes', 'host', 'sources', 'search',
    // Publication products (Waves 16-19): each publishes <product>.<type>.<action> via openvibe-publishing.
    'wiki', 'blog', 'news', 'reviews', 'deals', 'coupons', 'trade'];

function sourcePrefixes(env) {
    let map = Object.fromEntries(DEFAULT_SOURCES.map(s => [s, [`${s}.`]]));
    if (env.EVENTS_SOURCE_PREFIXES) {
        // JSON { "<source>": ["<source>.x.", ...] } replaces the default map.
        map = JSON.parse(env.EVENTS_SOURCE_PREFIXES);
    }
    for (const [source, prefixes] of Object.entries(map)) {
        if (!/^[a-z][a-z0-9-]{1,39}$/.test(source)) throw new Error(`EVENTS_SOURCE_PREFIXES: bad source "${source}"`);
        // app.* event types and app-<ulid> sources belong to developer apps (events.app.publish) only.
        if (source === 'app' || source.startsWith('app-')) throw new Error(`EVENTS_SOURCE_PREFIXES: "${source}" is reserved for developer apps`);
        if (!Array.isArray(prefixes) || !prefixes.length) throw new Error(`EVENTS_SOURCE_PREFIXES: ${source} needs at least one prefix`);
        for (const p of prefixes) {
            if (typeof p !== 'string' || p.split('.')[0] !== source) {
                throw new Error(`EVENTS_SOURCE_PREFIXES: prefix "${p}" of ${source} must start with "${source}."`);
            }
        }
    }
    return map;
}

function load(env = process.env) {
    const nodeEnv = env.NODE_ENV || 'development';
    const isProduction = nodeEnv === 'production';
    const port = int(env.PORT, 4300);
    const maxInflight = int(env.EVENTS_MAX_INFLIGHT, 20);
    return {
        port,
        host: env.HOST || '127.0.0.1',
        nodeEnv,
        isProduction,
        baseUrl: (env.BASE_URL || (isProduction ? 'https://events.openvibe.network' : `http://localhost:${port}`)).replace(/\/$/, ''),

        // Identity: OpenVibe.Network issues service tokens and user JWTs (RS256).
        networkUrl: (env.OV_NETWORK_URL || 'https://openvibe.network').replace(/\/$/, ''),
        networkInternalUrl: (env.OV_NETWORK_INTERNAL_URL || 'http://127.0.0.1:4000').replace(/\/$/, ''),
        issuer: (env.OV_NETWORK_ISSUER || env.OV_NETWORK_URL || 'https://openvibe.network').replace(/\/$/, ''),
        // PEM of the Network public key; normally fetched from the JWKS endpoint instead.
        networkPublicKey: env.OV_NETWORK_PUBLIC_KEY ? env.OV_NETWORK_PUBLIC_KEY.replace(/\\n/g, '\n') : null,
        audience: 'openvibe.events',
        // A user JWT is accepted when its aud contains one of these (Network does not mint
        // openvibe.events user tokens yet; the network-wide audience is the stable one).
        userAudiences: list(env.EVENTS_USER_AUDIENCES, ['openvibe.events', 'openvibe.network']),

        dbPath: env.EVENTS_DB_PATH || './data/events.db',
        retentionDays: int(env.EVENTS_RETENTION_DAYS, 30),
        receiptRetentionDays: int(env.EVENTS_RECEIPT_RETENTION_DAYS, 90),
        pruneIntervalMs: int(env.EVENTS_PRUNE_INTERVAL_MS, 60 * 60 * 1000),

        sourcePrefixes: sourcePrefixes(env),
        maxBatch: 100,
        maxPayloadBytes: int(env.EVENTS_MAX_PAYLOAD_BYTES, 64 * 1024),
        maxHops: int(env.EVENTS_MAX_HOPS, 8),

        // Delivery worker
        worker: {
            enabled: env.EVENTS_WORKER !== 'off',
            intervalMs: int(env.EVENTS_WORKER_INTERVAL_MS, 500),
            maxInflight,
            // Developer-app deliveries share at most this many of the slots (default: half).
            maxAppInflight: Math.max(1, Math.min(maxInflight, int(env.EVENTS_APP_MAX_INFLIGHT, Math.floor(maxInflight / 2)))),
            timeoutMs: int(env.EVENTS_DELIVERY_TIMEOUT_MS, 10000),
            maxAttempts: int(env.EVENTS_MAX_ATTEMPTS, 8),
            // Wait after attempt n fails = backoffMs[n-1] (the last value repeats).
            backoffMs: list(env.EVENTS_BACKOFF_MS, ['1000', '5000', '30000', '120000', '600000', '3600000']).map(Number),
        },
        // /api/ready reports `dlq` degraded (still ready) once more dead deliveries than this wait for replay.
        dlqDegradedAt: int(env.EVENTS_DLQ_DEGRADED_AT, 100),
        maxSubscriptionsPerConsumer: int(env.EVENTS_MAX_SUBSCRIPTIONS, 100),
        // Hostname patterns a first-party subscription endpoint may point at (SSRF guard). App
        // subscriptions (events.app.subscribe) use server/egress.js instead: public https only.
        endpointHosts: list(env.EVENTS_ENDPOINT_HOSTS, ['127.0.0.1', '*.openvibe.*']),

        // Developer apps (ADR-014; events.app.publish|read|subscribe). Quotas are per project and
        // environment and enforced here; 0 turns a limit off.
        apps: {
            enabled: env.EVENTS_APPS !== 'off',
            quotas: {
                production: {
                    publishPerMinute: int(env.EVENTS_APP_PUBLISH_PER_MINUTE, 120),
                    retainedBytes: int(env.EVENTS_APP_RETAINED_BYTES, 50 * 1024 * 1024),
                    maxSubscriptions: int(env.EVENTS_APP_MAX_SUBSCRIPTIONS, 20),
                },
                sandbox: {
                    publishPerMinute: int(env.EVENTS_APP_SANDBOX_PUBLISH_PER_MINUTE, 30),
                    retainedBytes: int(env.EVENTS_APP_SANDBOX_RETAINED_BYTES, 5 * 1024 * 1024),
                    maxSubscriptions: int(env.EVENTS_APP_SANDBOX_MAX_SUBSCRIPTIONS, 5),
                },
            },
            // Sandbox events are pruned sooner than everything else.
            sandboxRetentionDays: int(env.EVENTS_APP_SANDBOX_RETENTION_DAYS, 7),
        },

        // Realtime (SSE) gateway
        realtime: {
            maxConnections: int(env.REALTIME_MAX_CONNECTIONS, 2000),
            maxTopics: int(env.REALTIME_MAX_TOPICS, 20),
            heartbeatMs: int(env.REALTIME_HEARTBEAT_MS, 25000),
            replayMax: int(env.REALTIME_REPLAY_MAX, 1000),
            // Browsers are replayed public events this recent only (0: none); older ones are a gap.
            publicReplaySeconds: Math.max(0, int(env.REALTIME_PUBLIC_REPLAY_SECONDS, 300)),
            allowAnonymous: env.REALTIME_ALLOW_ANONYMOUS !== 'false',
            // Extra exact origins allowed besides https://*.openvibe.* (dev: http://localhost:3000).
            extraOrigins: list(env.REALTIME_CORS_ORIGINS, []),
        },
    };
}

module.exports = { load, DEFAULT_SOURCES };
