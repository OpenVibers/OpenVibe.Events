'use strict';
/**
 * cloud-v1 — a QUEUE-only push carrier over an SQS-compatible endpoint (ADR-042 decision 5), only when
 * CLOUD_QUEUE_URL is set.
 *
 * QUEUE: after a publish commits, signal(rows) POSTs { event_id, subscription_id } — one message per new delivery —
 * to the queue endpoint, SigV4-signed with node:crypto and sent with node's fetch. Nothing is consumed here: the
 * endpoint's own consumer claims those rows, and this process's poll is the recovery path. A lost or duplicated
 * message changes latency, never the outcome, because PostgreSQL stays the record and every delivery is committed
 * before the signal.
 *
 * Health: an EWMA of signal latency and a breaker on consecutive errors (../signals.js). healthy() is false while the
 * breaker is open, which drops cloud-v1 from the plan with a reason; pg-v1 (the poll) then carries delivery.
 *
 * Pricing: offer() names rc-cloud-queue-v1. No rate card with that id is written until the owner sets its price, so
 * openvibe-sdk/placement prices this paid provider at Infinity and never assumes it free — pg-v1/valkey-v1 keep
 * carrying QUEUE until the owner publishes the card.
 */
const crypto = require('crypto');
const { createSignals } = require('../signals');

const QUEUE_ACTION = 'SendMessage';
const QUEUE_VERSION = '2012-11-05';
const SERVICE = 'sqs';

const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex');
const hmac = (key, data) => crypto.createHmac('sha256', key).update(data).digest();
/** RFC 3986 encoding of one path segment, as SigV4 wants it. */
const encodeSegment = (s) => encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);

/** The SigningKey/Authorization for one request; `headers` are the ones to sign (lower-case names, host included). */
function signV4({ method, path: p, headers, payloadHash, region, service, accessKeyId, secretAccessKey, amzDate }) {
    const names = Object.keys(headers).map((h) => h.toLowerCase()).sort();
    const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), String(v).trim().replace(/\s+/g, ' ')]));
    const signedHeaders = names.join(';');
    const canonical = [method, p, '', names.map((n) => `${n}:${lower[n]}\n`).join(''), signedHeaders, payloadHash].join('\n');
    const day = amzDate.slice(0, 8);
    const scope = `${day}/${region}/${service}/aws4_request`;
    const toSign = ['AWS4-HMAC-SHA256', amzDate, scope, sha256(canonical)].join('\n');
    const key = hmac(hmac(hmac(hmac(`AWS4${secretAccessKey}`, day), region), service), 'aws4_request');
    const signature = crypto.createHmac('sha256', key).update(toSign).digest('hex');
    return `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
}

/** The queue URL: CLOUD_QUEUE_URL, with CLOUD_QUEUE_PREFIX (if any) added to its path. */
function queueUrl(url, prefix) {
    if (!prefix) return url;
    const u = new URL(url);
    u.pathname = `${u.pathname.replace(/\/+$/, '')}/${String(prefix).replace(/^\/+|\/+$/g, '')}`;
    return u.toString();
}

function createCloudCarrier({ url, region = 'us-east-1', prefix = '', accessKey = '', secretKey = '', clock = { now: () => Date.now() }, log = console, fetchImpl = fetch } = {}) {
    const signals = createSignals({ now: () => clock.now() });
    const endpoint = queueUrl(url, prefix);

    async function send(row) {
        const body = new URLSearchParams({
            Action: QUEUE_ACTION,
            Version: QUEUE_VERSION,
            MessageBody: JSON.stringify({ event_id: row.event_id, subscription_id: row.subscription_id }),
        }).toString();
        const u = new URL(endpoint);
        const amzDate = new Date(clock.now()).toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
        const payloadHash = sha256(body);
        // AWS signs content-type (form data), the payload hash, the date and host. Credentials are optional so a local
        // SQS-compatible endpoint without auth can be used; when present the request is SigV4-signed.
        const headers = { host: u.host, 'content-type': 'application/x-www-form-urlencoded; charset=utf-8', 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate };
        const sent = { 'content-type': headers['content-type'], 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate };
        if (accessKey && secretKey) sent.authorization = signV4({ method: 'POST', path: u.pathname, headers, payloadHash, region, service: SERVICE, accessKeyId: accessKey, secretAccessKey: secretKey, amzDate });
        const res = await fetchImpl(endpoint, { method: 'POST', headers: sent, body, redirect: 'manual' });
        if (!res.ok) throw new Error(`queue answered ${res.status}`);
    }

    async function signalQueue(rows) {
        const t0 = clock.now();
        try {
            for (const r of rows) await send(r);
            signals.record('queue', true, clock.now() - t0);
        } catch (err) {
            signals.record('queue', false);
            log.warn(`[cloud-v1] queue signal: ${err.message}`);   // the credentials are never logged
        }
    }

    return {
        id: 'cloud-v1',
        classes: new Set(['QUEUE']),

        // Nothing to consume: the endpoint's consumer claims the rows and this process's poll recovers what the signal
        // lost. start/stop exist to match the registry's adapter shape.
        async start() {},
        async stop() {},

        signal(rows, kind) { return kind === 'QUEUE' ? signalQueue(rows) : undefined; },
        healthy: () => signals.healthy('queue'),
        unhealthyReason: () => (signals.healthy('queue') ? null : 'breaker open (cloud-v1)'),
        signals,

        offer() {
            const ms = signals.ewma('queue') || 1;
            return {
                offer_id: 'cloud-v1',
                kind: 'provider',
                provider: 'cloud',
                adapter: 'cloud-v1',
                region: 'cloud',
                trust: 'first-party',
                capabilities: ['events:gateway'],
                latency_ms: { publish_p95: ms, ack_p95: ms },
                health: { status: 'up', checked_at: new Date(clock.now()).toISOString() },
                pricing: { model: 'per-operation', unit: 'queue-operation', rate_card: 'rc-cloud-queue-v1' },
                updated_at: new Date(clock.now()).toISOString(),
            };
        },
    };
}

module.exports = { createCloudCarrier, signV4, queueUrl };
