'use strict';
/**
 * Where the archive tier's monthly objects live (ADR-042 decision 8; scripts/events-archive.js only — never an online
 * path). Two adapters with one shape, { name, put(key, buf), get(key) -> Buffer | null }:
 *
 *   local  a directory, EVENTS_ARCHIVE_DIR (default data/archive). A put writes a temporary file, fsyncs it and
 *          renames it into place, so an object is either whole or absent.
 *   s3     an S3-compatible bucket, only when EVENTS_ARCHIVE_S3_BUCKET is set: path-style requests to
 *          EVENTS_ARCHIVE_S3_ENDPOINT (https://…), signed with AWS Signature V4 by node:crypto and sent with node's
 *          fetch — no dependency. EVENTS_ARCHIVE_S3_REGION (default us-east-1), EVENTS_ARCHIVE_S3_ACCESS_KEY_ID,
 *          EVENTS_ARCHIVE_S3_SECRET_ACCESS_KEY, EVENTS_ARCHIVE_S3_PREFIX (default events/). The body is signed too
 *          (x-amz-content-sha256), so the store refuses a body that changed on the way.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const KEY_RE = /^[a-z0-9][a-z0-9._/-]{0,200}$/;
const checkKey = (key) => {
    if (!KEY_RE.test(key) || key.includes('..')) throw new Error(`archive storage: bad object key ${JSON.stringify(key)}`);
    return key;
};

function localStorage({ dir }) {
    const root = path.resolve(dir);
    return {
        name: `local:${root}`,
        async put(key, buf) {
            const file = path.join(root, checkKey(key));
            fs.mkdirSync(path.dirname(file), { recursive: true });
            const tmp = `${file}.tmp-${process.pid}-${crypto.randomBytes(4).toString('hex')}`;
            const fd = fs.openSync(tmp, 'w', 0o640);
            try { fs.writeSync(fd, buf); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
            fs.renameSync(tmp, file);
        },
        async get(key) {
            try { return fs.readFileSync(path.join(root, checkKey(key))); } catch (err) { if (err.code === 'ENOENT') return null; throw err; }
        },
    };
}

const sha256 = (data) => crypto.createHash('sha256').update(data).digest('hex');
const hmac = (key, data) => crypto.createHmac('sha256', key).update(data).digest();
/** RFC 3986 encoding of one path segment, as SigV4 wants it. */
const encodeSegment = (s) => encodeURIComponent(s).replace(/[!'()*]/g, c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);

/**
 * AWS Signature V4 for one request: the Authorization header value. `headers` are the headers to sign (lower-case
 * names; host included), `path` the already-encoded absolute path, `query` the canonical query string ('' for none).
 */
function signV4({ method, path: p, query = '', headers, payloadHash, region, service, accessKeyId, secretAccessKey, amzDate }) {
    const names = Object.keys(headers).map(h => h.toLowerCase()).sort();
    const lower = Object.fromEntries(Object.entries(headers).map(([k, v]) => [k.toLowerCase(), String(v).trim().replace(/\s+/g, ' ')]));
    const signedHeaders = names.join(';');
    const canonical = [method, p, query, names.map(n => `${n}:${lower[n]}\n`).join(''), signedHeaders, payloadHash].join('\n');
    const day = amzDate.slice(0, 8);
    const scope = `${day}/${region}/${service}/aws4_request`;
    const toSign = ['AWS4-HMAC-SHA256', amzDate, scope, sha256(canonical)].join('\n');
    const key = hmac(hmac(hmac(hmac(`AWS4${secretAccessKey}`, day), region), service), 'aws4_request');
    const signature = crypto.createHmac('sha256', key).update(toSign).digest('hex');
    return `AWS4-HMAC-SHA256 Credential=${accessKeyId}/${scope}, SignedHeaders=${signedHeaders}, Signature=${signature}`;
}

function s3Storage({ endpoint, bucket, region = 'us-east-1', accessKeyId, secretAccessKey, prefix = 'events/', fetchImpl = fetch, now = () => new Date() }) {
    if (!endpoint || !bucket || !accessKeyId || !secretAccessKey) {
        throw new Error('archive storage: s3 needs EVENTS_ARCHIVE_S3_ENDPOINT, _BUCKET, _ACCESS_KEY_ID and _SECRET_ACCESS_KEY');
    }
    const base = new URL(endpoint);
    if (base.protocol !== 'https:' && !['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname)) {
        throw new Error('archive storage: EVENTS_ARCHIVE_S3_ENDPOINT must be https (http only on loopback)');
    }
    const basePath = base.pathname.replace(/\/+$/, '');
    async function request(method, key, body) {
        const objectPath = `${basePath}/${encodeSegment(bucket)}/${checkKey(`${prefix}${key}`).split('/').map(encodeSegment).join('/')}`;
        const amzDate = now().toISOString().replace(/[-:]/g, '').replace(/\.\d{3}/, '');
        const payloadHash = sha256(body || '');
        const headers = { host: base.host, 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate };
        const authorization = signV4({ method, path: objectPath, headers, payloadHash, region, service: 's3', accessKeyId, secretAccessKey, amzDate });
        const res = await fetchImpl(`${base.origin}${objectPath}`, {
            method, body, redirect: 'manual',
            headers: { 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate, authorization },
        });
        return res;
    }
    return {
        name: `s3:${base.host}/${bucket}/${prefix}`,
        async put(key, buf) {
            const res = await request('PUT', key, buf);
            if (!res.ok) throw new Error(`archive storage: PUT ${key} answered ${res.status}`);
        },
        async get(key) {
            const res = await request('GET', key);
            if (res.status === 404) return null;
            if (!res.ok) throw new Error(`archive storage: GET ${key} answered ${res.status}`);
            return Buffer.from(await res.arrayBuffer());
        },
    };
}

/** The adapter the environment names: s3 when EVENTS_ARCHIVE_S3_BUCKET is set, else the local directory. */
function fromEnv(env = process.env, { fetchImpl } = {}) {
    if (env.EVENTS_ARCHIVE_S3_BUCKET) {
        return s3Storage({
            endpoint: env.EVENTS_ARCHIVE_S3_ENDPOINT, bucket: env.EVENTS_ARCHIVE_S3_BUCKET, region: env.EVENTS_ARCHIVE_S3_REGION || undefined,
            accessKeyId: env.EVENTS_ARCHIVE_S3_ACCESS_KEY_ID, secretAccessKey: env.EVENTS_ARCHIVE_S3_SECRET_ACCESS_KEY,
            prefix: env.EVENTS_ARCHIVE_S3_PREFIX ?? undefined, ...(fetchImpl ? { fetchImpl } : {}),
        });
    }
    return localStorage({ dir: env.EVENTS_ARCHIVE_DIR || path.join(__dirname, '..', 'data', 'archive') });
}

module.exports = { localStorage, s3Storage, fromEnv, signV4 };
