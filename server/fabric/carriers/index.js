'use strict';
/**
 * The carrier registry (ADR-042 decisions 1-2, 5): one adapter per carrier, each with an id, the classes it can
 * carry, an offer, health, a signal() and a start/stop. pg-v1 is always present and carries every class; valkey-v1
 * is registered only when VALKEY_URL is set and carries QUEUE and TOPIC; nats-v1 is registered only when NATS_URL is
 * set and carries TOPIC over NATS Core; nats-js-v1 is registered when NATS_URL is set and NATS_JETSTREAM is not `off`
 * and carries STREAM over JetStream; cloud-v1 is registered only when CLOUD_QUEUE_URL is set and carries QUEUE.
 * Without them STREAM is pg-v1's.
 *
 * Config (server/config.js):
 *   EVENTS_CARRIERS           ids to instantiate (default pg-v1,valkey-v1,nats-v1,nats-js-v1,cloud-v1). An unknown id refuses to start.
 *   NATS_URL                  nats://[user:pass@]host:port of the cell's NATS (nats-v1, nats-js-v1); NATS_SUBJECT_PREFIX
 *                             its subjects; NATS_JETSTREAM=off leaves nats-js-v1 out; NATS_STREAM its stream name.
 *   CLOUD_QUEUE_URL           the SQS-compatible endpoint of cloud-v1 (CLOUD_QUEUE_REGION, _PREFIX, _ACCESS_KEY,
 *                             _SECRET_KEY); unset leaves cloud-v1 out.
 *   EVENTS_CARRIERS_DISABLED  ids excluded at once with reason `disabled by configuration` (the ADR's rollback).
 *
 * The planner asks offers(class) for one resource-offer@1 per eligible adapter and forClass(class) for the
 * candidates to explain (including the excluded ones). Signal dispatch is by the chosen carrier id.
 */
const { createPgCarrier } = require('./pg');
const { createValkeyCarrier, newInstanceId } = require('./valkey');
const { createNatsCarrier } = require('./nats');
const { createJetStreamCarrier } = require('./jetstream');
const { createCloudCarrier } = require('./cloud');

const KNOWN = ['pg-v1', 'valkey-v1', 'nats-v1', 'nats-js-v1', 'cloud-v1'];

function createCarriers({ config, clock = { now: () => Date.now() }, log = console, valkey = null } = {}) {
    const enabled = new Set(config.carriers.enabled);
    for (const id of enabled) {
        if (!KNOWN.includes(id)) throw new Error(`EVENTS_CARRIERS: unknown carrier adapter "${id}" (known: ${KNOWN.join(', ')})`);
    }
    const disabled = new Set(config.carriers.disabled);
    const off = (id) => !enabled.has(id) || disabled.has(id);

    const adapters = [createPgCarrier({ intervalMs: config.worker.intervalMs, now: () => clock.now() })];
    if (valkey) {
        const v = createValkeyCarrier({ valkey, clock, log, instanceId: newInstanceId() });
        v.disabledReason = off('valkey-v1') ? 'disabled by configuration' : null;
        adapters.push(v);
    }
    // nats-v1 and nats-js-v1 connect in start(), which skips a disabled adapter: switched off, they never open a socket.
    if (config.nats && config.nats.url) {
        adapters.push(createNatsCarrier({ url: config.nats.url, subjectPrefix: config.nats.subjectPrefix, clock, log, instanceId: newInstanceId() }));
        if (config.nats.jetstream) {
            adapters.push(createJetStreamCarrier({ url: config.nats.url, subjectPrefix: config.nats.subjectPrefix, stream: config.nats.stream, clock, log, instanceId: newInstanceId() }));
        }
    }
    // cloud-v1 signals over fetch in signal(); a disabled one is never signalled (signal() skips it), so it is inert.
    if (config.cloud && config.cloud.url) {
        adapters.push(createCloudCarrier({ url: config.cloud.url, region: config.cloud.region, prefix: config.cloud.prefix, accessKey: config.cloud.accessKey, secretKey: config.cloud.secretKey, clock, log }));
    }
    for (const a of adapters) if (!a.disabledReason && off(a.id)) a.disabledReason = 'disabled by configuration';

    const get = (id) => adapters.find((a) => a.id === id) || null;
    const forClass = (klass) => adapters.filter((a) => a.classes.has(klass));
    const eligible = (klass) => forClass(klass).filter((a) => !a.disabledReason);

    return {
        adapters,
        get,

        /** One resource-offer@1 per eligible adapter of `klass`; an unhealthy adapter's health is `down`. */
        offers(klass, now = clock.now()) {
            return eligible(klass).map((a) => {
                const o = a.offer(now);
                if (!a.healthy()) o.health = { ...o.health, status: 'down', checked_at: new Date(now).toISOString() };
                return o;
            });
        },

        /** The eligible adapters for a class, plus the disabled ones to explain. */
        forClass,
        eligible,

        /** Signal one carrier with rows (kind is 'QUEUE', 'TOPIC' or 'STREAM'); fail open — never throws to the caller. */
        async signal(carrierId, rows, kind) {
            const a = get(carrierId);
            if (!a || a.disabledReason || !rows || !rows.length) return;
            try { await a.signal(rows, kind); } catch (err) { log.warn(`[carriers] ${carrierId} signal: ${err.message}`); }
        },

        /** After a publish commits: QUEUE and STREAM deliveries to their carrier, TOPIC events to their channel. The
         *  worker's kick() beside this is what makes a local process fast; a remote process is woken by the carrier. */
        async signalPublish(events = [], deliveries = []) {
            for (const kind of ['QUEUE', 'STREAM']) {
                const byCarrier = new Map();
                for (const d of deliveries) {
                    if (d.carrier_class !== kind || !d.carrier) continue;
                    if (!byCarrier.has(d.carrier)) byCarrier.set(d.carrier, []);
                    byCarrier.get(d.carrier).push(d);
                }
                for (const [cid, rows] of byCarrier) await this.signal(cid, rows, kind);
            }

            const byTopic = new Map();
            for (const e of events) {
                if (e.carrier_class !== 'TOPIC' || !e.carrier) continue;
                if (!byTopic.has(e.carrier)) byTopic.set(e.carrier, []);
                byTopic.get(e.carrier).push(e);
            }
            for (const [cid, rows] of byTopic) await this.signal(cid, rows, 'TOPIC');
        },

        async start(handlers) {
            for (const a of adapters) { if (!a.disabledReason && a.start) await a.start(handlers); }
        },
        async stop() {
            for (const a of adapters) { if (a.stop) await a.stop().catch(() => {}); }
        },
        snapshot() {
            return adapters.map((a) => ({
                id: a.id, classes: [...a.classes], disabled: a.disabledReason || null,
                healthy: a.disabledReason ? false : a.healthy(),
                signals: a.signals ? a.signals.snapshot() : {},
            }));
        },
    };
}

module.exports = { createCarriers, KNOWN };
