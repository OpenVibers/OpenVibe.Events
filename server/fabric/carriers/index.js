'use strict';
/**
 * The carrier registry (ADR-042 decisions 1-2, 5): one adapter per carrier, each with an id, the classes it can
 * carry, an offer, health, a signal() and a start/stop. pg-v1 is always present and carries every class; valkey-v1
 * is registered only when VALKEY_URL is set and carries QUEUE and TOPIC; nats-v1 is registered only when NATS_URL is
 * set and carries TOPIC over NATS Core. JetStream is not built yet (it needs the host unit), so STREAM is left to pg-v1.
 *
 * Config (server/config.js):
 *   EVENTS_CARRIERS           ids to instantiate (default pg-v1,valkey-v1,nats-v1). An unknown id refuses to start.
 *   NATS_URL                  nats://[user:pass@]host:port of the cell's NATS (nats-v1); NATS_SUBJECT_PREFIX its subjects.
 *   EVENTS_CARRIERS_DISABLED  ids excluded at once with reason `disabled by configuration` (the ADR's rollback).
 *
 * The planner asks offers(class) for one resource-offer@1 per eligible adapter and forClass(class) for the
 * candidates to explain (including the excluded ones). Signal dispatch is by the chosen carrier id.
 */
const { createPgCarrier } = require('./pg');
const { createValkeyCarrier, newInstanceId } = require('./valkey');
const { createNatsCarrier } = require('./nats');

const KNOWN = ['pg-v1', 'valkey-v1', 'nats-v1'];

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
    // nats-v1 connects in start(), which skips a disabled adapter: switched off, it never opens a socket.
    if (config.nats && config.nats.url) {
        adapters.push(createNatsCarrier({ url: config.nats.url, subjectPrefix: config.nats.subjectPrefix, clock, log, instanceId: newInstanceId() }));
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

        /** Signal one carrier with rows (kind is 'QUEUE' or 'TOPIC'); fail open — never throws to the caller. */
        async signal(carrierId, rows, kind) {
            const a = get(carrierId);
            if (!a || a.disabledReason || !rows || !rows.length) return;
            try { await a.signal(rows, kind); } catch (err) { log.warn(`[carriers] ${carrierId} signal: ${err.message}`); }
        },

        /** After a publish commits: QUEUE deliveries to their stream, TOPIC events to their channel. The worker's
         *  kick() beside this is what makes a local process fast; a remote process is woken by the carrier. */
        async signalPublish(events = [], deliveries = []) {
            const byQueue = new Map();
            for (const d of deliveries) {
                if (d.carrier_class !== 'QUEUE' || !d.carrier) continue;
                if (!byQueue.has(d.carrier)) byQueue.set(d.carrier, []);
                byQueue.get(d.carrier).push(d);
            }
            for (const [cid, rows] of byQueue) await this.signal(cid, rows, 'QUEUE');

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
