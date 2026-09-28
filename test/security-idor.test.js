'use strict';
// IDOR (roadmap WS-R task 5): ids swapped between two apps, two projects, two environments and two
// services. With the seeded world of test/security-world.js:
//   - app A reads, rotates, disables and enables app B's subscription (and the production app A2's);
//     service media does the same to live's; each gets 404 and the subscriptions stay as they were
//   - a token for the same app id in the other environment, or naming another project, cannot touch
//     the app's subscription either (sandbox never touches production)
//   - app A publishes into B's namespace, as B's source or actor, into a first-party namespace; live
//     publishes as media or into app.*; each is refused and nothing is stored
//   - apps and services without the operator capability cannot read deliveries or replay anyone's
//     subscription; checkpoints of another project's pattern are refused
// Every refusal leaves the tables (subscriptions, events, deliveries, checkpoints) as they were;
// positive controls show each route works for its owner.
//   node test/security-idor.test.js
const assert = require('assert');
const { suite, appToken, envelope, request } = require('./helpers');
const { buildWorld, ALL } = require('./security-world');
const apps = require('../server/apps');

const t = suite('security-idor');
let w;
const snap = async () => JSON.stringify((await Promise.all(['subscriptions', 'events', 'deliveries', 'consumer_checkpoints'].map(async (tb) => await w.h.db.prepare(`SELECT * FROM "${tb}" ORDER BY 1, 2`).all()))));

t('world', async () => { w = await buildWorld(); });

t('another consumer\'s subscription: 404 on every route, nothing changes', async () => {
    const { tok, subs, ids } = w;
    const attempts = [
        ['appA', subs.appB.id], ['appA', subs.appA2.id], ['appB', subs.appA.id], ['appA2', subs.appA.id],
        ['media', subs.live.id], ['live', subs.media.id], ['live', subs.appA.id], ['appA', subs.live.id],
    ];
    // The same app id in the other environment, or naming another project: never the subscription's own.
    const crafted = {
        'A as production': appToken({ appId: ids.A, projectId: ids.PA, env: 'production', cap: ALL }),
        'A naming project B': appToken({ appId: ids.A, projectId: ids.PB, env: 'sandbox', cap: ALL }),
        'A2 as sandbox': appToken({ appId: ids.A2, projectId: ids.PA, env: 'sandbox', cap: ALL }),
    };
    const bad = [];
    const before = await snap();
    const tryAll = async (who, token, id) => {
        for (const [m, p, body] of [['GET', `/api/v1/subscriptions/${id}`], ['POST', `/api/v1/subscriptions/${id}/rotate-secret`, { secret: 'x'.repeat(40) }],
            ['POST', `/api/v1/subscriptions/${id}/disable`], ['POST', `/api/v1/subscriptions/${id}/enable`]]) {
            const r = await request(w.base, m, p, { token, body });
            if (r.status !== 404 && r.status !== 403) bad.push(`${who} ${m} ${p} → ${r.status}`);
            if (/"secret"|whsec_/.test(r.text)) bad.push(`${who} ${m} ${p}: carries a secret`);
        }
    };
    for (const [who, id] of attempts) await tryAll(who, tok[who], id);
    await tryAll('A as production', crafted['A as production'], subs.appA.id);
    await tryAll('A naming project B', crafted['A naming project B'], subs.appA.id);
    await tryAll('A2 as sandbox', crafted['A2 as sandbox'], subs.appA2.id);
    assert.deepStrictEqual(bad, [], bad.join('\n'));
    assert.strictEqual(await snap(), before, 'the refusals changed nothing');
    // Lists: the crafted tokens list nothing of A's.
    for (const [who, token] of Object.entries(crafted)) {
        const r = await request(w.base, 'GET', '/api/v1/subscriptions', { token });
        assert.ok(!(r.body && r.body.subscriptions || []).some((s) => s.id === subs.appA.id || s.id === subs.appA2.id), `${who} lists none of A's subscriptions`);
    }
    // Positive controls: each owner can.
    for (const [who, id] of [['appB', subs.appB.id], ['live', subs.live.id], ['appA', subs.appA.id]]) {
        assert.strictEqual((await request(w.base, 'POST', `/api/v1/subscriptions/${id}/disable`, { token: tok[who] })).status, 200, `${who} disables its own`);
        assert.strictEqual((await request(w.base, 'POST', `/api/v1/subscriptions/${id}/enable`, { token: tok[who] })).status, 200);
        const r = await request(w.base, 'POST', `/api/v1/subscriptions/${id}/rotate-secret`, { token: tok[who], body: {} });
        assert.strictEqual(r.status, 200); assert.match(r.body.secret, /^whsec_/);
    }
});

t('publishing into another namespace, source or project is refused and stores nothing', async () => {
    const { tok, ids } = w;
    const ev = (appId, projectId, over = {}) => envelope('x', { source: apps.appSource(appId), event_type: `app.${apps.projectKey(projectId)}.order.created`, actor: { type: 'app', id: appId }, subject: { type: 'order', id: '1' }, ...over });
    const before = await snap();
    const tries = [
        ['app A into B\'s namespace', tok.appA, ev(ids.A, ids.PB)],
        ['app A as B\'s source', tok.appA, ev(ids.A, ids.PA, { source: apps.appSource(ids.B) })],
        ['app A as B\'s actor', tok.appA, ev(ids.A, ids.PA, { actor: { type: 'app', id: ids.B } })],
        ['app A into live.*', tok.appA, ev(ids.A, ids.PA, { event_type: 'live.stream.started' })],
        ['app A as live', tok.appA, ev(ids.A, ids.PA, { source: 'live' })],
        ['app B with A\'s project id in the body', tok.appB, ev(ids.B, ids.PA, { project_id: ids.PA })],
        ['live as media', tok.live, envelope('media', { event_type: 'media.vod.ready' })],
        ['live into app.*', tok.live, envelope('live', { event_type: `app.${ids.keyA}.order.created` })],
        ['a reader without publish', tok.reader, envelope('search')],
        ['the operator', tok.ops, envelope('ops')],
        ['a person', tok.userX, envelope('live')],
        ['nobody', undefined, envelope('live')],
    ];
    const bad = [];
    for (const [what, token, body] of tries) {
        const r = await request(w.base, 'POST', '/api/v1/events', { token, body });
        if (r.status < 400) bad.push(`${what} → ${r.status}`);
        const r2 = await request(w.base, 'POST', '/api/v1/events', { token, body: { events: [body] } });
        if (r2.status < 400 && !(r2.body && Array.isArray(r2.body.results) && r2.body.results.every((x) => x.status >= 400 || x.error))) bad.push(`${what} (batch) → ${r2.status} ${r2.text.slice(0, 120)}`);
    }
    assert.deepStrictEqual(bad, [], bad.join('\n'));
    assert.strictEqual(await snap(), before, 'nothing was stored or queued');
    // Positive controls: each publishes into its own.
    assert.strictEqual((await request(w.base, 'POST', '/api/v1/events', { token: tok.appB, body: ev(ids.B, ids.PB) })).status, 201);
    assert.strictEqual((await request(w.base, 'POST', '/api/v1/events', { token: tok.live, body: envelope('live') })).status, 201);
});

t('deliveries, replay and checkpoints across consumers', async () => {
    const { tok, subs, ids, ev } = w;
    const before = await snap();
    const bad = [];
    for (const who of ['appA', 'appB', 'appA2', 'live', 'media', 'reader']) {
        for (const [m, p, body] of [['GET', `/api/v1/deliveries?subscription_id=${subs.appB.id}`], ['GET', '/api/v1/deliveries?status=dead'],
            ['POST', '/api/v1/deliveries/replay', { subscription_id: subs.appB.id, from_seq: 0 }], ['POST', '/api/v1/deliveries/replay', { subscription_id: subs.live.id, event_ids: Object.values(ev) }]]) {
            const r = await request(w.base, m, p, { token: tok[who], body });
            if (r.status !== 401 && r.status !== 403) bad.push(`${who} ${m} ${p} → ${r.status}`);
        }
    }
    // An app token carrying the operator capability is still an app.
    const appOps = appToken({ appId: ids.A2, projectId: ids.PA, env: 'production', cap: ['events.delivery.admin'] });
    for (const [m, p, body] of [['GET', '/api/v1/deliveries'], ['POST', '/api/v1/deliveries/replay', { subscription_id: subs.appB.id, from_seq: 0 }]]) {
        const r = await request(w.base, m, p, { token: appOps, body });
        if (r.status !== 403) bad.push(`app with events.delivery.admin ${m} ${p} → ${r.status}`);
    }
    // Checkpoints of another project's pattern.
    for (const [token, topic] of [[tok.appA, `app.${ids.keyB}.*`], [tok.appB, `app.${ids.keyA}.*`], [tok.appA, '*']]) {
        const r = await request(w.base, 'PUT', '/api/v1/checkpoints', { token, body: { topic, cursor: 99 } });
        if (r.status !== 403) bad.push(`checkpoint ${topic} → ${r.status}`);
    }
    assert.deepStrictEqual(bad, [], bad.join('\n'));
    assert.strictEqual(await snap(), before, 'the refusals changed nothing');
    const r = await request(w.base, 'POST', '/api/v1/deliveries/replay', { token: tok.ops, body: { subscription_id: subs.appB.id, from_seq: 0 } });
    assert.strictEqual(r.status, 200, 'the operator replays (positive control)');
});

t('stop', async () => { await w.stop(); });

t.run();
