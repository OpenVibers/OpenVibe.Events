'use strict';
// Project export tokens (OpenVibe.Network, roadmap WS-N task 9): OpenVibe.Codes pulls a project's app
// events for its owner or admin with a token Network mints as an app token of the project's export
// principal: sub app:app_<project ULID>, cap [events.app.read], ns [project_id, app.<project_id>.*],
// env, on_behalf_of, purpose export. Events takes it with no change of its own; this pins that it
// reads the project's events in the token's environment, page by page, and does nothing else.
const assert = require('assert');
const crypto = require('crypto');
const { ids, serviceAuth } = require('openvibe-contracts');
const apps = require('../server/apps');
const { ISSUER, privateKey, boot, request, appToken, envelope, suite } = require('./helpers');

const t = suite('export-tokens');
const P = `prj_${ids.ulid()}`;
const other = `prj_${ids.ulid()}`;
const key = apps.projectKey(P);
const appId = ids.newId('app');

/** A token shaped exactly as Network's mintExportToken shapes it. */
function exportToken(env, { project = P, cap = ['events.app.read'] } = {}) {
    const now = Math.floor(Date.now() / 1000);
    return serviceAuth.signServiceToken({
        iss: ISSUER, sub: `app:app_${project.replace(/^prj_/, '')}`, actor_type: 'app', aud: ['openvibe.events'], cap,
        ns: [project, `app.${project}.*`], project_id: project, env, on_behalf_of: ids.newId('user'), purpose: 'export',
        iat: now, exp: now + 300, jti: `tok_${crypto.randomBytes(8).toString('hex')}`,
    }, privateKey);
}

let h;
const seqs = { production: [], sandbox: [] };
const pull = (token, after = 0, limit = 100, topic = `app.${key}.*`) => request(h.base, 'GET', `/api/v1/events?topic=${encodeURIComponent(topic)}&after_seq=${after}&limit=${limit}`, { token });

t('boot and publish as the project\'s app, in both environments (and another project)', async () => {
    h = await boot();
    const publish = async (project, env, n) => {
        const r = await request(h.base, 'POST', '/api/v1/events', {
            token: appToken({ appId, projectId: project, env, cap: ['events.app.publish'] }),
            body: envelope('x', { source: apps.appSource(appId), event_type: `app.${apps.projectKey(project)}.order.n${n}`, actor: { type: 'app', id: appId }, visibility: 'internal' }),
        });
        assert.strictEqual(r.status, 201, r.text);
        return r.body.seq;
    };
    for (let i = 0; i < 5; i++) seqs.production.push(await publish(P, 'production', i));
    for (let i = 0; i < 2; i++) seqs.sandbox.push(await publish(P, 'sandbox', i));
    await publish(other, 'production', 9);
});

t('an export token pulls the project\'s events of its environment, page by page to the end', async () => {
    const got = [];
    let after = 0;
    let latest = null;
    for (let page = 0; page < 10; page++) {
        const r = await pull(exportToken('production'), after, 2);
        assert.strictEqual(r.status, 200, r.text);
        latest = latest === null ? r.body.latest_seq : latest;
        got.push(...r.body.events.map(e => e.seq));
        if (r.body.next_after_seq <= after || r.body.next_after_seq >= latest) break;
        after = r.body.next_after_seq;
    }
    assert.deepStrictEqual(got, seqs.production);
    const s = await pull(exportToken('sandbox'));
    assert.strictEqual(s.status, 200, s.text);
    assert.deepStrictEqual(s.body.events.map(e => e.seq), seqs.sandbox, 'the sandbox token sees the sandbox only');
});

t('it reads nothing else and writes nothing', async () => {
    let r = await pull(exportToken('production'), 0, 100, `app.${apps.projectKey(other)}.*`);
    assert.strictEqual(r.status, 403, 'another project\'s topic');
    r = await pull(exportToken('production', { project: other }), 0, 100);
    assert.strictEqual(r.status, 403, 'a token of another project does not name this project_key');
    const sub = `app_${P.replace(/^prj_/, '')}`;
    r = await request(h.base, 'POST', '/api/v1/events', { token: exportToken('production'), body: envelope('x', { source: apps.appSource(sub), event_type: `app.${key}.forged`, actor: { type: 'app', id: sub }, visibility: 'internal' }) });
    assert.strictEqual(r.status, 403, 'no publish');
    r = await request(h.base, 'GET', '/api/v1/subscriptions', { token: exportToken('production') });
    assert.strictEqual(r.status, 403, 'no subscriptions');
    r = await request(h.base, 'GET', '/api/v1/deliveries', { token: exportToken('production') });
    assert.ok([401, 403].includes(r.status), 'no operator routes');
});

t('stop', async () => { await h.stop(); });

t.run();
