'use strict';
/**
 * Events' control API (ADR-048, plan T13 step 10): OpenVibe.Services suspends and resumes a person's subscription by
 * its OVRN. Only a first-party service holding events.resource.control, only for the subscription's consumer and inside
 * the OVRN's project; delete and other actions refused; dry runs apply nothing; a repeat returns the first answer, the
 * same key with another request is a conflict; every answer is a common.resource-control-result@1.
 */
const assert = require('assert');
const contracts = require('openvibe-contracts');
const { boot, request, serviceToken, userToken, suite } = require('./helpers');

const t = suite('resource-control');
let h;
const owner = contracts.ids.newId('user');
const stranger = contracts.ids.newId('user');
const project = contracts.ids.newId('project');
const otherProject = contracts.ids.newId('project');
const sub = contracts.ids.newId('subscription');
const token = serviceToken('services', ['events.resource.control']);
let n = 0;
const key = () => `ctl-test-${++n}-${Date.now()}`;
const control = async (body, bearer = token) => request(h.base, 'POST', '/api/v1/resources/control', { token: bearer, body });
const ovrn = (prj = project) => contracts.resources.format({ service: 'events', project_id: prj, type: 'subscription', id: sub });
const ask = (action, extra = {}) => ({ action, resource: ovrn(), project_id: project, idempotency_key: key(), on_behalf_of: { type: 'user', id: owner }, ...extra });
const valid = (body) => {
    const v = contracts.validate('common.resource-control-result@1', body);
    assert.ok(v.valid, JSON.stringify(v.errors));
    return body;
};
const enabled = async () => Boolean(Number((await h.store.getSubscription(sub)).enabled));

t('boot with a person\'s subscription in a project', async () => {
    h = await boot();
    await h.store.createSubscription({ id: sub, consumer: owner, topicPattern: 'live.stream.*', endpoint: 'https://consumer.openvibe.network/hook', secret: 'whsec_test', projectId: project, env: 'production' });
});

t('only a first-party service with the capability may call it', async () => {
    assert.strictEqual((await control(ask('suspend'), serviceToken('services', ['events.resource.read']))).status, 403);
    assert.strictEqual((await control(ask('suspend'), userToken(owner))).status, 401, 'a user JWT is not a service token');
    assert.strictEqual((await control({ action: 'suspend' })).status, 400, 'not a control request');
});

t('suspend, a dry run first, then a repeat answered from the log', async () => {
    const dry = valid((await control(ask('suspend', { dry_run: true }))).body);
    assert.deepStrictEqual([dry.state, dry.result.dry_run], ['done', true]);
    assert.strictEqual(await enabled(), true, 'a dry run applies nothing');
    const body = ask('suspend');
    const r = await control(body);
    assert.strictEqual(r.status, 200);
    valid(r.body);
    assert.deepStrictEqual([r.body.state, r.body.result.status, r.body.result.changed], ['done', 'disabled', true]);
    assert.strictEqual(await enabled(), false);
    assert.deepStrictEqual((await control(body)).body, r.body, 'the first answer, again');
    assert.strictEqual((await control({ ...body, action: 'resume' })).status, 409);
    const again = valid((await control(ask('suspend'))).body);
    assert.deepStrictEqual([again.result.status, again.result.changed], ['disabled', false]);
});

t('resume; delete, another person and another project are refused', async () => {
    const r = valid((await control(ask('resume'))).body);
    assert.deepStrictEqual([r.state, r.result.status], ['done', 'active']);
    assert.strictEqual(await enabled(), true);
    const del = valid((await control(ask('delete'))).body);
    assert.deepStrictEqual([del.state, del.problem.code], ['refused', 'control.unsupported_action']);
    const theirs = valid((await control(ask('suspend', { on_behalf_of: { type: 'user', id: stranger } }))).body);
    assert.deepStrictEqual([theirs.state, theirs.problem.code], ['refused', 'resources.unknown_resource']);
    const elsewhere = valid((await control({ ...ask('suspend'), resource: ovrn(otherProject), project_id: otherProject })).body);
    assert.strictEqual(elsewhere.problem.code, 'resources.unknown_resource');
    const nobody = valid((await control(ask('suspend', { on_behalf_of: undefined }))).body);
    assert.strictEqual(nobody.problem.code, 'control.no_person');
    assert.strictEqual(await enabled(), true);
});

t('done', async () => { await h.stop(); });

t.run();
