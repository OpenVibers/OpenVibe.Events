'use strict';
// Events' authority resource index (ADR-048 section 3; capability events.resource.read):
// GET /api/v1/resources pages common.resource-summary@1 for the resources Events owns - its subscriptions
// (sub_, kind events.subscription) - and GET /api/v1/resources/:ovrn reads one by its computed ovrn.
// ?project=&kind=&owner=&cursor=&limit= are honoured; ?project= is the tenancy boundary, so a subscription of
// another project - or a project-less one - is never returned under it, while the unscoped first-party
// caller sees everything. An ovrn is present exactly when openvibe-contracts' contracts.resources.nameOf
// composes one: a project-scoped subscription is ovrn:events:<prj_…>:subscription/sub_…, a project-less
// (first-party) one has no project segment and so no name. events.queue is not a kind here: a project's
// queue is a derived carrier class, not a stored row.
const assert = require('assert');
const { validate, ids, resources } = require('openvibe-contracts');
const { boot, request, serviceToken, appToken, userToken, suite } = require('./helpers');

const t = suite('resource-index');
let h;

const reader = serviceToken('services', ['events.resource.read']);
const eventReader = serviceToken('live', ['events.event.read']);   // a token without the capability
const family = serviceToken('services', ['events.*']);             // the family grant the contracts rule honours
const app = appToken({ env: 'production', cap: ['events.resource.read'] });   // an app token is never judged on a first-party capability
const sandbox = appToken({ env: 'sandbox', cap: ['events.resource.read'] });

const prjA = `prj_${ids.ulid()}`;
const prjB = `prj_${ids.ulid()}`;
const usrA = ids.newId('user');
const usrB = ids.newId('user');
const appA = ids.newId('app');
const subSvc = ids.newId('subscription');    // first-party: a service consumer, no project (no name)
const subUsr = ids.newId('subscription');    // a person's: the consumer is a usr_ subject, no project
const subUsr2 = ids.newId('subscription');   // another subscription for the same person
const subOther = ids.newId('subscription');  // another person's subscription
const subA1 = ids.newId('subscription');     // project A
const subA2 = ids.newId('subscription');     // project A, later disabled
const subUsrProject = ids.newId('subscription'); // project A, person-owned
const subB = ids.newId('subscription');      // project B
// Sorted by (kind, id): one kind, so by id (ULID randomness, not creation order).
const expected = [subSvc, subUsr, subUsr2, subOther, subA1, subA2, subUsrProject, subB].sort();
const ovrnOfSub = (project, id) => `ovrn:events:${project}:subscription/${id}`;

const get = (p, token) => request(h.base, 'GET', p, { token });

t('boot + fixtures: first-party, person-owned and two projects\' subscriptions', async () => {
    h = await boot();
    const at = 'https://consumer.openvibe.network/hook';
    const add = (id, consumer, topic_pattern, projectId) => h.store.createSubscription({
        id, consumer, topicPattern: topic_pattern, endpoint: at, secret: 'whsec_test',
        ...(projectId ? { projectId, env: 'production' } : {}),
    });
    await add(subSvc, 'svc:live', 'live.*');
    await add(subUsr, usrA, 'live.vod.transcoded');
    await add(subUsr2, usrA, 'live.stream.started');
    await add(subOther, usrB, 'media.*');
    await add(subA1, `app:${appA}`, 'live.vod.*', prjA);
    await add(subA2, `app:${appA}`, 'media.*', prjA);
    await add(subUsrProject, usrA, 'games.*', prjA);
    await add(subB, `app:${ids.newId('app')}`, 'games.*', prjB);
    await h.store.setSubscriptionEnabled(subA2, false);
});

t('auth: 401 with no token, 403 without the capability (and for an app token), 200 with it', async () => {
    const noToken = await get('/api/v1/resources');
    assert.strictEqual(noToken.status, 401, noToken.text);
    assert.strictEqual(noToken.body.code, 'token.missing');

    const denied = await get('/api/v1/resources', eventReader);
    assert.strictEqual(denied.status, 403, denied.text);
    assert.strictEqual(denied.body.code, 'capability.denied');
    assert.ok(denied.body.detail.includes('events.resource.read'), denied.body.detail);

    // A first-party capability is never honoured from an app token, whatever its cap claim says; a sandbox
    // app token does not even reach that check (a first-party route refuses every sandbox token).
    const appDenied = await get('/api/v1/resources', app);
    assert.strictEqual(appDenied.status, 403, appDenied.text);
    assert.strictEqual(appDenied.body.code, 'capability.denied');
    const sandboxed = await get('/api/v1/resources', sandbox);
    assert.strictEqual(sandboxed.status, 401, sandboxed.text);
    assert.strictEqual(sandboxed.body.code, 'token.sandbox_refused');
    assert.strictEqual((await get('/api/v1/resources', userToken())).status, 401, 'a user JWT is not a service token');

    const list = await get('/api/v1/resources', reader);
    assert.strictEqual(list.status, 200, list.text);
    assert.strictEqual(list.headers.get('cache-control'), 'private, max-age=60');
    assert.strictEqual((await get('/api/v1/resources', family)).status, 200, 'the contracts family rule grants events.*');
});

t('the page: common.resource-list-result@1, every summary common.resource-summary@1, sorted by (kind, id)', async () => {
    const y = await get('/api/v1/resources', reader);
    assert.strictEqual(y.status, 200, y.text);
    assert.deepStrictEqual(Object.keys(y.body).sort(), ['next_cursor', 'resources'], 'the page carries only the contract fields');
    const pageCheck = validate('common.resource-list-result@1', y.body);
    assert.ok(pageCheck.valid, JSON.stringify(pageCheck.errors));
    for (const s of y.body.resources) {
        const one = validate('common.resource-summary@1', s);
        assert.ok(one.valid, `${s.id}: ${JSON.stringify(one.errors)}`);
    }
    assert.strictEqual(y.body.next_cursor, null, 'one page holds the whole index');
    assert.deepStrictEqual(y.body.resources.map((r) => [r.kind, r.id]), expected.map((id) => ['events.subscription', id]));
    assert.deepStrictEqual([...new Set(y.body.resources.map((r) => r.service))], ['events']);
    assert.deepStrictEqual([...new Set(y.body.resources.map((r) => r.kind))], ['events.subscription']);
});

t('each summary maps its own row: project_id, name, state, owner, and an ovrn only when nameable', async () => {
    const byId = new Map((await get('/api/v1/resources', reader)).body.resources.map((r) => [r.id, r]));
    assert.strictEqual(byId.get(subA1).project_id, prjA);
    assert.strictEqual(byId.get(subB).project_id, prjB);
    assert.strictEqual(byId.get(subSvc).name, 'live.*', 'the topic pattern is the subscription’s name');
    assert.strictEqual(byId.get(subA1).state, 'active');
    assert.strictEqual(byId.get(subA2).state, 'disabled', 'the state follows enabled');
    for (const id of expected) assert.ok(typeof byId.get(id).created_at === 'string' && byId.get(id).created_at.endsWith('Z'), id);

    assert.strictEqual(byId.get(subA1).ovrn, ovrnOfSub(prjA, subA1));
    assert.strictEqual(byId.get(subB).ovrn, ovrnOfSub(prjB, subB));
    assert.deepStrictEqual(resources.parse(byId.get(subA1).ovrn), { service: 'events', project_id: prjA, type: 'subscription', id: subA1 });
    [subSvc, subUsr, subUsr2, subOther].forEach((id) => assert.ok(!('ovrn' in byId.get(id)), `${id}: without a project segment there is no name`));
    [subSvc, subUsr, subUsr2, subOther].forEach((id) => assert.ok(!('project_id' in byId.get(id)), `${id}: a first-party subscription is not a project's`));
    assert.deepStrictEqual(byId.get(subUsr).owner, { type: 'user', id: usrA }, 'a person-owned subscription names its owner');
    assert.ok(!('owner' in byId.get(subSvc)), 'a service consumer is not an owner');
});

t('?project= is the tenancy boundary: only that project’s rows, never another’s and never a project-less one', async () => {
    const ids1 = async (q) => {
        const y = await get(`/api/v1/resources${q}`, reader);
        assert.strictEqual(y.status, 200, `${q}: ${y.text}`);
        assert.ok(validate('common.resource-list-result@1', y.body).valid);
        return y.body.resources.map((r) => r.id);
    };
    assert.deepStrictEqual(await ids1(`?project=${prjA}`), [subA1, subA2, subUsrProject].sort(), 'project A: never project B, never the project-less ones');
    assert.deepStrictEqual(await ids1(`?project=${prjB}`), [subB], 'project B is never mixed in');
    assert.deepStrictEqual(await ids1(`?project=${prjA}&kind=events.subscription`), [subA1, subA2, subUsrProject].sort(), 'kind narrows within the project');
    assert.deepStrictEqual(await ids1(`?project=${ids.newId('project')}`), [], 'an unknown project has no resources');
});

t('?kind= picks a kind; an unknown one (events.queue, events.unknown) is an empty page, not an error', async () => {
    const list = async (kind) => (await get(`/api/v1/resources?kind=${encodeURIComponent(kind)}`, reader)).body.resources.map((r) => r.id);
    assert.deepStrictEqual(await list('events.subscription'), expected);
    assert.deepStrictEqual(await list('events.queue'), [], 'a queue is a derived carrier class, not a stored row');
    assert.deepStrictEqual(await list('events.event'), [], 'an event is not a resource');
    assert.deepStrictEqual(await list('events.unknown'), []);
});

t('?owner= selects one person, combines with kind and project, and pages that person once', async () => {
    const owned = [subUsr, subUsr2, subUsrProject].sort();
    const page = async (query) => {
        const y = await get(`/api/v1/resources?${query}`, reader);
        assert.strictEqual(y.status, 200, y.text);
        const check = validate('common.resource-list-result@1', y.body);
        assert.ok(check.valid, JSON.stringify(check.errors));
        return y.body;
    };
    assert.deepStrictEqual((await page(`owner=${usrA}`)).resources.map((r) => r.id), owned);
    assert.deepStrictEqual((await page(`owner=${usrB}`)).resources.map((r) => r.id), [subOther]);
    assert.deepStrictEqual((await page(`owner=${usrA}&kind=events.subscription`)).resources.map((r) => r.id), owned);
    assert.deepStrictEqual((await page(`owner=${usrA}&kind=events.queue`)).resources, []);
    assert.deepStrictEqual((await page(`owner=${usrA}&project=${prjA}`)).resources.map((r) => r.id), [subUsrProject]);
    assert.deepStrictEqual((await page(`owner=${usrA}&project=${prjA}&kind=events.subscription`)).resources.map((r) => r.id), [subUsrProject]);
    assert.deepStrictEqual((await page(`owner=${usrB}&project=${prjA}`)).resources, []);
    assert.deepStrictEqual((await page(`owner=agt_${ids.ulid()}`)).resources, [], 'this index emits only user owners');
    assert.deepStrictEqual((await page('owner=')).resources.map((r) => r.id), expected, 'empty owner does not filter');

    const seen = [];
    let cursor = null;
    for (let pages = 0; ; pages++) {
        const query = `owner=${usrA}&limit=1${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`;
        const y = await page(query);
        assert.ok(y.resources.length <= 1);
        for (const r of y.resources) {
            assert.deepStrictEqual(r.owner, { type: 'user', id: usrA });
            seen.push(r.id);
        }
        if (y.next_cursor === null) break;
        cursor = y.next_cursor;
        assert.ok(pages < owned.length, 'the owner cursor chain never ended');
    }
    assert.deepStrictEqual(seen, owned, 'every owned resource appears exactly once in order');
});

t('the cursor pages the whole index once, in order, with no duplicates', async () => {
    const seen = [];
    let cursor = null;
    for (let pages = 0; ; pages++) {
        const y = await get(`/api/v1/resources?limit=2${cursor ? `&cursor=${encodeURIComponent(cursor)}` : ''}`, reader);
        assert.strictEqual(y.status, 200, y.text);
        assert.ok(validate('common.resource-list-result@1', y.body).valid);
        assert.ok(y.body.resources.length <= 2);
        seen.push(...y.body.resources.map((r) => r.id));
        if (y.body.next_cursor === null) { assert.strictEqual(seen.length, expected.length, 'the cursor chain ends at the end'); break; }
        assert.ok(typeof y.body.next_cursor === 'string' && y.body.next_cursor !== '');
        cursor = y.body.next_cursor;
        assert.ok(pages < 50, 'the cursor chain never ended');
    }
    assert.deepStrictEqual(seen, expected, 'no duplicates, none skipped, order preserved');
    assert.deepStrictEqual((await get(`/api/v1/resources?limit=1000`, reader)).body.resources.map((r) => r.id), expected, 'a large limit still pages one result');
    assert.strictEqual((await get('/api/v1/resources?limit=2', reader)).body.next_cursor, (await get('/api/v1/resources?limit=2', reader)).body.next_cursor, 'a cursor is derived from the page, not from the moment');
});

t('GET /:ovrn reads the same summary the list answers, and only for a name it composes', async () => {
    const list = await get('/api/v1/resources', reader);
    const byId = new Map(list.body.resources.map((r) => [r.id, r]));
    const one = await get(`/api/v1/resources/${encodeURIComponent(ovrnOfSub(prjA, subA1))}`, reader);
    assert.strictEqual(one.status, 200, one.text);
    assert.strictEqual(one.headers.get('cache-control'), 'private, max-age=60');
    const check = validate('common.resource-summary@1', one.body);
    assert.ok(check.valid, JSON.stringify(check.errors));
    assert.deepStrictEqual(one.body, byId.get(subA1), 'the same summary the list answers');

    const missing = [
        ['a project-less subscription (no name)', ovrnOfSub(prjA, subSvc)],
        ['a person-owned, project-less subscription', ovrnOfSub(prjA, subUsr)],
        ["another project's segment", ovrnOfSub(prjB, subA1)],
        ['an unknown subscription', ovrnOfSub(prjA, ids.newId('subscription'))],
        ['another service', `ovrn:network:${prjA}:subscription/${subA1}`],
        ['a project OVRN', `ovrn:events:${prjA}:project/${prjA}`],
        ['an event OVRN', `ovrn:events:${prjA}:event/${ids.newId('event')}`],
        ['a non-OVRN', 'nope'],
    ];
    for (const [why, name] of missing) {
        const y = await get(`/api/v1/resources/${encodeURIComponent(name)}`, reader);
        assert.strictEqual(y.status, 404, `${why}: ${y.text}`);
        assert.ok(/^application\/problem\+json/.test(y.headers.get('content-type')), why);
        assert.strictEqual(y.body.code, 'resources.unknown_resource', why);
    }
    assert.strictEqual((await get(`/api/v1/resources/${encodeURIComponent(ovrnOfSub(prjA, subA1))}`)).status, 401, 'the read is guarded too');
});

t('a bad query is 400 resources.bad_query', async () => {
    const bad = [['?project=nope', 'project not a prj_ id'], ['?project=prj_short', 'project too short'], ['?owner=svc:live', 'owner not a subject id'], ['?owner=usr_short', 'owner too short'], ['?owner[]=x', 'owner not a scalar'], ['?limit=0', 'limit below one'], ['?limit=abc', 'limit not a number'], ['?limit=99999', 'limit over the cap'], ['?cursor=***', 'cursor not one this index issued']];
    for (const [q, why] of bad) {
        const y = await get(`/api/v1/resources${q}`, reader);
        assert.strictEqual(y.status, 400, `${why}: ${y.text}`);
        assert.ok(/^application\/problem\+json/.test(y.headers.get('content-type')), why);
        assert.strictEqual(y.body.code, 'resources.bad_query', why);
    }
});

t('stop', async () => { await h.stop(); });

t.run();
