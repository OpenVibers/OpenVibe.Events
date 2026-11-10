'use strict';
/**
 * GitHub webhooks in (AL5): signature verification, normalization to github.* envelopes, idempotent redelivery.
 * The fixtures in test/fixtures/github are recorded GitHub payload shapes against a fake PR
 * (OpenVibers/OpenVibe.Codes#999); the HMAC is computed here with the test-only secret.
 */
const assert = require('assert');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { boot, request, serviceToken, suite } = require('./helpers');

const SECRET = 'test-only-github-secret-0001';
const OTHER_SECRET = 'test-only-github-secret-0002';

const fixture = (name) => fs.readFileSync(path.join(__dirname, 'fixtures', 'github', name));

function sign(raw, secret = SECRET) {
    return `sha256=${crypto.createHmac('sha256', secret).update(raw).digest('hex')}`;
}

const t = suite('github');
let h;
const read = serviceToken('codes', ['events.event.read']);

t('boot with the webhook secret', async () => {
    h = await boot({ env: { EVENTS_GITHUB_WEBHOOK_SECRET: `${SECRET},${OTHER_SECRET}` } });
});

async function deliver(name, { event, delivery, secret } = {}) {
    const raw = fixture(name);
    return await request(h.base, 'POST', '/api/v1/in/github', {
        raw,
        headers: {
            'content-type': 'application/json',
            'x-github-event': event,
            'x-github-delivery': delivery || `deli-${name.replace(/[^0-9a-z-]/gi, '')}-${event || 'ping'}-0000`,
            'x-hub-signature-256': sign(raw, secret),
        },
    });
}

async function deliverRaw(raw, { event, delivery, secret } = {}) {
    return await request(h.base, 'POST', '/api/v1/in/github', {
        raw,
        headers: {
            'content-type': 'application/json',
            'x-github-event': event,
            'x-github-delivery': delivery,
            'x-hub-signature-256': sign(raw, secret),
        },
    });
}

async function storedAs(eventId) {
    const r = await request(h.base, 'GET', `/api/v1/events/${eventId}`, { token: read });
    assert.strictEqual(r.status, 200, r.text);
    return r.body.event;
}

t('a bad signature is refused; ping answers 202 without storing', async () => {
    const raw = fixture('check-run-failed.json');
    let r = await request(h.base, 'POST', '/api/v1/in/github', {
        raw,
        headers: { 'content-type': 'application/json', 'x-github-event': 'check_run', 'x-github-delivery': 'bad-sig-1', 'x-hub-signature-256': sign(raw, 'wrong-secret-0000000000000000') },
    });
    assert.strictEqual(r.status, 401, r.text);
    assert.strictEqual(r.body.code, 'events.bad_signature');
    r = await deliver('check-run-failed.json', { event: 'ping', delivery: 'deadbeef-0000-4000-8000-000000000000' });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual(r.body.ignored, 'ping');
});

t('a failed check_run stores one github.check.completed per pull request, with the log tail', async () => {
    const r = await deliver('check-run-failed.json', { event: 'check_run', delivery: 'deadbeef-0000-4000-8000-000000000001' });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual(r.body.event_ids.length, 1);
    const env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.event_type, 'github.check.completed');
    assert.strictEqual(env.source, 'github');
    assert.deepStrictEqual(env.subject, { type: 'pr', id: 'openvibers/openvibe.codes#999' });
    assert.strictEqual(env.payload.kind, 'check_run');
    assert.strictEqual(env.payload.conclusion, 'failure');
    assert.strictEqual(env.payload.check_name, 'codes-ci / tests');
    assert.strictEqual(env.payload.repo, 'openvibers/openvibe.codes');
    assert.strictEqual(env.payload.head_sha, 'abc123def456abc123def456abc123def456abc1');
    assert.strictEqual(env.payload.details_url, 'https://github.com/OpenVibers/OpenVibe.Codes/actions/runs/111');
    assert.match(env.payload.output.text, /expected 0 to equal 1/);
    assert.strictEqual(env.payload.output.title, '1 failing test');
});

t('a redelivery answers the stored ids and stores nothing new', async () => {
    const first = await deliver('check-run-failed.json', { event: 'check_run', delivery: 'deadbeef-0000-4000-8000-000000000001' });
    assert.strictEqual(first.status, 202);
    assert.strictEqual(first.body.duplicate, true);
    assert.strictEqual(first.body.event_ids.length, 1);
    const again = await storedAs(first.body.event_ids[0]);
    assert.strictEqual(again.event_type, 'github.check.completed');
});

t('one delivery naming two pull requests stores both and claims the delivery once', async () => {
    const first = await deliver('check-run-failed-two-prs.json', { event: 'check_run', delivery: 'deadbeef-0000-4000-8000-000000000007' });
    assert.strictEqual(first.status, 202, first.text);
    assert.strictEqual(first.body.event_ids.length, 2);
    const one = await storedAs(first.body.event_ids[0]);
    const two = await storedAs(first.body.event_ids[1]);
    assert.deepStrictEqual([one.subject.id, two.subject.id], ['openvibers/openvibe.codes#888', 'openvibers/openvibe.codes#999']);
    const again = await deliver('check-run-failed-two-prs.json', { event: 'check_run', delivery: 'deadbeef-0000-4000-8000-000000000007' });
    assert.strictEqual(again.status, 202);
    assert.strictEqual(again.body.duplicate, true);
    assert.deepStrictEqual(again.body.event_ids, first.body.event_ids);
});

t('a check_suite failure normalizes to the same github.check.completed', async () => {
    const r = await deliver('check-suite-failed.json', { event: 'check_suite', delivery: 'deadbeef-0000-4000-8000-000000000008' });
    assert.strictEqual(r.status, 202, r.text);
    const env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.event_type, 'github.check.completed');
    assert.strictEqual(env.payload.kind, 'check_suite');
    assert.strictEqual(env.payload.conclusion, 'failure');
    assert.strictEqual(env.subject.id, 'openvibers/openvibe.codes#999');
});

t('a successful check, a stop comment, a bot comment, a conflict and a review each normalize', async () => {
    let r = await deliver('check-run-success.json', { event: 'check_run', delivery: 'deadbeef-0000-4000-8000-000000000002' });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual((await storedAs(r.body.event_ids[0])).payload.conclusion, 'success');

    r = await deliver('issue-comment-stop.json', { event: 'issue_comment', delivery: 'deadbeef-0000-4000-8000-000000000003' });
    assert.strictEqual(r.status, 202, r.text);
    let env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.event_type, 'github.pr.commented');
    assert.strictEqual(env.payload.author_type, 'User');
    assert.strictEqual(env.payload.comment_id, 1);
    assert.match(env.payload.body, /^stop/);

    r = await deliver('issue-comment-bot.json', { event: 'issue_comment', delivery: 'deadbeef-0000-4000-8000-000000000004' });
    assert.strictEqual(r.status, 202, r.text);
    env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.payload.author_type, 'Bot');

    r = await deliver('pull-request-dirty.json', { event: 'pull_request', delivery: 'deadbeef-0000-4000-8000-000000000005' });
    assert.strictEqual(r.status, 202, r.text);
    env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.event_type, 'github.pr.updated');
    assert.strictEqual(env.payload.mergeable_state, 'dirty');
    assert.strictEqual(env.payload.mergeable, false);
    assert.strictEqual(env.payload.changed_files, 5);
    assert.match(env.payload.files_url, /\/pulls\/999\/files$/);

    r = await deliver('pull-request-review-changes.json', { event: 'pull_request_review', delivery: 'deadbeef-0000-4000-8000-000000000006' });
    assert.strictEqual(r.status, 202, r.text);
    env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.payload.state, 'changes_requested');
    assert.strictEqual(env.payload.author_type, 'User');
    assert.strictEqual(env.payload.comment_id, 1);

    r = await deliver('pull-request-review-comment.json', { event: 'pull_request_review_comment', delivery: 'deadbeef-0000-4000-8000-000000000009' });
    assert.strictEqual(r.status, 202, r.text);
    env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.payload.path, 'server/github.js');
    assert.strictEqual(env.payload.line, 212);
    assert.strictEqual(env.payload.comment_id, 4);
});

t('a claim left by a crash between claim and store is released, not answered as a duplicate', async () => {
    const delivery = 'deadbeef-0000-4000-8000-00000000000d';
    await h.store.db.exec(
        'INSERT INTO github_deliveries (delivery_id, event_ids, event_types) VALUES ($1, $2::jsonb, $3::jsonb)',
        [delivery, JSON.stringify(['evt_01J8Z3Q4R5S6T7V8W9X0Y1Z2Q1']), JSON.stringify(['github.check.completed'])],
    );
    const r = await deliver('check-run-success.json', { event: 'check_run', delivery });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual(r.body.duplicate, undefined);
    assert.strictEqual(r.body.event_ids.length, 1);
    assert.notStrictEqual(r.body.event_ids[0], 'evt_01J8Z3Q4R5S6T7V8W9X0Y1Z2Q1');
    const env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.payload.conclusion, 'success');
});

t('a merged pull request ends the binding downstream: the envelope carries merged', async () => {
    const r = await deliver('pull-request-closed-merged.json', { event: 'pull_request', delivery: 'deadbeef-0000-4000-8000-00000000000a' });
    assert.strictEqual(r.status, 202, r.text);
    const env = await storedAs(r.body.event_ids[0]);
    assert.strictEqual(env.payload.action, 'closed');
    assert.strictEqual(env.payload.merged, true);
    assert.strictEqual(env.payload.base, 'main');
});

t('a bare approval and an edited comment are not new instructions', async () => {
    const r = await deliver('pull-request-review-approved.json', { event: 'pull_request_review', delivery: 'deadbeef-0000-4000-8000-00000000000b' });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual(r.body.ignored, 'pull_request_review');
    const r2 = await deliver('issue-comment-edited.json', { event: 'issue_comment', delivery: 'deadbeef-0000-4000-8000-00000000000c' });
    assert.strictEqual(r2.status, 202, r2.text);
    assert.strictEqual(r2.body.ignored, 'issue_comment');
});

t('an unfinished check and a non-PR issue store nothing', async () => {
    const raw = Buffer.from(JSON.stringify({
        action: 'created', check_run: { status: 'in_progress', name: 'x', pull_requests: [{ number: 999, url: 'https://api.github.com/repos/a/b/pulls/999' }] },
        repository: { full_name: 'a/b' },
    }));
    let r = await deliverRaw(raw, { event: 'check_run', delivery: '1c1cec1c-1111-4a1a-8a1a-111111111111' });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual(r.body.ignored, 'check_run');
    const plain = Buffer.from(JSON.stringify({
        action: 'created', issue: { number: 7 }, comment: { body: 'hi', user: { login: 'u', type: 'User' } },
        repository: { full_name: 'a/b' },
    }));
    r = await deliverRaw(plain, { event: 'issue_comment', delivery: '1cc0e7e1-1111-4a1a-8a1a-111111111111' });
    assert.strictEqual(r.status, 202, r.text);
    assert.strictEqual(r.body.ignored, 'issue_comment');
});

t('without a secret the route is off', async () => {
    const { createGithubIngress } = require('../server/github');
    const ing = createGithubIngress({ config: { github: { secrets: [] } }, store: h.store });
    assert.strictEqual(ing.stats().enabled, false);
    const fakeRes = { statusCode: 0, setHeader() {}, end() {} };
    await ing.handler({ headers: {}, ip: '127.0.0.9', body: Buffer.alloc(0) }, fakeRes);
    assert.strictEqual(fakeRes.statusCode, 404);
});

t('stop', async () => { await h.stop(); });

t.run();
