'use strict';
/**
 * openvibe-sdk/service in Events (plan T1, the handles family): the SIGTERM/SIGINT shutdown is the kit's
 * gracefulStop, not a hand-written handler. It drains the server for 8 s, then closes the handles (the
 * timers, carriers, realtime, worker, server, valkey and database; a rejection exits 1); past 10 s the
 * process exits 1. The static half reads server/index.js; the behavioural half drives gracefulStop with
 * the same options Events passes, exits stubbed.
 */
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const http = require('http');
const { gracefulStop } = require('openvibe-sdk/service');
const { suite } = require('./helpers');

const t = suite('service-kit');
const quiet = { log() {}, warn() {}, error() {} };
const source = fs.readFileSync(path.join(__dirname, '../server/index.js'), 'utf8');

t("server/index.js imports gracefulStop from openvibe-sdk/service and owns no signal handler", () => {
    assert.match(source, /const \{ gracefulStop \} = require\('openvibe-sdk\/service'\)/);
    assert.doesNotMatch(source, /process\.on\('SIGTERM'/);
    assert.doesNotMatch(source, /process\.on\('SIGINT'/);
    assert.doesNotMatch(source, /setTimeout\(\(\) => process\.exit\(1\), 10000\)/, 'the old 10 s exit-1 timer is gone');
});

t("the one stop names events, passes the handles and uses drainMs 8000 / deadlineMs 10000", () => {
    const call = source.slice(source.indexOf('gracefulStop('), source.indexOf('gracefulStop(') + 200);
    assert.match(call, /name: 'events'/);
    assert.match(call, /\bhandles\b/);
    assert.match(call, /drainMs: 8000/);
    assert.match(call, /deadlineMs: 10000/);
    assert.doesNotMatch(call, /deadlineExitCode/, 'the default exit 1 past the deadline is kept');
    assert.match(source, /server\.keepAliveTimeout = 65000/, 'the keep-alive timeout is kept');
});

t('the stop drains the server, then closes the handles; a failing close exits 1; a second call is a no-op', async () => {
    for (const fails of [false, true]) {
        const server = http.createServer((_req, res) => res.end('ok'));
        await new Promise((r) => server.listen(0, '127.0.0.1', r));
        const order = [];
        const handles = { close: async () => { order.push(server.listening ? 'close while listening' : 'close'); if (fails) throw new Error('db'); } };
        let exits = 0;
        let exited = null;
        const kit = gracefulStop({ name: 'events', server, handles, drainMs: 8000, deadlineMs: 10000, signals: false, exit: (c) => { exited = c; exits++; }, log: quiet });
        const [code, again] = await Promise.all([kit.stop('SIGTERM'), kit.stop('SIGINT')]);
        assert.deepStrictEqual(order, ['close'], 'the handles close after the server stopped listening');
        assert.strictEqual(code, fails ? 1 : 0);
        assert.strictEqual(again, code, 'a second signal changes nothing');
        assert.strictEqual(exited, code);
        assert.strictEqual(exits, 1, 'exit is called once');
        assert.strictEqual(kit.stopping(), true);
        assert.strictEqual(server.listening, false);
    }
});

t.run();
