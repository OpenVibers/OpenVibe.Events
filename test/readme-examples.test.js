'use strict';
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { suite } = require('./helpers');

const t = suite('readme-examples');
const readme = fs.readFileSync(path.join(__dirname, '..', 'README.md'), 'utf8');

t('realtime example gets a new ticket and resumes from the last cursor', async () => {
    const section = readme.split('## Realtime (SSE)\n')[1];
    assert.ok(section, 'realtime section exists');
    const example = section.match(/```js\n([\s\S]*?)\n```/);
    assert.ok(example, 'realtime JavaScript example exists');

    const streams = [];
    const retries = [];
    let requests = 0;
    class EventSource {
        constructor(url) { this.url = new URL(String(url)); this.closed = false; streams.push(this); }
        addEventListener() {}
        close() { this.closed = true; }
    }
    vm.runInNewContext(example[1], {
        URL, EventSource, networkJwt: 'test-user-token',
        fetch: async (_url, options) => {
            requests++;
            assert.strictEqual(options.headers.Authorization, 'Bearer test-user-token');
            return { ok: true, json: async () => ({
                ticket: `ticket-${requests}`,
                stream_url: 'https://events.openvibe.network/realtime/stream',
                topics: ['network.notification.*'],
            }) };
        },
        setTimeout: (callback, ms) => { assert.strictEqual(ms, 1000); retries.push(callback); },
    });
    await new Promise(resolve => setImmediate(resolve));
    assert.strictEqual(requests, 1);
    assert.strictEqual(streams[0].url.searchParams.get('ticket'), 'ticket-1');
    assert.strictEqual(streams[0].url.searchParams.get('topics'), 'network.notification.*');
    assert.strictEqual(streams[0].url.searchParams.has('last_event_id'), false);

    streams[0].onmessage({ lastEventId: 'opaque-cursor', data: '{"seq":1,"event":{}}' });
    streams[0].onerror();
    assert.strictEqual(streams[0].closed, true);
    retries.shift()();
    await new Promise(resolve => setImmediate(resolve));
    assert.strictEqual(requests, 2);
    assert.strictEqual(streams[1].url.searchParams.get('ticket'), 'ticket-2');
    assert.strictEqual(streams[1].url.searchParams.get('last_event_id'), 'opaque-cursor');
});

t.run();
