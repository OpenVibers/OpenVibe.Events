'use strict';
// Every service whose openvibe-contracts manifest declares events it produces may publish them: its id is one of the
// default sources (server/config.js), so a new producer cannot ship with its events refused as events.unknown_source
// (OpenVibe.Inventory's were, 2026-10-09). Events itself publishes its own events internally, not through the API.
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { DEFAULT_SOURCES } = require('../server/config');

const dir = path.join(path.dirname(require.resolve('openvibe-contracts/package.json')), 'manifests', 'services');
const manifests = fs.readdirSync(dir).filter((f) => f.endsWith('.json')).map((f) => JSON.parse(fs.readFileSync(path.join(dir, f), 'utf8')));
const producers = manifests.filter((m) => (m.eventsProduced || []).length).map((m) => m.id || m.service).filter((id) => id !== 'events');
const missing = producers.filter((id) => !DEFAULT_SOURCES.includes(id));
assert.deepStrictEqual(missing, [], `producers that may not publish: ${missing.join(', ')}`);
const outside = manifests.flatMap((m) => (m.eventsProduced || []).filter((t) => (m.id || m.service) !== 'events' && t.split('.')[0] !== (m.id || m.service) && !t.startsWith('provider.')).map((t) => `${m.id || m.service}: ${t}`));
assert.deepStrictEqual(outside, [], 'every produced event type is in its producer\'s own namespace');
console.log(`sources cover producers: ${producers.length} producers, all allowed`);
