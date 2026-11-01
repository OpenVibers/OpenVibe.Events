'use strict';
// Links inside an event's category and meta lines are underlined. Their colour alone differed from the line's text
// by 1.2:1 on Daylight, so they could not be told apart (axe link-in-text-block, 33 links, 2026-11-01).
const assert = require('assert');
const fs = require('fs');
const path = require('path');

const src = fs.readFileSync(path.join(__dirname, '..', 'server', 'home.js'), 'utf8');
assert.match(src, /\.hm-event-cat a,\.hm-event-meta a\{color:var\(--hm-accent-text\);text-decoration:underline;/);
assert.ok(!/\.hm-event-meta a\{[^}]*text-decoration:none/.test(src), 'not hidden until hover');
console.log('event links: all checks passed');
