#!/usr/bin/env node
'use strict';
/**
 * Operator rotation of a first-party subscription's secret, without the value ever being printed.
 *
 *   node scripts/rotate-subscription-secret.js --db <events.db> --subscription <sub_…>
 *        --env-file </etc/openvibe/<consumer>.env> --env-var <NAME> [--overlap-s 86400] [--apply]
 *
 * With --apply it (1) sets a new random secret on the subscription, keeping the old one signing next to it
 * for --overlap-s (default 1 day), and (2) writes NAME=<new secret> into the consumer's env file (replacing
 * the line, or appending it; a .bak-<time> copy is kept). Then restart the consumer: during the overlap
 * every delivery carries both signatures, so it verifies with either secret and nothing is missed.
 * Without --apply it only checks that the subscription and the env file exist and says what it would do.
 * Run as root (the env file is root-owned); the database is opened by path.
 */
const fs = require('fs');
const crypto = require('crypto');
const Database = require('better-sqlite3');

const args = process.argv.slice(2);
const opt = (k, d) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : d; };
const apply = args.includes('--apply');
const dbPath = opt('--db');
const subId = opt('--subscription');
const envFile = opt('--env-file');
const envVar = opt('--env-var');
const overlapS = Number(opt('--overlap-s', '86400'));

function fail(msg) { console.error(`rotate-subscription-secret: ${msg}`); process.exit(1); }
if (!dbPath || !subId || !envFile || !envVar) fail('--db, --subscription, --env-file and --env-var are required');
if (!/^[A-Z][A-Z0-9_]{1,63}$/.test(envVar)) fail('--env-var must be an env variable name');
if (!Number.isFinite(overlapS) || overlapS < 0 || overlapS > 7 * 86400) fail('--overlap-s is 0..604800');
if (!fs.existsSync(envFile)) fail(`${envFile} does not exist`);

const db = new Database(dbPath, { fileMustExist: true });
const sub = db.prepare('SELECT id, consumer, topic_pattern, project_id FROM subscriptions WHERE id = ?').get(subId);
if (!sub) fail(`no subscription ${subId}`);
if (sub.project_id) fail('developer-app subscriptions rotate through the API (POST /api/v1/subscriptions/:id/rotate-secret)');
const cols = new Set(db.prepare('PRAGMA table_info(subscriptions)').all().map((c) => c.name));
if (!cols.has('previous_secret')) fail('this Events database predates rotation; deploy Events first (it adds the columns at boot)');

const lines = fs.readFileSync(envFile, 'utf8').split('\n');
const has = lines.some((l) => l.startsWith(`${envVar}=`));
console.log(`subscription ${sub.id} (${sub.consumer}, ${sub.topic_pattern}); ${envFile}: ${envVar} ${has ? 'will be replaced' : 'will be appended'}; overlap ${overlapS}s`);
if (!apply) { console.log('dry run: nothing changed (add --apply)'); process.exit(0); }

const secret = `whsec_${crypto.randomBytes(32).toString('hex')}`;
const now = Date.now();
fs.copyFileSync(envFile, `${envFile}.bak-${new Date(now).toISOString().replace(/[:.]/g, '-')}`);
db.transaction(() => {
    db.prepare('UPDATE subscriptions SET previous_secret = secret, previous_secret_until = ?, secret = ?, updated_at = ? WHERE id = ?')
        .run(now + overlapS * 1000, secret, now, sub.id);
})();
let out;
if (has) out = lines.map((l) => (l.startsWith(`${envVar}=`) ? `${envVar}=${secret}` : l));
else { out = lines.slice(); while (out.length && out[out.length - 1] === '') out.pop(); out.push(`${envVar}=${secret}`, ''); }
fs.writeFileSync(envFile, out.join('\n'), { mode: fs.statSync(envFile).mode });
console.log(`rotated: both secrets sign until ${new Date(now + overlapS * 1000).toISOString()}; ${envVar} updated. Restart ${sub.consumer} now.`);
