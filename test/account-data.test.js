'use strict';
/**
 * ADR-033: Events' part of an account export and deletion. The events a person caused and the events about them
 * are theirs: the export carries their envelope (never the payload, which is the publishing service's record) and a
 * deletion turns them into tombstones, so the payload goes and the actor becomes the producer. Both tiers are
 * answered: the hot store and the replay archive.
 *
 * The guard fails when a new table with a person column is neither covered nor exempted with a reason.
 */
const assert = require('assert');
const { uncovered } = require('openvibe-sdk/account-data');
const { testDb } = require('./db');
const { TABLES, create } = require('../server/account-data');
const archive = require('../server/archive');

const A = 'usr_01JZ0000000000000000000AAA';
const B = 'usr_01JZ0000000000000000000BBB';

let seq = 0;
const nextSeq = () => ++seq;

/** One event in the hot store. */
async function insertEvent(db, { id, actor, subjectType, subjectId, payload }) {
    await db.exec(`INSERT INTO events (id, seq, event_type, version, source, actor, subject_type, subject_id, trace_id, priority, visibility, occurred_at, received_at, payload, hops, publisher)
        VALUES ($1, $2, 'chat.message.created', 1, 'chat', $3, $4, $5, 'trace-1', 'important', 'internal', '2026-10-01T00:00:00.000Z', 1, $6, 1, 'chat')`,
        [id, nextSeq(), JSON.stringify(actor), subjectType, subjectId, payload]);
}

/** One event in the replay tier: the actor and the payload live in the packed body. */
async function insertArchived(db, { id, actor, subjectType, subjectId, payload }) {
    const row = {
        id, seq: nextSeq(), event_type: 'chat.message.created', source: 'chat', subject_type: subjectType, subject_id: subjectId,
        visibility: 'internal', project_id: null, env: 'production', received_at: 1, redacted_at: null, redacts: 0,
        version: 1, actor: JSON.stringify(actor), subject_revision: null, trace_id: 'trace-1', priority: 'important',
        occurred_at: '2026-10-01T00:00:00.000Z', payload, hops: 1, publisher: 'chat', request_id: null, size_bytes: 10, redacted_by: null,
    };
    await db.exec(`INSERT INTO events_archive (id, seq, epoch, event_type, source, subject_type, subject_id, visibility, project_id, env, received_at, redacts, body, archived_at)
        VALUES ($1, $2, 1, $3, $4, $5, $6, $7, null, 'production', 1, 0, $8, 1)`,
        [id, row.seq, row.event_type, row.source, row.subject_type, row.subject_id, row.visibility, archive.pack(row)]);
}

(async () => {
    const d = await testDb();
    try {
        const accountData = create({ db: d.db });
        const missing = await uncovered(d.db, {
            tables: [...TABLES, 'events'],
            exempt: { account_data_events: 'the ledger of requests itself: which export and deletion Events applied' },
        });
        assert.deepStrictEqual(missing, [], `a table with a person column is neither covered nor exempted: ${missing.join(', ')}`);

        // Ada caused an event (her chat message) and is the subject of another (a notification for her).
        await insertEvent(d.db, { id: 'evt_01JZ0000000000000000000AAA', actor: { type: 'user', id: A }, subjectType: 'chat_message', subjectId: 'cm_1', payload: '{"message":"hello"}' });
        await insertEvent(d.db, { id: 'evt_01JZ0000000000000000000AAB', actor: { type: 'service', id: 'network' }, subjectType: 'user', subjectId: A, payload: '{"title":"a notification"}' });
        await insertEvent(d.db, { id: 'evt_01JZ0000000000000000000AAC', actor: { type: 'user', id: B }, subjectType: 'chat_message', subjectId: 'cm_2', payload: '{"message":"hi"}' });
        await insertArchived(d.db, { id: 'evt_01JZ0000000000000000000AAD', actor: { type: 'user', id: A }, subjectType: 'chat_message', subjectId: 'cm_3', payload: '{"message":"older"}' });

        const part = await accountData.exportPart(A);
        const file = part.files.find((f) => f.name === 'events.json');
        assert.ok(file, 'events.json is in the part');
        assert.deepStrictEqual(file.content.map((e) => e.id).sort(), ['evt_01JZ0000000000000000000AAA', 'evt_01JZ0000000000000000000AAB']);
        assert.ok(!JSON.stringify(part).includes('hello'), 'the payload is never exported');
        assert.ok(!JSON.stringify(part).includes('evt_01JZ0000000000000000000AAC'), 'nobody else\'s events');
        const replay = part.files.find((f) => f.name === 'events-replay.json');
        assert.ok(replay, 'the replay tier is in the part');
        assert.deepStrictEqual(replay.content.map((e) => e.id), ['evt_01JZ0000000000000000000AAD']);

        const counts = await accountData.erase([A]);
        assert.strictEqual(counts.erased.events, 2, 'her two events are tombstoned');
        assert.strictEqual(counts.erased.events_replay, 1, 'her replay event too');
        const hot = await d.db.many('SELECT id, payload, actor, redacted_at FROM events WHERE id IN ($1, $2, $3) ORDER BY id',
            ['evt_01JZ0000000000000000000AAA', 'evt_01JZ0000000000000000000AAB', 'evt_01JZ0000000000000000000AAC']);
        const ada = hot.find((e) => e.id === 'evt_01JZ0000000000000000000AAA');
        assert.ok(ada.redacted_at, 'her event is a tombstone');
        assert.strictEqual(JSON.parse(ada.payload).redacted, true, 'the payload is gone');
        assert.strictEqual(JSON.parse(ada.actor).type, 'service', 'no person stays linked to it');
        assert.ok(!JSON.stringify(hot).includes('hello'), 'her message is not in the log any more');
        const bo = hot.find((e) => e.id === 'evt_01JZ0000000000000000000AAC');
        assert.strictEqual(bo.redacted_at, null, 'Bo\'s event stays');
        assert.ok(JSON.parse(bo.actor).id === B, 'Bo stays in his own event');
        const archived = await d.db.many('SELECT body FROM events_archive WHERE id = $1', ['evt_01JZ0000000000000000000AAD']);
        const unpacked = archive.unpack(archived[0]);
        assert.strictEqual(JSON.parse(unpacked.payload).redacted, true, 'the replay copy is tombstoned too');
        assert.strictEqual(JSON.parse(unpacked.actor).type, 'service', 'no person stays linked to the replay copy');
        console.log('  ✓ export, tombstone and the guard');
    } finally { await d.close(); }
})().catch((err) => { console.error(err); process.exit(1); });
