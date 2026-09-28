'use strict';
/**
 * A migrated database for one test boot (ADR-035), from openvibe-sdk/testing: PGlite by default; with
 * EVENTS_TEST_STORE=pg (npm run test:pg) the PostgreSQL + PgBouncer containers, with roles and a schema of its own.
 */
const { createTestDb, pgAvailable } = require('openvibe-sdk/testing');
const { MIGRATIONS } = require('../server/store');

const testDb = ({ store = process.env.EVENTS_TEST_STORE || 'pglite', max = 4 } = {}) => createTestDb({ migrations: MIGRATIONS, store, service: 'events', max });

module.exports = { testDb, pgAvailable };
