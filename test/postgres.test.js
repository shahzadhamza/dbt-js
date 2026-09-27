// Postgres-only paths, with pg.Client stubbed — no server needed. Asserts the
// statements dbt-js issues rather than Postgres's behaviour.
import { test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { run, query } from '../src/api.js';

let issued;
let onQuery;
beforeEach(() => {
  issued = [];
  onQuery = () => ({ rows: [], rowCount: 0 });
  pg.Client.prototype.connect = async function () {};
  pg.Client.prototype.end = async function () {};
  pg.Client.prototype.query = async function (q, values) {
    issued.push(typeof q === 'string' ? { text: q, values } : q);
    return onQuery(typeof q === 'string' ? q : q.text);
  };
});

const config = { schema: 'an', connection: { type: 'postgres', host: 'h', database: 'd' } };

test('read-only query forces the single-statement extended protocol', async () => {
  await query({ config, sql: 'SET default_transaction_read_only = off; DROP TABLE x' });
  const userQuery = issued.at(-1);
  assert.equal(userQuery.queryMode, 'extended');
  assert.match(issued[0].text, /default_transaction_read_only = on/);
});

test('readOnly: false keeps the plain query path', async () => {
  await query({ config, sql: 'select 1', readOnly: false });
  assert.equal(issued.at(-1).queryMode, undefined);
});

test('view whose columns changed is rebuilt (drop + create in a txn)', async () => {
  onQuery = (sql) => {
    if (/information_schema\.tables/.test(sql)) return { rows: [{ table_type: 'VIEW' }] };
    if (/^CREATE OR REPLACE VIEW/.test(sql)) {
      throw Object.assign(new Error('cannot drop columns from view'), { code: '42P16' });
    }
    return { rows: [], rowCount: 0 };
  };
  const r = await run({ config, models: { v: 'select 1 as a' } });
  assert.ok(r.ok, JSON.stringify(r));
  const texts = issued.map((q) => q.text);
  const i = texts.indexOf('BEGIN');
  assert.deepEqual(texts.slice(i, i + 4).map((s) => s.split('\n')[0]), [
    'BEGIN',
    'DROP VIEW "an"."v" CASCADE',
    'CREATE VIEW "an"."v" AS',
    'COMMIT',
  ]);
});

test('other view errors still fail the model', async () => {
  onQuery = (sql) => {
    if (/^CREATE OR REPLACE VIEW/.test(sql)) throw Object.assign(new Error('boom'), { code: '42601' });
    return { rows: [] };
  };
  const r = await run({ config, models: { v: 'select 1 as a' } });
  assert.equal(r.models[0].status, 'fail');
  assert.equal(r.models[0].error, 'boom');
});

test('a failing ROLLBACK does not mask the original error', async () => {
  onQuery = (sql) => {
    if (/^CREATE TABLE/.test(sql)) throw new Error('real cause');
    if (sql === 'ROLLBACK') throw new Error('connection lost');
    return { rows: [] };
  };
  const r = await run({ config, models: { t: '/* config: {"materialized":"table"} */ select 1' } });
  assert.equal(r.models[0].error, 'real cause');
});

test('failed session setup closes the client', async () => {
  let ended = false;
  pg.Client.prototype.end = async function () {
    ended = true;
  };
  onQuery = (sql) => {
    if (/read_only = on/.test(sql)) throw new Error('setup failed');
    return { rows: [] };
  };
  await assert.rejects(query({ config, sql: 'select 1' }), /setup failed/);
  assert.ok(ended);
});
