// End-to-end through the public api against real SQLite and DuckDB files.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { run, test as runTests, seed, compile, query, debug } from '../src/api.js';

function project(files = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'dbtjs-'));
  for (const [path, body] of Object.entries(files)) {
    mkdirSync(join(dir, path, '..'), { recursive: true });
    writeFileSync(join(dir, path), body);
  }
  return dir;
}

const cfg = (json) => `/* config: ${JSON.stringify(json)} */\n`;

for (const backend of ['sqlite', 'duckdb']) {
  const config = { schema: 'main', connection: { type: backend, path: 'wh.db' } };

  test(`${backend}: seed → run → test`, async (t) => {
    const projectDir = project({ 'seeds/people.csv': 'id,name,active\n1,ann,true\n2,bob,false\n' });
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    const models = {
      people_v: `select * from {{ ref('people') }}`,
      people_t: cfg({ materialized: 'table', tests: { id: ['not_null', 'unique'], name: [{ accepted_values: ['ann', 'bob'] }] } }) +
        `select id, name from {{ ref('people_v') }};`,
    };
    assert.deepEqual((await seed({ projectDir, config })).seeds.map((s) => s.rowCount), [2]);
    const r = await run({ projectDir, config, models });
    assert.ok(r.ok, JSON.stringify(r));
    assert.deepEqual(r.models.map((m) => m.action), ['view', 'table']);
    const tr = await runTests({ projectDir, config, models });
    assert.ok(tr.ok);
    assert.equal(tr.tests.length, 3);
  });

  test(`${backend}: incremental append survives a reordered SELECT`, async (t) => {
    const projectDir = project();
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    const incr = cfg({ materialized: 'incremental' });
    await run({ projectDir, config, models: { ev: incr + `select 1 as id, 'a' as label` } });
    // same columns, swapped order, trailing semicolon and comment
    const r = await run({ projectDir, config, models: { ev: incr + `select 'b' as label, 2 as id; -- note` } });
    assert.ok(r.ok, JSON.stringify(r));
    const { rows } = await query({ projectDir, config, sql: 'select id, label from ev order by id' });
    assert.deepEqual(rows.map((x) => [Number(x.id), x.label]), [[1, 'a'], [2, 'b']]);
  });

  test(`${backend}: delete+insert replaces matching keys`, async (t) => {
    const projectDir = project();
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    const di = cfg({ materialized: 'incremental', strategy: 'delete+insert', unique_key: 'id' });
    await run({ projectDir, config, models: { d: di + `select 1 as id, 'old' as v union all select 2, 'keep'` } });
    await run({ projectDir, config, models: { d: di + `select 'new' as v, 1 as id` } });
    const { rows } = await query({ projectDir, config, sql: 'select id, v from d order by id' });
    assert.deepEqual(rows.map((x) => [Number(x.id), x.v]), [[1, 'new'], [2, 'keep']]);
  });

  test(`${backend}: microbatch builds, then re-runs idempotently`, async (t) => {
    const projectDir = project();
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    const day = (n) => new Date(Date.now() - n * 86400000).toISOString().slice(0, 10);
    const mb = cfg({ materialized: 'incremental', strategy: 'microbatch', event_time: 'd', begin: day(3), batch_size: 'day' });
    // full timestamps: SQLite compares text, and 'YYYY-MM-DD' sorts below 'YYYY-MM-DD 00:00:00'
    const sql = `select * from (select '${day(2)} 12:00:00' as d, 1 as n union all select '${day(1)} 12:00:00', 2) s
      where d >= '{{ batch_start }}' and d < '{{ batch_end }}'`;
    const models = { mb: mb + sql };
    assert.ok((await run({ projectDir, config, models })).ok);
    assert.ok((await run({ projectDir, config, models })).ok);
    const { rows } = await query({ projectDir, config, sql: 'select count(*) as c from mb' });
    assert.equal(Number(rows[0].c), 2);
    const [c] = await compile({ projectDir, config, models });
    assert.match(c.sql, /d >= '\d{4}-\d{2}-\d{2} 00:00:00'/);
  });

  test(`${backend}: a test that cannot run is a failure, not a throw`, async (t) => {
    const projectDir = project();
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    const models = {
      a: cfg({ tests: { missing_col: ['not_null'] } }) + 'select 1 as id',
      b: cfg({ tests: { id: ['not_null'] } }) + 'select 1 as id',
    };
    await run({ projectDir, config, models });
    const r = await runTests({ projectDir, config, models });
    assert.equal(r.ok, false);
    assert.equal(r.tests.length, 2, 'later tests still ran');
    assert.ok(r.tests[0].error);
    assert.equal(r.tests[1].pass, true);
  });

  test(`${backend}: read-only query rejects writes`, async (t) => {
    const projectDir = project();
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    await run({ projectDir, config, models: { x: cfg({ materialized: 'table' }) + 'select 1 as id' } });
    await assert.rejects(query({ projectDir, config, sql: 'insert into x values (2)' }));
    assert.equal((await query({ projectDir, config, sql: 'select * from x' })).rows.length, 1);
  });

  test(`${backend}: api rejects bad vars and unknown seed selections`, async (t) => {
    const projectDir = project({ 'seeds/s.csv': 'a\n1\n' });
    t.after(() => rmSync(projectDir, { recursive: true, force: true }));
    await assert.rejects(compile({ projectDir, config, vars: 'abc' }), /vars must be a plain object/);
    await assert.rejects(seed({ projectDir, config, select: 's,typo' }), /unknown seed 'typo'/);
  });
}

test('duckdb: a failed ATTACH closes the instance (file not left locked)', async (t) => {
  const projectDir = project();
  t.after(() => rmSync(projectDir, { recursive: true, force: true }));
  const bad = { schema: 'main', connection: { type: 'duckdb', path: 'wh.db', attach: [{ path: 'missing/nope.db', alias: 'ext' }] } };
  await assert.rejects(debug({ projectDir, config: bad, models: { m: 'select 1' } }));
  // on Windows a still-open instance keeps the file locked and this rm fails
  rmSync(join(projectDir, 'wh.db'), { force: true });
  const ok = await debug({ projectDir, config: { schema: 'main', connection: { type: 'duckdb', path: 'wh.db' } }, models: { m: 'select 1' } });
  assert.equal(ok.modelCount, 1);
});
