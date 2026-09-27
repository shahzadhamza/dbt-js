import { test } from 'node:test';
import assert from 'node:assert/strict';
import { computeBatches, parseInZone } from '../src/batches.js';
import { render, extractRefs } from '../src/render.js';
import { buildDag, expandSelection } from '../src/dag.js';
import { resolveConfig } from '../src/config.js';
import { loadProject } from '../src/project.js';
import { inferType } from '../src/seed.js';

const batches = (o) => computeBatches({ firstBuild: true, ...o });

test('batches: DST spring-forward hour does not hang and covers the gap', () => {
  const b = batches({ begin: '2026-03-08', batchSize: 'hour', end: '2026-03-08 04:00', timezone: 'America/New_York' });
  assert.deepEqual(b.map((x) => `${x.start}|${x.end}`), [
    '2026-03-08 00:00:00|2026-03-08 01:00:00',
    '2026-03-08 01:00:00|2026-03-08 03:00:00',
    '2026-03-08 03:00:00|2026-03-08 04:00:00',
  ]);
});

test('batches: DST fall-back hour is contiguous', () => {
  const b = batches({ begin: '2026-11-01', batchSize: 'hour', end: '2026-11-01 04:00', timezone: 'America/New_York' });
  for (let i = 1; i < b.length; i++) assert.equal(b[i].start, b[i - 1].end);
  assert.equal(b[0].start, '2026-11-01 00:00:00');
  assert.equal(b.at(-1).end, '2026-11-01 04:00:00');
});

test('batches: missing local midnight does not drift later day boundaries', () => {
  const b = batches({ begin: '2026-03-28', batchSize: 'day', end: '2026-03-31', timezone: 'Asia/Beirut' });
  assert.equal(b.at(-1).start, '2026-03-30 00:00:00');
  assert.equal(b.at(-1).end, '2026-03-31 00:00:00');
});

test('batches: two years of hourly UTC batches compute quickly', () => {
  const t0 = Date.now();
  const b = batches({ begin: '2024-01-01', batchSize: 'hour', end: '2026-01-01' });
  assert.equal(b.length, 17544);
  assert.ok(Date.now() - t0 < 1000, `took ${Date.now() - t0}ms`);
});

test('batches: normal run uses lookback, never before begin', () => {
  const now = new Date('2026-06-10T12:00:00Z');
  const b = computeBatches({ begin: '2026-06-09', batchSize: 'day', lookback: 3, firstBuild: false, now });
  assert.deepEqual(b.map((x) => x.start), ['2026-06-09 00:00:00', '2026-06-10 00:00:00']);
});

test('batches: empty window and invalid dates throw', () => {
  const now = new Date('2026-01-01T00:00:00Z');
  assert.throws(() => computeBatches({ begin: '2027-01-01', batchSize: 'day', firstBuild: true, now }), /window is empty/);
  assert.throws(() => parseInZone('nope'), /Invalid date/);
  assert.equal(parseInZone('2026-01-01T05:00:00Z', 'Asia/Tokyo').toISOString(), '2026-01-01T05:00:00.000Z');
  assert.equal(parseInZone('2026-01-01', 'Asia/Tokyo').toISOString(), '2025-12-31T15:00:00.000Z');
});

test('render: substitutions, incremental blocks, and leftovers', () => {
  const raw = `/* config: {"materialized":"table"} */
select * from {{ ref('a') }} join {{ source('raw', 'orders') }} using (id)
where x = '{{ var('k', 'dflt') }}' and tz = '{{ timezone }}'
{% if is_incremental() %} and ts > (select max(ts) from {{ this }}) {% endif %}`;
  const ctx = { name: 'm', schema: 's', vars: {}, sources: { raw: { schema: 'r', database: 'ext' } }, timezone: 'UTC' };
  const { sql } = render(raw, { ...ctx, isIncremental: false });
  assert.match(sql, /^select \* from "s"\."a" join "ext"\."r"\."orders"/);
  assert.match(sql, /x = 'dflt' and tz = 'UTC'/);
  assert.doesNotMatch(sql, /max\(ts\)/);
  assert.match(render(raw, { ...ctx, isIncremental: true }).sql, /from "s"\."m"/);
  assert.throws(() => render('{{ nope }}', ctx), /Unrecognized template/);
  assert.throws(() => render("{{ var('x') }}", ctx), /Missing var 'x'/);
  assert.throws(() => render("{{ source('zz', 't') }}", ctx), /undeclared source/);
  assert.deepEqual(extractRefs("{{ ref('a') }} {{ref(\"b\")}}"), ['a', 'b']);
});

test('dag: order, cycles, selection', () => {
  const models = [
    { name: 'c', rawSql: "{{ ref('b') }}" },
    { name: 'b', rawSql: "{{ ref('a') }} {{ ref('s') }}" },
    { name: 'a', rawSql: 'select 1' },
  ];
  const { nodes, order } = buildDag(models, [{ name: 's' }]);
  assert.ok(order.indexOf('a') < order.indexOf('b') && order.indexOf('b') < order.indexOf('c'));
  assert.deepEqual(expandSelection('+b', nodes, order).sort(), ['a', 'b', 's']);
  assert.deepEqual(expandSelection('b+', nodes, order), ['b', 'c']);
  assert.deepEqual(expandSelection(['a', 'c'], nodes, order), ['a', 'c']);
  assert.throws(() => expandSelection('zz', nodes, order), /unknown model\/seed 'zz'/);
  assert.throws(() => buildDag([{ name: 'x', rawSql: "{{ ref('y') }}" }, { name: 'y', rawSql: "{{ ref('x') }}" }], []), /Cycle/);
});

test('config: env interpolation runs before validation and alias derivation', () => {
  process.env.DBTJS_TEST_DB = '/data/sales.db';
  process.env.DBTJS_TEST_TYPE = 'duckdb';
  const input = {
    schema: 'main',
    connection: { type: '${DBTJS_TEST_TYPE}', path: ':memory:', attach: [{ path: '${DBTJS_TEST_DB}' }] },
  };
  const cfg = resolveConfig('/proj', input);
  assert.equal(cfg.connection.type, 'duckdb');
  assert.equal(cfg.connection.attach[0].alias, 'sales');
  assert.equal(input.connection.type, '${DBTJS_TEST_TYPE}', 'inline config is not mutated');
  assert.equal(input.connection.attach[0].alias, undefined);
});

test('config: inline config may carry function values (pg password callback)', () => {
  const password = async () => 'secret';
  const cfg = resolveConfig('/proj', { schema: 's', connection: { type: 'postgres', password } });
  assert.equal(cfg.connection.password, password);
});

test('config: validation errors', () => {
  assert.throws(() => resolveConfig('/p', { schema: 's' }), /"connection" object/);
  assert.throws(() => resolveConfig('/p', { schema: 's', connection: { type: 'oracle' } }), /connection.type/);
  assert.throws(() => resolveConfig('/p', { connection: { type: 'postgres' } }), /"schema" string/);
  assert.throws(() => resolveConfig('/p', { schema: 's', connection: { password: '${DBTJS_UNSET_X}' } }), /not set/);
  assert.throws(
    () => resolveConfig('/p', { schema: 's', connection: { type: 'duckdb', path: 'a.db', attach: [{ path: 'b.db', alias: 'a' }] } }),
    /collides with the main database/
  );
});

test('project: model config validation', () => {
  const load = (sql) => loadProject('/nonexistent', { models: { m: sql } });
  const cfg = (json) => `/* config: ${JSON.stringify(json)} */ select 1`;
  assert.equal(load('select 1').models[0].config.materialized, 'view');
  assert.throws(() => load(cfg({ materialized: 'nope' })), /unknown materialized/);
  assert.throws(() => load(cfg({ materialized: 'incremental', strategy: 'delete+insert' })), /unique_key/);
  assert.throws(() => load(cfg({ materialized: 'incremental', strategy: 'microbatch', event_time: 'ts', begin: '2026-01-01', batch_size: 'week' })), /batch_size/);
  assert.throws(() => load(cfg({ timezone: 'Mars/Olympus' })), /unknown timezone/);
  assert.throws(() => load(cfg({ tests: { c: [{ accepted_values: ['a', null] }] } })), /invalid test/);
  assert.throws(() => loadProject('/x', { models: { 'my-model': 'select 1' } }), /Invalid node name/);
});

test('seed: inferType', () => {
  assert.equal(inferType(['1', '-2', '']), 'integer');
  assert.equal(inferType(['1', '3000000000']), 'bigint');
  assert.equal(inferType(['1.5', '2']), 'numeric');
  assert.equal(inferType(['true', 'F']), 'boolean');
  assert.equal(inferType(['', '']), 'text');
  assert.equal(inferType(['a', '1']), 'text');
});
