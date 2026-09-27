import { quoteIdent, rel, relationColumns, relationKind, withTransaction } from './db.js';
import { render } from './render.js';
import { computeBatches } from './batches.js';

// Render context for a model's body and hooks; `compile` and `run` share it
// so the two can't drift. `extra` adds isIncremental / batchStart / batchEnd.
export function renderCtx(node, cfg, extra = {}) {
  return {
    name: node.name,
    schema: cfg.schema,
    vars: cfg.vars,
    sources: cfg.sources,
    timezone: node.config.timezone,
    isIncremental: false,
    ...extra,
  };
}

export async function runModel(client, node, cfg, opts = {}) {
  const { fullRefresh = false, eventTimeStart, eventTimeEnd, onBatch } = opts;
  const { name, config, rawSql } = node;
  const kind = await relationKind(client, cfg.schema, name);
  const isIncremental = config.materialized === 'incremental' && !fullRefresh && kind === 'r';
  const ctx = renderCtx(node, cfg, { isIncremental });

  // Computed before any hook runs, so an invalid window (e.g. a future
  // "begin") fails the model without pre_hook side effects.
  const batches =
    config.materialized === 'incremental' && config.strategy === 'microbatch'
      ? computeBatches({
          begin: config.begin,
          batchSize: config.batch_size,
          lookback: config.lookback,
          start: eventTimeStart,
          end: eventTimeEnd,
          firstBuild: !isIncremental,
          timezone: config.timezone,
        })
      : null;

  // Hooks run outside the materialization transaction, one statement each, so
  // they can use statements Postgres forbids inside a txn (VACUUM, CREATE
  // INDEX CONCURRENTLY). Microbatch runs them once per model, not per batch.
  await runHooks(client, config.pre_hook, 'pre_hook', ctx);

  if (batches) {
    return runMicrobatch(client, node, cfg, { batches, kind, firstBuild: !isIncremental, hookCtx: ctx, onBatch });
  }

  const { sql } = render(rawSql, ctx);
  const result = await materialize(client, { schema: cfg.schema, name, config, sql, kind, isIncremental });
  await runHooks(client, config.post_hook, 'post_hook', ctx);
  return result;
}

async function materialize(client, { schema, name, config, sql, kind, isIncremental }) {
  const target = rel(schema, name);

  if (config.materialized === 'view') {
    if (client.dialect === 'sqlite') {
      // no CREATE OR REPLACE VIEW; SQLite DDL is transactional, so the wrap
      // closes the window where the view would be absent
      return withTransaction(client, async () => {
        if (kind && kind !== 'v') await client.query(`DROP TABLE IF EXISTS ${target}`);
        await client.query(`DROP VIEW IF EXISTS ${target}`);
        await client.query(`CREATE VIEW ${target} AS\n${sql}`);
        return { action: 'view' };
      });
    }
    if (kind && kind !== 'v') await client.query(`DROP TABLE IF EXISTS ${target} CASCADE`);
    try {
      await client.query(`CREATE OR REPLACE VIEW ${target} AS\n${sql}`);
    } catch (e) {
      // Postgres only lets OR REPLACE append columns — dropping, renaming or
      // retyping one fails (42P16 / 42804). Rebuild instead; DDL is
      // transactional there, so readers never see the view missing.
      if (client.dialect !== 'postgres' || !['42P16', '42804'].includes(e.code)) throw e;
      await withTransaction(client, async () => {
        await client.query(`DROP VIEW ${target} CASCADE`);
        await client.query(`CREATE VIEW ${target} AS\n${sql}`);
      });
    }
    return { action: 'view' };
  }

  if (!isIncremental) {
    // table, or incremental first run / --full-refresh: transactional rebuild
    const rowCount = await rebuildTable(client, target, kind, sql);
    const action = config.materialized === 'table' ? 'table' : 'incremental (full build)';
    return { action, rowCount };
  }

  if (config.strategy === 'append') {
    const res = await insertByName(client, { schema, name }, asSubquery(sql));
    return { action: 'incremental append', rowCount: res.rowCount };
  }

  // delete+insert: compute the SELECT once into a temp table, swap within one txn
  const keys = Array.isArray(config.unique_key) ? config.unique_key : [config.unique_key];
  const temp = quoteIdent(`${name}__dbtjs_incr`);
  const sqlite = client.dialect === 'sqlite';
  const mysql = client.dialect === 'mysql';
  return withTransaction(client, async () => {
    // explicit DROP rather than ON COMMIT DROP — DuckDB silently ignores the latter
    await client.query(`CREATE TEMPORARY TABLE ${temp} AS\n${sql}`);
    const match = keys.map((k) => `t.${quoteIdent(k)} = i.${quoteIdent(k)}`).join(' AND ');
    // MySQL has no Postgres-style DELETE ... USING ... WHERE; its multi-table
    // form references the temp table once per statement, satisfying MySQL's
    // single-reference rule for TEMPORARY tables. SQLite has neither form —
    // correlated EXISTS against the aliased target instead.
    await client.query(
      sqlite
        ? `DELETE FROM ${target} AS t WHERE EXISTS (SELECT 1 FROM ${temp} i WHERE ${match})`
        : mysql
          ? `DELETE t FROM ${target} t JOIN ${temp} i ON ${match}`
          : `DELETE FROM ${target} t USING ${temp} i WHERE ${match}`
    );
    const res = await insertByName(client, { schema, name }, temp);
    // TEMPORARY keyword on MySQL: plain DROP TABLE implicitly commits,
    // which would break this transaction's atomicity
    await client.query(`DROP ${mysql ? 'TEMPORARY ' : ''}TABLE ${temp}`);
    return { action: 'incremental delete+insert', rowCount: res.rowCount };
  });
}

// Replace whatever holds the name (table or view) with CREATE TABLE AS, in one
// transaction. Returns the driver's row count (undefined where not reported).
async function rebuildTable(client, target, kind, sql) {
  const cascade = client.dialect === 'sqlite' ? '' : ' CASCADE'; // CASCADE is a SQLite syntax error
  return withTransaction(client, async () => {
    await client.query(`DROP ${kind === 'v' ? 'VIEW' : 'TABLE'} IF EXISTS ${target}${cascade}`);
    const res = await client.query(`CREATE TABLE ${target} AS\n${sql}`);
    return res.rowCount;
  });
}

// INSERT naming the target's columns and selecting them by name from `from`
// (a table, or asSubquery(sql)). A reordered model SELECT then still lands in
// the right columns, and a missing column is a clear error rather than a
// shifted row. New model columns are ignored, as in dbt's default
// on_schema_change.
async function insertByName(client, { schema, name }, from, columns) {
  const cols = (columns ?? (await relationColumns(client, schema, name))).map(quoteIdent).join(', ');
  return client.query(`INSERT INTO ${rel(schema, name)} (${cols})\nSELECT ${cols} FROM ${from}`);
}

// Newlines keep a trailing `-- comment` in the model from swallowing the
// closing paren; a trailing ';' (optionally followed by line comments) would
// be a syntax error inside the parens, so it's dropped.
const asSubquery = (sql) => `(\n${sql.replace(/;(\s*--[^\n]*)*\s*$/, '')}\n) dbtjs_src`;

async function runHooks(client, hooks, which, ctx) {
  for (const [i, hook] of hooks.entries()) {
    const { sql } = render(hook, ctx);
    try {
      await client.query(sql);
    } catch (e) {
      throw new Error(`${which}[${i}]: ${e.message}`);
    }
  }
}

// Microbatch: each pre-computed window is its own transaction that replaces the
// target rows inside it. A failed batch is recorded and the rest keep running
// (retry via --event-time-start/-end).
async function runMicrobatch(client, node, cfg, { batches, kind, firstBuild, hookCtx, onBatch }) {
  const { name, config, rawSql } = node;
  const schema = cfg.schema;
  const target = rel(schema, name);
  const et = quoteIdent(config.event_time);
  const sqlite = client.dialect === 'sqlite';
  const failed = [];
  let total = 0;
  let countUnknown = false;
  let created = !firstBuild;
  let columns; // fetched once, after the table is known to exist

  for (const b of batches) {
    const { sql } = render(
      rawSql,
      renderCtx(node, cfg, { isIncremental: !firstBuild, batchStart: b.start, batchEnd: b.end })
    );
    try {
      let rowCount;
      if (!created) {
        rowCount = await rebuildTable(client, target, kind, sql);
        created = true;
      } else {
        columns ??= await relationColumns(client, schema, name);
        rowCount = await withTransaction(client, async () => {
          // SQLite compares timestamps as text, and a day-granularity event_time
          // ('YYYY-MM-DD') sorts BELOW the batch boundary ('YYYY-MM-DD HH:MM:SS'
          // from computeBatches) — datetime() normalizes both shapes
          await client.query(
            sqlite
              ? `DELETE FROM ${target} WHERE datetime(${et}) >= datetime('${b.start}') AND datetime(${et}) < datetime('${b.end}')`
              : `DELETE FROM ${target} WHERE ${et} >= '${b.start}' AND ${et} < '${b.end}'`
          );
          const res = await insertByName(client, { schema, name }, asSubquery(sql), columns);
          return res.rowCount;
        });
      }
      if (rowCount == null) countUnknown = true;
      else total += rowCount;
      onBatch?.({ ...b, ok: true, rowCount });
    } catch (e) {
      onBatch?.({ ...b, ok: false, message: e.message });
      if (!created) {
        // the target doesn't exist yet, so no later batch can insert into it
        throw new Error(`first batch (${b.start}) failed: ${e.message}`);
      }
      failed.push({ ...b, message: e.message });
    }
  }

  // skipped on partial failure: the model is already 'fail', don't stamp a
  // success hook (grant, index, audit row) onto an incomplete build
  if (!failed.length) await runHooks(client, config.post_hook, 'post_hook', hookCtx);

  return {
    action: 'incremental microbatch',
    rowCount: countUnknown ? undefined : total,
    batchCount: batches.length,
    failedBatches: failed,
  };
}
