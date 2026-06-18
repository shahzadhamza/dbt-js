import { quoteIdent, rel, relationKind, withTransaction } from './db.js';
import { render } from './render.js';
import { computeBatches } from './batches.js';

export async function runModel(client, node, projectCfg, opts = {}) {
  const { fullRefresh = false, vars } = opts;
  const { name, config, rawSql } = node;
  const schema = projectCfg.schema;
  const target = rel(schema, name);
  const kind = await relationKind(client, schema, name);
  const isIncremental = config.materialized === 'incremental' && !fullRefresh && kind === 'r';

  const ctx = {
    name,
    schema,
    vars: vars ?? projectCfg.vars,
    isIncremental,
    sources: projectCfg.sources,
    timezone: config.timezone,
  };

  // Hooks run outside the materialization transaction, one statement each, so
  // they can use statements Postgres forbids inside a txn (VACUUM, CREATE
  // INDEX CONCURRENTLY). Microbatch runs them once per model, not per batch.
  await runHooks(client, config.pre_hook, 'pre_hook', ctx);

  if (config.materialized === 'incremental' && config.strategy === 'microbatch') {
    return runMicrobatch(client, node, projectCfg, { ...opts, kind, hookCtx: ctx });
  }

  const { sql } = render(rawSql, ctx);
  const result = await materialize(client, { name, config, sql, target, kind, isIncremental });
  await runHooks(client, config.post_hook, 'post_hook', ctx);
  return result;
}

async function materialize(client, { name, config, sql, target, kind, isIncremental }) {
  const sqlite = client.dialect === 'sqlite';
  const cascade = sqlite ? '' : ' CASCADE'; // CASCADE is a SQLite syntax error

  if (config.materialized === 'view') {
    if (sqlite) {
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
    await client.query(`CREATE OR REPLACE VIEW ${target} AS\n${sql}`);
    return { action: 'view' };
  }

  if (!isIncremental) {
    // table, or incremental first run / --full-refresh: transactional rebuild
    return withTransaction(client, async () => {
      if (kind === 'v') await client.query(`DROP VIEW IF EXISTS ${target}${cascade}`);
      else await client.query(`DROP TABLE IF EXISTS ${target}${cascade}`);
      const res = await client.query(`CREATE TABLE ${target} AS\n${sql}`);
      const action = config.materialized === 'table' ? 'table' : 'incremental (full build)';
      return { action, rowCount: res.rowCount };
    });
  }

  if (config.strategy === 'append') {
    const res = await client.query(`INSERT INTO ${target}\n${sql}`);
    return { action: 'incremental append', rowCount: res.rowCount };
  }

  // delete+insert: compute the SELECT once into a temp table, swap within one txn
  const keys = Array.isArray(config.unique_key) ? config.unique_key : [config.unique_key];
  const temp = quoteIdent(`${name}__dbtjs_incr`);
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
    const res = await client.query(`INSERT INTO ${target} SELECT * FROM ${temp}`);
    // TEMPORARY keyword on MySQL: plain DROP TABLE implicitly commits,
    // which would break this transaction's atomicity
    await client.query(`DROP ${mysql ? 'TEMPORARY ' : ''}TABLE ${temp}`);
    return { action: 'incremental delete+insert', rowCount: res.rowCount };
  });
}

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

// Microbatch: split the event-time range into aligned windows; each batch is its
// own transaction that replaces the target rows inside its window. A failed
// batch is recorded and the rest keep running (retry via --event-time-start/-end).
async function runMicrobatch(client, node, projectCfg, opts) {
  const { fullRefresh = false, vars, eventTimeStart, eventTimeEnd, onBatch, kind, hookCtx } = opts;
  const { name, config, rawSql } = node;
  const schema = projectCfg.schema;
  const target = rel(schema, name);
  const firstBuild = fullRefresh || kind !== 'r';

  const batches = computeBatches({
    begin: config.begin,
    batchSize: config.batch_size,
    lookback: config.lookback,
    start: eventTimeStart,
    end: eventTimeEnd,
    firstBuild,
    timezone: config.timezone,
  });

  const et = quoteIdent(config.event_time);
  const sqlite = client.dialect === 'sqlite';
  const cascade = sqlite ? '' : ' CASCADE';
  const failed = [];
  let total = 0;
  let countUnknown = false;
  let created = !firstBuild;

  for (const b of batches) {
    const { sql } = render(rawSql, {
      name,
      schema,
      vars: vars ?? projectCfg.vars,
      isIncremental: !firstBuild,
      sources: projectCfg.sources,
      batchStart: b.start,
      batchEnd: b.end,
      timezone: config.timezone,
    });
    try {
      let rowCount;
      if (!created) {
        rowCount = await withTransaction(client, async () => {
          if (kind === 'v') await client.query(`DROP VIEW IF EXISTS ${target}${cascade}`);
          else await client.query(`DROP TABLE IF EXISTS ${target}${cascade}`);
          const res = await client.query(`CREATE TABLE ${target} AS\n${sql}`);
          return res.rowCount;
        });
        created = true;
      } else {
        rowCount = await withTransaction(client, async () => {
          // SQLite compares timestamps as text, and a day-granularity event_time
          // ('YYYY-MM-DD') sorts BELOW the batch boundary ('YYYY-MM-DD HH:MM:SS'
          // from computeBatches) — datetime() normalizes both shapes
          await client.query(
            sqlite
              ? `DELETE FROM ${target} WHERE datetime(${et}) >= datetime('${b.start}') AND datetime(${et}) < datetime('${b.end}')`
              : `DELETE FROM ${target} WHERE ${et} >= '${b.start}' AND ${et} < '${b.end}'`
          );
          const res = await client.query(`INSERT INTO ${target}\n${sql}`);
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
