// Programmatic API — what `import 'dbt-js'` gives you. Every function takes a
// projectDir (default process.cwd()), opens its own connection, and closes it
// before returning. Loading/config errors throw; model and test failures are
// returned as ok: false. Nothing here writes to the console or exits the
// process — pass onEvent to observe progress.
//
// Instead of project files you can pass the project inline:
//   config — the contents of dbtjs.config.json as an object (file not read)
//   models — a { name: rawSql } map replacing models/*.sql (same format,
//            /* config: {...} */ comment included)
// projectDir then only anchors relative duckdb paths and locates seeds/.

import { resolveConfig } from './config.js';
import { loadProject } from './project.js';
import { buildDag, expandSelection, selectionTokens } from './dag.js';
import { connect, ensureSchema } from './db.js';
import { renderCtx, runModel } from './materialize.js';
import { buildTests, runTest } from './tests.js';
import { loadSeed } from './seed.js';
import { render } from './render.js';
import { computeBatches } from './batches.js';

function loadAll({ projectDir = process.cwd(), vars, config, models: inlineModels } = {}) {
  const cfg = resolveConfig(projectDir, config);
  if (vars != null) {
    if (typeof vars !== 'object' || Array.isArray(vars)) {
      throw new Error(`vars must be a plain object, got ${JSON.stringify(vars)}`);
    }
    cfg.vars = { ...cfg.vars, ...vars };
  }
  const { models, seeds } = loadProject(projectDir, { models: inlineModels });
  const { nodes, order } = buildDag(models, seeds);
  return { cfg, models, seeds, nodes, order, projectDir };
}

async function withClient(cfg, projectDir, fn, { readOnly = false } = {}) {
  const client = await connect(cfg.connection, { projectDir, readOnly, schema: cfg.schema });
  try {
    return await fn(client);
  } finally {
    await client.end();
  }
}

// → { ok, models: [{ name, status: 'ok'|'fail'|'skip', materialized, action?,
//     rowCount?, batchCount?, failedBatches?, durationMs?, error? }] }
export async function run(opts = {}) {
  const { select, fullRefresh = false, eventTimeStart, eventTimeEnd, onEvent } = opts;
  if (eventTimeEnd && !eventTimeStart) throw new Error('eventTimeEnd (--event-time-end) requires eventTimeStart (--event-time-start)');
  const { cfg, nodes, order, projectDir } = loadAll(opts);
  const selected = expandSelection(select, nodes, order).filter(
    (n) => nodes.get(n).type === 'model'
  );
  if (!selected.length) throw new Error('Nothing to run for this selection');

  return withClient(cfg, projectDir, async (client) => {
    await ensureSchema(client, cfg.schema);
    const models = [];
    const bad = new Set(); // failed or skipped — either blocks downstream
    for (const [i, name] of selected.entries()) {
      const node = nodes.get(name);
      const base = {
        type: 'model',
        name,
        materialized: node.config.materialized,
        index: i + 1,
        total: selected.length,
      };
      if (node.deps.some((d) => bad.has(d))) {
        const rec = { ...base, status: 'skip' };
        bad.add(name);
        models.push(rec);
        onEvent?.(rec);
        continue;
      }
      const start = Date.now();
      let rec;
      try {
        const result = await runModel(client, node, cfg, {
          fullRefresh,
          eventTimeStart,
          eventTimeEnd,
          onBatch: (b) => onEvent?.({ type: 'batch', model: name, ...b }),
        });
        const failedBatches = result.failedBatches ?? [];
        rec = {
          ...base,
          status: failedBatches.length ? 'fail' : 'ok',
          action: result.action,
          rowCount: result.rowCount,
          batchCount: result.batchCount,
          failedBatches,
          durationMs: Date.now() - start,
        };
        if (rec.status === 'fail') {
          rec.error = `${failedBatches.length} of ${result.batchCount} batches failed`;
          bad.add(name);
        }
      } catch (e) {
        rec = { ...base, status: 'fail', error: e.message, durationMs: Date.now() - start };
        bad.add(name);
      }
      models.push(rec);
      onEvent?.(rec);
    }
    return { ok: models.every((m) => m.status === 'ok'), models };
  });
}

// → { ok, tests: [{ id, model, pass, violations, sample, error? }] } — a test
//   that can't run (missing relation/column) is a failure with `error`, not a throw
export async function test(opts = {}) {
  const { select, onEvent } = opts;
  const { cfg, nodes, order, projectDir } = loadAll(opts);
  const selected = new Set(expandSelection(select, nodes, order));
  const models = order
    .filter((n) => selected.has(n) && nodes.get(n).type === 'model')
    .map((n) => nodes.get(n));
  const tests = buildTests(models, cfg.schema);
  if (!tests.length) return { ok: true, tests: [] };

  return withClient(cfg, projectDir, async (client) => {
    const results = [];
    for (const t of tests) {
      let r;
      try {
        r = await runTest(client, t);
      } catch (e) {
        r = { pass: false, error: e.message };
      }
      const rec = {
        type: 'test',
        id: t.id,
        model: t.model,
        pass: r.pass,
        violations: r.violations ?? 0,
        sample: r.sample ?? [],
        ...(r.error && { error: r.error }),
      };
      results.push(rec);
      onEvent?.(rec);
    }
    return { ok: results.every((r) => r.pass), tests: results };
  });
}

// → { ok: true, seeds: [{ name, rowCount, durationMs }] } — a failing seed throws
export async function seed(opts = {}) {
  const { select, onEvent } = opts;
  const { cfg, seeds, projectDir } = loadAll(opts);
  const wanted = select ? selectionTokens(select) : null;
  for (const name of wanted ?? []) {
    // same strictness as expandSelection: a typo shouldn't silently shrink the run
    if (!seeds.some((s) => s.name === name)) throw new Error(`--select: unknown seed '${name}'`);
  }
  const selected = wanted ? seeds.filter((s) => wanted.includes(s.name)) : seeds;
  if (!selected.length) throw new Error('No seeds match this selection');

  return withClient(cfg, projectDir, async (client) => {
    await ensureSchema(client, cfg.schema);
    const results = [];
    for (const [i, s] of selected.entries()) {
      const start = Date.now();
      const { rowCount } = await loadSeed(client, s, cfg);
      const rec = {
        type: 'seed',
        name: s.name,
        index: i + 1,
        total: selected.length,
        rowCount,
        durationMs: Date.now() - start,
      };
      results.push(rec);
      onEvent?.(rec);
    }
    return { ok: true, seeds: results };
  });
}

// → [{ name, materialized, sql, preHookSql, postHookSql }] — no DB connection needed
export async function compile(opts = {}) {
  const { select } = opts;
  const { cfg, nodes, order } = loadAll(opts);
  const selected = expandSelection(select, nodes, order).filter(
    (n) => nodes.get(n).type === 'model'
  );
  return selected.map((name) => {
    const node = nodes.get(name);
    let batchCtx = {};
    if (node.config.strategy === 'microbatch') {
      // show the current normal-run window as one span, so the output is runnable SQL
      const b = computeBatches({
        begin: node.config.begin,
        batchSize: node.config.batch_size,
        lookback: node.config.lookback,
        firstBuild: false,
        timezone: node.config.timezone,
      });
      batchCtx = { batchStart: b[0].start, batchEnd: b[b.length - 1].end };
    }
    // isIncremental stays false: compile is offline; run decides it against the live DB
    const ctx = renderCtx(node, cfg);
    const { sql } = render(node.rawSql, { ...ctx, ...batchCtx });
    // hooks render without batch context — batch_start/batch_end are body-only
    const preHookSql = node.config.pre_hook.map((h) => render(h, ctx).sql);
    const postHookSql = node.config.post_hook.map((h) => render(h, ctx).sql);
    return { name, materialized: node.config.materialized, sql, preHookSql, postHookSql };
  });
}

// → [{ name, kind, deps }] in execution order — no DB connection needed
export async function ls(opts = {}) {
  const { nodes, order } = loadAll(opts);
  return order.map((name) => {
    const node = nodes.get(name);
    return {
      name,
      kind: node.type === 'seed' ? 'seed' : node.config.materialized,
      deps: node.deps,
    };
  });
}

// → { rows, rowCount } — one arbitrary statement against the project's warehouse.
// Bypasses loadAll so it works on projects with zero models. readOnly (default)
// opens DuckDB with access_mode READ_ONLY / sets the Postgres session read-only.
export async function query(opts = {}) {
  const { sql, params, readOnly = true, projectDir = process.cwd(), config } = opts;
  if (typeof sql !== 'string' || !sql.trim()) throw new Error('sql is required');
  const cfg = resolveConfig(projectDir, config);
  return withClient(cfg, projectDir, (client) => client.query(sql, params), { readOnly });
}

// → { schema, modelCount, seedCount, target, database, version, attached }
//   — connectivity check. `attached` lists DuckDB ATTACH catalogs (empty on
//   other backends).
export async function debug(opts = {}) {
  const { cfg, models, seeds, projectDir } = loadAll(opts);
  const target = describeTarget(cfg.connection);
  return withClient(cfg, projectDir, async (client) => {
    const { rows } = await client.query(
      cfg.connection.type === 'mysql'
        ? 'SELECT DATABASE() AS db, VERSION() AS version'
        : cfg.connection.type === 'sqlite'
          ? 'SELECT sqlite_version() AS version'
          : 'SELECT current_database() AS db, version() AS version'
    );
    let attached = [];
    if (cfg.connection.type === 'duckdb') {
      // proves the ATTACHes actually ran, not just that config parsed
      const res = await client.query(
        `SELECT database_name AS alias, path, type, readonly FROM duckdb_databases()
         WHERE database_name NOT IN ('system', 'temp') AND NOT internal AND path IS NOT NULL
         ORDER BY database_name`
      );
      // exclude the main database (its path matches connection.path); postgres/
      // mysql attachments' paths are connection strings, so mask credentials
      attached = res.rows
        .filter((r) => r.path !== cfg.connection.path)
        .map((r) => ({ ...r, path: maskSecrets(r.path) }));
    }
    return {
      schema: cfg.schema,
      modelCount: models.length,
      seedCount: seeds.length,
      target,
      database: rows[0].db ?? cfg.connection.path,
      version: rows[0].version,
      attached,
    };
  });
}

function describeTarget(c) {
  if (c.type === 'duckdb' || c.type === 'sqlite') return `${c.type} ${c.path}`;
  if (c.connectionString || c.uri) return `${c.type} ${maskSecrets(c.connectionString ?? c.uri)}`;
  // unset fields fall back to the driver's defaults (libpq env vars for pg)
  const port = c.port ?? (c.type === 'mysql' ? 3306 : 5432);
  const user = c.user ? ` as ${c.user}` : '';
  return `${c.host ?? 'localhost'}:${port}/${c.database ?? '(default)'}${user}`;
}

// Hide passwords in URL (scheme://user:pass@host) and libpq/DuckDB-style
// key=value (password=...) connection strings.
function maskSecrets(s) {
  return String(s)
    .replace(/(\/\/[^:/@\s]*:)[^@\s]*@/, '$1***@')
    .replace(/\b(password|passwd|pwd)(\s*=\s*)('[^']*'|"[^"]*"|[^\s;]+)/gi, '$1$2***');
}
