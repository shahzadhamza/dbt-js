// Thin CLI over src/api.js: parse flags, format events as log lines, map
// result.ok to the exit code. All orchestration lives in the api.
import { parseArgs } from 'node:util';
import * as api from './api.js';

const USAGE = `Usage: dbt-js <command> [options]

Commands:
  run      Build models in dependency order
  test     Run data tests (not_null, unique, accepted_values)
  seed     Load seeds/*.csv into the target schema
  compile  Print compiled SQL without executing (is_incremental() = false)
  ls       List nodes in execution order
  debug    Check config and database connectivity

Options:
  --select SPEC          Comma-separated nodes; +name includes upstream, name+ downstream
  --full-refresh         Rebuild incremental models from scratch (run only)
  --vars JSON            Override project vars, e.g. --vars '{"start":"2026-06-01"}'
  --event-time-start TS  Backfill microbatch models from this time (run only)
  --event-time-end TS    End of the backfill window (requires --event-time-start)`;

export async function main(argv = process.argv.slice(2)) {
  let values, command;
  try {
    const parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        select: { type: 'string' },
        'full-refresh': { type: 'boolean', default: false },
        vars: { type: 'string' },
        'event-time-start': { type: 'string' },
        'event-time-end': { type: 'string' },
        help: { type: 'boolean', default: false },
      },
    });
    values = parsed.values;
    command = parsed.positionals[0];
  } catch (e) {
    console.error(`Error: ${e.message}\n\n${USAGE}`);
    process.exit(2);
  }

  if (!command || values.help) {
    console.log(USAGE);
    process.exit(command ? 0 : 2);
  }

  const commands = { run, test, seed, compile, ls, debug };
  if (!commands[command]) {
    console.error(`Error: unknown command '${command}'\n\n${USAGE}`);
    process.exit(2);
  }

  try {
    const ok = await commands[command](values);
    process.exit(ok ? 0 : 1);
  } catch (e) {
    console.error(`Error: ${e.message}`);
    process.exit(1);
  }
}

const pad = (s, n) => String(s).padEnd(n);

function baseOpts(values) {
  const opts = { select: values.select };
  if (values.vars) {
    try {
      opts.vars = JSON.parse(values.vars);
    } catch {
      throw new Error(`--vars must be valid JSON, got: ${values.vars}`);
    }
  }
  return opts;
}

function printModelEvent(e) {
  if (e.type === 'batch') {
    const detail = e.ok ? (e.rowCount != null ? `${e.rowCount} rows` : 'ok') : e.message;
    console.log(`      batch ${e.start} .. ${e.end}  ${e.ok ? 'OK' : 'FAIL'} (${detail})`);
    return;
  }
  const tag = `[${e.index}/${e.total}]`;
  if (e.status === 'skip') {
    console.log(`${tag} ${pad('SKIP', 5)} ${pad(e.materialized, 12)} ${e.name} (upstream failed)`);
  } else if (e.status === 'fail' && e.failedBatches?.length) {
    const f = e.failedBatches;
    console.log(
      `${tag} ${pad('FAIL', 5)} ${pad(e.action, 12)} ${e.name} — ${e.error}; ` +
      `retry with --select ${e.name} --event-time-start "${f[0].start}" --event-time-end "${f[f.length - 1].end}"`
    );
  } else if (e.status === 'fail') {
    console.log(`${tag} ${pad('FAIL', 5)} ${pad(e.materialized, 12)} ${e.name} — ${e.error}`);
  } else {
    const rows = e.rowCount != null ? `, ${e.rowCount} rows` : '';
    const batches = e.batchCount != null ? `, ${e.batchCount} batches` : '';
    console.log(`${tag} ${pad('OK', 5)} ${pad(e.action, 12)} ${e.name} (${e.durationMs}ms${rows}${batches})`);
  }
}

async function run(values) {
  if (values['event-time-end'] && !values['event-time-start']) {
    throw new Error('--event-time-end requires --event-time-start');
  }
  const result = await api.run({
    ...baseOpts(values),
    fullRefresh: values['full-refresh'],
    eventTimeStart: values['event-time-start'],
    eventTimeEnd: values['event-time-end'],
    onEvent: printModelEvent,
  });
  const counts = { ok: 0, fail: 0, skip: 0 };
  for (const m of result.models) counts[m.status]++;
  console.log(`\nDone: ${counts.ok} ok, ${counts.fail} failed, ${counts.skip} skipped`);
  return result.ok;
}

async function test(values) {
  const result = await api.test({
    ...baseOpts(values),
    onEvent: (e) => {
      if (e.pass) {
        console.log(`PASS ${e.id}`);
      } else {
        console.log(`FAIL ${e.id} (${e.violations} violating rows)`);
        for (const row of e.sample) console.log(`     ${JSON.stringify(row)}`);
      }
    },
  });
  if (!result.tests.length) {
    console.log('No tests defined.');
    return true;
  }
  const failures = result.tests.filter((t) => !t.pass).length;
  console.log(`\nDone: ${result.tests.length - failures} passed, ${failures} failed`);
  return result.ok;
}

async function seed(values) {
  const result = await api.seed({
    select: values.select,
    onEvent: (e) =>
      console.log(`[${e.index}/${e.total}] ${pad('OK', 5)} ${pad('seed', 12)} ${e.name} (${e.durationMs}ms, ${e.rowCount} rows)`),
  });
  return result.ok;
}

async function compile(values) {
  for (const m of await api.compile(baseOpts(values))) {
    console.log(`-- model: ${m.name} (${m.materialized})`);
    m.preHookSql.forEach((sql, i) => console.log(`-- pre_hook[${i}]:\n${sql}`));
    console.log(m.sql);
    m.postHookSql.forEach((sql, i) => console.log(`-- post_hook[${i}]:\n${sql}`));
    console.log('');
  }
  return true;
}

async function ls() {
  for (const n of await api.ls()) {
    const deps = n.deps.length ? `  <- ${n.deps.join(', ')}` : '';
    console.log(`${pad(n.kind, 12)} ${n.name}${deps}`);
  }
  return true;
}

async function debug() {
  const d = await api.debug();
  console.log(`config:  OK (schema "${d.schema}", ${d.modelCount} models, ${d.seedCount} seeds)`);
  console.log(`target:  ${d.target}`);
  console.log(`connect: OK (${d.database}, ${d.version.split(' on ')[0]})`);
  return true;
}
