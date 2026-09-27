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

// Exit codes: 0 ok, 1 model/test failure or runtime error, 2 usage error.
// Sets process.exitCode rather than calling process.exit(), which can cut
// off stdout still buffered for a pipe (`dbt-js compile | tee out.sql`).
export async function main(argv = process.argv.slice(2)) {
  process.exitCode = await dispatch(argv);
}

// Flags a command would otherwise silently ignore.
const RUN_ONLY = ['full-refresh', 'event-time-start', 'event-time-end'];
const UNSUPPORTED_FLAGS = {
  test: RUN_ONLY,
  seed: [...RUN_ONLY, 'vars'],
  compile: RUN_ONLY,
  ls: [...RUN_ONLY, 'select', 'vars'],
  debug: [...RUN_ONLY, 'select'],
};

async function dispatch(argv) {
  let values, positionals;
  try {
    const parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        select: { type: 'string' },
        'full-refresh': { type: 'boolean' },
        vars: { type: 'string' },
        'event-time-start': { type: 'string' },
        'event-time-end': { type: 'string' },
        help: { type: 'boolean', short: 'h' },
      },
    });
    ({ values, positionals } = parsed);
  } catch (e) {
    console.error(`Error: ${e.message}\n\n${USAGE}`);
    return 2;
  }

  const [command, ...extra] = positionals;
  if (values.help) {
    console.log(USAGE);
    return 0;
  }
  if (!command) {
    console.error(USAGE);
    return 2;
  }
  const commands = { run, test, seed, compile, ls, debug };
  if (!Object.hasOwn(commands, command)) {
    console.error(`Error: unknown command '${command}'\n\n${USAGE}`);
    return 2;
  }
  if (extra.length) {
    // `dbt-js run orders` is almost always a forgotten --select
    console.error(`Error: unexpected argument '${extra[0]}' (did you mean --select ${extra[0]}?)\n\n${USAGE}`);
    return 2;
  }
  const unsupported = UNSUPPORTED_FLAGS[command]?.find((flag) => values[flag] !== undefined);
  if (unsupported) {
    console.error(`Error: --${unsupported} is not supported by '${command}'\n\n${USAGE}`);
    return 2;
  }

  try {
    return (await commands[command](values)) ? 0 : 1;
  } catch (e) {
    console.error(`Error: ${e.message}`);
    return 1;
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
  const result = await api.run({
    ...baseOpts(values),
    fullRefresh: values['full-refresh'] ?? false,
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
      } else if (e.error) {
        console.log(`FAIL ${e.id} (error: ${e.error})`);
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

async function debug(values) {
  const d = await api.debug(baseOpts(values));
  console.log(`config:  OK (schema "${d.schema}", ${d.modelCount} models, ${d.seedCount} seeds)`);
  console.log(`target:  ${d.target}`);
  console.log(`connect: OK (${d.database}, ${d.version.split(' on ')[0]})`);
  for (const a of d.attached ?? []) {
    console.log(`attach:  OK (${a.alias} -> ${a.path}${a.type ? `, ${a.type}` : ''}${a.readonly ? ', read-only' : ''})`);
  }
  return true;
}
