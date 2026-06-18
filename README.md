# dbt-js

A minimalist dbt-like SQL transformation tool for Postgres, MySQL, SQLite, and DuckDB. Models are plain SQL `SELECT` files; dbt-js compiles them (resolving `ref()` / `source()` / `var()`), builds a dependency DAG, and executes everything inside the database in dependency order. Like dbt, it is transformation-only — it never extracts or moves data; raw data must already be in your database (or, with DuckDB, in files it can read in place).

Five dependencies: `pg`, `mysql2`, `better-sqlite3`, `@duckdb/node-api`, and `csv-parse` — the database drivers are loaded lazily, so each backend only pays for its own. Plain ESM JavaScript, no build step.

## Install

```sh
npm install -g dbt-js     # global CLI:  dbt-js <command>
npx dbt-js debug          # or run without installing
npm install dbt-js        # as a library, for embedding (see below)
```

Requires Node.js >= 20.

## Quick start

The published package ships just the CLI and library; the runnable examples live in the
[repository](https://github.com/<you>/dbt-js). Clone it to try the fully self-contained
DuckDB example (no database server needed):

```sh
git clone https://github.com/<you>/dbt-js && cd dbt-js
npm install
cd example-duckdb
node ../bin/dbt-js.js debug     # check config + connectivity
node ../bin/dbt-js.js seed      # load seeds/*.csv
node ../bin/dbt-js.js run       # build all models in DAG order
node ../bin/dbt-js.js test      # run data tests
```

(With `dbt-js` installed globally, the commands are just `dbt-js debug`, `dbt-js run`, etc.)
`example/` is the same project targeting a Postgres server instead, `example-mysql/` targets MySQL (a one-line Docker server is in its README), and `example-sqlite/` targets SQLite (also serverless).

## Project layout

A dbt-js project is a directory containing:

```
dbtjs.config.json   # connection, target schema, sources, vars
models/*.sql        # one SELECT per file; filename = model name
seeds/*.csv         # one table per file; filename = table name
```

### dbtjs.config.json

```json
{
  "connection": {
    "host": "localhost",
    "port": 5432,
    "user": "me",
    "password": "${DBTJS_PASSWORD}",
    "database": "mydb"
  },
  "schema": "analytics",
  "sources": { "raw": { "schema": "public" } },
  "vars": { "start": null },
  "seeds": { "columnTypes": { "my_seed": { "joined_on": "date" } } }
}
```

For MySQL, the same shape with `"type": "mysql"` (`port` defaults to 3306):

```json
{
  "connection": {
    "type": "mysql",
    "host": "localhost",
    "user": "me",
    "password": "${DBTJS_PASSWORD}",
    "database": "mydb"
  },
  "schema": "analytics"
}
```

For DuckDB and SQLite, the connection is just a file path (the warehouse is an embedded local file):

```json
{
  "connection": { "type": "duckdb", "path": "./warehouse.duckdb" },
  "schema": "analytics"
}
```

```json
{
  "connection": { "type": "sqlite", "path": "./warehouse.db" },
  "schema": "analytics"
}
```

- `connection.type` is `"postgres"` (default), `"mysql"`, `"sqlite"`, or `"duckdb"`.
- `${NAME}` in connection values is replaced from the environment (error if unset). Omit `password` entirely to let `pg` use `PGPASSWORD`.
- `schema` is where all models and seeds are created (`CREATE SCHEMA IF NOT EXISTS` runs automatically).
- `sources` maps a source name to a schema, used by `{{ source('name', 'table') }}`.
- `vars` are defaults, overridable per-invocation with `--vars '{"start": "2026-06-01"}'`.
- `seeds.columnTypes` overrides inferred CSV column types (the escape hatch for dates/timestamps).

## Models

A model is a single `SELECT`. Configuration lives in one leading block comment with a JSON body:

```sql
/* config: {
  "materialized": "incremental",
  "strategy": "delete+insert",
  "unique_key": "day",
  "tests": { "day": ["not_null", "unique"] }
} */
select ...
```

No config comment means `{ "materialized": "view" }`.

### Templating

| Expression | Becomes |
|---|---|
| `{{ ref('other_model') }}` | `"schema"."other_model"` — and declares a DAG dependency |
| `{{ this }}` | the current model's own table (for incremental high-water marks) |
| `{{ source('raw', 'orders') }}` | `"public"."orders"` (schema from `sources` config) |
| `{{ var('start') }}` / `{{ var('x', 0) }}` | the var's value, or the default; error if neither. Inserted verbatim — quote it yourself in SQL |
| `{{ batch_start }}` / `{{ batch_end }}` | the current batch window as `YYYY-MM-DD HH:MM:SS` (microbatch models only). Inserted verbatim — quote it yourself |
| `{% if is_incremental() %} ... {% endif %}` | body included only on incremental runs (table exists, not `--full-refresh`) |

That's the whole template language. Anything else inside `{{ }}` / `{% %}` is a compile error.

### Materializations

- **view** (default): `CREATE OR REPLACE VIEW`
- **table**: transactional `DROP TABLE ... CASCADE; CREATE TABLE ... AS SELECT` (atomic to readers; CASCADE-dropped downstream views are rebuilt later in the same run — for partial runs use `--select model+`)
- **incremental**: first run (or `--full-refresh`) builds like a table; after that only the rows your SELECT returns are applied, via a strategy:
  - `append` — plain `INSERT INTO ... SELECT` (immutable event data)
  - `delete+insert` — requires `unique_key` (string or array); deletes matching keys then inserts, in one transaction (idempotent re-runs)
  - `microbatch` — splits the event-time range into aligned windows and replaces each window in its own transaction (see below)

### Hooks

`pre_hook` / `post_hook` run extra SQL around a model's build — grants, indexes, `ANALYZE`, audit rows. Each is a string or array of strings, rendered with the same template language as the model body (everything except `batch_start` / `batch_end`):

```sql
/* config: {
  "materialized": "table",
  "post_hook": [
    "create index if not exists idx_daily_revenue_day on {{ this }} (day)",
    "grant select on {{ this }} to reporting"
  ]
} */
select ...
```

- Order: all pre-hooks → materialization → all post-hooks, each hook as its own statement.
- One deliberate divergence from dbt: hooks run **outside** the materialization transaction, so they can use statements Postgres forbids inside one (`VACUUM`, `CREATE INDEX CONCURRENTLY`). A failing pre-hook aborts the model before any build; a failing post-hook marks the model FAIL but the built relation remains — fix the hook and re-run.
- Microbatch models run hooks once per model (pre-hooks before the first batch, post-hooks after the last), not per batch; post-hooks are skipped when any batch failed.
- `{{ ref('x') }}` inside a hook declares a DAG dependency, same as in the body.

### Incremental pattern + backfill

```sql
select date_trunc('day', created_at)::date as day, count(*) as orders
from {{ ref('orders_enriched') }}
{% if is_incremental() %}
where created_at >= coalesce(
  nullif('{{ var("start", "") }}', '')::timestamptz,
  (select max(day) from {{ this }})::timestamptz)
{% endif %}
group by 1
```

- Normal run: processes from the table's own high-water mark (`max(day)`).
- Backfill: `dbt-js run --select daily_revenue --vars '{"start": "2026-01-01"}'` re-derives from that date; `delete+insert` makes it idempotent.
- Full rebuild: `dbt-js run --select daily_revenue --full-refresh`.

### Microbatch (dbt 1.9-style)

For batched, retryable backfills, use `strategy: "microbatch"`. dbt-js splits the time range into `batch_size` windows and runs each as its own transaction: `DELETE` the target rows whose `event_time` falls in the window, then `INSERT` the batch's rows. A failed batch is reported and the rest keep running.

```sql
/* config: {
  "materialized": "incremental",
  "strategy": "microbatch",
  "event_time": "day",
  "begin": "2026-01-01",
  "batch_size": "day",
  "lookback": 1
} */
select date_trunc('day', created_at)::date as day, count(*) as orders
from {{ ref('orders_enriched') }}
where created_at >= '{{ batch_start }}'::timestamptz
  and created_at <  '{{ batch_end }}'::timestamptz
group by 1
```

- `event_time` — column **of this model's output** bounding each batch (used by the engine's per-window DELETE).
- `begin` — start of history; first run and `--full-refresh` build every batch from here.
- `batch_size` — `hour` | `day` | `month` | `year`. Boundaries align to the model's `timezone` (default UTC).
- `lookback` (default 1) — a normal run reprocesses the current batch plus this many previous ones (no high-water mark, same as dbt).
- Backfill: `dbt-js run --select my_model --event-time-start 2026-06-02 --event-time-end 2026-06-04` rewrites exactly those windows (whole batches; end is exclusive). Idempotent by construction.
- No `is_incremental()` needed — the `batch_start`/`batch_end` filter applies on every run, including the first.
- If batches fail, the model exits FAIL listing the failed windows and the exact `--event-time-start/--event-time-end` retry command; other batches' work is kept.

One deliberate divergence from dbt: dbt auto-filters upstream `ref()`s by their declared `event_time`; dbt-js does no hidden query rewriting — you filter your input yourself with `{{ batch_start }}` / `{{ batch_end }}`.

### Timezone

Any model may set `"timezone"` in its config (a string IANA zone, default `"UTC"`):

- For microbatch models it aligns each window to that zone's wall-clock. `{{ batch_start }}` / `{{ batch_end }}` are emitted as naive `YYYY-MM-DD HH:MM:SS` **wall-clock strings in that zone**, so they compare directly against a locally-stored `event_time` column. A `"day"` batch in `"America/New_York"` therefore spans local midnight-to-midnight, not UTC.
- `{{ timezone }}` is available in **any** model's SQL (raw substitution — quote it yourself, e.g. `created_at at time zone '{{ timezone }}'`).
- `begin`, `--event-time-start`, and `--event-time-end` given as naive strings are interpreted as wall-clock in the model's `timezone`; strings with an explicit `Z`/offset stay absolute.
- DST caveat: with `batch_size: "hour"` in a DST zone the spring-forward/fall-back hour is irregular — prefer UTC for hour-grain, or day+ grain for zoned models.

## Tests

Declared per column in the model's config. Each compiles to a query returning violating rows; any row fails the test (exit code 1, with up to 10 sample rows printed).

- `"not_null"` — rows where the column is NULL
- `"unique"` — non-NULL values appearing more than once
- `{ "accepted_values": ["a", "b"] }` — non-NULL values outside the list

## Seeds

`dbt-js seed` loads each `seeds/*.csv` as a table (drop + create + insert, transactional). Column types are inferred (`integer`/`bigint`/`numeric`/`boolean`, else `text`; empty string → NULL); override per column via `seeds.columnTypes`. Models can `{{ ref('seed_name') }}` seeds.

## CLI

```
dbt-js run     [--select SPEC] [--full-refresh] [--vars JSON]
               [--event-time-start TS] [--event-time-end TS]   # microbatch backfill window
dbt-js test    [--select SPEC] [--vars JSON]
dbt-js seed    [--select SPEC]
dbt-js compile [--select SPEC] [--vars JSON]   # print compiled SQL, no DB needed
dbt-js ls                                       # nodes in execution order
dbt-js debug                                    # config + connectivity check
```

`--select` accepts comma-separated names; `+name` adds everything upstream, `name+` everything downstream (e.g. `--select orders_enriched+` rebuilds it and its dependents).

On failure, downstream models are skipped and reported; exit code is 1 if anything failed.

## Embedding in a Node.js app

The CLI is a thin wrapper over a programmatic API — `example-embed/` is a runnable ~70-line server using it. Install dbt-js as a dependency:

```sh
npm install dbt-js
```

```js
import { run, test, seed, compile, ls, debug } from 'dbt-js';

const result = await run({
  projectDir: './analytics',          // dir containing dbtjs.config.json — always pass this
  select: 'daily_revenue+',           // optional, same syntax as --select
  vars: { start: '2026-06-01' },      // optional, plain object (not a JSON string)
  fullRefresh: false,
  onEvent: (e) => logger.info(e),     // optional progress stream; omit for silence
});
// result = { ok, models: [{ name, status: 'ok'|'fail'|'skip', action, rowCount,
//            batchCount, failedBatches, durationMs, error }] }
```

The project can also be supplied inline instead of from files — handy when connection settings live in your app's config system or model SQL is generated:

```js
await run({
  config: {                                   // contents of dbtjs.config.json (file not read)
    connection: { host: 'db', port: 5432, user: 'analytics', password: process.env.PW, database: 'warehouse' },
    schema: 'analytics',
    sources: { raw: { schema: 'public' } },
  },
  models: {                                   // replaces models/*.sql — same format, config comment included
    stg_orders: "select * from {{ source('raw', 'orders') }} where deleted = false",
    order_counts: "/* config: { \"materialized\": \"table\" } */ select count(*) as n from {{ ref('stg_orders') }}",
  },
});
```

With both given, `projectDir` is optional — it then only anchors relative DuckDB paths and locates `seeds/` (file seeds remain `ref()`-able from inline models). Inline `config` goes through the same validation and `${ENV}` interpolation as the file; your object is not mutated.

- `run` also takes `eventTimeStart` / `eventTimeEnd` for microbatch backfills. `test` → `{ ok, tests: [{ id, pass, violations, sample }] }`; `seed` → `{ ok, seeds: [...] }`; `compile` → `[{ name, materialized, sql, preHookSql, postHookSql }]` (no DB needed); `ls` → `[{ name, kind, deps }]`; `debug` → connectivity info.
- Config or project errors **throw**; model/test failures come back as `ok: false` (mirrors the CLI's exit code 1).
- Every call opens its own connection and closes it before returning — nothing to pool.
- **Serialize runs yourself** (a one-promise queue is enough — see `example-embed/server.js`): DuckDB allows a single writer per file, so a scheduled refresh and an HTTP-triggered run must not overlap.
- Relative paths are anchored to `projectDir`, not your app's cwd: the DuckDB `connection.path` is resolved against it, and `read_csv('data/...')`-style paths in model SQL resolve via DuckDB's `file_search_path`.

## DuckDB notes

- `sources` resolve to schemas inside the same `.duckdb` file, exactly like Postgres schemas.
- Models can call DuckDB-native readers directly — `from read_csv('data/orders.csv')` or `read_parquet('...')` — no template syntax needed; raw data files never pass through dbt-js.
- DuckDB doesn't report row counts for full table builds (CTAS), so those log lines omit the count. Incremental and seed counts are reported normally.
- `:memory:` is a valid path but pointless for a CLI — each invocation is a separate process, so nothing would persist between `seed` and `run`.
- Attaching external databases (DuckDB `ATTACH`) is not supported in v1.
- One Postgres-specific change: pre-existing **materialized views** squatting on a model's name are no longer auto-dropped (relation detection now uses `information_schema`, which can't see them); you'd get a clear Postgres error at build time instead. dbt-js itself never creates materialized views.

## MySQL notes

Requires MySQL 8.0+ (`CREATE TABLE ... AS SELECT` under GTID consistency additionally needs 8.0.21+, and temp-table-in-transaction is disallowed when it's enforced).

- dbt-js enables `ANSI_QUOTES` for its session, so double quotes are **identifier** quotes exactly as on Postgres/DuckDB — write string literals with single quotes in model SQL (the habit you already have from Postgres).
- `schema` maps to a MySQL **database**: `CREATE SCHEMA IF NOT EXISTS` is `CREATE DATABASE`, so the connecting user needs the server-wide CREATE privilege (or pre-create the schema and grant on it — see `example-mysql/README.md`).
- MySQL DDL implicitly commits, so `table` and `--full-refresh` rebuilds (DROP + CREATE TABLE AS) are **not** atomic to readers the way they are on Postgres/DuckDB. `delete+insert` and microbatch window replacement remain fully transactional.
- No `CREATE INDEX IF NOT EXISTS` — use an idempotent post-hook like `analyze table {{ this }}`, or guard index creation yourself.
- Seed type inference maps `numeric` to `decimal(38,10)` (bare `NUMERIC` is `DECIMAL(10,0)` on MySQL and would round); `boolean` becomes `TINYINT(1)` with `true/false` loaded as `1/0`. Override per column via `seeds.columnTypes` as usual.
- Microbatch boundaries are computed in UTC and compared as `DATETIME` literals — prefer a `DATETIME` event-time column, or set the session time zone to UTC via mysql2's `timezone` connection option.
- Rows come back with `dateStrings: true` (dates as strings, JSON-safe, matching the DuckDB adapter); set `dateStrings: false` in the connection object to get JS `Date`s from the `query` API.

## SQLite notes

Driver: `better-sqlite3` (synchronous — a long-running statement blocks the embedding app's event loop; irrelevant for CLI use).

- `schema` maps to a **separate database file** `<schema>.db` next to `connection.path`, ATTACHed for the session (created automatically when writable). `"schema": "main"` keeps everything in the single main file — see `example-sqlite/README.md`.
- SQLite DDL is transactional, so **all** rebuilds — including `table` and `--full-refresh` — are atomic, like Postgres/DuckDB. One caveat: switching `journal_mode` to WAL in a hook removes crash atomicity for transactions spanning the main and attached files.
- There is no `DROP ... CASCADE`: dropping a table leaves dependent views dangling (they error when next queried) instead of dropping them.
- Type affinity gotchas: never `CAST(x AS DATETIME)` — `DATETIME` gets NUMERIC affinity, truncating `'2026-06-03'` to `2026`. Store timestamps as `'YYYY-MM-DD HH:MM:SS'` text; lexicographic comparison is chronological, and microbatch window boundaries are normalized with `datetime()` so day-granularity event-time columns work too.
- Seed `boolean` columns load as `1/0` (the text `'true'` would be falsy in `CASE WHEN`); `numeric` needs no special mapping (affinity stores decimals losslessly).
- The read-only `query` API opens the files with SQLite's readonly flag — writes fail with `SQLITE_READONLY`, and the database files must already exist.
- INTEGER values beyond 2^53 come back as imprecise JS numbers from the `query` API.

## License

MIT
