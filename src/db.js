// All database access lives here. connect() dispatches on connection.type and
// returns a uniform client: { query(sql, params) -> { rows, rowCount }, end() }.
// Drivers are imported lazily so each backend only loads its own.

export async function connect(connection, { projectDir, readOnly = false, schema } = {}) {
  const { type = 'postgres', ...rest } = connection;
  return type === 'duckdb'
    ? connectDuckdb(rest.path, projectDir, readOnly, rest.attach)
    : type === 'mysql'
      ? connectMysql(rest, readOnly)
      : type === 'sqlite'
        ? connectSqlite(rest, schema, readOnly)
        : connectPg(rest, readOnly);
}

async function connectPg(connection, readOnly = false) {
  const { default: pg } = await import('pg');
  const client = new pg.Client(connection);
  await client.connect();
  // Session-level read-only also applies inside data-modifying CTEs,
  // which a statement-keyword check can't catch.
  if (readOnly) await client.query('SET default_transaction_read_only = on');
  return {
    dialect: 'postgres',
    async query(sql, params) {
      const res = await client.query(sql, params);
      return { rows: res.rows, rowCount: res.rowCount ?? undefined };
    },
    end: () => client.end(),
  };
}

async function connectMysql(connection, readOnly = false) {
  const { default: mysql } = await import('mysql2/promise');
  const conn = await mysql.createConnection({
    dateStrings: true, // JSON-safe rows, matching the duckdb adapter
    ...connection,
    multipleStatements: false,
  });
  // render.js emits "schema"."name" with no dialect knowledge; ANSI_QUOTES
  // makes double-quoted identifiers valid for the whole session.
  await conn.query(`SET SESSION sql_mode = CONCAT_WS(',', NULLIF(@@sql_mode, ''), 'ANSI_QUOTES')`);
  if (readOnly) await conn.query('SET SESSION transaction_read_only = 1');
  return {
    dialect: 'mysql',
    async query(sql, params) {
      const q = toQmarks(sql, params);
      const [res] = await conn.query(q.sql, q.params); // rows[] or ResultSetHeader
      return Array.isArray(res)
        ? { rows: res, rowCount: res.length }
        : { rows: [], rowCount: res.affectedRows ?? undefined }; // DML/DDL; CTAS reports inserted rows
    },
    end: () => conn.end(),
  };
}

async function connectSqlite(connection, schema, readOnly = false) {
  // Synchronous driver; long statements block the event loop (fine for CLI use,
  // worth knowing when embedding).
  const { default: Database } = await import('better-sqlite3');
  const db = new Database(connection.path, readOnly ? { readonly: true } : {});
  // cfg.schema 'main'/'temp' are SQLite's built-in schemas (single-file mode).
  // Anything else lives in '<schema>.db' beside the main file, ATTACHed for the
  // whole session so "schema"."name" from render.js resolves. ATTACH inherits
  // the connection's readonly flag and can't create files read-only, so a
  // missing file is skipped there (queries then fail with "no such table").
  if (schema && schema !== 'main' && schema !== 'temp') {
    const { join, dirname } = await import('node:path');
    const { existsSync } = await import('node:fs');
    const path =
      connection.path === ':memory:' ? ':memory:' : join(dirname(connection.path), `${schema}.db`);
    if (!readOnly || path === ':memory:' || existsSync(path)) {
      db.prepare(`ATTACH DATABASE ? AS ${quoteIdent(schema)}`).run(path);
    }
  }
  return {
    dialect: 'sqlite',
    async query(sql, params) {
      const q = toQmarks(sql, params);
      const stmt = db.prepare(q.sql); // rejects multi-statement strings, like mysql's multipleStatements:false
      if (stmt.reader) {
        const rows = stmt.all(...(q.params ?? []));
        return { rows, rowCount: rows.length };
      }
      const info = stmt.run(...(q.params ?? []));
      // sqlite3_changes is only updated by INSERT/UPDATE/DELETE; after DDL/CTAS
      // it still holds the previous DML's count, so report undefined instead
      const dml = /^\s*(insert|update|delete)\b/i.test(q.sql);
      return { rows: [], rowCount: dml ? info.changes : undefined };
    },
    end: async () => db.close(),
  };
}

// Internal queries (seeds, tests, relationKind) use Postgres-style $N
// placeholders; model SQL never carries params, so user SQL is never rewritten.
function toQmarks(sql, params) {
  if (!params?.length) return { sql, params: undefined };
  const ordered = [];
  const out = sql.replace(/\$(\d+)/g, (_, n) => {
    ordered.push(params[n - 1]);
    return '?';
  });
  return { sql: out, params: ordered };
}

async function connectDuckdb(path, projectDir, readOnly = false, attach = []) {
  const { DuckDBInstance, ResultReturnType } = await import('@duckdb/node-api');
  const instance = await DuckDBInstance.create(path, readOnly ? { access_mode: 'READ_ONLY' } : undefined);
  const conn = await instance.connect();
  // Mount external databases as catalogs (referenced as "alias"."schema"."table").
  // Attachments are read-only by default; a read-only connection (the query API)
  // forces every attachment read-only too. DuckDB autoloads the sqlite/postgres/
  // mysql scanner extensions on demand, so no explicit INSTALL/LOAD is needed.
  for (const entry of attach ?? []) {
    const readOnlyAttach = readOnly || entry.read_only !== false;
    const opts = [];
    if (entry.type && entry.type !== 'duckdb') opts.push(`TYPE ${entry.type}`);
    if (readOnlyAttach) opts.push('READ_ONLY');
    const tail = opts.length ? ` (${opts.join(', ')})` : '';
    await conn.run(`ATTACH '${entry.path.replace(/'/g, "''")}' AS ${quoteIdent(entry.alias)}${tail}`);
  }
  if (projectDir) {
    // resolve read_csv('data/...') etc. against the project dir, not the app's cwd
    await conn.run(`SET file_search_path = '${projectDir.replace(/'/g, "''")}'`);
  }
  return {
    dialect: 'duckdb',
    async query(sql, params) {
      const result = await conn.run(sql, params?.length ? params : undefined);
      // Json variant returns BIGINT as string and dates as ISO strings —
      // safe for JSON.stringify and Number() alike.
      const rows = await result.getRowObjectsJson();
      const rowCount =
        result.returnType === ResultReturnType.CHANGED_ROWS ? result.rowsChanged
        : result.returnType === ResultReturnType.QUERY_RESULT ? rows.length
        : undefined; // DDL/CTAS: DuckDB doesn't report counts
      return { rows, rowCount };
    },
    async end() {
      conn.disconnectSync();
      instance.closeSync(); // checkpoints WAL, releases the file lock
    },
  };
}

export const quoteIdent = (s) => `"${String(s).replace(/"/g, '""')}"`;
export const rel = (schema, name) => `${quoteIdent(schema)}.${quoteIdent(name)}`;

export async function ensureSchema(client, schema) {
  // SQLite: CREATE SCHEMA doesn't exist; the schema was ATTACHed at connect
  // time (which creates the file when writable), so it's already ensured.
  if (client.dialect === 'sqlite') return;
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoteIdent(schema)}`);
}

// 'r' = table, 'v' = view, null = absent. information_schema works on all
// backends; the catalog predicate keeps DuckDB's temp catalog (schema "main")
// from colliding when the target schema is also "main". MySQL has no catalog
// (table_catalog is always 'def'; schema == database) so it skips the predicate.
// The alias forces a lowercase key — MySQL returns information_schema columns
// uppercase (TABLE_TYPE).
export async function relationKind(client, schema, name) {
  if (client.dialect === 'sqlite') {
    // no information_schema; pragma_table_list covers main + attached schemas
    const { rows } = await client.query(
      'SELECT type FROM pragma_table_list WHERE schema = $1 AND name = $2',
      [schema, name]
    );
    const t = rows[0]?.type; // lowercase 'table'/'view' (also 'shadow'/'virtual')
    return t === 'table' ? 'r' : t === 'view' ? 'v' : null;
  }
  const catalogPredicate =
    client.dialect === 'mysql' ? '' : 'table_catalog = current_database() AND ';
  const { rows } = await client.query(
    `SELECT table_type AS table_type FROM information_schema.tables
     WHERE ${catalogPredicate}table_schema = $1 AND table_name = $2`,
    [schema, name]
  );
  const t = rows[0]?.table_type;
  return t === 'BASE TABLE' ? 'r' : t === 'VIEW' ? 'v' : null;
}

export async function withTransaction(client, fn) {
  await client.query('BEGIN');
  try {
    const result = await fn();
    await client.query('COMMIT');
    return result;
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  }
}
