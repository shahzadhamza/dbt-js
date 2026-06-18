import { readFileSync } from 'node:fs';
import { parse } from 'csv-parse/sync';
import { quoteIdent, rel, withTransaction } from './db.js';

const BATCH_SIZE = 500;

export async function loadSeed(client, seed, projectCfg) {
  const rows = parse(readFileSync(seed.path, 'utf8'), {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  });
  if (!rows.length) throw new Error(`Seed '${seed.name}' has no data rows`);

  const columns = Object.keys(rows[0]);
  const overrides = projectCfg.seeds?.columnTypes?.[seed.name] ?? {};
  const mysql = client.dialect === 'mysql';
  const sqlite = client.dialect === 'sqlite';
  const types = columns.map((c) => {
    const t = overrides[c] ?? inferType(rows.map((r) => r[c]));
    // bare NUMERIC is DECIMAL(10,0) on MySQL — would silently round decimals
    return mysql && t === 'numeric' ? 'decimal(38,10)' : t;
  });
  const target = rel(projectCfg.schema, seed.name);
  // stay under SQLite's 32766-bind-variable cap (and Postgres's 65535) for wide CSVs
  const batchSize = Math.max(1, Math.min(BATCH_SIZE, Math.floor(32000 / columns.length)));

  await withTransaction(client, async () => {
    await client.query(`DROP TABLE IF EXISTS ${target}${sqlite ? '' : ' CASCADE'}`);
    const defs = columns.map((c, i) => `${quoteIdent(c)} ${types[i]}`).join(', ');
    await client.query(`CREATE TABLE ${target} (${defs})`);
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      const params = [];
      const tuples = batch.map(
        (row) =>
          `(${columns
            .map((c, j) => {
              let v = row[c] === '' ? null : row[c];
              // MySQL booleans are TINYINT(1); the string 'true' errors under
              // strict mode. SQLite would store the TEXT 'true', which is falsy
              // in CASE WHEN (and better-sqlite3 can't bind true/false anyway).
              if ((mysql || sqlite) && v !== null && types[j] === 'boolean')
                v = /^(true|t)$/i.test(v) ? 1 : 0;
              params.push(v);
              return `$${params.length}`;
            })
            .join(', ')})`
      );
      await client.query(`INSERT INTO ${target} VALUES ${tuples.join(', ')}`, params);
    }
  });
  return { rowCount: rows.length };
}

// Minimal inference: integer/bigint, numeric, boolean, else text.
// Empty strings load as NULL and are excluded from inference.
// Anything fancier (dates, etc.) → seeds.columnTypes override in dbtjs.config.json.
export function inferType(values) {
  const present = values.filter((v) => v !== '');
  if (!present.length) return 'text';
  if (present.every((v) => /^-?\d+$/.test(v))) {
    return present.some((v) => Math.abs(Number(v)) > 2147483647) ? 'bigint' : 'integer';
  }
  if (present.every((v) => /^-?\d*\.?\d+$/.test(v))) return 'numeric';
  if (present.every((v) => /^(true|false|t|f)$/i.test(v))) return 'boolean';
  return 'text';
}
