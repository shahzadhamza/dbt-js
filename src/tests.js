import { quoteIdent, rel } from './db.js';

// Each test compiles to a SELECT returning violating rows; any row = FAIL.
// NULLs only violate not_null (dbt semantics).
export function buildTests(models, schema) {
  const tests = [];
  for (const model of models) {
    for (const [column, specs] of Object.entries(model.config.tests ?? {})) {
      const target = rel(schema, model.name);
      const col = quoteIdent(column);
      for (const spec of specs) {
        if (spec === 'not_null') {
          tests.push({
            id: `${model.name}.${column}.not_null`,
            model: model.name,
            sql: `SELECT * FROM ${target} WHERE ${col} IS NULL`,
            params: [],
          });
        } else if (spec === 'unique') {
          tests.push({
            id: `${model.name}.${column}.unique`,
            model: model.name,
            sql: `SELECT ${col}, count(*) AS n FROM ${target} WHERE ${col} IS NOT NULL GROUP BY ${col} HAVING count(*) > 1`,
            params: [],
          });
        } else if (spec?.accepted_values?.length) {
          const placeholders = spec.accepted_values.map((_, i) => `$${i + 1}`).join(', ');
          tests.push({
            id: `${model.name}.${column}.accepted_values`,
            model: model.name,
            sql: `SELECT ${col}, count(*) AS n FROM ${target} WHERE ${col} IS NOT NULL AND ${col} NOT IN (${placeholders}) GROUP BY ${col}`,
            params: spec.accepted_values,
          });
        } else {
          throw new Error(`Unknown test ${JSON.stringify(spec)} on ${model.name}.${column}`);
        }
      }
    }
  }
  return tests;
}

export async function runTest(client, test) {
  const count = await client.query(`SELECT count(*) AS n FROM (${test.sql}) q`, test.params);
  const violations = Number(count.rows[0].n);
  if (violations === 0) return { pass: true };
  const sample = await client.query(`${test.sql} LIMIT 10`, test.params);
  return { pass: false, violations, sample: sample.rows };
}
