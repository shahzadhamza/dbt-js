// Minimal template renderer. Supported constructs:
//   {{ ref('model') }}  {{ this }}  {{ source('src', 'table') }}
//   {{ var('name') }}  {{ var('name', default) }}
//   {{ batch_start }}  {{ batch_end }}          (microbatch models only)
//   {{ timezone }}                              (the model's config timezone)
//   {% if is_incremental() %} ... {% endif %}   (no nesting)
const CONFIG_RE = /\/\*\s*config:\s*[\s\S]*?\*\//;
const IF_INCREMENTAL_RE = /\{%\s*if\s+is_incremental\(\)\s*%\}([\s\S]*?)\{%\s*endif\s*%\}/g;
const REF_RE = /\{\{\s*ref\(\s*['"](\w+)['"]\s*\)\s*\}\}/g;
const THIS_RE = /\{\{\s*this\s*\}\}/g;
const SOURCE_RE = /\{\{\s*source\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}/g;
const VAR_RE = /\{\{\s*var\(\s*['"](\w+)['"]\s*(?:,\s*('[^']*'|"[^"]*"|[^)\s]+))?\s*\)\s*\}\}/g;
const BATCH_RE = /\{\{\s*(batch_start|batch_end)\s*\}\}/g;
const TIMEZONE_RE = /\{\{\s*timezone\s*\}\}/g;
const LEFTOVER_RE = /\{\{[\s\S]*?\}\}|\{%[\s\S]*?%\}|\{\{|\{%/;

const quoteIdent = (s) => `"${s.replace(/"/g, '""')}"`;
const stripQuotes = (s) => (/^(['"]).*\1$/s.test(s) ? s.slice(1, -1) : s);

// Cheap dependency extraction for DAG building — scans ref() calls without
// rendering, so missing vars or incremental branches can't hide a dependency.
export function extractRefs(rawSql) {
  return [...rawSql.matchAll(REF_RE)].map((m) => m[1]);
}

// ctx: { name, schema, vars, isIncremental, sources, batchStart?, batchEnd?, timezone? }
export function render(rawSql, ctx) {
  const refs = [];
  let sql = rawSql.replace(CONFIG_RE, '');
  sql = sql.replace(IF_INCREMENTAL_RE, (_, body) => (ctx.isIncremental ? body : ''));
  sql = sql.replace(REF_RE, (_, name) => {
    refs.push(name);
    return `${quoteIdent(ctx.schema)}.${quoteIdent(name)}`;
  });
  sql = sql.replace(THIS_RE, () => `${quoteIdent(ctx.schema)}.${quoteIdent(ctx.name)}`);
  sql = sql.replace(SOURCE_RE, (_, src, table) => {
    const decl = ctx.sources?.[src];
    if (!decl?.schema) {
      throw new Error(
        `'${ctx.name}' uses undeclared source '${src}' — add it under "sources" in dbtjs.config.json`
      );
    }
    // an attached database (DuckDB ATTACH) adds a catalog qualifier:
    // "database"."schema"."table"; without it the name stays two-part
    const prefix = decl.database ? `${quoteIdent(decl.database)}.` : '';
    return `${prefix}${quoteIdent(decl.schema)}.${quoteIdent(table)}`;
  });
  if (ctx.batchStart != null) {
    // only microbatch runs supply these; elsewhere the token falls through to the leftover guard
    sql = sql.replace(BATCH_RE, (_, which) => (which === 'batch_start' ? ctx.batchStart : ctx.batchEnd));
  }
  // raw substitution (like batch_start) — author quotes it in SQL if needed
  sql = sql.replace(TIMEZONE_RE, ctx.timezone ?? 'UTC');
  sql = sql.replace(VAR_RE, (_, name, def) => {
    const value = ctx.vars?.[name];
    if (value !== undefined && value !== null) return String(value);
    if (def !== undefined) return stripQuotes(def);
    throw new Error(`Missing var '${name}' in '${ctx.name}' (no default given) — pass --vars '{"${name}": ...}'`);
  });
  const leftover = sql.match(LEFTOVER_RE);
  if (leftover) {
    throw new Error(`Unrecognized template expression in '${ctx.name}': ${leftover[0].slice(0, 80)}`);
  }
  return { sql: sql.trim(), refs };
}
