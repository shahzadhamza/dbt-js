import { readFileSync } from 'node:fs';
import { basename, extname, join, resolve } from 'node:path';

const ATTACH_TYPES = ['duckdb', 'sqlite', 'postgres', 'mysql'];
// DuckDB's built-in catalogs — an attachment can't reuse these names.
const RESERVED_ALIASES = new Set(['memory', 'system', 'temp']);
// postgres/mysql attachments take a connection string, not a filesystem path.
const isFileAttach = (type) => !type || type === 'duckdb' || type === 'sqlite';

export function loadConfig(cwd = process.cwd()) {
  const path = join(cwd, 'dbtjs.config.json');
  let raw;
  try {
    raw = readFileSync(path, 'utf8');
  } catch {
    throw new Error(`No dbtjs.config.json found in ${cwd}`);
  }
  let cfg;
  try {
    cfg = JSON.parse(raw);
  } catch (e) {
    throw new Error(`Invalid JSON in ${path}: ${e.message}`);
  }
  return validateConfig(cfg, cwd);
}

// Shared by the file path above and inline `config` objects passed to the api.
// Mutates and returns cfg (defaults, env interpolation, duckdb path resolution).
export function validateConfig(cfg, cwd = process.cwd()) {
  if (!cfg.connection || typeof cfg.connection !== 'object') {
    throw new Error('config must have a "connection" object');
  }
  cfg.connection.type ??= 'postgres';
  if (!['postgres', 'duckdb', 'mysql', 'sqlite'].includes(cfg.connection.type)) {
    throw new Error(
      `connection.type must be "postgres", "duckdb", "mysql" or "sqlite", got "${cfg.connection.type}"`
    );
  }
  if (['duckdb', 'sqlite'].includes(cfg.connection.type) && typeof cfg.connection.path !== 'string') {
    throw new Error(
      `${cfg.connection.type} connection requires a "path" string (file path or ":memory:")`
    );
  }
  if (cfg.connection.type === 'mysql') {
    if (typeof cfg.connection.database !== 'string') {
      throw new Error('mysql connection requires a "database" string');
    }
    cfg.connection.port ??= 3306;
  }
  if (cfg.connection.attach !== undefined) validateAttach(cfg.connection);
  if (!cfg.schema || typeof cfg.schema !== 'string') {
    throw new Error('config must have a "schema" string (target schema for models)');
  }
  for (const [key, value] of Object.entries(cfg.connection)) {
    if (typeof value === 'string') cfg.connection[key] = interpolateEnv(value, key);
  }
  if (['duckdb', 'sqlite'].includes(cfg.connection.type) && cfg.connection.path !== ':memory:') {
    // anchor to the project dir so embedding apps can run from any cwd
    cfg.connection.path = resolve(cwd, cfg.connection.path);
  }
  for (const [i, entry] of (cfg.connection.attach ?? []).entries()) {
    for (const [key, value] of Object.entries(entry)) {
      if (typeof value === 'string') entry[key] = interpolateEnv(value, `attach[${i}].${key}`);
    }
    // file-based attachments anchor to the project dir like connection.path;
    // postgres/mysql paths are connection strings — leave them untouched
    if (isFileAttach(entry.type) && entry.path !== ':memory:') {
      entry.path = resolve(cwd, entry.path);
    }
  }
  cfg.vars ??= {};
  cfg.sources ??= {};
  cfg.seeds ??= {};
  return cfg;
}

// Validates connection.attach (DuckDB only). Each entry mounts an external
// database as a catalog via ATTACH. Env interpolation and path resolution run
// later in validateConfig, alongside the main connection.
function validateAttach(connection) {
  if (connection.type !== 'duckdb') {
    throw new Error('"attach" is only supported for duckdb connections');
  }
  if (!Array.isArray(connection.attach)) {
    throw new Error('connection.attach must be an array of { alias, path } objects');
  }
  // main catalog name DuckDB derives from the file path (basename sans extension)
  const mainName =
    connection.path && connection.path !== ':memory:'
      ? basename(connection.path, extname(connection.path))
      : null;
  const seen = new Set();
  for (const [i, entry] of connection.attach.entries()) {
    const at = `connection.attach[${i}]`;
    if (!entry || typeof entry !== 'object') throw new Error(`${at} must be an object`);
    if (typeof entry.path !== 'string' || !entry.path) {
      throw new Error(`${at} requires a non-empty "path" string (file path or connection string)`);
    }
    if (entry.type !== undefined && !ATTACH_TYPES.includes(entry.type)) {
      throw new Error(`${at}.type must be one of ${ATTACH_TYPES.join(', ')}, got "${entry.type}"`);
    }
    if (entry.read_only !== undefined && typeof entry.read_only !== 'boolean') {
      throw new Error(`${at}.read_only must be a boolean`);
    }
    // alias is optional for file-based attachments — DuckDB derives it from the
    // path basename, and we mirror that so source().database can reference it.
    // A connection string (postgres/mysql) has no meaningful basename, so its
    // alias must be given explicitly.
    if (entry.alias === undefined && isFileAttach(entry.type) && entry.path !== ':memory:') {
      entry.alias = basename(entry.path, extname(entry.path));
    }
    if (typeof entry.alias !== 'string' || !entry.alias) {
      throw new Error(
        `${at} requires a non-empty "alias" string` +
          (isFileAttach(entry.type) ? '' : ` (required for ${entry.type} connection strings)`)
      );
    }
    if (RESERVED_ALIASES.has(entry.alias)) {
      throw new Error(`${at}.alias "${entry.alias}" is reserved by DuckDB — choose another name`);
    }
    if (mainName && entry.alias === mainName) {
      throw new Error(`${at}.alias "${entry.alias}" collides with the main database catalog — choose another name`);
    }
    if (seen.has(entry.alias)) {
      throw new Error(`${at}.alias "${entry.alias}" is used by more than one attachment`);
    }
    seen.add(entry.alias);
  }
}

function interpolateEnv(value, key) {
  return value.replace(/\$\{(\w+)\}/g, (_, name) => {
    const v = process.env[name];
    if (v === undefined) {
      throw new Error(`connection.${key} references \${${name}} but that environment variable is not set`);
    }
    return v;
  });
}
