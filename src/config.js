import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

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
  cfg.vars ??= {};
  cfg.sources ??= {};
  cfg.seeds ??= {};
  return cfg;
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
