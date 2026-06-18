import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { basename, join } from 'node:path';

const CONFIG_RE = /\/\*\s*config:\s*([\s\S]*?)\*\//;
const MATERIALIZATIONS = new Set(['view', 'table', 'incremental']);
const STRATEGIES = new Set(['append', 'delete+insert', 'microbatch']);
const BATCH_SIZES = new Set(['hour', 'day', 'month', 'year']);

// inlineModels: optional { name: rawSql } map (same format as a model file,
// config comment included) — when given, models/ is not scanned.
export function loadProject(cwd = process.cwd(), { models: inlineModels } = {}) {
  const models = [];
  if (inlineModels) {
    for (const [name, rawSql] of Object.entries(inlineModels)) {
      models.push({ name, rawSql, config: parseModelConfig(name, rawSql) });
    }
  } else {
    const modelsDir = join(cwd, 'models');
    if (existsSync(modelsDir)) {
      for (const file of readdirSync(modelsDir).filter((f) => f.endsWith('.sql')).sort()) {
        const path = join(modelsDir, file);
        const rawSql = readFileSync(path, 'utf8');
        const name = basename(file, '.sql');
        models.push({ name, path, rawSql, config: parseModelConfig(name, rawSql) });
      }
    }
  }

  const seeds = [];
  const seedsDir = join(cwd, 'seeds');
  if (existsSync(seedsDir)) {
    for (const file of readdirSync(seedsDir).filter((f) => f.endsWith('.csv')).sort()) {
      seeds.push({ name: basename(file, '.csv'), path: join(seedsDir, file) });
    }
  }

  const seen = new Set();
  for (const { name } of [...models, ...seeds]) {
    if (seen.has(name)) throw new Error(`Duplicate node name '${name}' across models/ and seeds/`);
    seen.add(name);
  }
  if (!models.length && !seeds.length) {
    throw new Error(`No models/*.sql or seeds/*.csv found in ${cwd} (and no inline models given)`);
  }
  return { models, seeds };
}

function parseModelConfig(name, rawSql) {
  const match = rawSql.match(CONFIG_RE);
  let config = {};
  if (match) {
    try {
      config = JSON.parse(match[1]);
    } catch (e) {
      throw new Error(`Invalid JSON in config comment of model '${name}': ${e.message}`);
    }
  }
  config.materialized ??= 'view';
  if (!MATERIALIZATIONS.has(config.materialized)) {
    throw new Error(`Model '${name}': unknown materialized '${config.materialized}' (use view|table|incremental)`);
  }
  config.timezone ??= 'UTC';
  if (typeof config.timezone !== 'string') {
    throw new Error(`Model '${name}': "timezone" must be a string (e.g. "UTC", "America/New_York")`);
  }
  try {
    // RangeError on an unknown IANA zone; 'UTC' is always valid
    new Intl.DateTimeFormat('en-US', { timeZone: config.timezone });
  } catch {
    throw new Error(`Model '${name}': unknown timezone '${config.timezone}' (use an IANA name like "America/New_York" or "UTC")`);
  }
  for (const key of ['pre_hook', 'post_hook']) {
    if (typeof config[key] === 'string') config[key] = [config[key]];
    config[key] ??= [];
    if (!Array.isArray(config[key]) || config[key].some((h) => typeof h !== 'string' || !h.trim())) {
      throw new Error(`Model '${name}': "${key}" must be a SQL string or array of SQL strings`);
    }
  }
  if (config.materialized === 'incremental') {
    config.strategy ??= 'append';
    if (!STRATEGIES.has(config.strategy)) {
      throw new Error(`Model '${name}': unknown strategy '${config.strategy}' (use append|delete+insert|microbatch)`);
    }
    if (config.strategy === 'delete+insert' && !config.unique_key) {
      throw new Error(`Model '${name}': strategy delete+insert requires "unique_key"`);
    }
    if (config.strategy === 'microbatch') {
      if (typeof config.event_time !== 'string' || !config.event_time) {
        throw new Error(`Model '${name}': microbatch requires "event_time" (a column of this model)`);
      }
      if (!config.begin || Number.isNaN(Date.parse(String(config.begin).replace(' ', 'T')))) {
        throw new Error(`Model '${name}': microbatch requires "begin" (start of history, e.g. "2026-01-01")`);
      }
      if (!BATCH_SIZES.has(config.batch_size)) {
        throw new Error(`Model '${name}': microbatch requires "batch_size" (hour|day|month|year)`);
      }
      config.lookback ??= 1;
      if (!Number.isInteger(config.lookback) || config.lookback < 0) {
        throw new Error(`Model '${name}': "lookback" must be a non-negative integer`);
      }
      if (config.unique_key) {
        throw new Error(`Model '${name}': "unique_key" is not used by microbatch (batches replace by event_time window)`);
      }
    }
  }
  return config;
}
