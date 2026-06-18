import { extractRefs } from './render.js';

// Returns { nodes: Map<name, node>, order: string[] } — dependencies before dependents.
// Seeds are DAG nodes with no deps so ref('seed_name') resolves and orders correctly;
// `run` does not load them (that's the seed command's job, like dbt).
export function buildDag(models, seeds) {
  const nodes = new Map();
  for (const seed of seeds) nodes.set(seed.name, { ...seed, type: 'seed', deps: [] });
  for (const model of models) {
    nodes.set(model.name, { ...model, type: 'model', deps: [...new Set(extractRefs(model.rawSql))] });
  }
  return { nodes, order: topoSort(nodes) };
}

export function topoSort(nodes) {
  const order = [];
  const state = new Map(); // 0/undefined = unvisited, 1 = in stack, 2 = done
  function visit(name, path) {
    if (state.get(name) === 2) return;
    if (state.get(name) === 1) {
      throw new Error(`Cycle detected: ${[...path, name].join(' -> ')}`);
    }
    if (!nodes.has(name)) {
      throw new Error(`'${path.at(-1)}' refs unknown model/seed '${name}'`);
    }
    state.set(name, 1);
    for (const dep of nodes.get(name).deps) visit(dep, [...path, name]);
    state.set(name, 2);
    order.push(name);
  }
  for (const name of nodes.keys()) visit(name, []);
  return order;
}

// spec: "a,b" | "+name" (name + upstream) | "name+" (name + downstream); null = everything.
export function expandSelection(spec, nodes, order) {
  if (!spec) return order;
  const reversed = new Map([...nodes.keys()].map((k) => [k, []]));
  for (const [name, node] of nodes) {
    for (const dep of node.deps) reversed.get(dep)?.push(name);
  }
  const selected = new Set();
  for (const token of spec.split(',').map((s) => s.trim()).filter(Boolean)) {
    const upstream = token.startsWith('+');
    const downstream = token.endsWith('+');
    const name = token.replace(/^\+/, '').replace(/\+$/, '');
    if (!nodes.has(name)) throw new Error(`--select: unknown model/seed '${name}'`);
    selected.add(name);
    if (upstream) for (const n of walk(name, (x) => nodes.get(x).deps)) selected.add(n);
    if (downstream) for (const n of walk(name, (x) => reversed.get(x))) selected.add(n);
  }
  return order.filter((n) => selected.has(n));
}

function walk(start, next) {
  const found = new Set();
  const queue = [start];
  while (queue.length) {
    for (const n of next(queue.shift())) {
      if (!found.has(n)) {
        found.add(n);
        queue.push(n);
      }
    }
  }
  return found;
}
