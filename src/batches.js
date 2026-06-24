// Batch window computation for the microbatch incremental strategy.
// Date math aligns to a configurable IANA timezone (default 'UTC') — windows
// snap to that zone's wall-clock boundaries. No DB access, no SQL dialect concerns.

const UNITS = ['hour', 'day', 'month', 'year'];

// Wall-clock components of an instant as seen in `tz`: { year, month(1-12),
// day, hour, minute, second }. Built on Intl so it tracks DST automatically.
function partsInZone(date, tz) {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone: tz,
    hourCycle: 'h23',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  const p = {};
  for (const part of dtf.formatToParts(date)) {
    if (part.type !== 'literal') p[part.type] = Number(part.value);
  }
  if (p.hour === 24) p.hour = 0; // some engines emit 24 for midnight under h23
  return p;
}

// The UTC instant for a wall-clock time interpreted in `tz`, DST-correct via an
// offset back-solve (one correction pass resolves spring-forward/fall-back).
function zonedWallToUtc({ year, month, day, hour, minute, second }, tz) {
  const offsetAt = (ms) => {
    const p = partsInZone(new Date(ms), tz);
    return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) - ms;
  };
  const naive = Date.UTC(year, month - 1, day, hour, minute, second);
  let utc = naive - offsetAt(naive);
  utc = naive - offsetAt(utc);
  return new Date(utc);
}

// Parse a date string into a UTC instant. A string carrying an explicit zone
// (Z or ±HH:MM) is an absolute instant; a naive 'YYYY-MM-DD[ HH:MM[:SS]]' is
// interpreted as wall-clock in `tz`.
export function parseInZone(s, tz = 'UTC') {
  const iso = String(s).trim().replace(' ', 'T');
  if (/(?:[zZ]|[+-]\d{2}:?\d{2})$/.test(iso)) {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) throw new Error(`Invalid date '${s}' (use YYYY-MM-DD or ISO 8601)`);
    return d;
  }
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2})(?::(\d{2}))?)?$/);
  if (!m) throw new Error(`Invalid date '${s}' (use YYYY-MM-DD or ISO 8601)`);
  return zonedWallToUtc(
    { year: +m[1], month: +m[2], day: +m[3], hour: +(m[4] || 0), minute: +(m[5] || 0), second: +(m[6] || 0) },
    tz
  );
}

// Back-compat alias — parsing in UTC.
export const parseUtc = (s) => parseInZone(s, 'UTC');

function truncTz(date, size, tz) {
  const p = partsInZone(date, tz);
  p.second = 0;
  p.minute = 0;
  if (size !== 'hour') p.hour = 0;
  if (size === 'month' || size === 'year') p.day = 1;
  if (size === 'year') p.month = 1;
  return zonedWallToUtc(p, tz);
}

function addBatchesTz(date, size, n, tz) {
  const p = partsInZone(date, tz);
  if (size === 'hour') p.hour += n;
  else if (size === 'day') p.day += n;
  else if (size === 'month') p.month += n;
  else p.year += n;
  return zonedWallToUtc(p, tz); // Date.UTC normalizes overflow (day 32, month 13, ...)
}

function fmtTz(date, tz) {
  const p = partsInZone(date, tz);
  const pad = (n) => String(n).padStart(2, '0');
  return `${p.year}-${pad(p.month)}-${pad(p.day)} ${pad(p.hour)}:${pad(p.minute)}:${pad(p.second)}`;
}

// Returns [{ start, end }] of aligned [start, end) windows as 'YYYY-MM-DD HH:MM:SS'.
//   first build / --full-refresh : trunc(begin) .. current batch end
//   normal run                   : trunc(now) - lookback .. current batch end
//   explicit backfill            : trunc(start) .. ceil(end)  (whole batches)
// The window never starts before `begin`.
export function computeBatches({ begin, batchSize, lookback = 1, start, end, firstBuild, timezone = 'UTC', now = new Date() }) {
  if (!UNITS.includes(batchSize)) throw new Error(`Invalid batch_size '${batchSize}' (use ${UNITS.join('|')})`);
  const beginAt = truncTz(parseInZone(begin, timezone), batchSize, timezone);

  let endAt;
  if (end) {
    const e = parseInZone(end, timezone);
    const t = truncTz(e, batchSize, timezone);
    endAt = e.getTime() === t.getTime() ? t : addBatchesTz(t, batchSize, 1, timezone);
  } else {
    endAt = addBatchesTz(truncTz(now, batchSize, timezone), batchSize, 1, timezone);
  }

  let startAt;
  if (start) startAt = truncTz(parseInZone(start, timezone), batchSize, timezone);
  else if (firstBuild) startAt = beginAt;
  else startAt = addBatchesTz(truncTz(now, batchSize, timezone), batchSize, -lookback, timezone);
  if (startAt < beginAt) startAt = beginAt;

  if (start && startAt >= endAt) {
    throw new Error(`--event-time-start (${fmtTz(startAt, timezone)}) must be before the end of the window (${fmtTz(endAt, timezone)})`);
  }
  // A future (or otherwise out-of-range) `begin` clamps startAt up to or past
  // endAt, yielding zero batches. Without this guard `compile` crashes on b[0]
  // and `run` silently reports success while never creating the table.
  if (startAt >= endAt) {
    throw new Error(
      `Microbatch window is empty: start ${fmtTz(startAt, timezone)} is not before end ${fmtTz(endAt, timezone)} — ` +
        `check that "begin" (${fmtTz(beginAt, timezone)}) is in the past relative to now (${fmtTz(now, timezone)})`
    );
  }

  const batches = [];
  for (let t = startAt; t < endAt; t = addBatchesTz(t, batchSize, 1, timezone)) {
    batches.push({ start: fmtTz(t, timezone), end: fmtTz(addBatchesTz(t, batchSize, 1, timezone), timezone) });
  }
  return batches;
}
