// Batch window computation for the microbatch incremental strategy.
// Date math aligns to a configurable IANA timezone (default 'UTC') — windows
// snap to that zone's wall-clock boundaries. No DB access, no SQL dialect concerns.

const UNITS = ['hour', 'day', 'month', 'year'];

const DAY_MS = 86_400_000;

// Constructing an Intl.DateTimeFormat is expensive (tens of µs) and this runs
// several times per batch, so keep one per zone.
const formatters = new Map();
function formatterFor(tz) {
  let dtf = formatters.get(tz);
  if (!dtf) {
    dtf = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      hourCycle: 'h23',
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    formatters.set(tz, dtf);
  }
  return dtf;
}

// Wall-clock components of an instant as seen in `tz`: { year, month(1-12),
// day, hour, minute, second }. Built on Intl so it tracks DST automatically.
function partsInZone(date, tz) {
  if (tz === 'UTC') {
    // the default zone — plain Date getters are far cheaper than Intl
    return {
      year: date.getUTCFullYear(),
      month: date.getUTCMonth() + 1,
      day: date.getUTCDate(),
      hour: date.getUTCHours(),
      minute: date.getUTCMinutes(),
      second: date.getUTCSeconds(),
    };
  }
  const p = {};
  for (const part of formatterFor(tz).formatToParts(date)) {
    if (part.type !== 'literal') p[part.type] = Number(part.value);
  }
  if (p.hour === 24) p.hour = 0; // some engines emit 24 for midnight under h23
  return p;
}

// The UTC instant for a wall-clock time interpreted in `tz`. Tries the zone's
// offset a day before and a day after. Equal offsets mean no transition is
// near; otherwise:
//   - one candidate round-trips        → that instant
//   - both round-trip (fall-back overlap) → the earlier instant
//   - neither round-trips (spring-forward gap) → shift forward past the gap,
//     so 02:30 on a 02:00→03:00 night becomes 03:30
function zonedWallToUtc({ year, month, day, hour, minute, second }, tz) {
  const naive = Date.UTC(year, month - 1, day, hour, minute, second);
  const offsetAt = (ms) => {
    const p = partsInZone(new Date(ms), tz);
    return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) - ms;
  };
  const before = naive - offsetAt(naive - DAY_MS);
  const after = naive - offsetAt(naive + DAY_MS);
  if (before === after) return new Date(before);
  const roundTrips = (ms) => {
    const p = partsInZone(new Date(ms), tz);
    return Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second) === naive;
  };
  const hits = [before, after].filter(roundTrips);
  return new Date(hits.length ? Math.min(...hits) : before);
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

// Wall-clock parts of `date` in `tz`, zeroed below `size`.
function truncParts(date, size, tz) {
  const p = partsInZone(date, tz);
  p.second = 0;
  p.minute = 0;
  if (size !== 'hour') p.hour = 0;
  if (size === 'month' || size === 'year') p.day = 1;
  if (size === 'year') p.month = 1;
  return p;
}

function truncTz(date, size, tz) {
  return zonedWallToUtc(truncParts(date, size, tz), tz);
}

// Steps from the *nominal* boundary: a boundary shifted by a DST gap (e.g.
// Beirut's missing midnight → 01:00) still advances to the next 00:00
// rather than carrying the shift into every later batch.
function addBatchesTz(date, size, n, tz) {
  const p = truncParts(date, size, tz);
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
  for (let t = startAt; t < endAt; ) {
    const next = addBatchesTz(t, batchSize, 1, timezone);
    // zonedWallToUtc always moves forward through gaps; this is a backstop
    // so a timezone-data surprise fails loudly instead of looping forever
    if (next <= t) throw new Error(`Internal error: batch boundary did not advance past ${fmtTz(t, timezone)} (${timezone})`);
    const batchStart = batches.at(-1)?.end ?? fmtTz(t, timezone); // previous end == this start
    batches.push({ start: batchStart, end: fmtTz(next, timezone) });
    t = next;
  }
  return batches;
}
