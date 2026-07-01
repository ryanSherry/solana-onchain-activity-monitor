"""
SQLite persistence for the on-chain activity monitor.

Replaces the per-day CSVs with a single DB. `sqlite3` is stdlib, so this stays
zero-dependency. The schema is derived from monitor.CSV_FIELDS with automatic
ALTER-based column evolution (adding a signal is a one-liner, no file migration),
each sample is an ACID insert (no truncate-then-write corruption window), and the
baselines / charts / percentiles read time-ranged slices in SQL instead of loading
whole files. A one-time importer folds any legacy per-day CSVs into the DB.

Only functions touch monitor at call time (never at import), so the mutual
monitor<->store import resolves lazily with no cycle.
"""
from __future__ import annotations

import calendar
import csv as _csv
import glob
import os
import sqlite3
import statistics
import threading
import time

import monitor

# Columns that hold text; every other CSV_FIELDS column is numeric (REAL).
_TEXT = {"timestamp_utc", "surge_level", "top_mover"}

_lock = threading.Lock()
_conns = {}


def _cols():
    return list(monitor.CSV_FIELDS)


def _coltype(name):
    return "TEXT" if name in _TEXT else "REAL"


def _quoted(cols):
    return ", ".join('"%s"' % n for n in cols)


def _conn(path):
    c = _conns.get(path)
    if c is None:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        c = sqlite3.connect(path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")     # concurrent read while writing
        c.execute("PRAGMA synchronous=NORMAL")
        c.execute("PRAGMA busy_timeout=5000")    # wait out a cross-process writer
        _ensure_schema(c)
        _conns[path] = c
    return c


def _ensure_schema(c):
    cols = _cols()
    defs = ["ts INTEGER"] + ['"%s" %s' % (n, _coltype(n)) for n in cols]
    c.execute("CREATE TABLE IF NOT EXISTS samples (%s)" % ", ".join(defs))
    c.execute("CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts)")
    have = {r[1] for r in c.execute("PRAGMA table_info(samples)")}
    for n in cols:                # add columns introduced since the table was made
        if n not in have:
            c.execute('ALTER TABLE samples ADD COLUMN "%s" %s' % (n, _coltype(n)))
    c.commit()


def _epoch(ts):
    try:
        return calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"))
    except (ValueError, TypeError):
        return None


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _rowvals(cols, row):
    vals = [_epoch(row.get("timestamp_utc"))]
    for n in cols:
        v = row.get(n)
        vals.append((v if v not in (None, "") else None) if n in _TEXT else _num(v))
    return vals


def append(path, row):
    """Persist one sample row (ACID: single insert + commit)."""
    cols = _cols()
    ph = ", ".join(["?"] * (len(cols) + 1))
    q = "INSERT INTO samples (ts, %s) VALUES (%s)" % (_quoted(cols), ph)
    with _lock:
        c = _conn(path)
        c.execute(q, _rowvals(cols, row))
        c.commit()


def read_since(path, seconds):
    """Rows whose ts is within the last `seconds`, oldest-first, as dicts
    keyed by CSV_FIELDS (numeric columns come back as float or None)."""
    cols = _cols()
    cutoff = int(time.time() - seconds)
    with _lock:
        c = _conn(path)
        rows = c.execute(
            "SELECT %s FROM samples WHERE ts >= ? ORDER BY ts ASC"
            % _quoted(cols), (cutoff,))
        return [dict(r) for r in rows]


def read_recent(path, limit):
    """The most-recent `limit` rows, oldest-first, as dicts."""
    cols = _cols()
    with _lock:
        c = _conn(path)
        rows = c.execute(
            "SELECT %s FROM samples WHERE ts IS NOT NULL ORDER BY ts DESC LIMIT %d"
            % (_quoted(cols), int(limit)))
        out = [dict(r) for r in rows]
    out.reverse()
    return out


def read_scores_since(path, seconds):
    """(ts_epoch, surge_score) pairs within the window, oldest-first -- the light
    query behind the trailing-week percentile baseline."""
    cutoff = int(time.time() - seconds)
    with _lock:
        c = _conn(path)
        return [(r[0], r[1]) for r in c.execute(
            "SELECT ts, surge_score FROM samples "
            "WHERE ts >= ? AND surge_score IS NOT NULL ORDER BY ts ASC", (cutoff,))]


def prune(path, keep_days):
    """Delete samples older than keep_days to bound DB growth. No VACUUM (it locks
    the whole DB) -- pages freed by the delete are reused by future inserts, so the
    file plateaus at ~the retention window rather than growing forever. keep_days
    <= 0 disables. Returns rows deleted."""
    if not keep_days or keep_days <= 0:
        return 0
    cutoff = int(time.time() - keep_days * 86400)
    with _lock:
        c = _conn(path)
        # also drop NULL-ts rows (bad-timestamp imports) -- they're invisible to
        # every read AND (NULL < cutoff being false) would never otherwise prune
        cur = c.execute("DELETE FROM samples WHERE ts < ? OR ts IS NULL", (cutoff,))
        c.commit()
        return cur.rowcount


def hourly_baselines(path, signals, days=7, min_samples=60, min_days=2):
    """Per-signal, per-UTC-hour robust baseline from the trailing `days` of
    history: {signal: {hour: (center, robust_sigma)}} where center=median and
    robust_sigma=1.4826*MAD. A bucket is included only when it has >= min_samples
    points AND spans >= min_days distinct days -- the day span is what stops a
    live surge from polluting its OWN hour baseline on thin history (median is
    only robust while today is a minority of the bucket). The per-signal seed
    floor is applied later in monitor.compute (it needs the seed).

    Uses its OWN read connection (WAL -> concurrent with the surge loop's writes),
    so the ~day-scale scan never blocks the 5s append on store's shared lock."""
    signals = list(signals)
    cutoff = int(time.time() - days * 86400)
    try:
        c = sqlite3.connect(path)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA busy_timeout=5000")     # wait out a concurrent writer
        try:
            rows = c.execute(
                "SELECT ts, %s FROM samples WHERE ts >= ?" % _quoted(signals),
                (cutoff,)).fetchall()
        finally:
            c.close()
    except sqlite3.Error:
        return {}                     # no table yet / unreadable -> all-window
    buckets = {s: [[] for _ in range(24)] for s in signals}
    daysets = {s: [set() for _ in range(24)] for s in signals}
    for r in rows:
        ts = r["ts"]
        if ts is None:
            continue
        tm = time.gmtime(ts)
        h, day = tm.tm_hour, (tm.tm_year, tm.tm_yday)
        for s in signals:
            v = r[s]
            if v is not None:
                buckets[s][h].append(v)
                daysets[s][h].add(day)
    out = {}
    for s in signals:
        hours = {}
        for h in range(24):
            vals = buckets[s][h]
            if len(vals) >= min_samples and len(daysets[s][h]) >= min_days:
                center = statistics.median(vals)
                mad = statistics.median([abs(x - center) for x in vals])
                hours[h] = (center, 1.4826 * mad)
        if hours:
            out[s] = hours
    return out


def import_csvs(path, csv_dir):
    """One-time: fold any legacy per-day activity_*.csv files into the DB. No-op
    once the DB already has rows. Returns the number of rows imported."""
    cols = _cols()
    ph = ", ".join(["?"] * (len(cols) + 1))
    q = "INSERT INTO samples (ts, %s) VALUES (%s)" % (_quoted(cols), ph)
    with _lock:
        c = _conn(path)
        if c.execute("SELECT COUNT(*) FROM samples").fetchone()[0]:
            return 0
        total = 0
        for p in sorted(glob.glob(os.path.join(csv_dir, "activity_*.csv"))):
            # a corrupt legacy CSV (e.g. one truncated by the old rewrite window)
            # must be skipped, not crash startup -- this runs before any loop's
            # try/except exists, in both entry points.
            try:
                with open(p, newline="") as f:
                    batch = [_rowvals(cols, r) for r in _csv.DictReader(f)]
                c.executemany(q, batch)
                total += len(batch)
            except (OSError, _csv.Error, ValueError) as e:
                print("[warn] skipping unreadable legacy CSV %s: %s" % (p, e))
        c.commit()
        return total
