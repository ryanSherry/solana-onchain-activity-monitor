#!/usr/bin/env python3
"""
Real-time web dashboard for the on-chain activity monitor.

Backend: stdlib http.server (no dependencies). A background thread runs the
same collection as monitor.py once per `--interval`, keeps a rolling history,
and the pump.fun launch stream updates continuously. The page polls /api/data
and redraws.

    python3 server.py                 # http://127.0.0.1:8888
    python3 server.py --port 9000 --interval 30

Endpoints:
    GET /            -> dashboard.html
    GET /api/data    -> latest snapshot + history + fresh pump rates + movers
    GET /api/surge   -> tiny backoff verdict for a lander to poll (advise_backoff,
                        throttle_factor, level, reason, action)
"""

from __future__ import annotations

import argparse
import bisect
import calendar
import json
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import monitor
import sources
import store

HERE = os.path.dirname(os.path.abspath(__file__))

# Columns charted as sparklines on the page.
HISTORY_KEYS = [
    "surge_score", "nonvote_tps", "meme_tps", "meme_fail_rate",
    "fee_p90", "fee_contention", "pump_launches_min", "skip_rate",
    "block_fill", "block_fail_rate", "block_fee_cu_p90",
]

_state = {
    "updated_at": None,
    "movers_updated_at": None,
    "interval": 30,
    "movers_interval": 15,
    "latest": {},
    "components": {},
    "meme_by_program": {},
    "jito": {},
    "sol": {},
    "movers": [],
    "rpc": [],                         # RPC pool health (failover status)
    "block": {},                       # recent-block landing conditions (fill/fail/fee)
    "surge_context": None,             # current surge vs the trailing-week distribution
    "history": deque(maxlen=480),      # fine-grained recent chart (~4h at 30s)
    "history24h": deque(maxlen=6000),  # {t, surge_score} for the 24h chart
}
_lock = threading.Lock()
_pump = None

# Last-known-good recent-block stats (block_fill / block_fail_rate / fee-per-CU).
# getBlock is heavy (~6 MB), so a dedicated slow loop refreshes this and the fast
# surge loop injects it into each row. Written by _block_loop, read by the surge
# loop and snapshot under _lock.
_block_latest = {}

# Time-of-day baselines: {signal: {utc_hour: (center, robust_sigma)}}, rebuilt
# from the SQLite history on a slow cadence. The surge loop passes the current
# hour's slice to compute() so heat is "unusual FOR THIS HOUR" (falls back to the
# rolling window for any signal/hour without enough history). Published wholesale
# under _lock; read (never mutated) by the surge loop.
_tod_baselines = {}

# Trailing-week surge_score distribution -> "how unusual is right now?" percentile.
# Touched only by the single surge loop thread (the snapshot reads the derived
# _state["surge_context"], never these), so no lock is needed here.
_BASELINE_DAYS = 7
_baseline = deque()        # (t_epoch, surge_score) over the trailing week
_baseline_sorted = []      # cached ascending scores, rebuilt ~once a minute


def _ts_to_epoch(ts):
    try:
        return calendar.timegm(time.strptime(ts, "%Y-%m-%dT%H:%M:%SZ"))
    except (ValueError, TypeError):
        return None


def _sample_from_row(row, t):
    s = {"t": t}
    for k in HISTORY_KEYS:
        s[k] = row.get(k)
    return s


def _seed_history(db):
    """Pre-populate the charts from the DB so they aren't empty on startup: the
    last 24h of rows feed both the recent (deque self-limits) and 24h buffers."""
    for r in store.read_since(db, 86400):
        t = _ts_to_epoch(r.get("timestamp_utc"))
        if t is None:
            continue
        _state["history"].append(_sample_from_row(r, t))
        _state["history24h"].append({"t": t, "surge_score": r.get("surge_score")})


def _seed_baseline(db):
    """Seed the trailing-week surge_score distribution from the DB (SQL slice)."""
    _baseline.extend(store.read_scores_since(db, _BASELINE_DAYS * 86400))
    _refresh_baseline()


def _refresh_baseline():
    """Drop entries older than a week and rebuild the cached sorted distribution."""
    global _baseline_sorted
    cutoff = time.time() - _BASELINE_DAYS * 86400
    while _baseline and _baseline[0][0] < cutoff:
        _baseline.popleft()
    _baseline_sorted = sorted(s for _, s in _baseline)


def _surge_context(cur):
    """Where the current surge sits in the trailing-week distribution: its
    percentile plus reference points (typical p50, p95, peak). None until there
    are enough samples (>=60) for the comparison to mean anything."""
    scores = _baseline_sorted
    n = len(scores)
    if cur is None or n < 60:
        return None
    q = lambda p: scores[min(n - 1, int(p * (n - 1)))]   # standard nearest-rank
    span_h = (_baseline[-1][0] - _baseline[0][0]) / 3600 if len(_baseline) > 1 else 0
    return {
        # strictly-less count, capped: the cached distribution lags the live value
        # by up to a minute, so a fresh high must not read a contradictory 100%
        "percentile": min(99, round(100 * bisect.bisect_left(scores, cur) / n)),
        "is_peak": cur >= scores[-1],          # at/above the trailing-week high
        "p50": q(0.50), "p95": q(0.95),
        "max": max(scores[-1], cur),           # never below the live value
        "hours": span_h, "n": n,
    }


def _downsample(samples, n=240):
    """Bucket {t, surge_score} samples into <=n points, keeping the PEAK per
    bucket so surges stay visible. Returns columnar {t, surge_score}."""
    pts = [s for s in samples if s.get("surge_score") is not None]
    if len(pts) <= n:
        return {"t": [s["t"] for s in pts], "surge_score": [s["surge_score"] for s in pts]}
    t0, t1 = pts[0]["t"], pts[-1]["t"]
    bw = (t1 - t0) / n or 1
    buckets = {}
    for s in pts:
        idx = min(n - 1, int((s["t"] - t0) / bw))
        if idx not in buckets or s["surge_score"] > buckets[idx]:
            buckets[idx] = s["surge_score"]
    out_t, out_v = [], []
    for idx in sorted(buckets):
        out_t.append(t0 + (idx + 0.5) * bw)
        out_v.append(buckets[idx])
    return {"t": out_t, "surge_score": out_v}


def _surge_loop(rpc, interval, db, retention_days=0):
    """Fast loop: RPC-only (no GeckoTerminal). Drives the surge gauge + charts.
    HelloMoon handles 7 calls/tick easily, so this can run every few seconds.
    Period-accurate: sleeps interval minus the time the tick actually took."""
    global _pump
    tracker = monitor.SurgeTracker()
    for r in store.read_recent(db, 2000):     # warm the rolling baselines
        tracker.update(r)
    _seed_history(db)
    _seed_baseline(db)
    try:
        import pumpstream
        _pump = pumpstream.PumpStream()
        _pump.start()
    except Exception as e:
        print(f"[warn] pump stream unavailable: {e}")

    last_h24 = 0.0
    last_jito = 0.0
    last_sol = 0.0
    last_health = 0.0
    last_ctx = 0.0
    last_prune = 0.0
    cur_skip = None
    while True:
        start = time.time()
        try:
            # with_movers=False => no GeckoTerminal calls in the fast loop
            row, _m, _c, meme_by_program = monitor.collect(
                rpc, with_movers=False, pump=_pump)
            # skip rate changes over a ~10-min window -- refresh every 15s and
            # carry the value forward on intervening ticks (last-known-good)
            if start - last_health >= 15:
                h = sources.network_health(rpc)
                if h.get("skip_rate") is not None:
                    cur_skip = h["skip_rate"]
                last_health = start
            row["skip_rate"] = cur_skip
            # inject last-known-good block stats (refreshed by _block_loop) so the
            # index sees fill / failure / fee-per-CU between the slow block samples
            with _lock:
                bl = _block_latest
            for k in ("block_fill", "block_fail_rate",
                      "block_fee_cu_p50", "block_fee_cu_p90"):
                row[k] = bl.get(k)
            # baseline for THIS UTC hour (falls back to the rolling window per
            # signal when that hour lacks enough history)
            hour = time.gmtime().tm_hour
            with _lock:
                tod = _tod_baselines
            cur = {s: hrs[hour] for s, hrs in tod.items() if hour in hrs}
            score, level, comps = tracker.compute(row, baselines=cur)
            row["surge_score"], row["surge_level"] = score, level

            store.append(db, row)
            tracker.update(row)

            now = time.time()
            # trailing-week distribution for the "how unusual is now?" percentile
            _baseline.append((now, score))
            if now - last_ctx >= 60:
                _refresh_baseline()
                last_ctx = now
            with _lock:
                _state["latest"] = row
                _state["rpc"] = rpc.status()
                _state["surge_context"] = _surge_context(score)
                _state["components"] = comps
                if meme_by_program:
                    _state["meme_by_program"] = meme_by_program
                _state["updated_at"] = now
                _state["history"].append(_sample_from_row(row, now))
                # 24h buffer only needs ~30s resolution (chart downsamples to 6-min buckets)
                if now - last_h24 >= 30:
                    _state["history24h"].append({"t": now, "surge_score": row.get("surge_score")})
                    last_h24 = now
            # Jito tip floor changes slowly -- refresh every ~10s (last-known-good)
            if now - last_jito >= 10:
                jito = sources.jito_tip_floor()
                if jito:
                    with _lock:
                        _state["jito"] = jito
                last_jito = now
            # SOL price -- slow-moving macro context, refresh every ~45s
            if now - last_sol >= 45:
                sp = sources.sol_price()
                if sp:
                    with _lock:
                        _state["sol"] = sp
                last_sol = now
            # retention: prune old rows every ~6h (bounds DB growth). Fires on the
            # first tick (last_prune=0), so it also cleans up at startup.
            if retention_days and now - last_prune >= 21600:
                last_prune = now      # set before: a prune failure still waits ~6h
                n = store.prune(db, retention_days)
                if n:
                    print(f"[retention] pruned {n} rows older than {retention_days}d")
        except Exception as e:
            print(f"[warn] surge tick failed: {e}")
        time.sleep(max(0.5, interval - (time.time() - start)))


def _movers_loop(interval):
    """Slow loop: GeckoTerminal trending movers only. Kept well under the
    ~30 req/min free limit. Last-known-good is preserved on a transient miss."""
    while True:
        start = time.time()
        try:
            movers = sources.trending_movers(20)  # wide pool; client filters to surgers
            if movers:
                with _lock:
                    _state["movers"] = movers
                    _state["movers_updated_at"] = time.time()
        except Exception as e:
            print(f"[warn] movers tick failed: {e}")
        time.sleep(max(1.0, interval - (time.time() - start)))


def _block_loop(rpc, interval, samples):
    """Slow loop: aggregate the last `samples` blocks per `interval` for direct
    landing conditions (fill / non-vote failure rate / landed fee-per-CU). getBlock
    is heavy (~6 MB each, no gzip), so this runs on its own thread off the surge
    tick; the surge loop injects the last-known-good into each row. Preserves the
    last value on a miss."""
    global _block_latest
    misses = 0
    while True:
        start = time.time()
        try:
            b = sources.block_stats(rpc, blocks=samples)
        except Exception as e:
            print(f"[warn] block tick failed: {e}")
            b = None
        if b:
            misses = 0
            with _lock:
                # publish a fresh dict wholesale (never mutate a published one) so
                # the snapshot can share the ref without copying it under the lock
                _block_latest = b
                _state["block"] = b
        else:
            misses += 1
            # stop voting frozen stats into the index once they're clearly stale:
            # an absent value is excluded from the weighted average, not held.
            if misses == 3:
                with _lock:
                    _block_latest = {}
                    _state["block"] = {}
        time.sleep(max(2.0, interval - (time.time() - start)))


def _tod_loop(db, days, min_samples, min_days, interval=1800):
    """Slow loop: rebuild the per-signal, per-hour baselines from history (they
    drift only over days). Computes once immediately, then every `interval`."""
    global _tod_baselines
    signals = [k for k, _, _ in monitor.SURGE_SIGNALS]
    while True:
        start = time.time()
        try:
            tod = store.hourly_baselines(db, signals, days=days,
                                         min_samples=min_samples, min_days=min_days)
            with _lock:
                _tod_baselines = tod
        except Exception as e:
            print(f"[warn] time-of-day baseline refresh failed: {e}")
        time.sleep(max(60.0, interval - (time.time() - start)))


_RPC_MIN_SAMPLES = 20   # recent calls before the RPC 429/latency axes are trusted


def _backoff_advice(latest, rpc):
    """Synthesize surge + our own RPC health + skip rate into a single actionable
    verdict -- machine-readable (advise_backoff / throttle_factor, C3) AND human
    (action, C6). throttle_factor is the suggested fraction of NON-critical sends
    to hold. Uses max-of-signals: if ANY axis says danger, back off. The reason is
    whichever axis drives it, so the operator (and the lander) know why."""
    def clamp(x):
        return max(0.0, min(1.0, x))

    score = latest.get("surge_score")
    level = latest.get("surge_level") or "CALM"
    skip = latest.get("skip_rate")
    active = next((e for e in rpc if e.get("active")), None) or {}
    r429 = active.get("rate_limited") or 0.0
    p99 = active.get("lat_p99_ms")
    rpc_samples = active.get("samples") or 0

    subs = []
    if score is not None:
        subs.append((clamp(score / 80.0), f"surge {level} ({int(score)})"))
    # the RPC axes need enough recent calls to be trustworthy -- a thin window
    # right after a failover must not escalate a lander to backoff on one blip
    if rpc_samples >= _RPC_MIN_SAMPLES:
        if r429:
            subs.append((clamp(r429 / 0.20), f"primary RPC rate-limited {round(r429 * 100)}%"))
        if p99 is not None:
            subs.append((clamp((p99 - 500) / 2000.0), f"primary RPC p99 {p99} ms"))
    if skip is not None:
        subs.append((clamp((skip - 0.05) / 0.15), f"slot skip {round(skip * 100)}%"))
    factor, reason = max(subs, key=lambda s: s[0]) if subs else (0.0, "no data yet")
    factor = round(factor, 2)

    if factor >= 0.8:
        lvl, action = "critical", "Back off hard — essential sends only, top-tier tips."
    elif factor >= 0.5:
        lvl, action = "backoff", "Hold non-critical sends; raise tips to ~p95."
    elif factor >= 0.2:
        lvl, action = "caution", "Busier than usual — prioritize, consider higher tips."
    else:
        lvl, action = "normal", "Landing normally — send freely."
    return {
        "advise_backoff": factor >= 0.5,
        "throttle_factor": factor,
        "level": lvl,
        "reason": reason,
        "action": action,
        "surge_score": score,
        "surge_level": level,
    }


def _advice():
    """Compact backoff verdict for the lightweight /api/surge endpoint (reads only
    what _backoff_advice needs -- no pump/history work)."""
    with _lock:
        latest = dict(_state["latest"])
        rpc = list(_state["rpc"])
    return _backoff_advice(latest, rpc)


def _snapshot():
    with _lock:
        latest = dict(_state["latest"])
        hist = list(_state["history"])
        hist24 = list(_state["history24h"])
        out = {
            "updated_at": _state["updated_at"],
            "movers_updated_at": _state["movers_updated_at"],
            "server_time": time.time(),
            "interval": _state["interval"],
            "movers_interval": _state["movers_interval"],
            "latest": latest,
            "components": _state["components"],
            "meme_by_program": _state["meme_by_program"],
            "jito": _state["jito"],
            "sol": _state["sol"],
            "movers": _state["movers"],
            "rpc": _state["rpc"],
            "block": _state["block"],
            "surge_context": _state["surge_context"],
        }
    # fresh pump rates (continuous between collection ticks)
    if _pump is not None:
        r = _pump.rates()
        out["pump"] = r
        if r.get("pump_connected"):
            out["latest"]["pump_launches_min"] = r["pump_launches_min"]
            out["latest"]["pump_graduations_min"] = r["pump_graduations_min"]
    # columnar history for charts
    cols = {k: [s.get(k) for s in hist] for k in HISTORY_KEYS}
    cols["t"] = [s["t"] for s in hist]
    out["history"] = cols
    out["history24h"] = _downsample(hist24)
    out["advise"] = _backoff_advice(out["latest"], out.get("rpc") or [])
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # quiet

    def _send(self, code, body, ctype, cache=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        if cache:
            self.send_header("Cache-Control", cache)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/index"):
            try:
                with open(os.path.join(HERE, "dashboard.html"), "rb") as f:
                    self._send(200, f.read(), "text/html; charset=utf-8")
            except FileNotFoundError:
                self._send(500, b"dashboard.html missing", "text/plain")
        elif self.path.startswith("/api/data"):
            body = json.dumps(_snapshot()).encode()
            self._send(200, body, "application/json", cache="no-store")
        elif self.path.startswith("/api/surge"):
            # tiny machine-readable verdict for a lander to poll and auto-throttle
            body = json.dumps(_advice()).encode()
            self._send(200, body, "application/json", cache="no-store")
        else:
            self._send(404, b"not found", "text/plain")


def main():
    ap = argparse.ArgumentParser(description="On-chain activity dashboard server")
    ap.add_argument("--rpc", default=None,
                    help="override RPC endpoints (comma/space list, highest "
                         "priority first); default = SOLANA_RPC + "
                         "SOLANA_RPC_FALLBACKS from the env file")
    ap.add_argument("--interval", type=int, default=5,
                    help="surge gauge refresh seconds (RPC, HelloMoon)")
    ap.add_argument("--movers-interval", type=int, default=15,
                    help="movers refresh seconds (GeckoTerminal, ~30/min limit)")
    ap.add_argument("--block-interval", type=int, default=30,
                    help="recent-block sampling seconds (getBlock is ~6 MB each, "
                         "so this is a slow loop; 0 disables block-level signals)")
    ap.add_argument("--block-samples", type=int, default=3,
                    help="blocks aggregated per sample (more = less noise, more "
                         "bandwidth: ~6 MB x this, per --block-interval)")
    ap.add_argument("--tod-days", type=int, default=7,
                    help="days of history for the time-of-day baseline "
                         "('unusual for this hour'); 0 = rolling window only")
    ap.add_argument("--tod-min-samples", type=int, default=60,
                    help="min samples in an hour bucket before it overrides the "
                         "rolling window for that signal/hour")
    ap.add_argument("--tod-min-days", type=int, default=2,
                    help="min distinct days an hour bucket must span (guards a "
                         "live surge from polluting its own thin-history baseline)")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8888)
    ap.add_argument("--db", default=os.path.join(HERE, "data", "monitor.db"),
                    help="SQLite history DB (was per-day CSVs)")
    ap.add_argument("--retention-days", type=int, default=90,
                    help="prune samples older than this (bounds DB growth); "
                         "0 = keep forever. Keep well above --tod-days / 7d.")
    args = ap.parse_args()

    rpc, endpoints = sources.build_pool(args.rpc)
    _state["rpc"] = rpc.status()

    # one-time: fold any legacy per-day CSVs into the DB (no-op once populated)
    imported = store.import_csvs(args.db, os.path.dirname(args.db))
    if imported:
        print(f"imported {imported} rows from legacy CSVs into {args.db}")

    _state["interval"] = args.interval
    _state["movers_interval"] = args.movers_interval
    threading.Thread(target=_surge_loop,
                     args=(rpc, args.interval, args.db, args.retention_days),
                     daemon=True).start()
    threading.Thread(target=_movers_loop,
                     args=(args.movers_interval,), daemon=True).start()
    if args.block_interval > 0:
        # own RpcPool so a heavy/slow getBlock failing over can't cool the surge
        # loop's primary and flap the fast tick to a fallback
        block_rpc = sources.build_pool(args.rpc)[0]
        threading.Thread(target=_block_loop,
                         args=(block_rpc, args.block_interval, args.block_samples),
                         daemon=True).start()
    if args.tod_days > 0:
        threading.Thread(target=_tod_loop,
                         args=(args.db, args.tod_days, args.tod_min_samples,
                               args.tod_min_days),
                         daemon=True).start()

    if 0 < args.retention_days < max(args.tod_days, 7):
        print(f"WARNING: --retention-days ({args.retention_days}) is below the "
              f"baseline lookback ({max(args.tod_days, 7)}d) -- the time-of-day / "
              "percentile baselines will be starved of history.")
    if endpoints == [sources.PUBLIC_RPC]:
        print("WARNING: no SOLANA_RPC configured -- using the public endpoint, "
              "which is heavily rate-limited and will make the dashboard flaky. "
              "Set SOLANA_RPC in .env.onchain-activity (see .env.onchain-activity.example).")

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"dashboard: http://{args.host}:{args.port}")
    print(f"RPC pool ({len(endpoints)}): "
          f"{', '.join(sources._node_label(e) for e in endpoints)}")
    blk = (f"block every {args.block_interval}s "
           f"({args.block_samples}x~6 MB/sample)"
           if args.block_interval > 0 else "block sampling off")
    print(f"surge every {args.interval}s · movers every {args.movers_interval}s · "
          f"{blk} — open the URL in a browser")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")
        if _pump:
            _pump.stop()


if __name__ == "__main__":
    main()
