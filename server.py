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
"""

from __future__ import annotations

import argparse
import bisect
import calendar
import csv
import glob
import json
import os
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import monitor
import sources

HERE = os.path.dirname(os.path.abspath(__file__))

# Columns charted as sparklines on the page.
HISTORY_KEYS = [
    "surge_score", "nonvote_tps", "meme_tps", "meme_fail_rate",
    "fee_p90", "fee_contention", "pump_launches_min", "skip_rate",
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
    "surge_context": None,             # current surge vs the trailing-week distribution
    "history": deque(maxlen=480),      # fine-grained recent chart (~4h at 30s)
    "history24h": deque(maxlen=6000),  # {t, surge_score} for the 24h chart
}
_lock = threading.Lock()
_pump = None

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


def _seed_history(csv_dir):
    """Pre-populate the charts from CSV so they aren't empty on startup.
    The recent chart uses today's rows; the 24h chart uses today + yesterday,
    filtered to the last 24h."""
    today = time.strftime("%Y%m%d", time.gmtime())
    yest = time.strftime("%Y%m%d", time.gmtime(time.time() - 86400))
    cutoff = time.time() - 86400

    for r in monitor.read_history_rows(csv_dir, day=today):
        t = _ts_to_epoch(r.get("timestamp_utc"))
        if t is not None:
            _state["history"].append(_sample_from_row(r, t))

    seen = []
    for day in (yest, today):
        for r in monitor.read_history_rows(csv_dir, day=day):
            t = _ts_to_epoch(r.get("timestamp_utc"))
            if t is not None and t >= cutoff:
                seen.append({"t": t, "surge_score": r.get("surge_score")})
    seen.sort(key=lambda s: s["t"])
    _state["history24h"].extend(seen)


def _seed_baseline(csv_dir):
    """Seed the trailing-week surge_score distribution from the daily CSVs."""
    cutoff = time.time() - _BASELINE_DAYS * 86400
    pts = []
    for d in range(_BASELINE_DAYS):
        day = time.strftime("%Y%m%d", time.gmtime(time.time() - d * 86400))
        for r in monitor.read_history_rows(csv_dir, day=day):
            t = _ts_to_epoch(r.get("timestamp_utc"))
            s = r.get("surge_score")
            if t is not None and s is not None and t >= cutoff:
                pts.append((t, s))
    pts.sort()
    _baseline.extend(pts)
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


def _surge_loop(rpc, interval, csv_dir):
    """Fast loop: RPC-only (no GeckoTerminal). Drives the surge gauge + charts.
    HelloMoon handles 7 calls/tick easily, so this can run every few seconds.
    Period-accurate: sleeps interval minus the time the tick actually took."""
    global _pump
    os.makedirs(csv_dir, exist_ok=True)
    tracker = monitor.SurgeTracker()
    for r in monitor.read_history_rows(csv_dir):
        tracker.update(r)
    _seed_history(csv_dir)
    _seed_baseline(csv_dir)
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
            score, level, comps = tracker.compute(row)
            row["surge_score"], row["surge_level"] = score, level

            day = time.strftime("%Y%m%d", time.gmtime())
            monitor.append_csv(os.path.join(csv_dir, f"activity_{day}.csv"), row)
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
    return out


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass  # quiet

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
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
            self._send(200, body, "application/json")
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
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8888)
    ap.add_argument("--csv-dir", default=os.path.join(HERE, "data"))
    args = ap.parse_args()

    rpc, endpoints = sources.build_pool(args.rpc)
    _state["rpc"] = rpc.status()

    _state["interval"] = args.interval
    _state["movers_interval"] = args.movers_interval
    threading.Thread(target=_surge_loop,
                     args=(rpc, args.interval, args.csv_dir), daemon=True).start()
    threading.Thread(target=_movers_loop,
                     args=(args.movers_interval,), daemon=True).start()

    if endpoints == [sources.PUBLIC_RPC]:
        print("WARNING: no SOLANA_RPC configured -- using the public endpoint, "
              "which is heavily rate-limited and will make the dashboard flaky. "
              "Set SOLANA_RPC in .env.onchain-activity (see .env.onchain-activity.example).")

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"dashboard: http://{args.host}:{args.port}")
    print(f"RPC pool ({len(endpoints)}): "
          f"{', '.join(sources._node_label(e) for e in endpoints)}")
    print(f"surge every {args.interval}s · movers every {args.movers_interval}s "
          f"— open the URL in a browser")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")
        if _pump:
            _pump.stop()


if __name__ == "__main__":
    main()
