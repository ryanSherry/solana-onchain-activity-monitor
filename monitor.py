#!/usr/bin/env python3
"""
On-chain activity surge monitor (Solana).

Measures, once a minute, the signals that predict when a transaction lander
gets crushed by meme-coin frenzy:

  Network congestion (the thing that actually exhausts your rate limits)
    - non-vote TPS                     getRecentPerformanceSamples
    - priority-fee p50/p90/p99         getRecentPrioritizationFees
    - "hot account" landing fee p90    fee to land while touching pump.fun/Raydium

  Meme driver (why it's surging)
    - new-pool launch rate (last 5m)   GeckoTerminal new_pools

  Biggest movers (what's driving it)
    - top trending pools, 1h vol + 5m txns   GeckoTerminal trending_pools

A composite 0-100 surge score is derived from how far non-vote TPS and the
hot-account landing fee deviate above their rolling baselines (trailing median
of recent samples; seeded with sane defaults until history accumulates).

Usage:
    python3 monitor.py                 # loop, print + append CSV every 60s
    python3 monitor.py --once          # single snapshot, then exit
    python3 monitor.py --interval 30   # poll every 30s
    python3 monitor.py --rpc https://<hellomoon-node>/   # use a custom RPC
    python3 monitor.py --no-movers     # skip GeckoTerminal (RPC signals only)
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import statistics
import sys
import time
from collections import deque

import sources

CSV_FIELDS = [
    "timestamp_utc", "nonvote_tps", "total_tps", "skip_rate",
    "fee_p50", "fee_p90", "fee_p99", "fee_hot_p90", "fee_contention",
    "meme_tps", "meme_fail_rate",
    "block_fill", "block_fail_rate", "block_fee_cu_p50", "block_fee_cu_p90",
    "pump_launches_min", "pump_graduations_min",
    "new_pools_5m", "surge_score", "surge_level",
    "top_mover", "top_mover_vol_h1",
]


# --------------------------------------------------------------------------- #
# Composite Surge Index
# --------------------------------------------------------------------------- #
# One tracked 0-100 number combining every signal. Each signal's "heat" (0-100)
# is how many ROBUST sigmas above its own rolling baseline it sits (median center,
# MAD spread -- see _heat / center_scale), so the index self-calibrates per signal
# and is variance-aware rather than keyed to a fixed multiple of the median. The
# heats are then weighted-averaged. Weights lean toward what predicts lander
# rate-limit pain: meme-venue submission load and the failure rate (bots racing
# and losing) dominate, network load and landing-fee pressure next, launch rate as
# a leading hint. Weights/thresholds are a reasoned prior, not yet calibrated to
# real rate-limit events -- tune SURGE_SIGNALS / _LEVELS once a surge is observed.
# The seed baseline is only the prior until each signal's rolling window fills.
MIN_HISTORY = 8            # samples before a signal's rolling baseline kicks in
BASELINE_WINDOW = 120      # samples kept for the rolling median (~10 min at 5s)

# Self-calibrating normalization: each signal's heat is how many ROBUST sigmas it
# sits above its own rolling baseline (median center, MAD spread), not a fixed
# multiple of the median. This is variance-aware -- a normally-steady signal lights
# up on a small move, a normally-volatile one needs a bigger move -- and robust to
# the heavy tails of on-chain data (median/MAD resist spikes; mean/std wouldn't).
_Z_FULL = 3.0              # robust-sigmas above baseline that map to heat 100
_SCALE_FLOOR_FRAC = 0.05   # floor the sigma at 5% of |baseline| so a near-constant
                           # signal isn't hair-triggered (without erasing the
                           # variance-awareness for genuinely steady signals)
_SEED_SCALE_FRAC = 0.5     # wide prior spread until the rolling window fills
_EPS = 1e-9

# (row key, weight, seed baseline used until history accumulates)
SURGE_SIGNALS = [
    ("meme_tps",           25, 1200.0),      # meme-venue submission load
    ("meme_fail_rate",     20, 0.45),        # bots racing and losing = the flood
    ("nonvote_tps",        20, 1400.0),      # overall network load
    ("block_fill",         15, 0.45),        # block compute utilization (direct congestion)
    ("fee_contention",     15, 0.10),        # how often there's competition to land
    ("block_fail_rate",    12, 0.20),        # network-wide non-vote tx failure rate
    ("fee_p90",            12, 3_000_000.0), # how expensive landing has become
    ("pump_launches_min",   8, 20.0),        # leading edge of a meme frenzy
]

# Pretty labels for the dashboard "drivers" breakdown.
SIGNAL_LABELS = {
    "meme_tps": "DEX trade rate",
    "meme_fail_rate": "DEX failure rate",
    "nonvote_tps": "Network TPS",
    "block_fill": "Block fill",
    "fee_contention": "Fee contention",
    "block_fail_rate": "Network fail rate",
    "fee_p90": "Landing fee",
    "pump_launches_min": "Launch rate",
}

_LEVELS = [(60, "SURGE"), (38, "ELEVATED"), (18, "BUSY"), (0, "CALM")]


def _heat(value, center, scale) -> float:
    """Heat 0-100 = how many robust sigmas ABOVE its baseline the signal sits
    (0 sigma -> 0, _Z_FULL sigma -> 100). Only upward deviations count."""
    if value is None or not scale or scale <= 0:
        return 0.0
    z = (value - center) / scale
    return max(0.0, min(100.0, z / _Z_FULL * 100.0))


def _floor_scale(sigma, center, seed):
    """Floor a robust sigma so a signal idling near zero keeps a sane minimum
    spread -- keyed off the current center AND the always-positive seed."""
    return max(sigma, _SCALE_FLOOR_FRAC * abs(center),
               _SCALE_FLOOR_FRAC * abs(seed), _EPS)


def level_for(index: int) -> str:
    return next(name for thr, name in _LEVELS if index >= thr)


class SurgeTracker:
    """Maintains per-signal rolling baselines and computes the composite index."""

    def __init__(self):
        self.hist = {k: deque(maxlen=BASELINE_WINDOW) for k, _, _ in SURGE_SIGNALS}

    def center_scale(self, key, seed):
        """Rolling (center, scale) for a signal: median + robust sigma
        (1.4826*MAD), floored. The floor keys off BOTH the current center and the
        always-positive seed, so a signal that idles near zero (fee_contention,
        the fail rates) still keeps a sane minimum spread and can't be
        hair-triggered by a tiny move. That floor plus update()'s consecutive
        dedup (which forbids an all-identical, MAD=0 window) together bound
        sensitivity -- keep both. Until the window fills, a wide prior around the
        seed so a fresh install isn't hair-triggered."""
        d = self.hist[key]
        if len(d) < MIN_HISTORY:
            return seed, _floor_scale(_SEED_SCALE_FRAC * abs(seed), seed, seed)
        center = statistics.median(d)
        mad = statistics.median([abs(x - center) for x in d])
        return center, _floor_scale(1.4826 * mad, center, seed)

    def warming(self) -> bool:
        return len(self.hist["nonvote_tps"]) < MIN_HISTORY

    def update(self, row):
        for k, _, _ in SURGE_SIGNALS:
            v = row.get(k)
            if v is None or v == "":
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            d = self.hist[k]
            # Skip an unchanged repeat. Slowly-sampled signals (block stats) are
            # carried forward into every fast tick; counting those duplicates
            # would cross MIN_HISTORY after 1-2 real samples and latch the
            # rolling baseline onto the current value, collapsing its heat.
            if d and d[-1] == fv:
                continue
            d.append(fv)

    def compute(self, row, baselines=None):
        """Return (index 0-100, level, components). Call BEFORE update(row).
        `baselines` optionally overrides a signal's (center, robust_sigma) with a
        time-of-day baseline (unusual FOR THIS HOUR); when absent for a key, the
        rolling window is used. The seed floor is applied to either source here."""
        acc = wsum = 0.0
        comps = {}
        for key, weight, seed in SURGE_SIGNALS:
            val = row.get(key)
            present = val is not None and val != ""
            if baselines and key in baselines:
                # time-of-day override. The seed floor still applies (guards a
                # signal that idles near zero at this hour); it can blunt an
                # unusually-tight hour, but that's preferred over false alarms.
                center, sigma = baselines[key]
                center, scale, source = center, _floor_scale(sigma, center, seed), "hour"
            else:
                center, scale = self.center_scale(key, seed)
                source = "window"
            heat = _heat(val, center, scale)
            comps[key] = {
                "label": SIGNAL_LABELS.get(key, key),
                "heat": round(heat),
                "weight": weight,
                "contribution": round(weight * heat / 100.0, 1),
                "value": val,
                "baseline": round(center, 3),
                "scale": round(scale, 3),
                "source": source,
                "present": present,
            }
            # a missing signal (e.g. block stats before the first sample, or a
            # failed fetch) must NOT vote -- otherwise it dilutes the index to 0.
            if present:
                acc += weight * heat
                wsum += weight
        index = int(round(acc / wsum)) if wsum else 0
        return index, level_for(index), comps


# --------------------------------------------------------------------------- #
# Collection
# --------------------------------------------------------------------------- #
def collect(rpc_url: str, with_movers: bool, pump=None) -> dict:
    row = {f: None for f in CSV_FIELDS}
    row["timestamp_utc"] = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    tps = sources.network_tps(rpc_url)
    row["nonvote_tps"] = tps.get("nonvote_tps")
    row["total_tps"] = tps.get("total_tps")

    fees = sources.priority_fees(rpc_url)
    row.update({k: fees[k] for k in
                ("fee_p50", "fee_p90", "fee_p99", "fee_hot_p90", "fee_contention")})

    meme = sources.meme_trade_rate(rpc_url)
    row["meme_tps"] = meme["meme_tps"]
    row["meme_fail_rate"] = meme["meme_fail_rate"]
    meme_by_program = meme["meme_by_program"]

    pump_connected = False
    if pump is not None:
        r = pump.rates()
        pump_connected = r.get("pump_connected", False)
        if pump_connected:
            row["pump_launches_min"] = r["pump_launches_min"]
            row["pump_graduations_min"] = r["pump_graduations_min"]

    movers = []
    if with_movers:
        row["new_pools_5m"] = sources.new_pool_rate(5)
        movers = sources.trending_movers(5)
        if movers:
            row["top_mover"] = movers[0]["symbol"]
            row["top_mover_vol_h1"] = movers[0]["vol_h1"]
    return row, movers, pump_connected, meme_by_program


# --------------------------------------------------------------------------- #
# Output
# --------------------------------------------------------------------------- #
def _fmt(n, kind=""):
    if n is None:
        return "  n/a"
    if kind == "usd":
        for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
            if abs(n) >= div:
                return f"${n/div:.1f}{suf}"
        return f"${n:.0f}"
    if kind == "int":
        return f"{int(n):,}"
    return f"{n:,.0f}"


_LEVEL_MARK = {"CALM": "·", "BUSY": "•", "ELEVATED": "▲", "SURGE": "■"}


def render(row, movers, warming, pump_connected=False, meme_by_program=None, comps=None):
    L = []
    L.append(f"\n[{row['timestamp_utc']}]  on-chain activity")
    L.append("─" * 56)
    L.append("Network")
    L.append(f"  non-vote TPS    {_fmt(row['nonvote_tps']):>10}     total {_fmt(row['total_tps'])}")
    L.append("Meme trade rate (tx/s · % failed, recent slots)")
    if meme_by_program:
        for name, d in meme_by_program.items():
            fr = f"{d['fail_rate']*100:.0f}% fail" if d.get('fail_rate') is not None else ""
            L.append(f"  {name:<9}      {_fmt(d.get('tps'),'int'):>8} tx/s   {fr}")
    fr_t = (f"{row['meme_fail_rate']*100:.0f}% fail"
            if row.get("meme_fail_rate") is not None else "")
    L.append(f"  total (approx)  {_fmt(row.get('meme_tps'),'int'):>8} tx/s   {fr_t}")
    L.append("Meme landing pressure (µlamports/CU)")
    L.append(f"  landing fee     p50 {_fmt(row['fee_p50'],'int'):>9}  "
             f"p90 {_fmt(row['fee_p90'],'int'):>9}  p99 {_fmt(row['fee_p99'],'int')}")
    cont = row.get("fee_contention")
    cont_s = f"{cont*100:.0f}% of slots" if cont is not None else "n/a"
    L.append(f"  fee contention  {cont_s:>10}")
    L.append("Meme launch activity (pump.fun, live)")
    if pump_connected:
        L.append(f"  launches/min    {_fmt(row['pump_launches_min'],'int'):>10}"
                 f"     graduations/min {_fmt(row['pump_graduations_min'],'int')}")
    else:
        L.append(f"  launches/min    {'connecting…':>10}")
    if row.get("new_pools_5m") is not None:
        L.append(f"  new pools/5m    {_fmt(row['new_pools_5m'],'int'):>10}  (GeckoTerminal)")
    if movers:
        L.append("Top movers (1h vol · 5m txns · 1h %)")
        for i, m in enumerate(movers, 1):
            chg = f"{m['chg_h1']:+.0f}%" if m['chg_h1'] is not None else "   ?"
            L.append(f"  {i}. {m['symbol'][:14]:<14} {_fmt(m['vol_h1'],'usd'):>8}  "
                     f"{_fmt(m['txns_5m'],'int'):>7} tx  {chg:>6}")
    mark = _LEVEL_MARK.get(row["surge_level"], "?")
    warm = "  (baseline warming up)" if warming else ""
    L.append("─" * 56)
    L.append(f"  SURGE INDEX  {row['surge_score']:>3}/100  {mark} {row['surge_level']}{warm}")
    if comps:
        top = sorted(comps.values(), key=lambda c: c["contribution"], reverse=True)[:2]
        drivers = ", ".join(c["label"] for c in top if c["contribution"] > 0)
        if drivers:
            L.append(f"  driven by    {drivers}")
    print("\n".join(L), flush=True)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description="Solana on-chain activity surge monitor")
    ap.add_argument("--rpc", default=None,
                    help="override RPC endpoints (comma/space list, highest "
                         "priority first); default = SOLANA_RPC + "
                         "SOLANA_RPC_FALLBACKS (with failover)")
    ap.add_argument("--interval", type=int, default=60, help="seconds between polls")
    ap.add_argument("--once", action="store_true", help="single snapshot then exit")
    ap.add_argument("--no-movers", action="store_true", help="skip GeckoTerminal calls")
    ap.add_argument("--no-pump", action="store_true",
                    help="skip the pump.fun launch-rate websocket")
    ap.add_argument("--db", default=os.path.join(os.path.dirname(__file__), "data", "monitor.db"),
                    help="SQLite history DB (was per-day CSVs)")
    args = ap.parse_args()

    import store
    rpc, endpoints = sources.build_pool(args.rpc)

    store.import_csvs(args.db, os.path.dirname(args.db))   # fold legacy CSVs once
    tracker = SurgeTracker()
    for r in store.read_recent(args.db, 2000):
        tracker.update(r)
    print(f"RPC pool ({len(endpoints)}): "
          f"{', '.join(sources._node_label(e) for e in endpoints)}", file=sys.stderr)
    print(f"poll every {args.interval}s · DB -> {args.db}", file=sys.stderr)

    pump = None
    if not args.no_pump and not args.once:
        try:
            import pumpstream
            pump = pumpstream.PumpStream()
            pump.start()
            print("pump.fun launch stream: starting…", file=sys.stderr)
        except Exception as e:
            print(f"[warn] pump stream unavailable: {e}", file=sys.stderr)

    while True:
        try:
            row, movers, pump_connected, meme_by_program = collect(
                rpc, with_movers=not args.no_movers, pump=pump)
            warming = tracker.warming()
            score, level, comps = tracker.compute(row)
            row["surge_score"], row["surge_level"] = score, level

            render(row, movers, warming, pump_connected, meme_by_program, comps)

            store.append(args.db, row)
            tracker.update(row)
        except Exception as e:  # keep the loop alive across transient API errors
            print(f"[warn] poll failed: {e}", file=sys.stderr, flush=True)

        if args.once:
            break
        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nstopped.", file=sys.stderr)
            break

    if pump:
        pump.stop()


if __name__ == "__main__":
    main()
