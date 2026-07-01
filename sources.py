"""
Data sources for the on-chain activity monitor.

All sources are FREE and need no API key:
  - Solana public RPC (api.mainnet-beta.solana.com): network congestion signals.
  - GeckoTerminal public API (api.geckoterminal.com): biggest movers + new-pool rate.

Stdlib only (urllib) so the monitor runs with zero `pip install`.

NOTE: we intentionally do NOT use the Orb Helius tenant here. A monitor that
polls every minute forever is sustained traffic that would burn that
irreplaceable tenant / trip the Surfshark account lockout. These two RPC
methods are unauthenticated on the public endpoint, so there is no
home-IP-exposure concern and no proxy needed.
"""

from __future__ import annotations

import json
import math
import os
import re
import threading
import time
import urllib.request
import urllib.error
from collections import deque
from concurrent.futures import ThreadPoolExecutor


def _load_local_env(path=None):
    """Load KEY=value lines from a project-local env file into os.environ,
    WITHOUT overriding already-set vars. The filename is namespaced
    (.env.onchain-activity) so it never clashes with an unrelated .env on the
    same machine, and it lives in this project's own folder regardless."""
    path = path or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                ".env.onchain-activity")
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except FileNotFoundError:
        pass


_load_local_env()

PUBLIC_RPC = "https://api.mainnet-beta.solana.com"

# Solana RPC endpoint. Set SOLANA_RPC (env var, or the gitignored
# .env.onchain-activity file) to your OWN node -- e.g. a HelloMoon / Helius /
# Triton URL -- for full speed. Such URLs embed an access token, so they are
# secrets and are intentionally NOT committed here. With nothing set, this
# falls back to the public endpoint, which is heavily rate-limited and will make
# the dashboard flaky under this tool's load (16 RPC calls / 5s).
RPC_CONFIGURED = bool(os.environ.get("SOLANA_RPC"))


def _split_urls(s: str) -> list:
    """Comma- or whitespace-separated URL list -> ordered, de-duplicated list."""
    if not s:
        return []
    parts = [p.strip() for p in re.split(r"[,\s]+", s) if p.strip()]
    return list(dict.fromkeys(parts))  # dedup, preserve order


# Ordered RPC endpoints, highest priority first. SOLANA_RPC is the primary (it
# may itself be a comma/space list); SOLANA_RPC_FALLBACKS are the fallbacks in
# the order to try them. With nothing set we fall back to the public endpoint.
# All three URLs embed access tokens -> secrets, kept in .env.onchain-activity.
RPC_ENDPOINTS = (
    _split_urls(os.environ.get("SOLANA_RPC", ""))
    + _split_urls(os.environ.get("SOLANA_RPC_FALLBACKS", ""))
)
RPC_ENDPOINTS = list(dict.fromkeys(RPC_ENDPOINTS)) or [PUBLIC_RPC]
DEFAULT_RPC = RPC_ENDPOINTS[0]  # primary (back-compat alias)

# Human-friendly region label per node, keyed by the HelloMoon node codename in
# the URL (survives token rotation). Cosmetic only -- makes a failover legible
# ("failover -> Amsterdam" beats an opaque codename). Update if nodes change; an
# unmapped node just falls back to showing its codename.
RPC_REGIONS = {
    "supernatural-atlas": "FRA",
    "terrestrial-nebula": "AMS",
    "aerial-aurora":      "NY",
}


def _region(url: str):
    for codename, region in RPC_REGIONS.items():
        if codename in url:
            return region
    return None
GECKO_BASE = "https://api.geckoterminal.com/api/v2"

_UA = "onchain-activity-monitor/0.1"

# The biggest Solana swap venues by TRANSACTION COUNT. Used for BOTH the
# trade/failure rate (getSignaturesForAddress) and the landing-fee pressure
# (getRecentPrioritizationFees). We rank by tx count, NOT USD volume, because
# each tx is a landing attempt -- the thing that stresses a lander. That's why
# high-$/low-tx MM venues (SolFi, Phoenix, OpenBook: all <5 tx/s despite big
# volume) are intentionally excluded -- they'd add double-counting noise without
# moving the landing-load signal. Validated 2026-06-30: all return data;
# Meteora DLMM/DAMM-v2, Orca, Raydium CLMM were previously uncounted.
HOT_VENUES = {
    "pump.fun":        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    "pumpswap":        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA",
    "raydium v4":      "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    "raydium cpmm":    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
    "raydium clmm":    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
    "meteora dlmm":    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
    "meteora pools":   "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB",
    "meteora damm v2": "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG",
    "orca":            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
}


# --------------------------------------------------------------------------- #
# Low-level HTTP
# --------------------------------------------------------------------------- #
def _http(req: urllib.request.Request, timeout: int = 20):
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode())


def _mask_url(url: str) -> str:
    """Host + last 4 chars of the token, for logs/UI -- never the full secret."""
    try:
        host = url.split("://", 1)[1].split("/", 1)[0]
        tail = url.rstrip("/")[-4:]
        return f"{host}/…{tail}" if "/" in url.split("://", 1)[1] else host
    except Exception:
        return "rpc"


def _node_label(url: str) -> str:
    """Region name if known (e.g. 'Amsterdam'), else the masked host -- for
    logs and the startup banner, where a region is more legible than a codename."""
    return _region(url) or _mask_url(url)


class RpcAppError(RuntimeError):
    """A JSON-RPC *application* error (the node answered with an `error` payload).
    Distinct from a transport/availability failure: it means the request itself
    was rejected (bad params, unsupported range, ...), NOT that the node is
    unhealthy -- so the pool must NOT fail over or cool the node on it (every
    node would reject it identically, and a healthy node would be benched)."""


class RpcRateLimit(RuntimeError):
    """The node rate-limited us -- an HTTP 429 OR a JSON-RPC 'rate exceeded' error
    body (HTTP 200). Unlike a plain RpcAppError this DOES fail over and cool the
    node (a different endpoint/token may not be throttled), and it's the signal
    the tool exists to warn about. NOT a subclass of RpcAppError on purpose."""


def _looks_ratelimited(err) -> bool:
    """Whether a JSON-RPC error body signals rate limiting (providers vary)."""
    if isinstance(err, dict):
        code, msg = err.get("code"), str(err.get("message", "")).lower()
    else:
        code, msg = None, str(err).lower()
    if code in (429, -32005, -32429):        # common rate-limit codes across providers
        return True
    return ("rate limit" in msg or "rate-limit" in msg or "rate exceeded" in msg
            or "too many" in msg or "429" in msg)


def _rpc_call_one(url: str, method: str, params=None, timeout: int = 20):
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    ).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": _UA},
        method="POST",
    )
    data = _http(req, timeout=timeout)
    if "error" in data:
        if _looks_ratelimited(data["error"]):    # 429 as a 200 body -> fail over + count
            raise RpcRateLimit(f"RPC {method} rate-limited: {data['error']}")
        raise RpcAppError(f"RPC {method} error: {data['error']}")
    return data["result"]


def _is_ratelimit(exc) -> bool:
    """True for a rate-limit signal -- HTTP 429 or a JSON-body 'rate exceeded'
    (RpcRateLimit) -- the thing the whole tool exists to warn about."""
    return isinstance(exc, RpcRateLimit) or (
        isinstance(exc, urllib.error.HTTPError) and exc.code == 429)


class RpcPool:
    """Ordered RPC endpoints with automatic failover and self-healing failback.

    A call always prefers the highest-priority endpoint NOT currently in a
    failure cooldown; on a transport/availability error it cools that endpoint
    down (exponential backoff, capped) and falls through to the next one. Because
    selection always restarts from the top, a recovered primary reclaims traffic
    on its own as soon as its cooldown lapses -- no background probe thread, no
    manual reset. Thread-safe: the surge loop fires ~12 calls/tick concurrently.

    JSON-RPC *application* errors (RpcAppError) are NOT failover events -- they
    raise straight through without cooling or switching, since every node would
    reject the request identically (see RpcAppError).
    """
    BASE_COOLDOWN = 20.0   # seconds for a first failure
    MAX_COOLDOWN = 300.0   # cap after repeated failures
    HEALTH_WINDOW = 500    # recent calls kept per endpoint for latency/error stats

    def __init__(self, urls, timeout: int = 20):
        urls = [u for u in dict.fromkeys(urls) if u]
        if not urls:
            raise ValueError("RpcPool needs at least one endpoint")
        self._eps = [{"url": u, "failed_until": 0.0, "streak": 0,
                      "calls": deque(maxlen=self.HEALTH_WINDOW)}  # (latency_s, status)
                     for u in urls]
        self._timeout = timeout
        self._lock = threading.Lock()
        self._active = urls[0]

    @property
    def primary(self) -> str:
        return self._eps[0]["url"]

    def _ordered(self, now):
        ready = [e for e in self._eps if e["failed_until"] <= now]
        cooling = [e for e in self._eps if e["failed_until"] > now]
        # if every endpoint is cooling, still try -- soonest-recovered first
        return ready + sorted(cooling, key=lambda e: e["failed_until"])

    def call(self, method, params=None, timeout=None):
        last_exc = None
        for e in self._ordered(time.time()):
            t0 = time.time()
            try:
                result = _rpc_call_one(e["url"], method, params,
                                       timeout or self._timeout)
            except RpcAppError:
                # request-level rejection, not a node-health signal: record it but
                # don't cool or fail over -- the caller's try/except handles it.
                with self._lock:
                    e["calls"].append((time.time() - t0, "app"))
                raise
            except Exception as exc:
                last_exc = exc
                dt = time.time() - t0
                status = "ratelimited" if _is_ratelimit(exc) else "error"
                with self._lock:
                    e["calls"].append((dt, status))
                    now = time.time()
                    # Only escalate on a genuinely fresh failure -- concurrent
                    # siblings in the same tick must not inflate the streak.
                    if e["failed_until"] <= now:
                        e["streak"] += 1
                    backoff = min(self.BASE_COOLDOWN * 2 ** (e["streak"] - 1),
                                  self.MAX_COOLDOWN)
                    e["failed_until"] = now + backoff
                print(f"[rpc] {_node_label(e['url'])} failed on {method}: {exc} "
                      f"-> cooldown {int(backoff)}s", flush=True)
                continue
            dt = time.time() - t0        # measure the round trip, not lock-wait
            with self._lock:
                e["calls"].append((dt, "ok"))
                e["streak"] = 0
                e["failed_until"] = 0.0
                if self._active != e["url"]:
                    prev, self._active = self._active, e["url"]
                    print(f"[rpc] switched to {_node_label(e['url'])} "
                          f"(from {_node_label(prev)})", flush=True)
            return result
        raise last_exc if last_exc else RuntimeError("no RPC endpoints")

    def status(self):
        """Per-endpoint health snapshot for /api/data (tokens masked): failover
        state + recent latency percentiles and error / rate-limit rates -- the
        earliest read on 'is our own RPC path degrading / about to be throttled'."""
        now = time.time()

        def pct(vals, p):
            return round(vals[min(len(vals) - 1, int(p * len(vals)))] * 1000) if vals else None

        with self._lock:
            out = []
            for i, e in enumerate(self._eps):
                calls = list(e["calls"])
                oks = sorted(lat for lat, s in calls if s == "ok")
                errs = sum(1 for _, s in calls if s == "error")
                r429 = sum(1 for _, s in calls if s == "ratelimited")
                # denominator excludes app rejections (not a node-health signal),
                # so a spate of unsupported-method errors can't dilute the rates
                hn = len(oks) + errs + r429
                out.append({
                    "endpoint": _mask_url(e["url"]),
                    "region": _region(e["url"]),     # short code (FRA/AMS/NY) or null
                    "priority": i,                   # 0 = primary
                    "healthy": e["failed_until"] <= now,
                    "cooldown_s": max(0, math.ceil(e["failed_until"] - now)),
                    "active": e["url"] == self._active,
                    "lat_p50_ms": pct(oks, 0.50),
                    "lat_p99_ms": pct(oks, 0.99),
                    "err_rate": round(errs / hn, 3) if hn else None,
                    "rate_limited": round(r429 / hn, 3) if hn else None,
                    "samples": hn,
                })
            return out


def rpc_call(endpoint, method: str, params=None, timeout: int = 20):
    """Dispatch: an RpcPool (failover) or a single URL string (direct)."""
    if isinstance(endpoint, RpcPool):
        return endpoint.call(method, params, timeout)
    return _rpc_call_one(endpoint, method, params, timeout)


def build_pool(override=None):
    """Build the RpcPool shared by both entry points. `override` is an optional
    comma/space-separated endpoint list (the `--rpc` flag); a blank/empty
    override (incl. whitespace-only) cleanly falls back to the configured
    RPC_ENDPOINTS instead of crashing. Returns (pool, endpoints)."""
    endpoints = _split_urls(override) or RPC_ENDPOINTS
    return RpcPool(endpoints), endpoints


def _gecko_get(path: str, timeout: int = 20):
    req = urllib.request.Request(
        f"{GECKO_BASE}{path}",
        headers={"Accept": "application/json;version=20230302", "User-Agent": _UA},
    )
    return _http(req, timeout=timeout)


# --------------------------------------------------------------------------- #
# Solana network congestion
# --------------------------------------------------------------------------- #
def network_tps(rpc_url: str) -> dict:
    """Most-recent 60s performance sample -> TPS figures."""
    samples = rpc_call(rpc_url, "getRecentPerformanceSamples", [2])
    if not samples:
        return {"nonvote_tps": None, "total_tps": None}
    s = samples[0]
    period = s.get("samplePeriodSecs") or 60
    total = s.get("numTransactions", 0)
    nonvote = s.get("numNonVoteTransactions")
    # numNonVoteTransactions is present on modern validators; fall back to total.
    return {
        "nonvote_tps": round((nonvote if nonvote is not None else total) / period, 1),
        "total_tps": round(total / period, 1),
        "slot": s.get("slot"),
    }


def _percentiles(values, pcts=(50, 90, 99)):
    if not values:
        return {p: None for p in pcts}
    vs = sorted(values)
    out = {}
    for p in pcts:
        # nearest-rank percentile
        k = max(0, min(len(vs) - 1, int(round((p / 100.0) * (len(vs) - 1)))))
        out[p] = vs[k]
    return out


def priority_fees(rpc_url: str) -> dict:
    """
    Meme-venue landing-fee pressure over the recent slot window (~150 slots/~60s).

    For each hot venue we read its per-slot floor priority fee, then pool the
    NON-ZERO floors across venues. Percentiles describe the fee level you must
    pay to land near meme activity; `contention` (fraction of slots with any
    fee competition) describes how often that competition is happening. Both
    climb during a surge. Units: micro-lamports per compute unit.

    Network-wide getRecentPrioritizationFees is intentionally NOT used: its
    floor is ~always 0 (some 0-fee tx lands every slot), so it never moves.
    """
    def fetch(acct):
        try:
            return rpc_call(rpc_url, "getRecentPrioritizationFees", [[acct]])
        except Exception:
            return None

    nonzero: list[int] = []
    slots_total = 0
    slots_hot = 0
    with ThreadPoolExecutor(max_workers=min(8, len(HOT_VENUES))) as ex:
        responses = list(ex.map(fetch, list(HOT_VENUES.values())))
    for r in responses:
        if not r:
            continue
        fees = [e.get("prioritizationFee", 0) for e in r]
        slots_total += len(fees)
        for f in fees:
            if f > 0:
                nonzero.append(f)
                slots_hot += 1

    pct = _percentiles(nonzero)
    contention = round(slots_hot / slots_total, 3) if slots_total else None
    return {
        "fee_p50": pct[50],
        "fee_p90": pct[90],
        "fee_p99": pct[99],
        "fee_hot_p90": pct[90],   # alias kept for the scorer / CSV
        "fee_contention": contention,
    }


# --------------------------------------------------------------------------- #
# Meme-venue trade rate + failure rate
# --------------------------------------------------------------------------- #
def meme_trade_rate(rpc_url: str) -> dict:
    """
    Transaction rate and FAILURE rate at each meme venue, measured over the
    recent slot window the venue's last ~1000 signatures span.

    The window self-sizes: a hot venue's 1000 sigs cover only a few seconds, so
    this reads a near-instantaneous rate regardless of how often we poll -- no
    need to sample more than once a minute to get high temporal resolution.

    Failure rate is the headline signal: bots racing and losing at meme venues
    (err != null) is exactly the submission flood that exhausts a lander's rate
    limits. tx/s here counts ALL signatures (landed + failed) = total load.
    """
    SLOT_SECS = 0.4

    def fetch(item):
        name, pid = item
        try:
            return name, rpc_call(rpc_url, "getSignaturesForAddress", [pid, {"limit": 1000}])
        except Exception:
            return name, None

    per = {}
    total_tps = 0.0
    total_sigs = 0
    total_failed = 0
    with ThreadPoolExecutor(max_workers=min(8, len(HOT_VENUES))) as ex:
        results = list(ex.map(fetch, list(HOT_VENUES.items())))
    for name, sigs in results:
        if not sigs:
            continue
        slots = [s["slot"] for s in sigs]
        span_slots = slots[0] - slots[-1]          # newest first -> positive
        # at very high rates 1000 sigs span <1 slot; floor at 1 slot so the
        # busiest venue (pumpswap) still yields a rate instead of dropping out
        span_s = max(span_slots, 1) * SLOT_SECS
        failed = sum(1 for s in sigs if s.get("err"))
        tps = round(len(sigs) / span_s)
        per[name] = {
            "tps": tps,
            "fail_rate": round(failed / len(sigs), 3),
            "n": len(sigs),
        }
        total_tps += tps
        total_sigs += len(sigs)
        total_failed += failed

    return {
        "meme_tps": round(total_tps) if total_tps else None,
        "meme_fail_rate": round(total_failed / total_sigs, 3) if total_sigs else None,
        "meme_by_program": per,
    }


# --------------------------------------------------------------------------- #
# Jito bundle tip floor — landing-competition signal
# --------------------------------------------------------------------------- #
JITO_TIP_FLOOR = "https://bundles.jito.wtf/api/v1/bundles/tip_floor"


def jito_tip_floor() -> dict:
    """What recently-landed Jito bundles are tipping, by percentile. This is the
    tip you're competing against to land a bundle right now — the most direct
    landing-pressure signal for a tip-based lander. Free, no key.
    Input values are SOL; we return LAMPORTS (cleaner integers than tiny SOL)."""
    try:
        req = urllib.request.Request(JITO_TIP_FLOOR, headers={"User-Agent": _UA})
        data = _http(req, timeout=15)
    except Exception:
        return {}
    o = (data[0] if isinstance(data, list) and data else data) or {}
    SOL = 1_000_000_000

    def lam(key):
        v = o.get(key)
        return round(v * SOL) if isinstance(v, (int, float)) else None

    return {
        "p50": lam("landed_tips_50th_percentile"),
        "p75": lam("landed_tips_75th_percentile"),
        "p95": lam("landed_tips_95th_percentile"),
        "p99": lam("landed_tips_99th_percentile"),
        "ema50": lam("ema_landed_tips_50th_percentile"),
    }


# --------------------------------------------------------------------------- #
# Network production health + macro context
# --------------------------------------------------------------------------- #
def network_health(rpc_url: str) -> dict:
    """Recent slot SKIP RATE -- the fraction of leader slots that produced no
    block over the last ~1500 slots (~10 min). A distinct congestion axis: when
    skips climb, transactions are harder to land regardless of fee or volume.
    (lastSlot is omitted because production data lags the confirmed tip.)"""
    try:
        slot = rpc_call(rpc_url, "getSlot", [{"commitment": "confirmed"}])
        bp = rpc_call(rpc_url, "getBlockProduction",
                      [{"range": {"firstSlot": max(0, slot - 1500)}}])
    except Exception:
        try:
            bp = rpc_call(rpc_url, "getBlockProduction", [])  # epoch fallback
        except Exception:
            return {"skip_rate": None}
    by = ((bp or {}).get("value") or {}).get("byIdentity") or {}
    leader = sum(v[0] for v in by.values())
    prod = sum(v[1] for v in by.values())
    return {"skip_rate": round(1 - prod / leader, 4) if leader else None}


# Per-block compute limit (Solana MAX_BLOCK_UNITS). Used for block-fill %.
# VERIFY against the network if a CU-limit increase (SIMD) ships -- it's a
# protocol constant, not exposed by RPC.
BLOCK_CU_LIMIT = 48_000_000
_VOTE_PROGRAM = "Vote111111111111111111111111111111111111111"

# Infra programs that appear in ~every tx -- excluded from the venue-drift tally
# (they're not swap venues). Everything else is a candidate "venue".
_INFRA_PROGRAMS = {
    "11111111111111111111111111111111",            # System
    _VOTE_PROGRAM,                                  # Vote
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # Associated Token
    "ComputeBudget111111111111111111111111111111",  # Compute Budget
    "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr",  # Memo v2
    "Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo",  # Memo v1
}


def _venue_by_program():
    return {pid: name for name, pid in HOT_VENUES.items()}


def block_stats(rpc, blocks: int = 3) -> dict:
    """Aggregate the last `blocks` finalized blocks -> direct landing signals:
      - block_fill:        mean compute-used / BLOCK_CU_LIMIT (direct congestion)
      - block_fail_rate:   pooled NON-VOTE tx failure rate (bots racing and losing)
      - block_fee_cu_p50/p90: pooled landed priority-fee-per-CU (micro-lamports/CU) =
                           the real clearing price to land, vs the prioritization floor

    A single block is a very noisy estimate (fill can swing 0.4->1.0 block to
    block), so we pool several: with congestion evolving over minutes, reducing
    per-sample noise matters more than freshness. Vote txs are excluded from the
    failure/fee figures (they never fail and pay no priority). Heavy (~6 MB PER
    block, no gzip) -> sample on a SLOW cadence. Returns {} on failure."""
    try:
        slot = rpc_call(rpc, "getSlot", [{"commitment": "finalized"}])
    except Exception:
        return {}
    got = []
    s, tries, max_tries = slot, 0, blocks * 2 + 4   # budget for skipped slots
    while len(got) < blocks and tries < max_tries:
        try:
            b = rpc_call(rpc, "getBlock", [s, {
                "encoding": "json", "maxSupportedTransactionVersion": 0,
                "transactionDetails": "full", "rewards": False}], timeout=20)
            if b and b.get("transactions"):         # skip skipped/empty/null slots
                got.append((s, b))
        except Exception:
            pass
        s -= 1
        tries += 1
    if not got:
        return {}
    total_cu = nonvote = nonvote_fail = total_txs = 0
    fee_per_cu = []
    prog_txs = {}                     # program -> # of non-vote txs invoking it
    for _, b in got:
        for t in b["transactions"]:
            total_txs += 1
            meta = t.get("meta") or {}
            cu = meta.get("computeUnitsConsumed") or 0
            total_cu += cu
            msg = (t.get("transaction") or {}).get("message") or {}
            if _VOTE_PROGRAM in (msg.get("accountKeys") or []):
                continue
            nonvote += 1
            if meta.get("err") is not None:
                nonvote_fail += 1
            sigs = (t.get("transaction") or {}).get("signatures") or []
            priority = max(0, (meta.get("fee") or 0) - 5000 * len(sigs))  # strip base
            if cu > 0:
                fee_per_cu.append(priority / cu * 1e6)   # micro-lamports per CU
            # venue-drift tally: which programs are handling load? Resolve the
            # instruction's programIdIndex against static keys + ALT-loaded ones,
            # count each program once per tx, drop infra programs.
            keys = ((msg.get("accountKeys") or [])
                    + ((meta.get("loadedAddresses") or {}).get("writable") or [])
                    + ((meta.get("loadedAddresses") or {}).get("readonly") or []))
            progs = set()
            for ins in (msg.get("instructions") or []):
                idx = ins.get("programIdIndex")
                if idx is not None and 0 <= idx < len(keys) and keys[idx] not in _INFRA_PROGRAMS:
                    progs.add(keys[idx])
            for p in progs:
                prog_txs[p] = prog_txs.get(p, 0) + 1
    fee_per_cu.sort()
    n = len(got)
    # top program counts for THIS sample -- the server aggregates across samples
    # into a stable venue_top (this ~1s-of-chain snapshot alone is too noisy)
    venue_counts = dict(sorted(prog_txs.items(), key=lambda kv: -kv[1])[:40])

    def q(p):
        return round(fee_per_cu[min(len(fee_per_cu) - 1, int(p * len(fee_per_cu)))])
    return {
        "block_fill": round(min(1.0, total_cu / (n * BLOCK_CU_LIMIT)), 4),
        "block_fail_rate": round(nonvote_fail / nonvote, 4) if nonvote else None,
        "block_fee_cu_p50": q(0.50) if fee_per_cu else None,
        "block_fee_cu_p90": q(0.90) if fee_per_cu else None,
        "block_slot": got[0][0],       # newest slot sampled
        "block_txs": total_txs,
        "block_nonvote": nonvote,
        "block_count": n,              # blocks actually aggregated
        "venue_counts": venue_counts,  # {program: txs} this sample (server aggregates)
    }


COINGECKO_SOL = ("https://api.coingecko.com/api/v3/simple/price"
                 "?ids=solana&vs_currencies=usd&include_24hr_change=true")


def sol_price() -> dict:
    """SOL spot price + 24h change (CoinGecko, free, no key). Macro context for
    *why* activity is surging — big SOL moves drive on-chain frenzies."""
    try:
        req = urllib.request.Request(COINGECKO_SOL, headers={"User-Agent": _UA})
        d = _http(req, timeout=15)
    except Exception:
        return {}
    s = (d or {}).get("solana") or {}
    return {"price": _f(s.get("usd")), "change_24h": _f(s.get("usd_24h_change"))}


# --------------------------------------------------------------------------- #
# Biggest movers (GeckoTerminal)
# --------------------------------------------------------------------------- #
def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def trending_movers(limit: int = 5) -> list[dict]:
    """Top Solana pools by trending rank, with 1h volume + 5m txn counts."""
    try:
        data = _gecko_get("/networks/solana/trending_pools?duration=1h")
    except Exception:
        return []
    out = []
    for pool in data.get("data", [])[:limit]:
        a = pool.get("attributes", {})
        name = a.get("name", "?")
        symbol = name.split(" / ")[0].strip() if name else "?"
        vol = (a.get("volume_usd") or {})
        tx = (a.get("transactions") or {})
        tx5 = tx.get("m5") or {}
        chg = (a.get("price_change_percentage") or {})
        addr = a.get("address")
        buys5 = tx5.get("buys", 0) or 0
        sells5 = tx5.get("sells", 0) or 0
        mc = _f(a.get("market_cap_usd")) or _f(a.get("fdv_usd"))
        vol_h1 = _f(vol.get("h1"))
        created = a.get("pool_created_at")
        created_epoch = None
        if created:
            try:
                import datetime as _dt
                created_epoch = _dt.datetime.fromisoformat(
                    created.replace("Z", "+00:00")).timestamp()
            except (ValueError, AttributeError):
                pass
        out.append(
            {
                "symbol": symbol,
                "name": name,
                "created_at": created_epoch,                # pool/token age (epoch secs)
                "price": _f(a.get("base_token_price_usd")),
                "mc": mc,                                   # market cap (FDV fallback)
                "liquidity": _f(a.get("reserve_in_usd")),
                "vol_h1": vol_h1,
                "vol_h24": _f(vol.get("h24")),
                # velocity = how many times the market cap trades per hour (turnover)
                "velocity": (vol_h1 / mc) if (vol_h1 and mc) else None,
                "txns_5m": buys5 + sells5,
                "buys_5m": buys5,
                "sells_5m": sells5,
                "chg_m5": _f(chg.get("m5")),
                "chg_h1": _f(chg.get("h1")),
                "chg_h24": _f(chg.get("h24")),
                "url": f"https://www.geckoterminal.com/solana/pools/{addr}" if addr else None,
            }
        )
    return out


def new_pool_rate(window_min: int = 5) -> int | None:
    """
    Count of brand-new Solana pools created within the last `window_min` minutes.
    A proxy for meme launch-rate -- the leading edge of a surge.
    Returns None if the timestamp parse fails entirely.
    """
    import datetime as _dt

    try:
        data = _gecko_get("/networks/solana/new_pools")
    except Exception:
        return None
    now = _dt.datetime.now(_dt.timezone.utc)
    cutoff = now - _dt.timedelta(minutes=window_min)
    count = 0
    parsed_any = False
    for pool in data.get("data", []):
        created = (pool.get("attributes") or {}).get("pool_created_at")
        if not created:
            continue
        try:
            ts = _dt.datetime.fromisoformat(created.replace("Z", "+00:00"))
            parsed_any = True
            if ts >= cutoff:
                count += 1
        except (ValueError, AttributeError):
            continue
    return count if parsed_any else None
