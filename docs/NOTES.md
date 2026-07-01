# Design Notes & Gotchas

Non-obvious decisions and learnings from building this. Read before changing the
data model or the surge/movers logic — it'll save you time. Referenced from
`CLAUDE.md`. For the user-facing reference, see `README.md`.

---

## 1. The Surge Index and the Surging-coins panel measure DIFFERENT things (important)

This is the #1 source of confusion. They come from different data sources at
different scopes and will **not** line up:

- **Network Surge Index** = an **on-chain aggregate** computed from RPC: total
  transaction rate, failure rate, fees, and launch rate across the 9 DEXes. It
  knows *how congested the network is*, but **not which specific tokens** are
  responsible.
- **Surging meme coins** = coins from GeckoTerminal trending (top-20 pool),
  scored to **real upward surges** by trading volume + transaction count (§7).

So when the network surges, the 5 trending coins won't necessarily reflect it —
they're a different lens from a different source.

### Why a "surge" is often NOT a bunch of coins pumping up

Even with perfect token coverage, a surge frequently isn't a rally:

- **Failed transactions dominate.** We measure 40–90% failure rates on
  pumpswap/pump.fun — that's **bots racing and losing**, not coins mooning. It
  inflates the trade-rate and failure-rate signals.
- **pump.fun launch frenzies** — thousands of throwaway new tokens created +
  sniped. They never reach GeckoTerminal "trending."
- **Dumping/selling** — a coin crashing generates huge tx volume → feeds the
  surge → but shows as **↓** in movers (we intentionally filter to upward-only).

So an ELEVATED/SURGE reading with mostly red/↓ movers is **expected and correct**
— it usually means bot-spam / launch frenzy / sell-off, not a clean rally.

### What we do NOT have, and how to add it

We do **not** attribute the surge to specific tokens. To do that properly you'd
enumerate the most-active pools from the on-chain activity we already count
(heavier — parse hot pools from the DEX signatures, or a slower side job).
GeckoTerminal can sort pools by `h24_tx_count_desc` but only over 24h (laggy);
there's no real-time 5m/1h tx-sorted endpoint on the free tier.

The **venue + driver breakdown in Technical Details already answers "what's
driving it" at the venue level** (e.g. "pumpswap 1.3k tx/s, 90% fail" = bot
frenzy on pump tokens). Token-level attribution is the open gap.

---

## 2. DEX venue coverage — ranked by TX COUNT, not USD volume

`sources.HOT_VENUES` (9 venues) drives both the trade/failure rate and the
landing-fee pressure. We rank by **transaction count** because each tx is a
landing attempt — the thing that stresses a lander. That's why high-$/low-tx
market-maker venues (SolFi ~5/s, Phoenix ~2/s, OpenBook ~0/s) are **excluded**:
huge USD volume, negligible tx load.

**To add a venue:** first verify the program ID returns a real tx rate —
`getSignaturesForAddress(pid, {limit:1000})`, then `sigs ÷ slot-span`. Only add
if it's materially active. Per-venue RPC calls are parallelized
(`ThreadPoolExecutor`) so the 5s tick holds even at 9+ venues. Launchpad program
IDs (LetsBonk, Boop, Believe) tested dead/empty as of 2026-06-30 — don't add on a
guess.

---

## 3. pump.fun stream — no global trade firehose

pumpportal.fun's free websocket gives `subscribeNewToken` (launches) and
`subscribeMigration` (graduations), which we use. It has **no all-trades
firehose**; subscribing to trades of fresh mints yields ~nothing (most new
tokens get the creator's buy then die — measured 0 trades over 35s on 20
newborns). That's why the index uses **launch rate**, not a websocket trade rate.

---

## 4. RPC notes

- **The URL is a secret** (token in the path). Keep it in `.env.onchain-activity`
  (gitignored, loaded by `sources._load_local_env`). Never commit it. Public RPC
  is a degraded fallback (rate-limited under our ~16 calls/5s load).
- **`getBlockProduction` lags the confirmed tip by ~12s** on every Solana node
  (it's a derived stat, not a node fault). Omit the explicit `lastSlot`;
  irrelevant for the ~10-min skip-rate window.
- **`getRecentPrioritizationFees` network-wide is ~always 0** — query per hot
  account and pool the non-zero floors instead.
- **At very high rates 1000 sigs span <1 slot** — floor the span at 1 slot so the
  busiest venue (pumpswap) still yields a rate instead of dropping out.

### RPC failover (`sources.RpcPool`)

- **Config:** `SOLANA_RPC` is the primary; `SOLANA_RPC_FALLBACKS` is an ordered,
  comma/space-separated list. `sources.RPC_ENDPOINTS` = primary + fallbacks
  (de-duped). `DEFAULT_RPC` stays as an alias for the primary (back-compat).
- **One chokepoint:** every fetcher threads its `rpc` arg into `sources.rpc_call`,
  which now dispatches on type — an `RpcPool` (failover) or a plain URL string
  (direct, unchanged). So the pool is passed wherever a URL string used to go;
  no fetcher signatures changed.
- **Why cooldown-based, not a health-checker thread:** selection always restarts
  from the highest-priority endpoint NOT in cooldown. A failed endpoint gets a
  short exponential-backoff cooldown (20s → 300s cap); when it lapses the
  endpoint is naturally probed first again, so a recovered primary **reclaims
  traffic on its own** — no extra thread, no manual reset, no flapping state.
- **Concurrency:** the surge loop fires ~12 calls/tick, several in parallel
  (`meme_trade_rate`). The pool is lock-guarded, and the streak/backoff only
  escalates on a *genuinely fresh* failure (`failed_until <= now`) so concurrent
  siblings in the same tick don't inflate the cooldown.
- **Tokens never leak:** `_mask_url` (host + last-4) is the most a token-bearing
  URL is ever rendered as. Logs and the startup banner use `_node_label`
  (region code via `RPC_REGIONS`, e.g. `FRA`/`AMS`/`NY`, falling back to the
  masked host); `/api/data` `rpc[]` carries both `region` and masked `endpoint`,
  and the dashboard badge shows the region with the codename in its tooltip.
  Region labels are cosmetic — update `RPC_REGIONS` if the nodes change.
- **Only transport/availability errors fail over:** DNS / connection / timeout /
  HTTP 4xx-5xx (incl. 429) cool a node and advance to the next. JSON-RPC
  *application* errors (`data["error"]` → `RpcAppError`) do NOT — they raise
  straight through without cooling or switching. Rationale: methods like
  `getBlockProduction` with a `firstSlot` range legitimately return errors
  (`network_health` already wraps them in try/except); treating those as
  node-health failures would bench a healthy node and flap traffic for no
  outage, and every node would reject the request identically anyway. If all
  endpoints fail a transport error, the last exception propagates — same
  degradation as the old single-URL path.
- **Escalating backoff applies across cooldown cycles, not within one:** a node
  is re-probed only after its cooldown lapses, and each *post-lapse* failure
  bumps the streak (20s→300s). A single-endpoint pool is the exception — it has
  nowhere to fail over, so it keeps retrying the only node every tick (the
  streak stays at the base), which is the desired behaviour there.

---

## 5. Why no gRPC (Yellowstone/Geyser)

A stream of every DEX transaction is a firehose (thousands/sec during a surge)
that would need heavy server-side aggregation and could swamp a laptop. The
`getSignaturesForAddress` polling gives the rate cheaply and the window
self-sizes. Only revisit with a narrowly-filtered subscription.

---

## 6. Architecture

- **Background loops** (`server.py`): a fast **surge loop** (RPC, ~5s, no
  GeckoTerminal), a slow **movers loop** (GeckoTerminal, ~15s), and a slow
  **block loop** (`getBlock`, ~30s). Decoupled so the gauge feels live while
  staying under GeckoTerminal's ~30/min limit. Jito 10s · skip rate 15s · SOL
  price 45s. All period-accurate (sleep = interval − work). The block loop uses
  its **own RpcPool** (getBlock is ~6 MB × `--block-samples` and heavy — a slow
  one must not cool the surge loop's primary and flap the fast tick).
- **Block signals** (`sources.block_stats`): pool the last N blocks (default 3)
  because one block is a very noisy estimate (fill swings 0.4→1.0 block to block).
  Vote txs (~half the block) are excluded from the failure/fee figures. Only
  **fill %** truly needs `getBlock`; failure and fee-per-CU have cheaper,
  better-sampled sources (`getSignaturesForAddress`, `getRecentPrioritizationFees`),
  so the block fee-per-CU is displayed but kept OUT of the weighted index.
- **Last-known-good** on transient source misses (movers, venue data) so the UI
  never blanks; per-section freshness tells the truth when a source lags.
- **Surge Index** lives in `monitor.py` (`SurgeTracker`, `SURGE_SIGNALS`,
  `_LEVELS`). Weights/thresholds are a **reasoned prior, NOT calibrated** to real
  rate-limit events — calibrating against the lander's actual throttling is the
  single highest-value next step. `compute()` excludes a **missing** signal (None)
  from the weighted average so an absent source can't deflate the score to 0.
- **Persistence** is `store.py` (SQLite, stdlib — still zero-dep). One
  `data/monitor.db`; schema is derived from `CSV_FIELDS` with `ALTER TABLE ADD
  COLUMN` evolution (adding a signal is a one-liner, no file migration), appends
  are ACID (no truncate-then-write corruption window the old CSV had), and
  baselines/charts/percentile read time-ranged **SQL slices**. `monitor` and
  `store` import each other, but only inside functions (never at module top), so
  the cycle resolves lazily. WAL mode; DB access is effectively single-writer
  (only the surge/terminal loop writes). Legacy per-day CSVs are imported once
  then unused. See improvements.md D2.

---

## 7. "Surging meme coins" panel — algo & decay (was "Top meme movers")

Shows ONLY coins whose trading is straining the network with a genuine upward
surge. Server fetches a wide pool (top-20 GeckoTerminal trending; the top 5 are
often all flat/down while real surgers sit lower); the client scores each, keeps
surgers, sorts, shows top 8. Empty → "No meme coins surging up right now"
(mirrors a calm network).

**KEY LEARNING (calibrated against live data 2026-06-30): the real surges are
usually LOW market cap explosions** (e.g. a $140k-MC coin doing $500k/hr and 260
trades/5m at +150%). So **market cap is the WRONG filter** — an MC floor would
exclude the biggest real surges. **VOLUME + TRANSACTION COUNT is the
discriminator** between a real surge and a rug-tick: at the same MC, a $36k/hr
micro-trade token is noise while a $500k/hr frenzy is a real surge.

Surge score (0–5), `surgeScore()` in `dashboard.html` — all thresholds are named
constants (tunable):
- **Gates** (else 0): up ≥ 5% over 1h · not crashing (5m > −10%) · 1h volume
  ≥ $75k (the real filter) · MC ≥ $50k (drops dust only).
- **Magnitude**: `0.40·volume + 0.35·tx_count + 0.25·momentum`, each normalized
  (volume log-scaled $75k→$2M; tx 30→300 per 5m; momentum 0→+75%/1h). Volume +
  tx = the network strain that hits the lander; momentum = how hard it's pumping.

**Sticky decay (how coins leave the list):** per-coin displayed score is held in
`surgeMem` and updated once per 15s movers tick: `disp = max(live, prev −
SURGE_DECAY)`. A surge pops in at its live score; when it stops, `disp` decays
toward 0 over ~45–60s (4→3→2→1→gone) and the row dims (`opacity`). This avoids
both flicker (coins oscillating around the threshold) and abrupt disappearance,
and works even after a coin leaves GeckoTerminal's pool (we keep its last data).
Tune the fade via `SURGE_DECAY`.

---

## 8. UI freshness language (keep uniform)

- **Surge gauge** = "live · 5s" pulse + a refresh countdown bar that fills over
  5s and ages green→amber→orange→red (same `freshColor`); flips to "stalled" if
  the loop dies.
- **Movers** = "updated Xs ago" colored by the same `freshColor` over its 15s
  cadence.
- **Technical details** = a live/aging indicator shown **only when expanded**.
- Each refresh flashes the gauge number (white blink + scale pulse).

---

## 9. Code traps that already bit us (don't reintroduce)

- **`threading.Thread` has an internal `self._handle`** — naming a method
  `_handle` on a Thread subclass silently breaks it (use `_on_message`, etc.).
- **CSS class-name collisions:** the tooltip-direction class was named `.down`
  and inherited `.down{color:red}` from the price-change cells → red "?".
  Namespace UI utility classes (`.tdown`).
- **Zero dependencies** is a hard rule — hand-roll (see `pumpstream.py`'s stdlib
  websocket) rather than adding a pip package.
