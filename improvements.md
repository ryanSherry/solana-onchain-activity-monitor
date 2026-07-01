# Improvements roadmap

Living backlog for the Solana On-Chain Activity Monitor. The product's job:
**warn a transaction-landing operator before meme-coin frenzies make txs hard to
land / exhaust their RPC rate limits.** Every item is judged against that.

Constraints (hard): **zero dependencies** (Python stdlib + hand-rolled only),
free data sources, the RPC URL is the only secret. Verify any new RPC method /
program ID / constant against the live node before building (see CLAUDE.md).

Status legend: ✅ shipped · 🚧 in progress · ⏭️ next · 📋 backlog

---

## Shipped
- ✅ **RPC failover pool** (`sources.RpcPool`) — primary FRA → AMS → NY, transport-
  only failover, auto-failback, masked tokens, `/api/data` `rpc[]` + header badge.
- ✅ **Dashboard restyle** — Solana-tinted theme, mono telemetry, surge rail.
- ✅ **Readable micro-prices** — DexScreener-style `$0.0₄508`.
- ✅ **Surge percentile context** — current vs trailing-7d distribution (`surge_context`).
- ✅ **"Tip to land now"** — Jito tip recommendation scaled to congestion.

---

## A. New data sources & signals  (anchor the index to real landing conditions)

### A1. Block-level data via `getBlock`  🚧 (DOING FIRST)
Sample a recent block every few seconds; from its landed txs derive:
- **Block fill %** = Σ `computeUnitsConsumed` ÷ per-block CU limit (~48M — VERIFY
  current value against the node). The *direct* congestion measure.
- **Fee-per-CU of landed txs** (p50/p90) = the real market-clearing price to land
  (better than the Jito tip floor for non-bundle sends).
- **Network-wide tx failure rate** = `meta.err` ÷ total. Spikes when bots spam.
Why: turns the Surge Index from a proxy into something grounded in what actually
landed. Cost: `getBlock` is heavy → sample sparingly (1 block / few s), throttle
to stay inside the ~16-calls/5s budget. Becomes the ground-truth anchor for A/B
algo work below.

### A2. Leader-schedule risk  📋
Combine `getLeaderSchedule` with per-leader skip rates (already derivable from the
`getBlockProduction` `byIdentity` call we make) → surface "the next N leaders
include a known skipper." Forward-looking landing risk, nearly free (reuses a call).

### A3. Hot writable-account priority fees  📋
`priority_fees` samples programs today; the local fee market that decides if *your*
tx lands is keyed on the writable accounts it touches (pump.fun global / bonding-
curve account, hottest token mints). Sample `getRecentPrioritizationFees` for those.

### A4. Macro context  📋
Add BTC + total-market change and SOL realized volatility (CoinGecko, cheap) beside
the SOL price. Risk-on regimes drive Solana frenzies — leading-ish context.

---

## B. Algorithm upgrades

### B1. Self-calibrating normalization  📋 (biggest algo win)
Replace fixed seed baselines (`SURGE_SIGNALS`) with rolling per-signal normalization
— EWMA mean/σ z-score or rolling quantiles, ideally bucketed by time-of-day. Makes
the index adapt to the real distribution instead of a hand-tuned prior. Extends the
percentile work already shipped.

### B2. Leading vs current-stress sub-indices  📋
Split into an "early warning" sub-index (pump-launch acceleration, fee-market slope)
and a "stress now" sub-index (skip rate, failure rate, block fill). Gives lead time
*and* a current read instead of one blended number.

### B3. Anchor to a ground-truth target  📋
Once A1 exists, define the real target (fee-per-CU to land @p90, or failure rate)
and have the index track/predict it. With the calibration hook (C5), fit a simple
logistic nowcast — P(landing trouble in next 10 min) — on CSV history. All stdlib.

---

## C. Product / ops features

### C1. Alerting  📋 (high impact, deferred by request)
Push to Slack/Discord webhook (stdlib `urllib` POST) on: surge → Elevated/Surging,
skip-rate spike, RPC failover. Turns a passive dashboard into an active warning.

### C2. RPC self-health  📋
`RpcPool.call` already sees every error/latency — record per-node **response latency
(p50/p99)** and **429/error rate**, expose as a panel + alert. The earliest, most
direct read on "are we about to be throttled."

### C3. Machine-readable backoff signal  📋
`advise_backoff` boolean (or `/api/surge`) the lander polls to auto-throttle.
Promotes the tool from "warn the human" to "automatically protect the lander."

### C4. Responsive / mobile  ⏭️ (NEXT after A1)
`@media` breakpoints: stack the fixed-300px gauge + verdict, handle the 9-column
movers table (scroll or hide columns). Tailnet has phones; glanceability anywhere
is the core use. Pure CSS, zero-dep. (Visual change — needs a real-device eyeball.)

### C5. Surge Index calibration hooks  📋
Let an operator mark a real rate-limit incident ("throttled at 14:32") and overlay
it on the surge chart → tune thresholds against ground truth. Feeds B3.

### C6. "Recommended action" line  📋
Synthesize state into one directive: "Surging — hold non-critical sends, bump tips
to p95." Human-readable sibling of C3, extends "tip to land now."

### C7. Per-source health / staleness  📋
Mark when GeckoTerminal / Jito / CoinGecko / pump stream goes stale or errors, so
the operator trusts the numbers during the moments that matter.

---

## D. Engineering

### D1. Test suite  📋 (high leverage)
The repo has zero tests. Add stdlib `unittest` for `RpcPool` failover/failback,
`_surge_context` boundaries, and the Surge Index scoring. Locks in load-bearing
logic as we keep iterating.

### D2. Migrate persistence to SQLite  ⏭️ (NEXT after A1)
Replace the per-day CSVs with a single SQLite DB (`sqlite3` is stdlib -> still
zero-dependency). Motivation surfaced while adding A1's columns: CSV schema
evolution is painful (in-place header migration) and the in-place rewrite had a
truncate-then-write corruption window (now mitigated with an atomic temp+replace,
but it's a band-aid). SQLite gives `ALTER TABLE ADD COLUMN`, ACID durability, and
lets the baselines / 7-day percentile / time-of-day baseline (B1) be SQL
aggregations instead of "load every row into Python and sort." Touches
`append_csv`, `read_history_rows`, `_seed_history`, `_seed_baseline`; keep a CSV
export for the human-greppable use. Subsumes D3 (retention = `DELETE WHERE`).

### D3. Data retention  📋
Old data grows unbounded; prune beyond the baseline window (becomes a trivial
`DELETE WHERE ts < ...` once on SQLite, D2).

---

## Suggested order
**A1 (block-level data)** → **C4 (responsive)** → **D1 (tests)** → **B1
(self-calibrating)** → then C2 / A2 / C3 as appetite allows. A1 first because it
becomes the ground-truth anchor the algorithm work (B1–B3) builds on.
