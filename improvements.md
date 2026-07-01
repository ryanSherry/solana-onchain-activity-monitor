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

### A1. Block-level data via `getBlock`  ✅ shipped
`sources.block_stats` pools the last N blocks (`--block-samples`, default 3) into:
- **Block fill %** = Σ `computeUnitsConsumed` ÷ 48M CU (verify the constant if a
  SIMD raises it) — folded into the index (w15).
- **Non-vote tx failure rate** — folded into the index (w12).
- **Landed fee-per-CU** (p50/p90) — displayed; kept OUT of the index (overlaps the
  cheaper, better-sampled `fee_p90`).
Its own RpcPool + slow cadence + TTL so the ~6 MB×samples fetch can't flap the
surge loop. The ground-truth anchor for the algo work (B1–B3). (Idea if we want
it cheaper: only `getBlock` for fill; keep failure/fee from the cheap sources.)

### A2. Leader-schedule risk  ⏸️ deprioritized (wrong altitude — kept for the record)
Idea: combine `getLeaderSchedule` with per-leader skip rates (derivable from the
`getBlockProduction` `byIdentity` call we make) → "the next N leaders include a
known skipper." Forward-looking landing risk, nearly free.
**Why parked:** its value needs action on a ~1.6s per-leader horizon. A human
watching a 5s dashboard can't act on that, and the automated lander that could
already gets leader-aware TPU forwarding natively — this would duplicate a concern
that lives in the send loop, not a monitoring tool. The *human-actionable* version
("leaders are broadly skipping → landing is harder now") is the **aggregate skip
rate we already track** and feed into the index + backoff advice. Revisit only if
a per-slot automated consumer ever wants it.

### A3. Hot writable-account priority fees  📋
`priority_fees` samples programs today; the local fee market that decides if *your*
tx lands is keyed on the writable accounts it touches (pump.fun global / bonding-
curve account, hottest token mints). Sample `getRecentPrioritizationFees` for those.

### A5. Venue-list drift detection  ✅ shipped
`HOT_VENUES` is a static list, so a migrating meme scene silently escapes coverage.
`block_stats` tallies, in the SAME pass over the blocks it already fetches, the
busiest programs by non-vote tx count (resolving each top-level instruction's
`programIdIndex` through static keys + ALT-loaded addresses, dropping infra
programs) and returns per-sample `venue_counts` ({program: txs}, top 40). The
**server aggregates the last ~10 samples** (`_aggregate_venue_top`, `_venue_samples`
deque) into a STABLE top-12 `venue_top` — a single ~1s-of-chain block sample is far
too noisy to read as "drift." The dashboard shows the aggregate with untracked
programs (no `venue` name) in **amber, informational only**: aggregators (Jupiter)
and perps rank high perennially but must NOT be added (they CPI into venues we
already track), so the tool doesn't cry wolf — an untracked program that *stays*
near the top is the operator's cue to validate (`getSignaturesForAddress` tx rate)
and add it. First run surfaced an untracked program busier than tracked pumpswap.
Near-free (reuses the getBlock data). **Known limit:** top-level-instruction
counting only, so a tracked venue reached via an aggregator CPI is under-counted
(fine for "is this program on our list?"; the operator judges, so it doesn't
mislead). Next idea: auto-open a "validate this program" checklist for a
persistently-high untracked program.

### A4. Macro context  📋
Add BTC + total-market change and SOL realized volatility (CoinGecko, cheap) beside
the SOL price. Risk-on regimes drive Solana frenzies — leading-ish context.

---

## B. Algorithm upgrades

### B1. Self-calibrating normalization  ✅ shipped
Each signal's heat is now a **robust z-score** — how many sigmas above its own
rolling baseline (median center + MAD spread, floored) it sits — instead of a fixed
multiple of the median. Variance-aware (a steady signal lights up on a small move,
a volatile one needs more) and robust to heavy tails. Seed baseline is only the
prior until the window fills. See monitor.py `_heat`/`center_scale`, NOTES §6.
**Known limit (inherent to the ~10-min rolling window):** a surge that develops
*inside* the window inflates that signal's own MAD, damping its onset heat (and a
sustained surge >window drifts the median up and latches — partly shared with the
old median-baseline code). Fine for the common *sudden* meme spike (window is
still pre-spike → high heat); weaker for slow ramps.
**B1b — time-of-day baseline: ✅ shipped.** `store.hourly_baselines` builds each
signal's per-UTC-hour robust (center, sigma) from history; `server._tod_loop`
refreshes it ~half-hourly and the surge loop passes the current hour's slice to
`compute()`, so heat is "unusual FOR THIS HOUR." Falls back per signal/hour to the
rolling window until a bucket has `--tod-min-samples` AND spans `--tod-min-days`
distinct days (default 2) — the day-span guard is what stops a *live* surge from
polluting its own thin-history baseline (so B1b activates only once there's real
cross-day history; until then it's the proven B1 window). Directly mitigates the
ramp/latching limit above. `--tod-days` (default 7, 0=off). Gets sharper as more
history accrues (weekday vs weekend etc.). `comps[...]["source"]` shows `hour` vs
`window`. Uses its own read connection so the scan never stalls the 5s append.

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

### C2. RPC self-health  ✅ shipped
`RpcPool` records every call's latency + outcome (ok / HTTP-429 / error) in a
per-endpoint rolling window (`HEALTH_WINDOW=500`); `status()` exposes **p50/p99
latency**, **error rate**, and **rate-limit (429) rate** per node at `/api/data`
`rpc[]`, rendered as an "RPC endpoint health" panel in Technical Details. The
earliest, most direct read on "is our own path degrading / about to be throttled."
App errors (`RpcAppError`) are recorded but not counted as node errors.
**Follow-up:** feed rising latency / 429 into alerting (C1) and the backoff
signal (C3).

### C3. Machine-readable backoff signal  ✅ shipped (with C6)
`server._backoff_advice` synthesizes surge + our own RPC health (C2: 429/latency)
+ skip rate into one verdict via max-of-signals (any axis says danger -> back off,
and the `reason` says which). Exposed as a tiny `/api/surge` endpoint the lander
polls -- `advise_backoff` (bool), `throttle_factor` (0-1 = fraction of NON-critical
sends to hold), `level`, `reason`, `action` -- and embedded in `/api/data`
`advise`. Turns "warn the human" into "protect the lander." Also drives **C6** (the
human "recommended action" line on the dashboard, colored by level). Thresholds
are a prior; calibrate with C5. Follow-up: feed it into alerting (C1).

### C4. Responsive / mobile  ⏭️ (NEXT after A1)
`@media` breakpoints: stack the fixed-300px gauge + verdict, handle the 9-column
movers table (scroll or hide columns). Tailnet has phones; glanceability anywhere
is the core use. Pure CSS, zero-dep. (Visual change — needs a real-device eyeball.)

### C5. Surge Index calibration hooks  ✅ shipped
A **⚑ Mark incident** button (and `POST /api/incident`, `{note}` optional) records
a real rate-limit/landing incident *now*, stamped with the current surge score/
level, into an `incidents` table (kept forever -- prune never touches it). Recent
incidents come back on `/api/data` and are drawn as flagged markers on both surge
charts, so you can eyeball how the index read when landing actually broke. The raw
signals at each incident's `ts` live in `samples` for offline calibration. This is
the ground-truth feed that **unblocks B3** (anchor the index to a measured target /
fit a nowcast). Next: an analysis view/query that scores the index against the
marked incidents and suggests threshold tweaks.

### C6. "Recommended action" line  ✅ shipped (with C3)
The dashboard now shows a single directive under the verdict ("Hold non-critical
sends; raise tips to ~p95 · surge ELEVATED (40) · hold ~50%"), colored by level,
driven by the same `_backoff_advice` synthesis as C3's `/api/surge`.

### C7. Per-source health / staleness  ✅ shipped
Each external feed (RPC/surge, Jito, GeckoTerminal movers, block data, SOL price,
pump stream) stamps a last-good time; `server._source_health` classifies each
fresh / stale / down (age vs 3x/6x its cadence), exposed at `/api/data` `sources`
and rendered as a "Data source health" pill strip in Technical Details. So a
frozen last-known-good value is *visible* rather than silently trusted.

---

## D. Engineering

### D1. Test suite  ✅ shipped
`tests/` (stdlib `unittest`, no network, fakes injected): `RpcPool`
failover/failback/cooldown + app-vs-transport split; `block_stats` fill/fail/
fee/vote-filter/aggregation/skipped-slot; `SurgeTracker` missing-signal exclusion
+ baseline dedup + heat/levels; `store` round-trip/time-slices/NULL-ts/schema-add/
CSV-import; `_surge_context` percentile/cap/is_peak boundaries. Run:
`python3 -m unittest discover -t . -s tests`. (Next: wire into CI / a pre-push hook.)

### D2. Migrate persistence to SQLite  ✅ shipped
`store.py` (stdlib `sqlite3`, still zero-dependency): one `data/monitor.db`,
schema derived from `CSV_FIELDS` with `ALTER TABLE ADD COLUMN` evolution, ACID
appends (no corruption window), and SQL time-range slices for baselines / charts
/ the 7-day percentile. Legacy per-day CSVs are imported once on first run then
unused. Also fixed a latent index bug: `compute()` now excludes missing signals
from the weighted average. (Follow-up idea: a `store.export_csv()` for the
human-greppable use if wanted; and B1's time-of-day baseline is now a cheap SQL
`GROUP BY`.)

### D3. Data retention  ✅ shipped
`store.prune(db, keep_days)` deletes samples older than `--retention-days`
(default 90, 0=off); the surge loop runs it every ~6h (and once at startup). No
VACUUM (it locks the DB) -- freed pages are reused by inserts, so the file
plateaus at ~the window instead of growing forever. Kept well above the 7-day
baseline lookback.

---

## Suggested order
**A1 (block-level data)** → **C4 (responsive)** → **D1 (tests)** → **B1
(self-calibrating)** → then C2 / A2 / C3 as appetite allows. A1 first because it
becomes the ground-truth anchor the algorithm work (B1–B3) builds on.
