# Solana On-Chain Activity Monitor

A self-contained dashboard that answers one question for a transaction-landing
operator: **"Is Solana surging right now, and is it about to stress our lander's
rate limits?"** When meme-coin trading floods the network, transactions get
expensive to land and rate limits get tested — this tool makes that visible in
real time, with a single tracked **Surge Index**, live charts, and the top
upward-moving coins driving the volume.

Built to be **dependency-free** (Python 3.9+ standard library only — no
`pip install`) and to run continuously on a laptop or small box.

---

## Quick start

```bash
cd ~/onchain-activity
python3 server.py                 # web dashboard -> http://127.0.0.1:8888
```

Open **http://127.0.0.1:8888** in a browser. That's it.

Other entry points:

```bash
python3 server.py --port 9000 --interval 5 --movers-interval 15
python3 monitor.py                # terminal version (no browser), prints once a minute
python3 monitor.py --once         # single snapshot then exit
```

Stop the server: `pkill -f "server.py --port 8888"` (or Ctrl-C if foreground).
Logs (when backgrounded): `/tmp/onchain-dash.log`.

---

## Setup & configuration

**No dependencies.** It uses only the Python standard library. If `python3`
runs, the app runs.

**Configuration is via one optional setting — the Solana RPC URL.** Resolution
order (first wins):

1. `--rpc <url>` command-line flag (accepts a comma/space list = primary first)
2. `SOLANA_RPC` (primary) + `SOLANA_RPC_FALLBACKS` (ordered backups) env vars
3. the same two keys in the `.env.onchain-activity` file in this folder
4. the hardcoded public fallback in `sources.py`

So out of the box it just works; you only need to set anything if you want a
different RPC endpoint.

### RPC failover (primary + backups)

For resilience you can give the app an **ordered pool** of RPC nodes instead of
one. `SOLANA_RPC` is the primary; `SOLANA_RPC_FALLBACKS` is a comma/space-
separated list of backups, highest priority first:

```bash
SOLANA_RPC=https://<primary-node>/<token>
SOLANA_RPC_FALLBACKS=https://<backup-1>/<token>, https://<backup-2>/<token>
```

Behaviour (`sources.RpcPool`): every call prefers the highest-priority node that
isn't in a failure cooldown; on a transport/availability error (timeout /
connection / HTTP 4xx-5xx incl. 429) it cools that node down briefly and falls
through to the next. JSON-RPC *application* errors (a node answering with an
`error` payload — e.g. an out-of-range query) are **not** treated as failover
events; they raise through without benching the node. A recovered node is
**automatically promoted back** to primary once its cooldown lapses — no manual
reset. Live status (which node is active, per-node health,
optional region label) is exposed at `/api/data` under `rpc[]` and shown as a
badge in the dashboard header. Leave `SOLANA_RPC_FALLBACKS` blank to run a single
node — behaviour is then identical to before.

### About the `.env` clash you asked about

You do **not** need a `.env`, and there is **no clash risk**, because:

- The only configurable secret is the RPC URL.
- Env files are **per-directory / per-app** — they are not global. A file in
  this project's folder is read only by this project.

If you still want to externalize the RPC URL into a file (recommended, so it's
not in source), copy the example to a **namespaced** filename that can't be
confused with any other `.env` on the machine:

```bash
cp .env.onchain-activity.example .env.onchain-activity
# then edit .env.onchain-activity and set SOLANA_RPC=...
```

`.env.onchain-activity` is gitignored. The loader (`sources._load_local_env`)
reads it and sets the var **without overriding** anything already exported, so a
shell-level `SOLANA_RPC` always wins.

### Sensitive data

| Item | Where it is | Sensitivity |
|------|-------------|-------------|
| **HelloMoon RPC URL** | `sources.DEFAULT_RPC` (fallback) or `.env.onchain-activity` | **Secret** — the URL path embeds an access token. Treat the whole URL as a credential. |
| Everything else | — | None. CoinGecko, GeckoTerminal, Jito, and pumpportal are all keyless public endpoints. |

There are **no API keys, passwords, or tokens** beyond that one RPC URL. To move
machines cleanly: put the URL in `.env.onchain-activity` and scrub the fallback
in `sources.py` if you want zero secrets in source.

---

## How it works — the Surge Index

The headline number is a composite **0–100 Surge Index**. Every input signal is
measured as how far **above its own recent baseline** it sits ("heat", 0–100),
and the heats are weighted-averaged. Weights lean toward what predicts
rate-limit pain:

| Signal | Weight | Why |
|--------|-------:|-----|
| DEX trade rate (`meme_tps`) | 25 | swap-tx submission load across 9 venues |
| DEX failure rate (`meme_fail_rate`) | 20 | bots racing & losing = the flood shape |
| Network TPS (`nonvote_tps`) | 20 | overall network load |
| Fee contention (`fee_contention`) | 15 | how *often* there's competition to land |
| Landing fee p90 (`fee_p90`) | 12 | how expensive landing has become |
| Launch rate (`pump_launches_min`) | 8 | leading edge of a meme frenzy |

- **heat** = `clamp((value / baseline − 1) × 50, 0, 100)` → 1× baseline = 0,
  2× = 50, 3×+ = 100.
- **baseline** = rolling median of the last 120 samples (seeded with sane
  defaults until ~8 samples accumulate).
- **Levels:** `CALM` < 18 ≤ `BUSY` < 38 ≤ `ELEVATED` < 60 ≤ `SURGE`.

The weights and thresholds live in **`monitor.py` → `SURGE_SIGNALS` and
`_LEVELS`** and are a *reasoned prior, not yet calibrated to real rate-limit
events*. The highest-value next step is to overlay your lander's actual
throttling events and tune them to your data.

> **Skip rate** (`getBlockProduction`) is collected and displayed as a distinct
> network-health signal but is **not** in the index yet — it's a candidate to
> add after calibration.

---

## Data sources

| Source | Auth | Provides | Refresh |
|--------|------|----------|--------:|
| **HelloMoon RPC** (Solana) | URL token | TPS, priority fees (9 venues), trade + failure rate (9 venues), slot skip rate | 5s (skip rate 15s) |
| **pumpportal.fun** (websocket) | none | pump.fun launches/min + graduations/min | live (continuous) |
| **GeckoTerminal** | none | top movers (price, MC, volume, age, % change) | 15s |
| **Jito** (`bundles.jito.wtf`) | none | bundle tip floor (p50/p75/p95/p99, SOL) | 10s |
| **CoinGecko** | none | SOL price + 24h change | 45s |

RPC is the only authenticated source; the rest are free public endpoints.
GeckoTerminal's free limit is ~30 req/min — we use ~4/min.

### The 9 DEX venues (ranked by transaction count, not USD volume)

`pump.fun`, `pumpswap`, `raydium v4`, `raydium cpmm`, `raydium clmm`,
`meteora dlmm`, `meteora pools`, `meteora damm v2`, `orca`.

We rank by **tx count** because each transaction is a landing attempt — the
thing that stresses a lander. High-$/low-tx market-maker venues (SolFi, Phoenix,
OpenBook — all <5 tx/s despite large volume) are intentionally excluded: they'd
add double-counting noise without moving the landing-load signal.

---

## Dashboard guide

- **Header** — SOL price + 24h change (macro context).
- **Network Surge Level** — the gauge (fluid needle), the plain-English verdict
  ("Is the surge still surging?"), a **Recent (20-min)** chart and a **24-hour**
  chart (fixed window; empty space = time not yet collected; "peak N" marker).
  The green **"live · 5s"** badge means the gauge is updating.
- **Jito bundle tip floor** — what bundles are tipping (in SOL) to land right
  now, by percentile. The most lander-relevant external signal.
- **Surging meme coins** — only coins whose trading is straining the network
  with a genuine upward pump. **Surge** (0–5) is driven by 1h volume +
  transaction count (the load that hits the lander) plus momentum — *not* market
  cap, since the real surges are often low-MC explosions (see `docs/NOTES.md`
  §7). Coins fade out over ~a minute once they cool; an empty list means nothing
  is surging. **Age** color-coded (green = fresh → gray = established). 15s.
- **Technical details** (collapsed) — driver heat bars, per-metric sparkline
  cards (incl. slot skip rate), and the 9-venue trade/failure breakdown. Shows a
  freshness indicator only when expanded.

The browser tab title and favicon also reflect the level (🟢/🔵/🟠/🔴), so you
can read it at a glance when the tab is backgrounded.

---

## Performance & cadences

A surge tick makes ~16 RPC calls (1 TPS + 9 fee + 9 signature − overlap) **in
parallel** (`ThreadPoolExecutor`), finishing in ~1–2s, comfortably inside the 5s
interval. The loops are period-accurate (sleep = interval − work time).

- **surge gauge / charts / details:** every 5s (RPC; HelloMoon handles it easily)
- **movers:** every 15s (GeckoTerminal, well under its 30/min limit)
- **Jito tip floor:** 10s · **skip rate:** 15s · **SOL price:** 45s
- **pump launches:** continuous websocket
- **browser → server poll:** 4s (freshness counters tick every 1s)

Polling faster doesn't improve accuracy — the meme-rate window self-sizes to a
few seconds and Solana blocks are ~0.4s.

---

## Data & files

```
server.py        web dashboard backend (stdlib http.server + 2 background loops)
dashboard.html   the dashboard page (self-contained; canvas charts, no CDN)
monitor.py       terminal version + the Surge Index algorithm (SurgeTracker)
sources.py       all data fetchers (RPC, GeckoTerminal, Jito, CoinGecko) + config
store.py         SQLite persistence (stdlib sqlite3) — history/baselines/percentile
pumpstream.py    pump.fun launch/graduation websocket (hand-written RFC6455 client)
data/            monitor.db SQLite history (gitignored)
.env.onchain-activity[.example]   local config (RPC URL)
```

Both `server.py` and `monitor.py` write samples to the same SQLite DB
(`data/monitor.db`), so history persists across restarts and warms the Surge
Index baselines on startup. Any legacy `data/activity_<YYYYMMDD>.csv` files are
imported into the DB once on first run, then unused.

---

## Design decisions / FAQ

- **The network is SURGING but the Surging-coins panel is empty — bug?** No.
  The Surge Index is an **on-chain aggregate** (tx/fee/fail/launch rates across
  9 DEXes); the Surging-coins panel is a separate GeckoTerminal view scored to
  genuine upward pumps. They measure different things, and a surge is often
  **not** coins pumping up: much of it is failed txs (bots racing — we see
  40–90% failure rates), pump.fun launch frenzies (throwaway new tokens), and
  dumping (crashes generate huge volume but aren't surges). The venue/driver
  breakdown in Technical Details shows what's actually driving it.
  Token-level attribution of the surge is a known gap (see `docs/NOTES.md` §1).
- **Why not gRPC (Yellowstone/Geyser)?** A stream of every DEX transaction is a
  firehose (thousands/sec during a surge) that would need heavy server-side
  aggregation and could swamp a laptop. `getSignaturesForAddress` polling gives
  the rate cheaply. Revisit only with a narrowly-filtered subscription.
- **Why tx count, not USD volume, for DEX coverage?** Each tx is a landing
  attempt; that's what stresses a lander. A venue can have huge volume but few
  txs (big MM trades) and contribute almost nothing to landing load.
- **`getBlockProduction` lags the tip by ~12s — is that our node?** No, it's a
  property of that Solana method on every RPC; irrelevant for a 10-min skip-rate
  window (we just omit the explicit `lastSlot`).
- **Why does "total DEX tx/s" sometimes exceed network TPS?** It's a loose
  upper bound — multi-hop routes count at each venue they touch, and per-venue
  windows are sampled at slightly different instants. Trust the per-venue rates;
  the total is for at-a-glance trend only.

---

## Possible next steps

- **Calibrate the index** against your lander's real rate-limit events
  (Grafana/ScyllaDB) — turns "index 65" into "we throttle at 65." Highest value.
- **Telegram alert** when the index crosses a threshold for N minutes
  (hysteresis design ready; needs a bot token + chat ID).
- **Fold skip rate** into the index once calibrated.
