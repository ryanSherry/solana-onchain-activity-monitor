# CLAUDE.md — Solana On-Chain Activity Monitor

Operating guide for Claude Code.
- **README.md** — full user-facing reference (setup, algorithm, data sources).
- **docs/NOTES.md** — design decisions & gotchas from the build. **Read this
  before changing the data model, the Surge Index, or the movers logic** — it
  explains the non-obvious stuff (esp. why the Surge Index and Top Movers
  measure different things, and why a surge often isn't "coins pumping up").

This is a **dependency-free** (Python 3.9+ stdlib only) real-time dashboard that
tracks whether Solana is surging — a composite 0–100 **Surge Index**, live
charts, Jito tip floor, and the top upward-moving meme coins. Purpose: warn a
transaction-landing operator before meme-coin frenzies exhaust their rate limits.

---

## First-time setup on a fresh clone

Do these in order. It's intentionally tiny — there is nothing to install.

1. **Check Python:** `python3 --version` (need ≥ 3.9). **Do not** `pip install`
   anything — this project is standard-library only by design.

2. **Configure the RPC URL** (the only setup step). The app needs a Solana RPC
   endpoint that can handle ~16 calls/5s.
   - If `SOLANA_RPC` is already exported in the shell, you're done — skip ahead.
   - Otherwise: `cp .env.onchain-activity.example .env.onchain-activity`, then
     **ask the user for their Solana RPC URL** (a HelloMoon / Helius / Triton
     node) and write it as `SOLANA_RPC=<url>` in that file. This file is
     gitignored — the secret never enters the repo.
   - If the user can't provide one, the app still runs on the public endpoint
     (`api.mainnet-beta.solana.com`) but it's heavily rate-limited and the
     dashboard will be flaky. The server prints a warning in this case.
   - **Optional failover:** set `SOLANA_RPC_FALLBACKS=<url2>, <url3>` (ordered,
     comma/space-separated backups) for an automatic failover pool. See README
     "RPC failover" and docs/NOTES.md §4 (`sources.RpcPool`). Region labels for
     the badge/logs live in `sources.RPC_REGIONS`.

3. **Start it** (background):
   ```bash
   nohup python3 server.py --port 8888 > /tmp/onchain-dash.log 2>&1 &
   ```

4. **Verify:** wait ~10s, then
   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8888/        # expect 200
   curl -s http://127.0.0.1:8888/api/data | python3 -c "import sys,json;j=json.load(sys.stdin);print('surge',j['latest'].get('surge_score'),j['latest'].get('surge_level'),'| venues',len(j['meme_by_program']),'| sol',j.get('sol',{}).get('price'))"
   ```
   Then tell the user to open **http://127.0.0.1:8888**.

---

## Run / stop / inspect

| Action | Command |
|--------|---------|
| Start (foreground) | `python3 server.py` |
| Start (background) | `nohup python3 server.py --port 8888 > /tmp/onchain-dash.log 2>&1 &` |
| Stop | `pkill -f "server.py --port 8888"` |
| Logs | `cat /tmp/onchain-dash.log` |
| Live data (JSON) | `curl -s http://127.0.0.1:8888/api/data \| python3 -m json.tool \| head -40` |
| Terminal-only version | `python3 monitor.py` (no browser; prints every 60s) |
| Custom cadence/port | `python3 server.py --port 9000 --interval 5 --movers-interval 15` |
| **Run tests** | `python3 -m unittest discover -t . -s tests` (stdlib only, no network) |

To screenshot/verify the UI, drive it with Playwright if available (a headless
`chromium.launch()` → `goto('http://127.0.0.1:8888')` → `screenshot`). Check for
`pageerror`/console errors.

---

## Files

| File | Role |
|------|------|
| `server.py` | dashboard backend: stdlib `http.server` + background loops (fast surge, slow movers, slow block sampler) |
| `dashboard.html` | the page — self-contained, hand-drawn canvas charts, no CDN/JS deps |
| `monitor.py` | terminal version **and** the Surge Index algorithm (`SurgeTracker`, `SURGE_SIGNALS`, `_LEVELS`) |
| `sources.py` | all data fetchers + config (`_load_local_env`, `RpcPool`, `HOT_VENUES`, `block_stats`) |
| `store.py` | SQLite persistence (stdlib `sqlite3`) — sample history, baselines, percentile |
| `tests/` | stdlib `unittest` suite (RpcPool, block_stats, Surge Index, store, surge_context) — no network, fakes injected |
| `pumpstream.py` | pump.fun launch/graduation websocket (hand-written RFC6455 client) |
| `data/` | `monitor.db` SQLite history (gitignored) — warms the Surge Index baselines on startup. Legacy per-day CSVs, if present, are imported once then unused. |

---

## Conventions when editing

- **Zero dependencies.** Standard library only. Never add a pip package; if a
  feature seems to need one (e.g. a websocket), hand-roll it with stdlib (see
  `pumpstream.py`).
- **Surge Index tuning lives in `monitor.py`:** `SURGE_SIGNALS` (signal weights +
  seed baselines) and `_LEVELS` (thresholds). They're a prior, not calibrated to
  real rate-limit events.
- **Adding a DEX venue:** ALWAYS verify the program ID returns a real tx rate
  first — `getSignaturesForAddress(pid, {limit:1000})`, compute sigs ÷ slot-span.
  We rank venues by **transaction count, not USD volume** (each tx = a landing
  attempt). Only then add to `sources.HOT_VENUES`. Per-venue RPC calls are
  parallelized (`ThreadPoolExecutor`) so the 5s tick holds.
- **Cadences** (in `server.py`): surge 5s · movers 15s · Jito 10s · skip rate
  15s · SOL price 45s · pump launches continuous. Loops are period-accurate.
- **The RPC URL is a secret.** Keep it in `.env.onchain-activity` (gitignored);
  never hardcode/commit it.
- **Verify before claiming done:** hit `/api/data` and/or screenshot the UI.

## Data sources (all free; RPC is the only one needing a URL/token)

HelloMoon RPC (TPS, fees, 9-venue trade/fail rate, skip rate) · pumpportal.fun
websocket (launches) · Jito `bundles.jito.wtf` (tip floor) · GeckoTerminal
(movers) · CoinGecko (SOL price). See README "Data sources" for the full table.
