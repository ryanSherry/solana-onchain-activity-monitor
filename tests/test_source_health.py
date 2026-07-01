"""server._source_health: per-source freshness (fresh < 3x cadence, stale < 6x,
else down) so a frozen last-known-good feed is visible, not silently trusted."""
import time
import unittest

from . import _helper  # noqa: F401
import server


class SourceHealthTest(unittest.TestCase):
    def test_status_by_age(self):
        now = time.time()
        snap = {
            "updated_at": now - 4,          # RPC cadence 5  -> fresh (<15)
            "jito_at": now - 40,            # Jito cadence 10 -> stale (30..60)
            "movers_updated_at": now - 200, # Movers cadence 15 -> down (>=90)
            "block_at": None,               # never sampled -> waiting
            "sol_at": now - 100,            # SOL cadence 45 -> fresh (<135)
        }
        h = {s["name"]: s for s in server._source_health(snap, pump_connected=True)}
        self.assertEqual(h["Surge loop"]["status"], "fresh")
        self.assertEqual(h["Jito tips"]["status"], "stale")
        self.assertEqual(h["Movers (Gecko)"]["status"], "down")
        self.assertEqual(h["Block data"]["status"], "waiting")
        self.assertEqual(h["Block data"]["age_s"], None)
        self.assertEqual(h["SOL price"]["status"], "fresh")
        self.assertEqual(h["pump.fun stream"]["status"], "fresh")

    def test_pump_and_empty(self):
        h = {s["name"]: s for s in server._source_health({}, pump_connected=False)}
        self.assertEqual(h["pump.fun stream"]["status"], "down")   # not connected
        self.assertEqual(h["Surge loop"]["status"], "waiting")     # no timestamp yet

    def test_cadence_from_configured_interval(self):
        # with --interval 30 the loop bumps updated_at ~every 30s; a 30s age must
        # still read fresh (cadence follows the config, not a hardcoded 5)
        now = time.time()
        h = {s["name"]: s for s in server._source_health(
            {"interval": 30, "updated_at": now - 30}, pump_connected=True)}
        self.assertEqual(h["Surge loop"]["status"], "fresh")       # 30 < 3*30=90

    def test_disabled_source_omitted(self):
        h = {s["name"]: s for s in server._source_health(
            {"block_interval": 0}, pump_connected=True)}
        self.assertNotIn("Block data", h)                          # block sampling off


if __name__ == "__main__":
    unittest.main()
