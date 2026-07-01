"""Surge Index: _heat mapping, level thresholds, and SurgeTracker.compute's
missing-signal exclusion + baseline dedup (both fixed real bugs)."""
import unittest

from . import _helper  # noqa: F401
import monitor


class HeatTest(unittest.TestCase):
    def test_heat_zscore_mapping(self):
        # _heat(value, center, scale): robust-sigmas above center -> 0..100
        self.assertEqual(monitor._heat(None, 1.0, 1.0), 0.0)  # missing value
        self.assertEqual(monitor._heat(10, 1.0, 0), 0.0)      # zero scale guarded
        self.assertEqual(monitor._heat(1.0, 1.0, 1.0), 0.0)   # 0 sigma -> 0
        self.assertEqual(monitor._heat(2.5, 1.0, 1.0), 50.0)  # +1.5 sigma -> 50
        self.assertEqual(monitor._heat(4.0, 1.0, 1.0), 100.0) # +3 sigma -> 100
        self.assertEqual(monitor._heat(10.0, 1.0, 1.0), 100.0)  # capped
        self.assertEqual(monitor._heat(0.5, 1.0, 1.0), 0.0)   # below center -> 0

    def test_variance_aware(self):
        # the SAME move (+150 over each ~median) is HOTTER for a steadier signal --
        # and both signals are driven through the MAD path (scale > the 5%-of-center
        # floor), so this genuinely exercises the variance mechanism, not the floor.
        t = monitor.SurgeTracker()
        for v in (920, 940, 960, 980, 1000, 1020, 1040, 1060, 1080, 1100):
            t.update({"nonvote_tps": float(v)})               # moderate spread
        for v in (600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500):
            t.update({"meme_tps": float(v)})                  # wide spread
        _, _, comps = t.compute({"nonvote_tps": 1160.0, "meme_tps": 1200.0})  # ~+150 each
        a, b = comps["nonvote_tps"], comps["meme_tps"]
        self.assertGreater(a["scale"], 0.05 * a["baseline"])  # MAD, not the floor
        self.assertGreater(b["scale"], 0.05 * b["baseline"])
        self.assertGreater(a["heat"], b["heat"])              # steadier -> hotter


class LevelTest(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(monitor.level_for(100), "SURGE")
        self.assertEqual(monitor.level_for(60), "SURGE")
        self.assertEqual(monitor.level_for(59), "ELEVATED")
        self.assertEqual(monitor.level_for(38), "ELEVATED")
        self.assertEqual(monitor.level_for(18), "BUSY")
        self.assertEqual(monitor.level_for(0), "CALM")


class TrackerTest(unittest.TestCase):
    def _keys(self):
        return [k for k, _, _ in monitor.SURGE_SIGNALS]

    def test_all_missing_gives_zero_not_crash(self):
        t = monitor.SurgeTracker()
        idx, lvl, comps = t.compute({})                      # no signals present
        self.assertEqual(idx, 0)                             # no ZeroDivisionError
        self.assertEqual(lvl, "CALM")
        self.assertTrue(all(not c["present"] for c in comps.values()))

    def test_real_zero_is_present(self):
        t = monitor.SurgeTracker()
        _, _, comps = t.compute({k: 0.0 for k in self._keys()})
        self.assertTrue(all(c["present"] for c in comps.values()))  # 0.0 != missing

    def test_baseline_dedups_carried_forward_values(self):
        t = monitor.SurgeTracker()
        for _ in range(20):                                  # same value 20 ticks
            t.update({"nonvote_tps": 1000.0})
        self.assertEqual(len(t.hist["nonvote_tps"]), 1)      # only 1 distinct kept
        for v in range(1, 9):                                # 8 distinct values
            t.update({"nonvote_tps": float(v)})
        self.assertEqual(len(t.hist["nonvote_tps"]), 9)

    def test_baselines_override_and_fallback(self):
        t = monitor.SurgeTracker()
        override = {"nonvote_tps": (1000.0, 100.0)}   # time-of-day (center, sigma)
        _, _, comps = t.compute(
            {"nonvote_tps": 1300.0, "meme_tps": 5000.0}, baselines=override)
        a = comps["nonvote_tps"]
        self.assertEqual(a["source"], "hour")         # used the override...
        self.assertEqual(a["baseline"], 1000.0)       # ...its center
        self.assertEqual(a["heat"], 100)              # z=(1300-1000)/100=3 -> 100
        # a signal with no override falls back to the rolling window
        self.assertEqual(comps["meme_tps"]["source"], "window")

    def test_missing_signals_do_not_deflate_index(self):
        t = monitor.SurgeTracker()
        for v in (100, 110, 90, 105, 95, 102, 98, 101):      # warm one baseline ~100
            t.update({"nonvote_tps": float(v)})
        idx, _, comps = t.compute({"nonvote_tps": 1e5})      # >> baseline -> heat caps 100
        self.assertEqual(comps["nonvote_tps"]["heat"], 100)
        # index == the sole present signal's heat: absent signals didn't dilute it
        self.assertEqual(idx, comps["nonvote_tps"]["heat"])


if __name__ == "__main__":
    unittest.main()
