"""Surge Index: _heat mapping, level thresholds, and SurgeTracker.compute's
missing-signal exclusion + baseline dedup (both fixed real bugs)."""
import unittest

from . import _helper  # noqa: F401
import monitor


class HeatTest(unittest.TestCase):
    def test_heat_mapping(self):
        self.assertEqual(monitor._heat(None, 1.0), 0.0)     # missing value
        self.assertEqual(monitor._heat(10, 0), 0.0)          # zero baseline guarded
        self.assertEqual(monitor._heat(1.0, 1.0), 0.0)       # 1x baseline -> 0
        self.assertEqual(monitor._heat(2.0, 1.0), 50.0)      # 2x -> 50
        self.assertEqual(monitor._heat(3.0, 1.0), 100.0)     # 3x -> 100
        self.assertEqual(monitor._heat(9.0, 1.0), 100.0)     # capped at 100
        self.assertEqual(monitor._heat(0.5, 1.0), 0.0)       # below baseline -> 0


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
