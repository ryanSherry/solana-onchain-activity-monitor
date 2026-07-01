"""server._surge_context: percentile vs the trailing distribution, the cap at 99,
the is_peak flag, peak never below the live value, and the < 60-sample guard."""
import collections
import unittest

from . import _helper  # noqa: F401
import server


class SurgeContextTest(unittest.TestCase):
    def setUp(self):
        self._b, self._bs = server._baseline, server._baseline_sorted

    def tearDown(self):
        server._baseline, server._baseline_sorted = self._b, self._bs

    def _dist(self, scores, span_h=5.0):
        server._baseline_sorted = sorted(float(s) for s in scores)
        server._baseline = collections.deque(
            [(0.0, float(scores[0])), (span_h * 3600.0, float(scores[-1]))])

    def test_none_below_min_samples(self):
        self._dist(list(range(30)))                # n=30 < 60
        self.assertIsNone(server._surge_context(10))

    def test_none_when_cur_missing(self):
        self._dist(list(range(100)))
        self.assertIsNone(server._surge_context(None))

    def test_midrange_percentile_is_exact(self):
        self._dist(list(range(100)))               # 0..99, n=100
        c = server._surge_context(50)
        self.assertFalse(c["is_peak"])
        # EXACT 50: bisect_left counts the 50 values strictly below 50. Reverting
        # to bisect_right would give 51 here -- the whole point of the fix.
        self.assertEqual(c["percentile"], 50)
        self.assertEqual(c["max"], 99)

    def test_cap_at_99_on_tie(self):
        self._dist(list(range(100)))
        c = server._surge_context(99)              # ties the max
        self.assertEqual(c["percentile"], 99)      # capped, never a nonsense 100
        self.assertTrue(c["is_peak"])

    def test_fresh_high_peak_not_below_live(self):
        self._dist(list(range(100)))
        c = server._surge_context(250)             # above everything in the window
        self.assertLessEqual(c["percentile"], 99)
        self.assertTrue(c["is_peak"])
        self.assertGreaterEqual(c["max"], 250)     # peak reflects the live value


if __name__ == "__main__":
    unittest.main()
