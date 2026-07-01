"""server._backoff_advice: the surge + RPC-health + skip-rate synthesis behind
/api/surge (the machine-readable backoff signal) and the dashboard action line."""
import unittest

from . import _helper  # noqa: F401
import server


def _rpc(rate_limited=0.0, p99=None, samples=100):
    return [{"active": True, "rate_limited": rate_limited,
             "lat_p99_ms": p99, "samples": samples}]


class BackoffAdviceTest(unittest.TestCase):
    def test_calm_is_normal(self):
        a = server._backoff_advice(
            {"surge_score": 4, "surge_level": "CALM", "skip_rate": 0.05}, _rpc())
        self.assertFalse(a["advise_backoff"])
        self.assertEqual(a["level"], "normal")
        self.assertLess(a["throttle_factor"], 0.2)

    def test_surge_drives_backoff(self):
        a = server._backoff_advice(
            {"surge_score": 64, "surge_level": "SURGE", "skip_rate": 0.05}, _rpc())
        self.assertTrue(a["advise_backoff"])
        self.assertGreaterEqual(a["throttle_factor"], 0.5)
        self.assertIn("surge", a["reason"])

    def test_ratelimit_drives_backoff_even_when_calm(self):
        # our own primary being throttled forces backoff regardless of surge
        a = server._backoff_advice(
            {"surge_score": 4, "surge_level": "CALM", "skip_rate": 0.05},
            _rpc(rate_limited=0.25))
        self.assertTrue(a["advise_backoff"])
        self.assertEqual(a["level"], "critical")
        self.assertIn("rate-limited", a["reason"])

    def test_thin_sample_rpc_is_ignored(self):
        # 50% 429 on only 3 recent calls must NOT escalate (small-sample guard)
        a = server._backoff_advice(
            {"surge_score": 4, "surge_level": "CALM", "skip_rate": 0.05},
            _rpc(rate_limited=0.5, samples=3))
        self.assertFalse(a["advise_backoff"])
        self.assertEqual(a["level"], "normal")

    def test_skip_rate_drives(self):
        a = server._backoff_advice(
            {"surge_score": 4, "surge_level": "CALM", "skip_rate": 0.20}, _rpc())
        self.assertTrue(a["advise_backoff"])
        self.assertIn("skip", a["reason"])

    def test_max_of_signals_picks_the_worst(self):
        # low surge but a moderately rate-limited primary -> 429 axis drives it
        a = server._backoff_advice(
            {"surge_score": 10, "surge_level": "CALM", "skip_rate": 0.05},
            _rpc(rate_limited=0.08))
        self.assertIn("rate-limited", a["reason"])

    def test_no_data(self):
        a = server._backoff_advice({}, [])
        self.assertEqual(a["throttle_factor"], 0.0)
        self.assertFalse(a["advise_backoff"])


if __name__ == "__main__":
    unittest.main()
