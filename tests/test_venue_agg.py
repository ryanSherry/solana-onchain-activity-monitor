"""server._aggregate_venue_top: sum per-sample program counts over the rolling
window into a STABLE busiest-programs list, named if tracked (venue) else None
(untracked). No auto-flag -- aggregators/perps rank high but must not be added,
so the operator judges."""
import unittest

from . import _helper  # noqa: F401
import server
import sources


class VenueAggTest(unittest.TestCase):
    def test_sums_across_samples_and_names_tracked(self):
        pump = sources.HOT_VENUES["pump.fun"]
        samples = [
            {pump: 2, "Untrk1111111111111111111111111111111111111": 5},
            {pump: 3, "Untrk1111111111111111111111111111111111111": 1,
             "Untrk2222222222222222222222222222222222222": 4},
        ]
        top = {v["program"]: v for v in server._aggregate_venue_top(samples)}
        self.assertEqual(top[pump]["txs"], 5)                 # 2 + 3 across window
        self.assertEqual(top[pump]["venue"], "pump.fun")      # tracked -> named
        self.assertEqual(top["Untrk1111111111111111111111111111111111111"]["txs"], 6)  # 5 + 1
        self.assertIsNone(top["Untrk1111111111111111111111111111111111111"]["venue"])  # untracked

    def test_ranks_by_total_and_caps_at_12(self):
        samples = [{f"P{i:043d}": (20 - i) for i in range(20)}]
        top = server._aggregate_venue_top(samples)
        self.assertEqual(len(top), 12)                        # top 12 only
        self.assertEqual(top[0]["program"], "P" + "0" * 43)  # busiest (i=0 -> 20 tx)
        txs = [v["txs"] for v in top]
        self.assertEqual(txs, sorted(txs, reverse=True))      # descending

    def test_empty_window(self):
        self.assertEqual(server._aggregate_venue_top([]), [])


if __name__ == "__main__":
    unittest.main()
