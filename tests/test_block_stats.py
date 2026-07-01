"""block_stats: fill / non-vote failure rate / fee-per-CU math, vote filtering,
multi-block aggregation, skipped-slot step-back, result:null handling."""
import unittest

from . import _helper  # noqa: F401
import sources

VOTE = sources._VOTE_PROGRAM


def tx(cu, fee, err=None, vote=False, sigs=1, program=None):
    prog = VOTE if vote else (program or "Prog1111111111111111111111111111111111111111")
    return {
        "transaction": {"signatures": ["s"] * sigs,
                        "message": {"accountKeys": [prog],
                                    "instructions": [{"programIdIndex": 0}]}},
        "meta": {"computeUnitsConsumed": cu, "fee": fee, "err": err},
    }


class BlockStatsTest(unittest.TestCase):
    def setUp(self):
        self._orig = sources._rpc_call_one

    def tearDown(self):
        sources._rpc_call_one = self._orig

    def _patch(self, blocks_by_slot, slot=1000):
        def fake(url, method, params=None, timeout=20):
            if method == "getSlot":
                return slot
            if method == "getBlock":
                b = blocks_by_slot.get(params[0], "SKIP")
                if b == "SKIP":
                    raise sources.RpcAppError("slot skipped")   # mainnet -32007
                return b
            raise AssertionError("unexpected method " + method)
        sources._rpc_call_one = fake

    def test_single_block_math(self):
        blk = {"transactions": [
            tx(2100, 5000, vote=True),             # vote: excluded from nonvote/fee
            tx(100000, 5000, err=None),            # nonvote ok, priority 0
            tx(50000, 55000, err={"e": 1}),        # nonvote FAIL, priority 50000
        ]}
        self._patch({1000: blk})
        r = sources.block_stats("u", blocks=1)
        self.assertEqual(r["block_count"], 1)
        self.assertEqual(r["block_txs"], 3)
        self.assertEqual(r["block_nonvote"], 2)                # vote excluded
        self.assertEqual(r["block_slot"], 1000)
        # fill uses ALL cu incl vote: (2100+100000+50000)/48e6
        self.assertEqual(r["block_fill"], round(152100 / 48_000_000, 4))
        self.assertEqual(r["block_fail_rate"], 0.5)            # 1 of 2 nonvote failed
        # priority-fee/CU (µlam/CU): ok=0/100000*1e6=0, fail=50000/50000*1e6=1e6
        self.assertEqual(r["block_fee_cu_p50"], 1000000)
        self.assertEqual(r["block_fee_cu_p90"], 1000000)

    def test_fee_per_cu_percentiles_and_multisig(self):
        # 4 nonvote txs with DISTINCT priority-fee/CU so p50 != p90, plus a 2-sig
        # tx to exercise the per-signature base-fee strip (5000 * len(sigs)).
        txs = [
            tx(100000, 5000 + 100000, sigs=1),         # priority 100000 -> 1e6 µlam/CU
            tx(100000, 5000 + 200000, sigs=1),         # -> 2e6
            tx(100000, 5000 + 300000, sigs=1),         # -> 3e6
            tx(100000, 10000 + 400000, sigs=2),        # base 5000*2=10000; 400000 -> 4e6
        ]
        self._patch({1000: {"transactions": txs}})
        r = sources.block_stats("u", blocks=1)
        # sorted [1e6,2e6,3e6,4e6]: p50 idx int(.5*4)=2 -> 3e6; p90 idx int(.9*4)=3 -> 4e6
        self.assertEqual(r["block_fee_cu_p50"], 3_000_000)
        self.assertEqual(r["block_fee_cu_p90"], 4_000_000)   # multisig priced right

    def test_multi_block_aggregation_pools_txs(self):
        blk = {"transactions": [tx(100000, 5000), tx(100000, 5000, err={"e": 1})]}
        self._patch({1000: blk, 999: blk, 998: blk})
        r = sources.block_stats("u", blocks=3)
        self.assertEqual(r["block_count"], 3)
        self.assertEqual(r["block_nonvote"], 6)               # 2 nonvote x 3 blocks
        self.assertEqual(r["block_fail_rate"], 0.5)

    def test_steps_back_over_skipped_slots(self):
        blk = {"transactions": [tx(100000, 5000)]}
        self._patch({998: blk})                                # 1000, 999 skipped
        r = sources.block_stats("u", blocks=1)
        self.assertEqual(r["block_slot"], 998)

    def test_result_null_steps_back(self):
        blk = {"transactions": [tx(100000, 5000)]}

        def fake(url, method, params=None, timeout=20):
            if method == "getSlot":
                return 1000
            return None if params[0] == 1000 else blk      # null on the top slot
        sources._rpc_call_one = fake
        r = sources.block_stats("u", blocks=1)
        self.assertEqual(r["block_slot"], 999)

    def test_venue_counts_tally_excludes_vote_and_infra(self):
        pump = sources.HOT_VENUES["pump.fun"]
        newdex = "NewMemeDex1111111111111111111111111111111111"
        blk = {"transactions": [
            tx(100000, 5000, program=pump),        # tracked venue x2
            tx(100000, 5000, program=pump),
            tx(100000, 5000, program=newdex),      # untracked
            tx(2100, 5000, vote=True),             # vote -> not tallied
            tx(100000, 5000, program="ComputeBudget111111111111111111111111111111"),  # infra
        ]}
        self._patch({1000: blk})
        vc = sources.block_stats("u", blocks=1)["venue_counts"]
        self.assertEqual(vc[pump], 2)                          # counted per program
        self.assertEqual(vc[newdex], 1)
        self.assertNotIn(VOTE, vc)                             # vote excluded
        self.assertNotIn("ComputeBudget111111111111111111111111111111", vc)  # infra excluded

    def test_all_skipped_returns_empty(self):
        self._patch({})                                        # every getBlock raises
        self.assertEqual(sources.block_stats("u", blocks=1), {})

    def test_getslot_failure_returns_empty(self):
        def fake(url, method, params=None, timeout=20):
            raise ConnectionError("down")
        sources._rpc_call_one = fake
        self.assertEqual(sources.block_stats("u"), {})

    def test_partial_aggregate_divides_by_actual_count(self):
        # only 2 of the requested 3 blocks are available -> fill averaged over 2
        blk = {"transactions": [tx(24_000_000, 5000)]}        # half-full block
        self._patch({1000: blk, 999: blk})                     # 998.. skipped
        r = sources.block_stats("u", blocks=3)
        self.assertEqual(r["block_count"], 2)
        self.assertEqual(r["block_fill"], round(48_000_000 / (2 * 48_000_000), 4))  # 0.5


if __name__ == "__main__":
    unittest.main()
