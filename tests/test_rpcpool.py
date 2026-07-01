"""RpcPool: failover on transport errors, NO failover on app errors, cooldown
skip, self-healing failback, all-down propagation."""
import time
import unittest

from . import _helper  # noqa: F401  (sys.path setup)
import sources


class RpcPoolTest(unittest.TestCase):
    def setUp(self):
        self._orig = sources._rpc_call_one
        self.urls = {"A": "https://a/x", "B": "https://b/x", "C": "https://c/x"}
        self.mode = {}          # name -> 'ok' | 'app' | 'transport'
        self.calls = []

        def fake(url, method, params=None, timeout=20):
            name = next(k for k, v in self.urls.items() if v == url)
            self.calls.append(name)
            m = self.mode.get(name, "ok")
            if m == "app":
                raise sources.RpcAppError("bad params")
            if m == "transport":
                raise ConnectionError("node down")
            return "ok-" + name
        sources._rpc_call_one = fake

    def tearDown(self):
        sources._rpc_call_one = self._orig

    def _pool(self):
        p = sources.RpcPool([self.urls["A"], self.urls["B"], self.urls["C"]])
        p.BASE_COOLDOWN = 1.0   # keep the failback test fast
        return p

    def test_prefers_primary(self):
        p = self._pool()
        self.assertEqual(p.call("m"), "ok-A")
        self.assertEqual(self.calls, ["A"])

    def test_failover_on_transport_error(self):
        self.mode["A"] = "transport"
        p = self._pool()
        self.assertEqual(p.call("m"), "ok-B")
        self.assertEqual(self.calls, ["A", "B"])          # tried A, fell to B
        self.assertFalse(p.status()[0]["healthy"])         # A cooled

    def test_app_error_does_not_failover_or_cool(self):
        self.mode["A"] = "app"
        p = self._pool()
        with self.assertRaises(sources.RpcAppError):
            p.call("m")
        self.assertEqual(self.calls, ["A"])                # did NOT try B
        self.assertTrue(p.status()[0]["healthy"])          # A NOT cooled

    def test_skips_cooling_node(self):
        self.mode["A"] = "transport"
        p = self._pool()
        p.call("m")                                        # cools A, lands on B
        self.calls.clear()
        self.assertEqual(p.call("m"), "ok-B")
        self.assertEqual(self.calls, ["B"])                # A skipped while cooling

    def test_failback_after_cooldown(self):
        self.mode["A"] = "transport"
        p = self._pool()
        p.call("m")                                        # A cooled 1s
        self.mode["A"] = "ok"
        time.sleep(1.1)                                    # cooldown lapses
        self.calls.clear()
        self.assertEqual(p.call("m"), "ok-A")              # primary reclaimed
        self.assertEqual(self.calls[0], "A")

    def test_all_down_raises_last_error(self):
        for k in self.urls:
            self.mode[k] = "transport"
        p = self._pool()
        with self.assertRaises(ConnectionError):
            p.call("m")
        self.assertEqual(self.calls, ["A", "B", "C"])      # tried every endpoint

    def test_single_endpoint_string_bypasses_pool(self):
        # rpc_call with a plain URL string dispatches straight to _rpc_call_one
        self.assertEqual(sources.rpc_call(self.urls["A"], "m"), "ok-A")


if __name__ == "__main__":
    unittest.main()
