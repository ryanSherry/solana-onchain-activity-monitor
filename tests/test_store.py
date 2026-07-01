"""store.py: append/read round-trip, time-range slices, NULL-ts exclusion,
schema evolution (ALTER ADD COLUMN), and legacy-CSV import (idempotent + skips
a corrupt file)."""
import contextlib
import io
import os
import shutil
import tempfile
import time
import unittest

from . import _helper  # noqa: F401
import monitor
import store


def _now_ts():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = os.path.join(self.dir, "t.db")
        store._conns.clear()          # isolate the connection cache per test

    def tearDown(self):
        for c in store._conns.values():
            c.close()
        store._conns.clear()
        shutil.rmtree(self.dir, ignore_errors=True)

    def _row(self, ts, score, **kw):
        r = {"timestamp_utc": ts, "surge_score": score, "surge_level": "CALM"}
        r.update(kw)
        return r

    def test_append_read_roundtrip_and_coercion(self):
        store.append(self.db, self._row(_now_ts(), 5, nonvote_tps=1200.5))
        rows = store.read_recent(self.db, 10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["surge_score"], 5.0)        # numeric coerced
        self.assertEqual(rows[0]["nonvote_tps"], 1200.5)
        self.assertEqual(rows[0]["surge_level"], "CALM")     # text preserved

    def test_read_since_window(self):
        store.append(self.db, self._row(_now_ts(), 7))                 # recent
        store.append(self.db, self._row("2020-01-01T00:00:00Z", 3))    # old
        rows = store.read_since(self.db, 3600)
        self.assertEqual([r["surge_score"] for r in rows], [7.0])      # old excluded

    def test_read_scores_since_excludes_null_score(self):
        store.append(self.db, self._row(_now_ts(), 8))
        store.append(self.db, self._row(_now_ts(), None))             # no surge_score
        pairs = store.read_scores_since(self.db, 3600)
        self.assertEqual([s for _, s in pairs], [8.0])
        self.assertTrue(all(isinstance(t, int) for t, _ in pairs))    # epoch ints

    def test_read_recent_excludes_null_ts(self):
        store.append(self.db, self._row(_now_ts(), 5))
        store.append(self.db, self._row("not-a-timestamp", 9))       # -> ts NULL
        rows = store.read_recent(self.db, 10)
        self.assertEqual([r["surge_score"] for r in rows], [5.0])     # NULL-ts gone

    def test_schema_add_column(self):
        store.append(self.db, self._row(_now_ts(), 5))               # table created
        orig = monitor.CSV_FIELDS
        try:
            monitor.CSV_FIELDS = orig + ["new_signal"]               # a new column
            store._conns.clear()                                     # force re-ensure
            store.append(self.db, {"timestamp_utc": _now_ts(),
                                   "surge_score": 6, "new_signal": 1.5})
            rows = store.read_recent(self.db, 10)
            self.assertIn("new_signal", rows[-1])
            self.assertEqual(rows[-1]["new_signal"], 1.5)
        finally:
            monitor.CSV_FIELDS = orig
            store._conns.clear()

    def test_import_csvs_idempotent(self):
        path = os.path.join(self.dir, "activity_20260101.csv")
        with open(path, "w", newline="") as f:
            f.write("timestamp_utc,surge_score\n%s,5\n%s,6\n" % (_now_ts(), _now_ts()))
        self.assertEqual(store.import_csvs(self.db, self.dir), 2)
        self.assertEqual(store.import_csvs(self.db, self.dir), 0)     # no-op 2nd time
        self.assertEqual(len(store.read_recent(self.db, 10)), 2)

    def test_import_skips_corrupt_file(self):
        with open(os.path.join(self.dir, "activity_bad.csv"), "wb") as f:
            f.write(b"timestamp_utc,surge_score\n\xff\xfe bad\x00\n")   # non-utf8 + NUL
        good = os.path.join(self.dir, "activity_20260102.csv")
        with open(good, "w") as f:
            f.write("timestamp_utc,surge_score\n%s,4\n" % _now_ts())
        with contextlib.redirect_stdout(io.StringIO()) as out:         # mutes the warn
            n = store.import_csvs(self.db, self.dir)                    # must NOT raise
        self.assertEqual(n, 1)                                         # good file only
        self.assertIn("skipping unreadable", out.getvalue())           # warned once
        # and NO phantom rows from the corrupt file were persisted
        self.assertEqual(len(store.read_recent(self.db, 10)), 1)


if __name__ == "__main__":
    unittest.main()
