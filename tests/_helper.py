"""Put the repo root on sys.path so tests import server/monitor/sources/store
regardless of how they're invoked."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
