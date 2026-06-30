"""
Real-time pump.fun activity stream via the free pumpportal.fun websocket.

Runs in a background daemon thread and maintains rolling 60s counts of:
  - launches    (new token creations)         subscribeNewToken
  - graduations (migrations to a DEX)          subscribeMigration

The main monitor reads `.rates()` once per tick. Launch rate is the leading
edge of a meme surge -- it climbs before network congestion peaks.

NOTE: a global trade-rate is intentionally NOT collected. pumpportal's free
tier has no all-trades firehose; subscribing to trades of fresh mints yields
~nothing (most new tokens get the creator's initial buy and then die). Trade
activity on the tokens that actually drive volume is covered instead by
GeckoTerminal's 5m txn counts in the movers section of the monitor.

Zero dependencies: a minimal RFC6455 client is implemented over ssl sockets so
the monitor still runs with no `pip install`. Single known endpoint, text
frames only, with reconnect/backoff.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import ssl
import struct
import threading
import time
from collections import deque

WS_HOST = "pumpportal.fun"
WS_PORT = 443
WS_PATH = "/api/data"

WINDOW_SECS = 60      # rolling window for per-minute rates


# --------------------------------------------------------------------------- #
# Minimal websocket client (RFC6455, client side)
# --------------------------------------------------------------------------- #
class _WSConn:
    def __init__(self, host, port, path, timeout=20):
        raw = socket.create_connection((host, port), timeout=timeout)
        ctx = ssl.create_default_context()
        self.sock = ctx.wrap_socket(raw, server_hostname=host)
        self.sock.settimeout(timeout)
        self._buf = b""
        self._handshake(host, path)

    def _handshake(self, host, path):
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"Origin: https://{host}\r\n\r\n"
        )
        self.sock.sendall(req.encode())
        # read headers up to the blank line
        while b"\r\n\r\n" not in self._buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("ws handshake: connection closed")
            self._buf += chunk
        head, self._buf = self._buf.split(b"\r\n\r\n", 1)
        if b" 101 " not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"ws handshake failed: {head[:80]!r}")

    def _recv_exact(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("ws: connection closed")
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _recv_frame(self):
        b0, b1 = self._recv_exact(2)
        fin = b0 & 0x80
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F
        if length == 126:
            length = struct.unpack(">H", self._recv_exact(2))[0]
        elif length == 127:
            length = struct.unpack(">Q", self._recv_exact(8))[0]
        mask = self._recv_exact(4) if masked else b""
        payload = self._recv_exact(length) if length else b""
        if masked:
            payload = bytes(c ^ mask[i % 4] for i, c in enumerate(payload))
        return fin, opcode, payload

    def _send_frame(self, payload: bytes, opcode=0x1):
        header = bytearray([0x80 | opcode])
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        mask = os.urandom(4)
        header += mask
        self.sock.sendall(bytes(header) + bytes(c ^ mask[i % 4]
                                                for i, c in enumerate(payload)))

    def send_text(self, text: str):
        self._send_frame(text.encode())

    def recv_message(self) -> str | None:
        """Return one complete text message, handling control + fragmented frames."""
        buf = bytearray()
        data_op = None
        while True:
            fin, opcode, payload = self._recv_frame()
            if opcode == 0x8:                       # close
                raise ConnectionError("ws closed by server")
            if opcode == 0x9:                       # ping -> pong
                self._send_frame(payload, opcode=0xA)
                continue
            if opcode == 0xA:                       # pong
                continue
            if opcode != 0x0:                       # new data frame
                data_op = opcode
            buf += payload
            if fin:
                if data_op == 0x1:
                    return buf.decode("utf-8", "replace")
                buf, data_op = bytearray(), None    # ignore binary

    def close(self):
        try:
            self._send_frame(b"", opcode=0x8)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Stream
# --------------------------------------------------------------------------- #
class PumpStream(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._launches = deque()       # timestamps
        self._graduations = deque()
        self.connected = False
        self.last_event = 0.0

    # -- public ------------------------------------------------------------- #
    def rates(self) -> dict:
        now = time.time()
        with self._lock:
            self._trim(now)
            return {
                "pump_launches_min": len(self._launches),
                "pump_graduations_min": len(self._graduations),
                "pump_connected": self.connected,
            }

    def stop(self):
        self._stop.set()

    # -- internals ---------------------------------------------------------- #
    def _trim(self, now):
        cutoff = now - WINDOW_SECS
        for dq in (self._launches, self._graduations):
            while dq and dq[0] < cutoff:
                dq.popleft()

    def run(self):
        backoff = 1
        while not self._stop.is_set():
            conn = None
            try:
                conn = _WSConn(WS_HOST, WS_PORT, WS_PATH)
                conn.send_text(json.dumps({"method": "subscribeNewToken"}))
                conn.send_text(json.dumps({"method": "subscribeMigration"}))
                with self._lock:
                    self.connected = True
                backoff = 1
                while not self._stop.is_set():
                    try:
                        msg = conn.recv_message()
                    except socket.timeout:
                        continue
                    if msg:
                        self._on_message(msg)
            except Exception:
                pass
            finally:
                with self._lock:
                    self.connected = False
                if conn:
                    conn.close()
            if self._stop.is_set():
                break
            time.sleep(min(backoff, 30))
            backoff *= 2

    def _on_message(self, msg: str):
        try:
            data = json.loads(msg)
        except ValueError:
            return
        if not isinstance(data, dict):
            return
        tx = data.get("txType")
        now = time.time()
        if tx == "create":
            with self._lock:
                self._launches.append(now)
                self.last_event = now
        elif tx in ("migrate", "migration"):
            with self._lock:
                self._graduations.append(now)
                self.last_event = now
