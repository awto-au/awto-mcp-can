#!/usr/bin/env python3
# Run with the free-threaded CPython build for real parallelism:
#   /usr/bin/python3.14t test_harness.py -v
# Fedora install: sudo dnf install python3.14-freethreading
"""
test_harness.py  —  self-contained test suite for awto-mcp-can.

Layers:
  1. Protocol unit tests (no I/O)
  2. CanWorker unit tests (python-can virtual bus, no kernel module needed)
  3. Integration: live daemon socket + concurrent clients

Run:
    python3 test_harness.py [-v]

No real hardware required — uses python-can's ``virtual`` bus.
"""

import json
import logging
import os
import socket
import sys
import sysconfig
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import can

sys.path.insert(0, str(Path(__file__).parent))

from protocol import (
    ERR_BUSDOWN,
    ERR_DENIED,
    ERR_FILTER,
    ERR_TIMEOUT,
    format_can_id,
    format_data,
    make_err,
    make_ok,
    parse_can_id,
    parse_data,
    parse_filters,
    recv_response,
    send_request,
)
from can_daemon import CanWorker, handle_client

logging.basicConfig(
    level=logging.WARNING,
    format="test[%(process)d]: %(levelname)-8s %(name)s: %(message)s",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _gil_status() -> str:
    try:
        enabled = sys._is_gil_enabled()  # type: ignore[attr-defined]
        return "ENABLED (classic GIL)" if enabled else "disabled (free-threaded)"
    except AttributeError:
        pass
    if sysconfig.get_config_var("Py_GIL_DISABLED"):
        return "disabled (free-threaded)"
    return "ENABLED (classic GIL)"


def _virtual_pair(channel: str | None = None) -> tuple[can.BusABC, can.BusABC]:
    """Two python-can virtual bus endpoints connected to the same channel."""
    chan = channel or f"awto-test-{os.getpid()}-{time.time_ns()}"
    a = can.Bus(channel=chan, bustype="virtual", preserve_timestamps=True)
    b = can.Bus(channel=chan, bustype="virtual", preserve_timestamps=True)
    return a, b


def _worker_with_virtual(allow: tuple[int, ...] | None = None) -> tuple[CanWorker, can.BusABC]:
    """Build a CanWorker driving one end of a virtual pair; return (worker, peer)."""
    chan = f"awto-test-{os.getpid()}-{time.time_ns()}"
    bus_w = can.Bus(channel=chan, bustype="virtual", preserve_timestamps=True)
    peer  = can.Bus(channel=chan, bustype="virtual", preserve_timestamps=True)
    worker = CanWorker(
        interface=chan, bitrate=250_000, bustype="virtual",
        tx_allow=allow if allow is not None else (),
        bus=bus_w,
    )
    return worker, peer


# ---------------------------------------------------------------------------
# Layer 1 — Protocol unit tests
# ---------------------------------------------------------------------------

class TestProtocol(unittest.TestCase):

    def test_make_ok(self):
        r = make_ok("pong")
        self.assertTrue(r["ok"])
        self.assertEqual(r["response"], "pong")

    def test_make_err_with_code(self):
        r = make_err("nope", code=ERR_BUSDOWN)
        self.assertFalse(r["ok"])
        self.assertEqual(r["error"], "nope")
        self.assertEqual(r["code"], ERR_BUSDOWN)

    def test_parse_can_id_hex(self):
        self.assertEqual(parse_can_id("6f0"), 0x6F0)
        self.assertEqual(parse_can_id("0x6F0"), 0x6F0)
        self.assertEqual(parse_can_id(0x7FF), 0x7FF)

    def test_parse_can_id_extended(self):
        self.assertEqual(parse_can_id("1abcdef0", ext=True), 0x1ABCDEF0)
        with self.assertRaises(ValueError):
            parse_can_id("800")  # >0x7FF for 11-bit

    def test_parse_can_id_bad(self):
        with self.assertRaises(ValueError):
            parse_can_id("xyz")
        with self.assertRaises(ValueError):
            parse_can_id("")

    def test_parse_data(self):
        self.assertEqual(parse_data("01e803"), b"\x01\xe8\x03")
        self.assertEqual(parse_data(""), b"")
        self.assertEqual(parse_data("01 e8 03"), b"\x01\xe8\x03")
        with self.assertRaises(ValueError):
            parse_data("0x1")  # odd length after stripping
        with self.assertRaises(ValueError):
            parse_data("zz")

    def test_format_helpers(self):
        self.assertEqual(format_can_id(0x6F0), "6f0")
        self.assertEqual(format_data(b"\x01\xe8\x03"), "01e803")

    def test_parse_filters(self):
        f = parse_filters([{"id": "6f0", "mask": "7ff"}])
        self.assertEqual(f, [{"can_id": 0x6F0, "can_mask": 0x7FF, "extended": False}])

    def test_parse_filters_bad(self):
        with self.assertRaises(ValueError):
            parse_filters([{"mask": "7ff"}])    # missing id
        with self.assertRaises(ValueError):
            parse_filters([{"id": "xyz"}])

    def test_send_recv_roundtrip(self):
        a, b = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            payload = make_ok("pong")
            a.sendall((json.dumps(payload) + "\n").encode())
            self.assertEqual(recv_response(b), payload)
        finally:
            a.close(); b.close()


# ---------------------------------------------------------------------------
# Layer 2 — CanWorker unit tests (virtual bus)
# ---------------------------------------------------------------------------

class TestCanWorker(unittest.TestCase):

    def setUp(self):
        # No allowlist by default for tests
        self.worker, self.peer = _worker_with_virtual(allow=())

    def tearDown(self):
        self.worker.close()
        self.peer.shutdown()

    def test_send_appears_on_peer(self):
        self.worker.send(0x6F0, b"\x01\xe8\x03")
        msg = self.peer.recv(timeout=0.5)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.arbitration_id, 0x6F0)
        self.assertEqual(bytes(msg.data), b"\x01\xe8\x03")

    def test_send_classic_dlc_limit(self):
        with self.assertRaises(ValueError):
            self.worker.send(0x100, b"\x00" * 9)

    def test_dry_run_does_not_transmit(self):
        self.worker.send(0x100, b"\x55", dry_run=True)
        msg = self.peer.recv(timeout=0.1)
        self.assertIsNone(msg)

    def test_recv_with_filter(self):
        # Send two frames from peer; only one matches filter.
        # NB: python-can Message.is_extended_id defaults to True, so explicit.
        self.peer.send(can.Message(arbitration_id=0x6F0, data=b"\x01", is_extended_id=False))
        self.peer.send(can.Message(arbitration_id=0x100, data=b"\x02", is_extended_id=False))
        frames = self.worker.recv(
            filters=[{"can_id": 0x6F0, "can_mask": 0x7FF, "extended": False}],
            max_frames=2, timeout_ms=300,
        )
        # The virtual backend honours filters; expect exactly one match.
        ids = [int(f["id"], 16) for f in frames]
        self.assertIn(0x6F0, ids)
        self.assertNotIn(0x100, ids)

    def test_request_returns_first_reply(self):
        # Background "responder" on the peer
        def _responder():
            msg = self.peer.recv(timeout=2)
            if msg is not None and msg.arbitration_id == 0x6F0:
                self.peer.send(can.Message(arbitration_id=0x6F1, data=b"\xaa\xbb", is_extended_id=False))

        t = threading.Thread(target=_responder, daemon=True)
        t.start()
        reply = self.worker.request(
            can_id=0x6F0, data=b"\x01\xe8\x03",
            reply_id=0x6F1, reply_mask=0x7FF,
            timeout_ms=500,
        )
        t.join(timeout=2)
        self.assertEqual(reply["id"], "6f1")
        self.assertEqual(reply["data"], "aabb")

    def test_request_timeout(self):
        with self.assertRaises(TimeoutError):
            self.worker.request(0x100, b"\x00", 0x101, 0x7FF, timeout_ms=50)

    def test_allowlist_blocks_unlisted_id(self):
        worker, peer = _worker_with_virtual(allow=(0x6F0,))
        try:
            with self.assertRaises(PermissionError):
                worker.send(0x100, b"\x00")
            # allowed id passes
            worker.send(0x6F0, b"\x00")
            self.assertIsNotNone(peer.recv(timeout=0.5))
        finally:
            worker.close(); peer.shutdown()


# ---------------------------------------------------------------------------
# Layer 3 — Daemon integration over real Unix socket
# ---------------------------------------------------------------------------

class _DaemonThread(threading.Thread):
    def __init__(self, worker: CanWorker, sock_path: str) -> None:
        super().__init__(daemon=True)
        self._worker = worker
        self._sock_path = sock_path
        self._stop = threading.Event()
        self.server_sock: socket.socket | None = None
        self.ready = threading.Event()

    def run(self) -> None:
        if os.path.exists(self._sock_path):
            os.unlink(self._sock_path)
        self.server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_sock.bind(self._sock_path)
        self.server_sock.listen(32)
        self.server_sock.settimeout(0.2)
        self.ready.set()
        while not self._stop.is_set():
            try:
                conn, _ = self.server_sock.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            t = threading.Thread(
                target=handle_client,
                args=(conn, conn.fileno(), self._worker),
                daemon=True,
            )
            t.start()

    def stop(self) -> None:
        self._stop.set()
        if self.server_sock:
            try:
                self.server_sock.close()
            except OSError:
                pass
        if os.path.exists(self._sock_path):
            os.unlink(self._sock_path)


def _client(sock_path: str, req: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(sock_path)
        return send_request(s, req)


class TestIntegration(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.mktemp(suffix=".sock", prefix="awto_can_test_")
        self.worker, self.peer = _worker_with_virtual(allow=())
        self._daemon = _DaemonThread(self.worker, self._tmp)
        self._daemon.start()
        self._daemon.ready.wait(timeout=2)

    def tearDown(self):
        self._daemon.stop()
        self._daemon.join(timeout=2)
        self.worker.close()
        self.peer.shutdown()

    def test_ping(self):
        resp = _client(self._tmp, {"cmd": "ping"})
        self.assertTrue(resp["ok"])
        self.assertEqual(resp["response"], "pong")

    def test_info(self):
        resp = _client(self._tmp, {"cmd": "info"})
        self.assertTrue(resp["ok"])
        self.assertIn("interface", resp["info"])

    def test_send_via_daemon(self):
        resp = _client(self._tmp, {
            "cmd": "send", "id": "6f0", "data": "01e803",
        })
        self.assertTrue(resp["ok"], resp)
        self.assertEqual(resp["frame"]["id"], "6f0")
        msg = self.peer.recv(timeout=0.5)
        self.assertIsNotNone(msg)
        self.assertEqual(msg.arbitration_id, 0x6F0)

    def test_send_bad_hex_returns_error(self):
        resp = _client(self._tmp, {"cmd": "send", "id": "zzz"})
        self.assertFalse(resp["ok"])
        self.assertIn("send:", resp["error"])

    def test_recv_bad_filter_returns_efilter(self):
        resp = _client(self._tmp, {"cmd": "recv", "filters": [{"mask": "7ff"}]})
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["code"], ERR_FILTER)

    def test_request_timeout_returns_etimeout(self):
        resp = _client(self._tmp, {
            "cmd": "request", "id": "6f0", "data": "01",
            "reply_id": "6f1", "reply_mask": "7ff", "timeout_ms": 50,
        })
        self.assertFalse(resp["ok"])
        self.assertEqual(resp["code"], ERR_TIMEOUT)

    def test_unknown_cmd(self):
        resp = _client(self._tmp, {"cmd": "explode"})
        self.assertFalse(resp["ok"])
        self.assertIn("unknown cmd", resp["error"])

    def test_concurrent_clients(self):
        N = 16
        def _do(i: int) -> dict:
            return _client(self._tmp, {"cmd": "ping"})
        with ThreadPoolExecutor(max_workers=N) as pool:
            futs = [pool.submit(_do, i) for i in range(N)]
            for f in as_completed(futs):
                resp = f.result(timeout=5)
                self.assertTrue(resp["ok"])

    def test_allowlist_denied_via_daemon(self):
        # Replace worker with one that has a real allowlist
        self._daemon.stop()
        self._daemon.join(timeout=2)
        self.worker.close()
        self.worker, self.peer_replacement = _worker_with_virtual(allow=(0x6F0,))
        self._daemon = _DaemonThread(self.worker, self._tmp)
        self._daemon.start()
        self._daemon.ready.wait(timeout=2)
        try:
            resp = _client(self._tmp, {"cmd": "send", "id": "100", "data": "00"})
            self.assertFalse(resp["ok"])
            self.assertEqual(resp["code"], ERR_DENIED)
        finally:
            self.peer_replacement.shutdown()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Python {sys.version}")
    print(f"GIL: {_gil_status()}")
    print()
    unittest.main(verbosity=2)
