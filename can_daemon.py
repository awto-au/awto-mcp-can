#!/usr/bin/env python3
"""
awto-can-daemon  —  owns the SocketCAN bus, multiplexes it over a Unix socket.

Usage:
    python can_daemon.py [--interface can0] [--bitrate 250000]
                          [--socket /tmp/awto-can.sock]
                          [--dbc docs/awto_htc.dbc]

Clients connect to the Unix socket and exchange JSON-lines (see protocol.py).
A single ``CanWorker`` owns the bus and serialises access through a
threading.Lock so multiple clients (CLI, MCP server, tests) coexist safely.

Designed for the free-threaded (no-GIL) CPython build: python3.14t.
"""

import argparse
import datetime
import json
import logging
import logging.handlers
import os
import socket
import sys
import threading
import time
from typing import Any

import can

try:
    import cantools
    from cantools.database.can.database import Database as DbcDatabase
    _HAS_CANTOOLS = True
except ImportError:                 # cantools optional at import time
    cantools = None                 # type: ignore[assignment]
    DbcDatabase = Any               # type: ignore[misc,assignment]
    _HAS_CANTOOLS = False

from protocol import (
    DEFAULT_BITRATE,
    DEFAULT_INTERFACE,
    DEFAULT_RECV_MS,
    DEFAULT_SOCKET_PATH,
    DEFAULT_TIMEOUT_MS,
    DEFAULT_TX_ALLOW,
    ERR_BUSDOWN,
    ERR_BUSY,
    ERR_DBC,
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
)

log = logging.getLogger("daemon")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging(ident: str, level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)

    try:
        syslog = logging.handlers.SysLogHandler(
            address="/dev/log",
            facility=logging.handlers.SysLogHandler.LOG_DAEMON,
        )
        syslog.ident = f"{ident}: "
        syslog.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        root.addHandler(syslog)
    except OSError:
        pass

    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(
        logging.Formatter(f"{ident}[%(process)d]: %(levelname)-8s %(name)s: %(message)s")
    )
    root.addHandler(stderr)


# ---------------------------------------------------------------------------
# Frame helpers
# ---------------------------------------------------------------------------

def _frame_to_dict(msg: can.Message) -> dict[str, Any]:
    return {
        "id": format_can_id(msg.arbitration_id),
        "data": format_data(bytes(msg.data)),
        "ext": bool(msg.is_extended_id),
        "rtr": bool(msg.is_remote_frame),
        "err": bool(msg.is_error_frame),
        "dlc": msg.dlc,
        "ts_mono": float(msg.timestamp) if msg.timestamp else time.monotonic(),
        "ts_wall": datetime.datetime.now(datetime.UTC).isoformat(timespec="microseconds"),
    }


# ---------------------------------------------------------------------------
# CAN worker
# ---------------------------------------------------------------------------

class CanWorker:
    """Owns the CAN bus and exposes thread-safe TX / RX / request methods.

    The worker is intentionally simple in v0.1: capture-to-disk, ISO-TP,
    and DBC validation are tracked under their own GitHub issues and will
    be added incrementally.
    """

    MAX_CLASSIC_DLC = 8

    def __init__(
        self,
        interface: str = DEFAULT_INTERFACE,
        bitrate: int = DEFAULT_BITRATE,
        bustype: str = "socketcan",
        tx_allow: tuple[int, ...] = DEFAULT_TX_ALLOW,
        bus: can.BusABC | None = None,
    ) -> None:
        self._interface = interface
        self._bitrate = bitrate
        self._bustype = bustype
        self._tx_allow = frozenset(tx_allow) if tx_allow else None
        self._bus: can.BusABC | None = bus
        self._lock = threading.Lock()
        self._dbc: DbcDatabase | None = None
        self._dbc_path: str | None = None
        # rx counters for observability (see issue #4 dodgy-probe detection)
        self._tx_count = 0
        self._rx_count = 0

    # ------------------------------------------------------------------
    @property
    def interface(self) -> str:
        return self._interface

    @property
    def bitrate(self) -> int:
        return self._bitrate

    @property
    def is_open(self) -> bool:
        return self._bus is not None

    # ------------------------------------------------------------------
    def open(self) -> None:
        if self._bus is not None:
            return
        log.info("opening %s bus %s @ %d bps", self._bustype, self._interface, self._bitrate)
        self._bus = can.Bus(
            channel=self._interface,
            bustype=self._bustype,
            bitrate=self._bitrate,
        )

    def close(self) -> None:
        if self._bus is None:
            return
        try:
            self._bus.shutdown()
        except can.CanError as exc:
            log.warning("bus shutdown error: %s", exc)
        self._bus = None

    # ------------------------------------------------------------------
    def info(self) -> dict[str, Any]:
        return {
            "interface": self._interface,
            "bitrate": self._bitrate,
            "bustype": self._bustype,
            "is_open": self.is_open,
            "dbc_path": self._dbc_path,
            "tx_allow": sorted(self._tx_allow) if self._tx_allow else None,
            "tx_count": self._tx_count,
            "rx_count": self._rx_count,
        }

    # ------------------------------------------------------------------
    def _check_tx_allow(self, can_id: int) -> None:
        if self._tx_allow is None:
            return
        if can_id not in self._tx_allow:
            raise PermissionError(
                f"CAN id 0x{can_id:x} not in TX allowlist"
            )

    def send(
        self,
        can_id: int,
        data: bytes,
        ext: bool = False,
        rtr: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        if not ext and len(data) > self.MAX_CLASSIC_DLC:
            raise ValueError(
                f"classic CAN payload limited to {self.MAX_CLASSIC_DLC} bytes"
            )
        self._check_tx_allow(can_id)
        msg = can.Message(
            arbitration_id=can_id,
            data=data,
            is_extended_id=ext,
            is_remote_frame=rtr,
        )
        with self._lock:
            if self._bus is None:
                raise IOError("bus not open")
            if not dry_run:
                self._bus.send(msg, timeout=0.05)
                self._tx_count += 1
            log.debug("TX id=0x%x data=%s ext=%s rtr=%s dry=%s",
                      can_id, data.hex(), ext, rtr, dry_run)
        return _frame_to_dict(msg)

    # ------------------------------------------------------------------
    def recv(
        self,
        filters: list[dict[str, Any]] | None = None,
        max_frames: int = 1,
        timeout_ms: int = DEFAULT_RECV_MS,
        decode: bool = False,
    ) -> list[dict[str, Any]]:
        with self._lock:
            if self._bus is None:
                raise IOError("bus not open")
            try:
                self._bus.set_filters(filters)
            except can.CanError as exc:
                raise ValueError(f"bus filter rejected: {exc}") from exc

            deadline = time.monotonic() + timeout_ms / 1000.0
            out: list[dict[str, Any]] = []
            while len(out) < max_frames and time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                msg = self._bus.recv(timeout=min(remaining, 0.1))
                if msg is None:
                    continue
                self._rx_count += 1
                d = _frame_to_dict(msg)
                if decode and self._dbc is not None:
                    d["decoded"] = self._safe_decode(msg.arbitration_id, msg.data)
                out.append(d)
        return out

    # ------------------------------------------------------------------
    def request(
        self,
        can_id: int,
        data: bytes,
        reply_id: int,
        reply_mask: int,
        ext: bool = False,
        timeout_ms: int = DEFAULT_TIMEOUT_MS,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Send *data* on *can_id* and return the first matching reply frame.

        Implemented under a single lock so the reply window starts before the
        TX is observed by other clients.
        """
        self._check_tx_allow(can_id)
        if not ext and len(data) > self.MAX_CLASSIC_DLC:
            raise ValueError(
                f"classic CAN payload limited to {self.MAX_CLASSIC_DLC} bytes"
            )
        with self._lock:
            if self._bus is None:
                raise IOError("bus not open")
            self._bus.set_filters([
                {"can_id": reply_id, "can_mask": reply_mask, "extended": ext}
            ])
            tx = can.Message(
                arbitration_id=can_id,
                data=data,
                is_extended_id=ext,
            )
            if dry_run:
                return _frame_to_dict(tx)
            self._bus.send(tx, timeout=0.05)
            self._tx_count += 1
            deadline = time.monotonic() + timeout_ms / 1000.0
            while time.monotonic() < deadline:
                remaining = deadline - time.monotonic()
                msg = self._bus.recv(timeout=min(remaining, 0.05))
                if msg is None:
                    continue
                self._rx_count += 1
                return _frame_to_dict(msg)
            raise TimeoutError(f"no reply within {timeout_ms} ms")

    # ------------------------------------------------------------------
    # DBC support
    # ------------------------------------------------------------------
    def dbc_load(self, path: str) -> str:
        if not _HAS_CANTOOLS:
            raise RuntimeError("cantools not installed")
        db = cantools.database.load_file(path)
        with self._lock:
            self._dbc = db
            self._dbc_path = os.path.abspath(path)
        log.info("DBC loaded: %s (%d messages)", self._dbc_path, len(db.messages))
        return self._dbc_path

    def dbc_encode(self, message: str, signals: dict[str, Any]) -> tuple[int, bytes]:
        if self._dbc is None:
            raise RuntimeError("no DBC loaded")
        msg = self._dbc.get_message_by_name(message)
        data = msg.encode(signals)
        return msg.frame_id, bytes(data)

    def dbc_decode(self, can_id: int, data: bytes) -> tuple[str, dict[str, Any]]:
        if self._dbc is None:
            raise RuntimeError("no DBC loaded")
        msg = self._dbc.get_message_by_frame_id(can_id)
        decoded = msg.decode(data)
        return msg.name, dict(decoded)

    def _safe_decode(self, can_id: int, data: bytes) -> dict[str, Any] | None:
        if self._dbc is None:
            return None
        try:
            name, sig = self.dbc_decode(can_id, data)
            return {"message": name, "signals": sig}
        except (KeyError, ValueError):
            return None


# ---------------------------------------------------------------------------
# Client connection handler
# ---------------------------------------------------------------------------

def handle_client(conn: socket.socket, addr: int, worker: CanWorker) -> None:
    log.debug("client connected: %s", addr)
    buf = bytearray()
    try:
        while True:
            chunk = conn.recv(65536)
            if not chunk:
                break
            buf.extend(chunk)

            while b"\n" in buf:
                raw, _, buf = buf.partition(b"\n")
                if not raw.strip():
                    continue
                try:
                    req = json.loads(raw.decode())
                except json.JSONDecodeError as exc:
                    _send(conn, make_err(f"bad JSON: {exc}"))
                    continue

                _dispatch(conn, worker, req)

    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        conn.close()
        log.debug("client disconnected: %s", addr)


def _dispatch(conn: socket.socket, worker: CanWorker, req: dict[str, Any]) -> None:
    cmd = req.get("cmd", "")

    if cmd == "ping":
        _send(conn, make_ok("pong"))
        return

    if cmd == "info":
        _send(conn, {"ok": True, "info": worker.info()})
        return

    if cmd == "send":
        try:
            ext = bool(req.get("ext", False))
            cid = parse_can_id(req["id"], ext=ext)
            data = parse_data(req.get("data", ""))
        except (KeyError, ValueError) as exc:
            _send(conn, make_err(f"send: {exc}"))
            return
        try:
            frame = worker.send(
                cid, data, ext=ext,
                rtr=bool(req.get("rtr", False)),
                dry_run=bool(req.get("dry_run", False)),
            )
            _send(conn, {"ok": True, "frame": frame})
        except PermissionError as exc:
            _send(conn, make_err(str(exc), code=ERR_DENIED))
        except IOError as exc:
            _send(conn, make_err(str(exc), code=ERR_BUSDOWN))
        except can.CanError as exc:
            _send(conn, make_err(f"send failed: {exc}", code=ERR_BUSY))
        return

    if cmd == "recv":
        try:
            filters = parse_filters(req.get("filters"))
        except ValueError as exc:
            _send(conn, make_err(f"recv: {exc}", code=ERR_FILTER))
            return
        try:
            frames = worker.recv(
                filters=filters or None,
                max_frames=int(req.get("max", 1)),
                timeout_ms=int(req.get("timeout_ms", DEFAULT_RECV_MS)),
                decode=bool(req.get("decode", False)),
            )
            _send(conn, {"ok": True, "frames": frames})
        except IOError as exc:
            _send(conn, make_err(str(exc), code=ERR_BUSDOWN))
        return

    if cmd == "request":
        try:
            ext = bool(req.get("ext", False))
            cid = parse_can_id(req["id"], ext=ext)
            data = parse_data(req.get("data", ""))
            rid = parse_can_id(req["reply_id"], ext=ext)
            mask = parse_can_id(
                req.get("reply_mask", 0x1FFFFFFF if ext else 0x7FF),
                ext=ext,
            )
        except (KeyError, ValueError) as exc:
            _send(conn, make_err(f"request: {exc}"))
            return
        try:
            reply = worker.request(
                cid, data, rid, mask, ext=ext,
                timeout_ms=int(req.get("timeout_ms", DEFAULT_TIMEOUT_MS)),
                dry_run=bool(req.get("dry_run", False)),
            )
            _send(conn, {"ok": True, "frame": reply})
        except TimeoutError as exc:
            _send(conn, make_err(str(exc), code=ERR_TIMEOUT))
        except PermissionError as exc:
            _send(conn, make_err(str(exc), code=ERR_DENIED))
        except IOError as exc:
            _send(conn, make_err(str(exc), code=ERR_BUSDOWN))
        return

    if cmd == "dbc_load":
        try:
            path = worker.dbc_load(req["path"])
            _send(conn, {"ok": True, "dbc_path": path})
        except (KeyError, RuntimeError, FileNotFoundError) as exc:
            _send(conn, make_err(f"dbc_load: {exc}", code=ERR_DBC))
        return

    if cmd == "dbc_encode":
        try:
            cid, data = worker.dbc_encode(req["message"], dict(req.get("signals", {})))
            _send(conn, {"ok": True, "id": format_can_id(cid), "data": format_data(data)})
        except (KeyError, RuntimeError) as exc:
            _send(conn, make_err(f"dbc_encode: {exc}", code=ERR_DBC))
        return

    if cmd == "dbc_decode":
        try:
            ext = bool(req.get("ext", False))
            cid = parse_can_id(req["id"], ext=ext)
            data = parse_data(req["data"])
            name, signals = worker.dbc_decode(cid, data)
            _send(conn, {"ok": True, "message": name, "signals": signals})
        except (KeyError, ValueError, RuntimeError) as exc:
            _send(conn, make_err(f"dbc_decode: {exc}", code=ERR_DBC))
        return

    _send(conn, make_err(f"unknown cmd: {cmd!r}"))


def _send(conn: socket.socket, obj: dict[str, Any]) -> None:
    try:
        conn.sendall((json.dumps(obj) + "\n").encode())
    except (BrokenPipeError, OSError):
        pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="awto CAN daemon")
    ap.add_argument("--interface", default=DEFAULT_INTERFACE,
                    help="SocketCAN interface (default can0)")
    ap.add_argument("--bitrate", default=DEFAULT_BITRATE, type=int,
                    help="CAN bitrate in bps (default 250000)")
    ap.add_argument("--bustype", default="socketcan",
                    help="python-can bus type (default socketcan; use 'virtual' for tests)")
    ap.add_argument("--socket", default=DEFAULT_SOCKET_PATH,
                    help="Unix socket path")
    ap.add_argument("--dbc", default=None, help="optional DBC file to load at startup")
    ap.add_argument("--no-allowlist", action="store_true",
                    help="disable TX arbitration-id allowlist (default deny outside CON/PDM)")
    ap.add_argument("--log-level", default="INFO",
                    choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = ap.parse_args()

    _setup_logging("awto-can-daemon", args.log_level)

    worker = CanWorker(
        interface=args.interface,
        bitrate=args.bitrate,
        bustype=args.bustype,
        tx_allow=() if args.no_allowlist else DEFAULT_TX_ALLOW,
    )
    try:
        worker.open()
    except can.CanError as exc:
        log.error("cannot open CAN bus: %s", exc)
        sys.exit(1)
    if args.dbc:
        try:
            worker.dbc_load(args.dbc)
        except (RuntimeError, FileNotFoundError) as exc:
            log.error("dbc load failed: %s", exc)
            worker.close()
            sys.exit(1)

    if os.path.exists(args.socket):
        os.unlink(args.socket)
    server_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(args.socket)
    os.chmod(args.socket, 0o600)
    server_sock.listen(8)

    log.info("listening on %s (ctrl-c to stop)", args.socket)

    try:
        while True:
            conn, _ = server_sock.accept()
            t = threading.Thread(
                target=handle_client,
                args=(conn, conn.fileno(), worker),
                daemon=True,
            )
            t.start()
    except KeyboardInterrupt:
        log.info("shutting down")
    finally:
        server_sock.close()
        if os.path.exists(args.socket):
            os.unlink(args.socket)
        worker.close()


if __name__ == "__main__":
    main()
