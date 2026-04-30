"""
Shared protocol helpers for awto-mcp-can.

The daemon and all clients speak JSON-lines over a Unix domain socket.

Wire schema:
    - Arbitration IDs and frame data are lowercase hex strings without ``0x``.
    - Booleans encode flags such as extended IDs and remote-transmission frames.
    - Timestamps use ``ts_mono`` (CLOCK_MONOTONIC float seconds) and
      ``ts_wall`` (RFC 3339 UTC string).

Requests  (client → daemon):
    {"cmd": "ping"}
    {"cmd": "info"}
    {"cmd": "bus_up",   "bitrate": 250000}
    {"cmd": "bus_down"}
    {"cmd": "send",     "id": "6f0", "data": "01e803", "ext": false, "rtr": false,
                          "dry_run": false}
    {"cmd": "recv",     "filters": [{"id": "6f0", "mask": "7ff", "ext": false}],
                          "max": 16, "timeout_ms": 200, "decode": false}
    {"cmd": "request",  "id": "6f0", "data": "01e803", "ext": false,
                          "reply_id": "6f1", "reply_mask": "7ff",
                          "timeout_ms": 100, "dry_run": false}
    {"cmd": "dbc_load", "path": "docs/awto_htc.dbc"}
    {"cmd": "dbc_encode", "message": "ConBeep", "signals": {"Ms": 1000}}
    {"cmd": "dbc_decode", "id": "6f0", "data": "01e803"}

Responses (daemon → client):
    {"ok": true,  "response": "<ascii>"}                 # ping / generic
    {"ok": true,  "info":   {...}}                       # info
    {"ok": true,  "frame":  {...}}                       # send / request
    {"ok": true,  "frames": [...]}                       # recv
    {"ok": true,  "id": "6f0", "data": "01e803"}         # dbc_encode
    {"ok": true,  "message": "ConBeep", "signals": {...}}# dbc_decode
    {"ok": false, "error": "<msg>", "code": "EBUSDOWN"}  # errors carry stable codes

Stable error codes (see README.md):
    EBUSDOWN  EFILTER  ETIMEOUT  EDBC  EBUSY  EBUSOFF  EDENIED
"""

import json
import socket
from typing import Any, Iterable

DEFAULT_SOCKET_PATH = "/tmp/awto-can.sock"
DEFAULT_INTERFACE   = "can0"
DEFAULT_BITRATE     = 250_000
DEFAULT_TIMEOUT_MS  = 100         # request/response default per requirements
DEFAULT_RECV_MS     = 200

# Default deny: nothing TX-forbidden until config supplies an allowlist.
# CON/PDM CMD ids per README — both 11-bit, standard.
DEFAULT_TX_ALLOW: tuple[int, ...] = (0x6F0, 0x7F0)

# Stable error codes carried in response["code"].
ERR_BUSDOWN = "EBUSDOWN"
ERR_FILTER  = "EFILTER"
ERR_TIMEOUT = "ETIMEOUT"
ERR_DBC     = "EDBC"
ERR_BUSY    = "EBUSY"
ERR_BUSOFF  = "EBUSOFF"
ERR_DENIED  = "EDENIED"


# ---------------------------------------------------------------------------
# Socket framing — JSON-lines (newline terminated)
# ---------------------------------------------------------------------------

def send_request(sock: socket.socket, req: dict[str, Any]) -> dict[str, Any]:
    """Send a JSON-lines request and return the parsed response."""
    sock.sendall((json.dumps(req) + "\n").encode())
    return recv_response(sock)


def recv_response(sock: socket.socket) -> dict[str, Any]:
    """Read one newline-terminated JSON line from *sock*."""
    buf = bytearray()
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError("daemon closed connection")
        buf.extend(chunk)
        if b"\n" in buf:
            line, _, _ = buf.partition(b"\n")
            return json.loads(line.decode())


# ---------------------------------------------------------------------------
# Result builders
# ---------------------------------------------------------------------------

def make_ok(response: str = "ok", **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": True, "response": response}
    out.update(extra)
    return out


def make_err(error: str, code: str | None = None, **extra: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"ok": False, "error": error}
    if code:
        out["code"] = code
    out.update(extra)
    return out


# ---------------------------------------------------------------------------
# Hex helpers — validate strictly, never silently truncate
# ---------------------------------------------------------------------------

def parse_can_id(value: str | int, ext: bool = False) -> int:
    """Parse an arbitration id from a hex string (no ``0x``) or integer.

    Raises ValueError on malformed input or out-of-range ids.
    """
    if isinstance(value, int):
        cid = value
    else:
        s = value.strip().lower().removeprefix("0x")
        if not s or any(c not in "0123456789abcdef" for c in s):
            raise ValueError(f"bad CAN id hex: {value!r}")
        cid = int(s, 16)
    limit = 0x1FFFFFFF if ext else 0x7FF
    if cid < 0 or cid > limit:
        raise ValueError(
            f"CAN id 0x{cid:x} out of range for {'29' if ext else '11'}-bit"
        )
    return cid


def format_can_id(cid: int) -> str:
    return f"{cid:x}"


def parse_data(value: str | bytes) -> bytes:
    """Parse a CAN data payload (hex string or bytes). Empty is allowed."""
    if isinstance(value, (bytes, bytearray)):
        data = bytes(value)
    else:
        s = value.strip().lower().replace(" ", "").removeprefix("0x")
        if len(s) % 2:
            raise ValueError(f"bad data hex (odd length): {value!r}")
        if any(c not in "0123456789abcdef" for c in s):
            raise ValueError(f"bad data hex: {value!r}")
        data = bytes.fromhex(s) if s else b""
    if len(data) > 64:
        # CAN-FD up to 64; classical CAN up to 8. Daemon enforces per-iface.
        raise ValueError(f"data too long ({len(data)} bytes)")
    return data


def format_data(data: bytes) -> str:
    return data.hex()


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def parse_filters(raw: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    """Validate and normalise a list of {id, mask, ext} filters.

    Raises ValueError on any malformed entry — caller maps to ERR_FILTER.
    """
    if raw is None:
        return []
    out: list[dict[str, Any]] = []
    for i, f in enumerate(raw):
        if not isinstance(f, dict):
            raise ValueError(f"filter[{i}] not an object")
        ext = bool(f.get("ext", False))
        try:
            cid = parse_can_id(f["id"], ext=ext)
            mask = parse_can_id(f.get("mask", 0x1FFFFFFF if ext else 0x7FF), ext=ext)
        except KeyError as exc:
            raise ValueError(f"filter[{i}] missing field: {exc.args[0]!r}") from None
        out.append({"can_id": cid, "can_mask": mask, "extended": ext})
    return out
