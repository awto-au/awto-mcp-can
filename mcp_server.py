#!/usr/bin/env python3
"""
awto-can MCP server  —  exposes the CAN daemon as MCP tools for Copilot.

Runs as a stdio MCP server (VS Code launches it via .vscode/mcp.json).
Connects to ``can_daemon`` over the Unix socket; the daemon owns the bus.

Tools exposed to Copilot:
  can_ping()                                                      health-check
  can_info()                                                      iface / bitrate / DBC / counters
  can_send(id, data, ext=False, rtr=False, dry_run=False)         transmit one frame
  can_recv(filters=[], max=1, timeout_ms=200, decode=False)       receive frames
  can_request(id, data, reply_id, reply_mask, timeout_ms=100)     send + await reply
  can_dbc_load(path)                                              load / hot-reload DBC
  can_dbc_encode(message, signals)                                encode by name
  can_dbc_decode(id, data, ext=False)                             decode raw frame

Awto helpers (per README):
  con_beep(ms)                                                    0x6f0#01<u16le>
  pdm_channel(ch, on)                                             toggle a PDM output
  pdm_telemetry(ch)                                               request/response read
"""

import logging
import logging.handlers
import os
import socket
import struct
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from mcp.server.fastmcp import FastMCP

from protocol import (
    DEFAULT_RECV_MS,
    DEFAULT_SOCKET_PATH as _DEFAULT_SOCKET_PATH,
    DEFAULT_TIMEOUT_MS,
    send_request,
)


def _sock_path() -> str:
    return os.environ.get("AWTO_SOCKET", _DEFAULT_SOCKET_PATH)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    try:
        syslog = logging.handlers.SysLogHandler(
            address="/dev/log",
            facility=logging.handlers.SysLogHandler.LOG_USER,
        )
        syslog.ident = "awto-mcp-can: "
        syslog.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        root.addHandler(syslog)
    except OSError:
        pass
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(
        logging.Formatter("awto-mcp-can[%(process)d]: %(levelname)-8s %(name)s: %(message)s")
    )
    root.addHandler(stderr)


_setup_logging()
log = logging.getLogger("mcp")

mcp = FastMCP(
    "awto-can",
    instructions="Persistent SocketCAN interface (TX/RX, request/response, DBC).",
)


# ---------------------------------------------------------------------------
# Daemon helper
# ---------------------------------------------------------------------------

def _call(req: dict[str, Any]) -> dict[str, Any]:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(_sock_path())
            return send_request(sock, req)
    except FileNotFoundError:
        return {"ok": False, "error": f"daemon socket not found at {_sock_path()}"}
    except ConnectionRefusedError:
        return {"ok": False, "error": "daemon is not running"}
    except OSError as exc:
        return {"ok": False, "error": f"socket error: {exc}"}


def _err_text(resp: dict[str, Any]) -> str:
    code = resp.get("code")
    prefix = f"error[{code}]" if code else "error"
    return f"{prefix}: {resp.get('error', 'unknown')}"


# ---------------------------------------------------------------------------
# MCP tools — generic CAN
# ---------------------------------------------------------------------------

@mcp.tool()
def can_ping() -> str:
    """Check the CAN daemon is alive."""
    resp = _call({"cmd": "ping"})
    if not resp.get("ok"):
        return _err_text(resp)
    return f"ok ({resp.get('response', 'pong')})"


@mcp.tool()
def can_info() -> dict:
    """Return daemon state: interface, bitrate, bustype, dbc_path, counters."""
    resp = _call({"cmd": "info"})
    if not resp.get("ok"):
        return {"error": resp.get("error", "unknown")}
    return resp.get("info", {})


@mcp.tool()
def can_send(
    id: str,
    data: str = "",
    ext: bool = False,
    rtr: bool = False,
    dry_run: bool = False,
) -> dict:
    """Transmit a single CAN frame.

    Args:
        id:       Arbitration id, lowercase hex (no '0x'), e.g. '6f0'.
        data:     Payload as hex (no '0x'); empty for zero-length frame.
        ext:      Use 29-bit extended id.
        rtr:      Remote-transmission frame.
        dry_run:  Encode + log without writing to the bus.
    """
    resp = _call({
        "cmd": "send", "id": id, "data": data,
        "ext": ext, "rtr": rtr, "dry_run": dry_run,
    })
    if not resp.get("ok"):
        return {"error": _err_text(resp)}
    return resp.get("frame", {})


@mcp.tool()
def can_recv(
    filters: list[dict] | None = None,
    max: int = 1,
    timeout_ms: int = DEFAULT_RECV_MS,
    decode: bool = False,
) -> list[dict]:
    """Receive up to *max* frames matching *filters* within *timeout_ms*.

    Each filter is ``{"id": "<hex>", "mask": "<hex>", "ext": false}``.
    If ``decode`` and a DBC is loaded, each frame includes a ``decoded`` field.
    """
    resp = _call({
        "cmd": "recv", "filters": filters or [],
        "max": max, "timeout_ms": timeout_ms, "decode": decode,
    })
    if not resp.get("ok"):
        return [{"error": _err_text(resp)}]
    return resp.get("frames", [])


@mcp.tool()
def can_request(
    id: str,
    data: str,
    reply_id: str,
    reply_mask: str = "7ff",
    ext: bool = False,
    timeout_ms: int = DEFAULT_TIMEOUT_MS,
    dry_run: bool = False,
) -> dict:
    """Send a frame and return the first matching reply within *timeout_ms*."""
    resp = _call({
        "cmd": "request", "id": id, "data": data, "ext": ext,
        "reply_id": reply_id, "reply_mask": reply_mask,
        "timeout_ms": timeout_ms, "dry_run": dry_run,
    })
    if not resp.get("ok"):
        return {"error": _err_text(resp)}
    return resp.get("frame", {})


@mcp.tool()
def can_dbc_load(path: str) -> str:
    """Load (or hot-reload) a DBC file. Path is relative to the daemon's cwd."""
    resp = _call({"cmd": "dbc_load", "path": path})
    if not resp.get("ok"):
        return _err_text(resp)
    return f"loaded {resp.get('dbc_path', path)}"


@mcp.tool()
def can_dbc_encode(message: str, signals: dict) -> dict:
    """Encode a DBC message by name. Returns ``{id, data}`` (hex strings)."""
    resp = _call({"cmd": "dbc_encode", "message": message, "signals": signals})
    if not resp.get("ok"):
        return {"error": _err_text(resp)}
    return {"id": resp.get("id"), "data": resp.get("data")}


@mcp.tool()
def can_dbc_decode(id: str, data: str, ext: bool = False) -> dict:
    """Decode a raw frame via the loaded DBC."""
    resp = _call({"cmd": "dbc_decode", "id": id, "data": data, "ext": ext})
    if not resp.get("ok"):
        return {"error": _err_text(resp)}
    return {"message": resp.get("message"), "signals": resp.get("signals")}


# ---------------------------------------------------------------------------
# MCP tools — Awto helpers (per README)
# ---------------------------------------------------------------------------

CON_CMD_ID  = 0x6F0
PDM_CMD_ID  = 0x7F0
CMD_BEEP    = 0x01
CMD_PDM_SET = 0x02      # placeholder — adjust when the firmware spec lands
CMD_PDM_TEL = 0x03      # placeholder — adjust when the firmware spec lands


@mcp.tool()
def con_beep(ms: int = 1000) -> dict:
    """Beep the CON probe for *ms* milliseconds. Sends 0x6f0#01<u16le>."""
    if ms < 0 or ms > 0xFFFF:
        return {"error": "ms out of range (0..65535)"}
    payload = bytes([CMD_BEEP]) + struct.pack("<H", ms)
    return can_send(f"{CON_CMD_ID:x}", payload.hex())


@mcp.tool()
def pdm_channel(ch: int, on: bool) -> dict:
    """Toggle a PDM output channel. (CMD byte placeholder until firmware spec lands.)"""
    if ch < 0 or ch > 0xFF:
        return {"error": "channel out of range (0..255)"}
    payload = bytes([CMD_PDM_SET, ch & 0xFF, 1 if on else 0])
    return can_send(f"{PDM_CMD_ID:x}", payload.hex())


@mcp.tool()
def pdm_telemetry(ch: int, timeout_ms: int = DEFAULT_TIMEOUT_MS) -> dict:
    """Request telemetry for a PDM channel and return the first reply frame."""
    if ch < 0 or ch > 0xFF:
        return {"error": "channel out of range (0..255)"}
    payload = bytes([CMD_PDM_TEL, ch & 0xFF])
    return can_request(
        id=f"{PDM_CMD_ID:x}",
        data=payload.hex(),
        reply_id=f"{PDM_CMD_ID + 1:x}",
        reply_mask="7ff",
        timeout_ms=timeout_ms,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
