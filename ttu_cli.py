#!/usr/bin/env python3
"""
ttu  —  CLI client for the awto CAN daemon.

Subcommands:

    ttu_cli.py ping                                  daemon health check
    ttu_cli.py info                                  show interface / bitrate / DBC
    ttu_cli.py send 6f0 01e803                       TX a frame (data hex, no 0x)
    ttu_cli.py recv --filter 6f0/7ff --max 5         RX with arbitration-id mask
    ttu_cli.py request 6f0 01e803 --reply 6f1        TX + await first reply
    ttu_cli.py dbc-load docs/awto_htc.dbc            load / hot-reload DBC
    ttu_cli.py dbc-encode ConBeep Ms=1000            encode a DBC message
    ttu_cli.py dbc-decode 6f0 01e803                 decode raw frame via DBC

Stdin pipe:
    echo "6f0 01e803" | ttu_cli.py send              accepts "id data" from stdin

Requires the free-threaded (no-GIL) CPython build: python3.14t
"""

import argparse
import json
import logging
import logging.handlers
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from protocol import (
    DEFAULT_RECV_MS,
    DEFAULT_SOCKET_PATH,
    DEFAULT_TIMEOUT_MS,
    send_request,
)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    root = logging.getLogger()
    root.setLevel(level)
    try:
        syslog = logging.handlers.SysLogHandler(
            address="/dev/log",
            facility=logging.handlers.SysLogHandler.LOG_USER,
        )
        syslog.ident = "awto-ttu: "
        syslog.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
        root.addHandler(syslog)
    except OSError:
        pass
    stderr = logging.StreamHandler(sys.stderr)
    stderr.setFormatter(
        logging.Formatter("ttu[%(process)d]: %(levelname)-8s %(message)s")
    )
    root.addHandler(stderr)


log = logging.getLogger("cli")


# ---------------------------------------------------------------------------
# Daemon I/O
# ---------------------------------------------------------------------------

def _call(req: dict, sock_path: str = DEFAULT_SOCKET_PATH) -> dict:
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(sock_path)
            return send_request(sock, req)
    except FileNotFoundError:
        print(
            f"error: daemon socket not found at {sock_path}\n"
            "       Start the daemon first:  python can_daemon.py",
            file=sys.stderr,
        )
        sys.exit(1)
    except ConnectionRefusedError:
        print("error: daemon is not running", file=sys.stderr)
        sys.exit(1)


def _print_response(resp: dict) -> None:
    if "info" in resp:
        for k, v in resp["info"].items():
            print(f"{k}: {v}")
        return
    if "frames" in resp:
        for f in resp["frames"]:
            _print_frame(f)
        return
    if "frame" in resp:
        _print_frame(resp["frame"])
        return
    if "id" in resp and "data" in resp:
        print(f"{resp['id']}#{resp['data']}")
        return
    if "message" in resp and "signals" in resp:
        print(f"{resp['message']}: {json.dumps(resp['signals'], default=str)}")
        return
    if "response" in resp:
        print(resp["response"])
        return
    print(json.dumps(resp))


def _print_frame(f: dict) -> None:
    flags = []
    if f.get("ext"):
        flags.append("ext")
    if f.get("rtr"):
        flags.append("rtr")
    if f.get("err"):
        flags.append("err")
    flag_str = f" [{','.join(flags)}]" if flags else ""
    line = f"{f.get('ts_mono', 0):.6f}  {f['id']}#{f['data']}{flag_str}"
    if "decoded" in f and f["decoded"]:
        line += f"  -> {f['decoded']}"
    print(line)


# ---------------------------------------------------------------------------
# Argument parsers
# ---------------------------------------------------------------------------

def _parse_filter(spec: str) -> dict:
    """Parse 'ID' or 'ID/MASK' (hex, no 0x). Optional ':ext' suffix."""
    ext = False
    if spec.endswith(":ext"):
        ext = True
        spec = spec[:-4]
    if "/" in spec:
        cid, mask = spec.split("/", 1)
    else:
        cid, mask = spec, "1fffffff" if ext else "7ff"
    return {"id": cid, "mask": mask, "ext": ext}


def _parse_signals(items: list[str]) -> dict:
    """Parse ``Name=Value`` pairs into a signals dict (numeric if possible)."""
    out: dict = {}
    for item in items:
        if "=" not in item:
            raise SystemExit(f"bad signal spec (need NAME=VALUE): {item!r}")
        k, v = item.split("=", 1)
        try:
            out[k] = int(v, 0)
        except ValueError:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return out


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        prog="ttu",
        description="Send/receive CAN frames via the awto CAN daemon.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--verbose", "-v", action="store_true", help="debug logging")
    ap.add_argument("--socket", default=DEFAULT_SOCKET_PATH,
                    help=f"daemon socket (default {DEFAULT_SOCKET_PATH})")

    sub = ap.add_subparsers(dest="subcmd", metavar="SUBCMD")

    sub.add_parser("ping", help="health-check the daemon")
    sub.add_parser("info", help="show interface / bitrate / DBC / counters")

    sp_send = sub.add_parser("send", help="send a CAN frame")
    sp_send.add_argument("id",   nargs="?", help="arbitration id (hex, no 0x)")
    sp_send.add_argument("data", nargs="?", default="", help="payload (hex, no 0x; up to 8 bytes)")
    sp_send.add_argument("--ext", action="store_true", help="extended (29-bit) id")
    sp_send.add_argument("--rtr", action="store_true", help="remote-transmission frame")
    sp_send.add_argument("--dry-run", action="store_true", help="encode + log, do not transmit")

    sp_recv = sub.add_parser("recv", help="receive frames")
    sp_recv.add_argument("--filter", action="append", default=[],
                         help="ID or ID/MASK (hex), optional ':ext'; repeatable")
    sp_recv.add_argument("--max", type=int, default=1, help="max frames to return")
    sp_recv.add_argument("--timeout", type=int, default=DEFAULT_RECV_MS, help="timeout ms")
    sp_recv.add_argument("--decode", action="store_true", help="DBC-decode each frame if possible")

    sp_req = sub.add_parser("request", help="send + await reply")
    sp_req.add_argument("id")
    sp_req.add_argument("data")
    sp_req.add_argument("--reply", required=True, help="reply ID (or ID/MASK) hex")
    sp_req.add_argument("--ext", action="store_true")
    sp_req.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_MS)
    sp_req.add_argument("--dry-run", action="store_true")

    sp_ld = sub.add_parser("dbc-load", help="load (or hot-reload) DBC file")
    sp_ld.add_argument("path")

    sp_enc = sub.add_parser("dbc-encode", help="encode a DBC message")
    sp_enc.add_argument("message")
    sp_enc.add_argument("signals", nargs="+", help="NAME=VALUE pairs")

    sp_dec = sub.add_parser("dbc-decode", help="decode a raw frame via DBC")
    sp_dec.add_argument("id")
    sp_dec.add_argument("data")
    sp_dec.add_argument("--ext", action="store_true")

    args = ap.parse_args()
    _setup_logging(args.verbose)

    if args.subcmd is None:
        ap.print_help()
        sys.exit(2)

    if args.subcmd == "ping":
        resp = _call({"cmd": "ping"}, args.socket)
    elif args.subcmd == "info":
        resp = _call({"cmd": "info"}, args.socket)
    elif args.subcmd == "send":
        cid, data = args.id, args.data
        if cid is None:
            if sys.stdin.isatty():
                print("error: send requires id [data] (or pipe 'id data' to stdin)",
                      file=sys.stderr)
                sys.exit(2)
            parts = sys.stdin.read().strip().split()
            if not parts:
                print("error: empty stdin", file=sys.stderr)
                sys.exit(2)
            cid = parts[0]
            data = parts[1] if len(parts) > 1 else ""
        resp = _call({
            "cmd": "send", "id": cid, "data": data,
            "ext": args.ext, "rtr": args.rtr, "dry_run": args.dry_run,
        }, args.socket)
    elif args.subcmd == "recv":
        resp = _call({
            "cmd": "recv",
            "filters": [_parse_filter(s) for s in args.filter],
            "max": args.max,
            "timeout_ms": args.timeout,
            "decode": args.decode,
        }, args.socket)
    elif args.subcmd == "request":
        rspec = _parse_filter(args.reply)
        if args.ext:
            rspec["ext"] = True
        resp = _call({
            "cmd": "request",
            "id": args.id, "data": args.data, "ext": args.ext,
            "reply_id": rspec["id"], "reply_mask": rspec["mask"],
            "timeout_ms": args.timeout, "dry_run": args.dry_run,
        }, args.socket)
    elif args.subcmd == "dbc-load":
        resp = _call({"cmd": "dbc_load", "path": args.path}, args.socket)
    elif args.subcmd == "dbc-encode":
        resp = _call({
            "cmd": "dbc_encode",
            "message": args.message,
            "signals": _parse_signals(args.signals),
        }, args.socket)
    elif args.subcmd == "dbc-decode":
        resp = _call({
            "cmd": "dbc_decode",
            "id": args.id, "data": args.data, "ext": args.ext,
        }, args.socket)
    else:
        ap.print_help()
        sys.exit(2)

    if not resp.get("ok"):
        code = resp.get("code")
        prefix = f"error[{code}]" if code else "error"
        print(f"{prefix}: {resp.get('error', 'unknown')}", file=sys.stderr)
        sys.exit(1)
    _print_response(resp)


if __name__ == "__main__":
    main()
