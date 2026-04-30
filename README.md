# awto-mcp-can

MCP server for SocketCAN. Provides a fast, persistent daemon for CAN bus operations
(TX/RX, request/response, ISO-TP, DBC encode/decode, capture/replay) exposed over
both MCP stdio and a Unix socket so any tool — AI agent, CLI, test script — can
issue sub-millisecond CAN ops without paying Python import startup cost on each
call.

Sibling project: [`awto-mcp-serial`](https://github.com/awto-au/awto-mcp-serial).

---

## Status

v0.1 — design / requirements. See open issues for the work breakdown.

## Target environment

- Python 3.13+ (Python 3.14 free-threaded recommended; binary `python3.14t`)
- Linux SocketCAN (`can0` @ 250 kbps default)
- `python-can`, `cantools`, kernel `can-isotp` module
- Default DBC: [`docs/awto_htc.dbc`](https://github.com/awto-au/l8-427/blob/main/docs/awto_htc.dbc) (from `awto-au/l8-427`)
- Hardware: STM32F427VG-based CON / PDM probes on a shared HTC CAN bus

| Probe | ST-LINK SN                  | tty           | CMD CAN-ID |
|-------|-----------------------------|---------------|------------|
| CON 935 | `004D00373033510135393935` | `/dev/ttyACM0` (2.48 Mbaud) | `0x6F0` |
| PDM 330 | `005600343431511837393330` | `/dev/ttyACM1` (2.48 Mbaud) | `0x7F0` |

Example raw CAN command (1000 ms beep on CON):
`cansend can0 6F0#01E803`  (CMD=`0x01` BEEP, PARAM=`u16 LE` ms)

---

## Functional requirements

### Bus lifecycle
- Bring `can0` up/down with configurable bitrate.
- Auto-recover from bus-off; surface controller restarts as events.
- Coexist with `candump`, `cansend`, and `tio` running concurrently.

### Transmit
- Single frame and burst send.
- Standard and extended IDs, 0–8 byte payload.
- p99 send latency < 10 ms from MCP request to socket write.

### Receive
- Subscribe with arbitration-ID / mask filters.
- Sustain 5 kfps RX with no drops at 250 kbps.
- Optional decode via DBC on the way out.

### Request / response
- Send a frame, await first matching reply by ID/mask, with timeout.
- Default timeout 100 ms, configurable per call.

### DBC
- Encode/decode all frames against the active DBC.
- Expose message/signal schema as a queryable resource.
- Hot-reload DBC without restarting the daemon.

### ISO-TP
- Multi-frame send/receive via kernel `can-isotp`.
- Configurable STmin, BS, padding.

### Capture & replay
- **Always-on traffic capture** to rotating logs:
  - Raw `.blf` (or `.asc`) — every frame, untouched.
  - Decoded `.jsonl` — wallclock + monotonic ts, decoded signals where DBC matches.
  - Size- and time-based rotation, retention configurable.
- Replay a capture into the bus (or vcan0) at original or scaled rate.

### Real-time DBC validation
- Decode every received frame against the active DBC and emit structured events for:
  - Unknown arbitration IDs (not in DBC).
  - Signal value out of declared min/max range.
  - DLC mismatch (length disagrees with DBC message length).
  - Missing periodic frames (cycle-time exceeded by configurable factor).
- Events are streamed on the daemon socket and written to a dedicated error log.

### Bus failure / dodgy probe detection (P0)
- Per-source / per-controller counters:
  - bus-off, error-warning, error-passive, RX overflow, TX failed, controller restarts.
- Attribute failures to the originating transceiver / probe where possible
  (HTC CAN-ID prefix → CON/PDM, plus controller error-frame source).
- Realtime alerts on the event stream when thresholds are crossed.
- Cross-reference to ST-LINK / USB flakiness (libusb `errno=14`,
  `[get_usbfs_fd] File doesn't exist`) when colocated.

### Awto helpers (built-ins)
- `con.beep(ms)` → sends `0x6F0#01<u16le>`.
- `pdm.channel(ch, on|off)` → toggles a PDM output.
- `pdm.telemetry(ch)` → request/response read of channel telemetry.

---

## Non-functional requirements

- **Latency**: p99 < 10 ms request → socket write.
- **Throughput**: 5 kfps sustained RX, capture, and decode without drops.
- **Single daemon** per host; subsequent clients attach to existing socket.
- **Persistence**: daemon survives client disconnects, DBC reloads, bus bounces.
- **Recovery**: auto-restart controller on bus-off; never wedge.
- **Observability**: every TX/RX/error visible in capture log and event stream.

## Wire schema (daemon ↔ client)

- IDs and data as hex strings (no leading `0x`, lowercase).
- Timestamps: `ts_mono` (float seconds, CLOCK_MONOTONIC) **and** `ts_wall` (RFC 3339 UTC).
- Errors use stable codes:
  - `EBUSDOWN` — interface not up.
  - `ETIMEOUT` — request/response timeout.
  - `EDBC` — encode/decode failure or schema mismatch.
  - `EFILTER` — filter expression invalid.
  - `EBUSY` — too many in-flight requests.
  - `EBUSOFF` — controller in bus-off, recovery in progress.

## Security

- Arbitration-ID allowlist enforced on TX (configurable, default deny dangerous ranges).
- `dry_run` flag on every TX op — encodes + logs but does not send.
- Per-client rate limiter.
- `confirmation_token` required for ops that drive real loads (PDM channel ON, etc.).

## Packaging

- Transports: MCP stdio **and** Unix socket `/tmp/awto-can.sock`.
- Config: `~/.config/awto-can/config.toml` (interface, bitrate, dbc path, allowlist,
  rotation, retention, alert thresholds).
- Runs under systemd user unit; binds socket on start, releases on stop.

## Test plan

1. `vcan0` loopback: TX → RX byte-equal; DBC roundtrip encode→decode→equal.
2. Request/response RTT distribution under idle bus; check p99 < 10 ms.
3. 5 kfps stress on `vcan0` for 60 s; assert zero drops, capture intact.
4. Bus bounce: kill `can0`, daemon detects, reports `EBUSDOWN`, recovers on `up`.
5. Coexistence: run `candump can0` and `tio` in parallel, daemon unaffected.
6. DBC validation: inject out-of-range signal, unknown ID, wrong DLC — assert events.
7. Dodgy-probe sim: force bus-off via error injection, assert per-source counter
   increments and alert fires.

---

## Repository layout (planned, mirrors `awto-mcp-serial`)

```
mcp_server.py        # MCP stdio entry point
can_daemon.py        # long-lived daemon; owns the socket + bus
protocol.py          # wire schema (requests, responses, events)
ttu_cli.py           # CLI client (one-shot ops over the socket)
test_harness.py      # vcan-based test harness
docs/                # design notes, this README expanded, alert taxonomy
scripts/             # systemd unit, install helpers
pyproject.toml
README.md            # this file
ISSUES.md            # mirror of open requirements issues
```
