#!/usr/bin/env python3
"""
CAN event replay tester for trained car-alarm CAN rules.

V10 changes:
  * keeps all V9 diagnostics and timing output;
  * automatically creates report directories;
  * when --report-json/--report-csv are not provided, saves reports to
    reports/<root-name>/report.json and reports/<root-name>/report.csv.

V9 changes:
  * keeps V8 millisecond timestamps on every console log line;
  * adds real replay elapsed time and slowdown factor for every TRC replay;
  * changes BEL output from noisy WARNING to NOTE by default, because many SLCAN
    adapters return BEL when the bus has no ACK while the alarm can still receive frames;
  * prints a compact failed-test list at the end;
  * calculates TRC duration from max(timestamp)-min(timestamp), so files with a
    timestamp reset no longer show duration≈0.00s.

It scans a directory like:

kia/
  1/
    idle.trc
    open.trc
    toggle.trc
  17/
    idle.trc
    button.trc

For every numeric folder:
  * state event: replays idle.trc first if present, then preferably toggle.trc
    and also open.trc if present. The alarm UART log is checked for
    "CAN event <id> changed to ON". By default:
      - toggle.trc must produce 1..5 ON activations;
      - open.trc must produce exactly 1 ON activation.
  * button event: replays idle.trc first if present, then button.trc and checks
    alarm UART log for exactly N lines: "CAN event <id> (...) triggered".

Requires:
  pip install pyserial

Example:
  python can_event_replay_tester.py --root kia --can-port COM7 --log-port COM8 \
      --slcan-baud 115200 --log-baud 115200 --can-bitrate 500000

Reports are saved automatically to:
  reports/kia/report.json
  reports/kia/report.csv
"""

from __future__ import annotations

import argparse
import builtins
import csv
import json
import os
import queue
import re
import sys
import threading
import time
from datetime import datetime
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, Optional


# ----------------------------- console logging -----------------------------

_ORIGINAL_PRINT = builtins.print
_PRINT_LOCK = threading.Lock()


def _pc_timestamp_ms() -> str:
    now = datetime.now()
    return f"[{now:%H:%M:%S}.{now.microsecond // 1000:03d}]"


def _timestamped_print(*args, **kwargs) -> None:
    """Prefix every console print with the PC local time, including milliseconds.

    This intentionally wraps normal print() so all existing diagnostic messages,
    including messages from background reader threads, get a timestamp without
    having to modify every call site manually.
    """
    # Keep writes from several threads on one line and in one piece.
    with _PRINT_LOCK:
        if args:
            _ORIGINAL_PRINT(_pc_timestamp_ms(), *args, **kwargs)
        else:
            _ORIGINAL_PRINT(_pc_timestamp_ms(), **kwargs)


builtins.print = _timestamped_print

try:
    import serial  # type: ignore
except ImportError:  # pragma: no cover - user environment helper
    serial = None  # type: ignore


# ----------------------------- TRC parsing -----------------------------

@dataclass(frozen=True)
class CanFrame:
    t_ms: float
    can_id: int
    data: bytes
    is_extended: bool = False

    @property
    def dlc(self) -> int:
        return len(self.data)


_INT_TOKEN_RE = re.compile(r"^(?:0x)?[0-9A-Fa-f]+h?$")
_FLOAT_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?$")


def _clean_token(tok: str) -> str:
    return tok.strip().strip(",;[](){}")


def _parse_hex_int(tok: str) -> Optional[int]:
    tok = _clean_token(tok)
    if not tok:
        return None
    if tok.lower().endswith("h"):
        tok = tok[:-1]
    if tok.lower().startswith("0x"):
        tok = tok[2:]
    if not _INT_TOKEN_RE.match(tok):
        return None
    try:
        return int(tok, 16)
    except ValueError:
        return None


def _parse_decimal_int(tok: str) -> Optional[int]:
    tok = _clean_token(tok)
    if not tok.isdigit():
        return None
    try:
        return int(tok, 10)
    except ValueError:
        return None


def _parse_float(tok: str) -> Optional[float]:
    tok = _clean_token(tok)
    if not _FLOAT_RE.match(tok):
        return None
    try:
        return float(tok)
    except ValueError:
        return None


def _tokenize_trc_line(line: str) -> list[str]:
    # Remove common PCAN/CAN-Hacker comments and separators.
    if not line.strip():
        return []
    if line.lstrip().startswith((";", "//", "#")):
        return []

    # Keep the data side before an inline comment, but do not destroy timestamp decimals.
    for marker in (" //", " #"):
        if marker in line:
            line = line.split(marker, 1)[0]

    line = line.replace("\t", " ")
    line = line.replace(",", " ")
    line = line.replace("|", " ")
    return [_clean_token(t) for t in line.split() if _clean_token(t)]


def _parse_canhacker_tsv_frame(line: str, allow_dlc0: bool = False) -> Optional[CanFrame]:
    """Parse CAN-Hacker-style TRC rows.

    Example row from the user's recordings::

        12,741657\t1\t00000004\t356\t7\t00 00 00 00 00 00 00   \t00000000\t...

    Columns are:
      0: timestamp in seconds, with comma or dot decimal separator
      1: bus/channel
      2: flags
      3: CAN ID, hex without 0x
      4: DLC, decimal
      5: data bytes as hex pairs

    The previous generic parser could mistakenly treat the channel/flags columns as
    DLC/data and produce bogus frames like t0640. This explicit parser prevents that.
    """
    raw = line.strip()
    if not raw or raw.startswith(("@", "#", ";", "//")):
        return None

    # This format is tab-separated. Be a little tolerant and allow multiple spaces
    # between first columns if tabs were converted by an editor.
    fields = [x.strip() for x in line.rstrip("\r\n").split("\t")]
    if len(fields) < 6:
        # Fallback for whitespace-aligned copies of the same format:
        # timestamp channel flags id dlc byte byte byte ...
        m = re.match(
            r"^\s*(?P<ts>\d+[,.]\d+)\s+"
            r"(?P<ch>\d+)\s+"
            r"(?P<flags>[0-9A-Fa-f]{8})\s+"
            r"(?P<id>[0-9A-Fa-f]{1,8})\s+"
            r"(?P<dlc>[0-8])\s+"
            r"(?P<rest>.*)$",
            line,
        )
        if not m:
            return None
        fields = [m.group("ts"), m.group("ch"), m.group("flags"), m.group("id"), m.group("dlc"), m.group("rest")]

    ts_s = fields[0].replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d+)?", ts_s):
        return None
    if not fields[1].isdigit():
        return None
    if not re.fullmatch(r"[0-9A-Fa-f]{8}", fields[2]):
        return None
    if not re.fullmatch(r"[0-9A-Fa-f]{1,8}", fields[3]):
        return None
    if not fields[4].isdigit():
        return None

    try:
        t_ms = float(ts_s) * 1000.0
        can_id = int(fields[3], 16)
        dlc = int(fields[4], 10)
    except ValueError:
        return None

    if dlc < 0 or dlc > 8:
        return None
    if dlc == 0 and not allow_dlc0:
        return None

    data_tokens = re.findall(r"(?<![0-9A-Fa-f])[0-9A-Fa-f]{2}(?![0-9A-Fa-f])", fields[5])
    if len(data_tokens) < dlc:
        return None

    data = bytes(int(x, 16) for x in data_tokens[:dlc])
    return CanFrame(t_ms=t_ms, can_id=can_id, data=data, is_extended=(can_id > 0x7FF))


def _parse_slcan_text_frame(line: str, line_no: int, allow_dlc0: bool = False) -> Optional[CanFrame]:
    raw = line.strip()
    if not raw:
        return None
    # Drop CR and common separators/comments. Do not parse SLCAN control commands here.
    raw = raw.split()[0].strip().rstrip("\r")
    if not raw or raw[0] not in {"t", "T"}:
        return None

    try:
        if raw[0] == "t":
            if len(raw) < 5:
                return None
            can_id = int(raw[1:4], 16)
            dlc = int(raw[4], 16)
            data_start = 5
            is_extended = False
        else:
            if len(raw) < 10:
                return None
            can_id = int(raw[1:9], 16)
            dlc = int(raw[9], 16)
            data_start = 10
            is_extended = True
    except ValueError:
        return None

    if dlc > 8:
        return None
    if dlc == 0 and not allow_dlc0:
        return None

    data_hex = raw[data_start:data_start + dlc * 2]
    if len(data_hex) != dlc * 2:
        return None
    try:
        data = bytes.fromhex(data_hex)
    except ValueError:
        return None

    return CanFrame(t_ms=float(line_no), can_id=can_id, data=data, is_extended=is_extended)


def parse_trc(path: Path, allow_dlc0: bool = False) -> list[CanFrame]:
    """Parse a classic-CAN TRC file into frames.

    The parser is intentionally tolerant because PCAN, CAN-Hacker and other tools
    write slightly different .trc layouts. It searches for a DLC token followed by
    DLC data bytes; the closest hex token before DLC is treated as CAN ID.
    """
    frames: list[CanFrame] = []

    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line_no, line in enumerate(f, start=1):
            # CAN-Hacker-style TRC from the user's recordings. This must be
            # tried before the generic parser, otherwise the flags/channel columns
            # can be misdetected as DLC/data.
            ch = _parse_canhacker_tsv_frame(line, allow_dlc0=allow_dlc0)
            if ch is not None:
                frames.append(ch)
                continue

            # Some tools already store frames in LAWICEL/SLCAN text format.
            # Example: t0648AABBCCDDEEFF0011
            sl = _parse_slcan_text_frame(line, line_no, allow_dlc0=allow_dlc0)
            if sl is not None:
                frames.append(sl)
                continue

            tokens = _tokenize_trc_line(line)
            if len(tokens) < 4:
                continue

            # Timestamp: use the first token containing a decimal point if possible,
            # otherwise the first numeric token after an optional message number.
            t_ms: Optional[float] = None
            for tok in tokens:
                if "." in tok:
                    val = _parse_float(tok)
                    if val is not None:
                        t_ms = val
                        break
            if t_ms is None:
                for tok in tokens[1:] if len(tokens) > 1 else tokens:
                    val = _parse_float(tok)
                    if val is not None:
                        t_ms = val
                        break
            if t_ms is None:
                t_ms = 0.0

            parsed: Optional[CanFrame] = None

            # Find DLC followed by DLC bytes. Only classic CAN is sent by LAWICEL SLCAN,
            # so DLC > 8 is skipped here.
            for dlc_idx, tok in enumerate(tokens):
                dlc = _parse_decimal_int(tok)
                if dlc is None or dlc < 0 or dlc > 8:
                    continue
                # In real automotive TRC files a DLC=0 data frame is rare.
                # Accepting every "0" as a valid DLC makes the parser too eager
                # and can create bogus commands like t0640. Enable --allow-dlc0
                # only if your recordings intentionally contain zero-length CAN frames.
                if dlc == 0 and not allow_dlc0:
                    continue
                if dlc_idx + 1 + dlc > len(tokens):
                    continue

                data_vals: list[int] = []
                ok = True
                for b_tok in tokens[dlc_idx + 1 : dlc_idx + 1 + dlc]:
                    b = _parse_hex_int(b_tok)
                    if b is None or b < 0 or b > 0xFF:
                        ok = False
                        break
                    data_vals.append(b)
                if not ok:
                    continue

                # CAN ID is normally the closest hex value before DLC.
                can_id: Optional[int] = None
                for id_idx in range(dlc_idx - 1, -1, -1):
                    low = tokens[id_idx].lower()
                    if low in {"rx", "tx", "r", "t", "d", "dt", "fd", "data", "can", "std", "ext"}:
                        continue
                    maybe_id = _parse_hex_int(tokens[id_idx])
                    if maybe_id is None:
                        continue
                    # Skip tiny channel numbers if there is another plausible ID closer
                    # to DLC. Because we iterate backwards, the first plausible value wins.
                    can_id = maybe_id
                    break

                if can_id is None:
                    continue

                parsed = CanFrame(
                    t_ms=t_ms,
                    can_id=can_id,
                    data=bytes(data_vals),
                    is_extended=can_id > 0x7FF,
                )
                break

            if parsed is not None:
                frames.append(parsed)
            else:
                # Non-frame header lines are normal; keep silent.
                pass

    if not frames:
        raise ValueError(f"No CAN frames parsed from {path}")

    # Normalize first timestamp to zero while preserving intervals.
    base = frames[0].t_ms
    return [CanFrame(f.t_ms - base, f.can_id, f.data, f.is_extended) for f in frames]


# ----------------------------- SLCAN sender -----------------------------

SLCAN_BITRATE_COMMAND = {
    10000: "S0",
    20000: "S1",
    50000: "S2",
    100000: "S3",
    125000: "S4",
    250000: "S5",
    500000: "S6",
    800000: "S7",
    1000000: "S8",
}


def frame_to_slcan_command(frame: CanFrame) -> str:
    if frame.dlc > 8:
        raise ValueError("SLCAN classic frame cannot send DLC > 8")
    data_hex = "".join(f"{b:02X}" for b in frame.data)
    if frame.is_extended:
        return f"T{frame.can_id:08X}{frame.dlc:X}{data_hex}"
    return f"t{frame.can_id:03X}{frame.dlc:X}{data_hex}"


class SlcanSender:
    def __init__(
        self,
        port: str,
        baudrate: int,
        can_bitrate: int,
        init_adapter: bool = True,
        write_timeout: float = 5.0,
        drain_readback: bool = True,
        dump_slcan_rx: bool = False,
    ) -> None:
        if serial is None:
            raise RuntimeError("pyserial is not installed. Run: pip install pyserial")

        self.port = port
        self.baudrate = baudrate
        self.can_bitrate = can_bitrate
        self.init_adapter = init_adapter
        self.write_timeout = write_timeout
        self.drain_readback = drain_readback
        self.dump_slcan_rx = dump_slcan_rx
        self.ser: serial.Serial
        self._rx_stop = threading.Event()
        self._rx_thread: Optional[threading.Thread] = None
        self._rx_lock = threading.Lock()
        self._rx_bytes_total = 0
        self._rx_lines_total = 0
        self._rx_bel_total = 0
        self._rx_last_error = ""
        self._open_serial_and_init()

    def _rx_worker(self) -> None:
        buf = b""
        while not self._rx_stop.is_set():
            try:
                chunk = self.ser.read(4096)
            except Exception as exc:
                with self._rx_lock:
                    self._rx_last_error = str(exc)
                time.sleep(0.05)
                continue

            if not chunk:
                continue

            with self._rx_lock:
                self._rx_bytes_total += len(chunk)
                self._rx_bel_total += chunk.count(b"\x07")

            if self.dump_slcan_rx:
                safe = chunk.replace(b"\r", b"<CR>").replace(b"\n", b"<LF>")
                try:
                    print(f"    SLCAN RX: {safe.decode('ascii', errors='replace')}", flush=True)
                except Exception:
                    print(f"    SLCAN RX bytes: {chunk!r}", flush=True)

            buf += chunk
            # Many SLCAN adapters answer commands with CR. Others echo frames or
            # forward received CAN frames as SLCAN text. Count and discard all of it;
            # otherwise the PC RX queue can fill after a few thousand frames and the
            # adapter firmware may stop consuming TX commands, which looks like a
            # Write timeout on the Python side.
            while b"\r" in buf or b"\n" in buf:
                r_pos = buf.find(b"\r") if b"\r" in buf else 10**9
                n_pos = buf.find(b"\n") if b"\n" in buf else 10**9
                pos = min(r_pos, n_pos)
                _line, buf = buf[:pos], buf[pos + 1 :]
                with self._rx_lock:
                    self._rx_lines_total += 1

    def _start_rx_drain(self) -> None:
        if not self.drain_readback:
            return
        self._rx_stop.clear()
        self._rx_thread = threading.Thread(target=self._rx_worker, daemon=True)
        self._rx_thread.start()

    def _stop_rx_drain(self) -> None:
        self._rx_stop.set()
        if self._rx_thread is not None:
            self._rx_thread.join(timeout=1.0)
            self._rx_thread = None

    def rx_stats(self, reset: bool = False) -> dict[str, int | str]:
        with self._rx_lock:
            stats = {
                "rx_bytes": self._rx_bytes_total,
                "rx_lines": self._rx_lines_total,
                "rx_bel": self._rx_bel_total,
                "rx_last_error": self._rx_last_error,
            }
            if reset:
                self._rx_bytes_total = 0
                self._rx_lines_total = 0
                self._rx_bel_total = 0
                self._rx_last_error = ""
        return stats

    def _open_serial_and_init(self) -> None:
        self._stop_rx_drain()
        self.ser = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            timeout=0.02,
            write_timeout=self.write_timeout,
        )
        time.sleep(0.25)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()
        self.rx_stats(reset=True)
        self._start_rx_drain()

        if self.init_adapter:
            cmd = SLCAN_BITRATE_COMMAND.get(self.can_bitrate)
            if cmd is None:
                valid = ", ".join(str(x) for x in sorted(SLCAN_BITRATE_COMMAND))
                raise ValueError(f"Unsupported SLCAN bitrate {self.can_bitrate}. Valid: {valid}")
            self._send_slcan_command("C", force_flush=True)
            time.sleep(0.08)
            self._send_slcan_command(cmd, force_flush=True)
            time.sleep(0.08)
            self._send_slcan_command("O", force_flush=True)
            time.sleep(0.12)

    def close(self) -> None:
        self._stop_rx_drain()
        try:
            self.ser.close()
        except Exception:
            pass

    def reopen(self, attempts: int = 8, delay_s: float = 1.0) -> bool:
        self.close()
        last_exc: Optional[BaseException] = None
        for i in range(1, max(1, attempts) + 1):
            try:
                time.sleep(delay_s)
                self._open_serial_and_init()
                return True
            except Exception as exc:
                last_exc = exc
                print(f"    reconnect attempt {i}/{attempts} failed: {exc}", flush=True)
        print(f"    SLCAN reconnect failed: {last_exc}", flush=True)
        return False

    def _send_slcan_command(self, cmd: str, force_flush: bool = True) -> None:
        if not cmd.endswith("\r"):
            cmd += "\r"
        self.ser.write(cmd.encode("ascii"))
        if force_flush:
            self.ser.flush()

    def send_frame(self, frame: CanFrame, flush: bool = False) -> None:
        cmd = frame_to_slcan_command(frame) + "\r"
        self.ser.write(cmd.encode("ascii"))
        if flush:
            self.ser.flush()

    @staticmethod
    def estimated_uart_time_ms(frame: CanFrame, baudrate: int, margin: float) -> float:
        # UART is usually 8N1, so every byte takes ~10 bits on the wire.
        # SLCAN command length includes the trailing \r.
        bytes_on_uart = len(frame_to_slcan_command(frame)) + 1
        return (bytes_on_uart * 10.0 * 1000.0 / max(baudrate, 1)) * max(margin, 1.0)

    def replay(
        self,
        frames: Iterable[CanFrame],
        speed: float = 1.0,
        min_gap_ms: float = 0.0,
        auto_throttle: bool = True,
        auto_throttle_margin: float = 1.6,
        auto_throttle_min_ms: float = 1.5,
        flush_every: int = 1,
    ) -> int:
        sent = 0
        prev_ms: Optional[float] = None
        for frame in frames:
            if prev_ms is not None:
                delay_ms = max(0.0, (frame.t_ms - prev_ms) / max(speed, 1e-9))
                delay_ms = max(delay_ms, min_gap_ms)
                if auto_throttle:
                    delay_ms = max(
                        delay_ms,
                        self.estimated_uart_time_ms(frame, self.baudrate, auto_throttle_margin),
                        auto_throttle_min_ms,
                    )
                if delay_ms > 0:
                    time.sleep(delay_ms / 1000.0)
            sent += 1
            self.send_frame(frame, flush=(flush_every > 0 and (sent % flush_every) == 0))
            prev_ms = frame.t_ms
        try:
            self.ser.flush()
        except Exception:
            # Let the caller handle the original serial failure pattern.
            raise
        return sent


# ----------------------------- UART log reader -----------------------------

class UartLogReader:
    def __init__(self, port: str, baudrate: int) -> None:
        if serial is None:
            raise RuntimeError("pyserial is not installed. Run: pip install pyserial")
        self.ser = serial.Serial(port=port, baudrate=baudrate, timeout=0.05)
        time.sleep(0.15)
        self.ser.reset_input_buffer()
        self._q: queue.Queue[tuple[float, str]] = queue.Queue()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        try:
            self.ser.close()
        except Exception:
            pass

    def clear(self) -> None:
        try:
            self.ser.reset_input_buffer()
        except Exception:
            pass
        while True:
            try:
                self._q.get_nowait()
            except queue.Empty:
                break

    def drain(self) -> list[str]:
        lines: list[str] = []
        while True:
            try:
                _, line = self._q.get_nowait()
                lines.append(line)
            except queue.Empty:
                break
        return lines

    def _run(self) -> None:
        buf = b""
        while not self._stop.is_set():
            try:
                chunk = self.ser.read(1024)
            except Exception as exc:
                self._q.put((time.monotonic(), f"<log-reader-error> {exc}"))
                time.sleep(0.2)
                continue

            if not chunk:
                continue

            buf += chunk
            while b"\n" in buf:
                raw, buf = buf.split(b"\n", 1)
                line = raw.decode("utf-8", errors="replace").rstrip("\r")
                if line.strip():
                    self._q.put((time.monotonic(), line))


# ----------------------------- Event detection -----------------------------

RE_CHANGED = re.compile(r"CAN\s+event\s+(\d+)\s+changed\s+to\s+(ON|OFF)", re.IGNORECASE)
RE_TRIGGERED = re.compile(r"CAN\s+event\s+(\d+)(?:\s+\(([^)]*)\))?\s+triggered", re.IGNORECASE)


@dataclass
class TestResult:
    event_id: int
    event_type: str
    trc_file: str
    passed: bool
    reason: str
    frames_sent: int
    on_count: int
    off_count: int
    triggered_count: int
    event_name: str = ""
    matched_lines: list[str] | None = None
    replay_elapsed_s: float = 0.0
    rx_bytes: int = 0
    rx_bel: int = 0


def analyze_event_logs(event_id: int, lines: list[str]) -> tuple[int, int, int, str, list[str]]:
    on_count = 0
    off_count = 0
    triggered_count = 0
    event_name = ""
    matched: list[str] = []

    for line in lines:
        m = RE_CHANGED.search(line)
        if m and int(m.group(1)) == event_id:
            state = m.group(2).upper()
            if state == "ON":
                on_count += 1
            elif state == "OFF":
                off_count += 1
            matched.append(line)
            continue

        m = RE_TRIGGERED.search(line)
        if m and int(m.group(1)) == event_id:
            triggered_count += 1
            if m.group(2):
                event_name = m.group(2)
            matched.append(line)

    return on_count, off_count, triggered_count, event_name, matched


# ----------------------------- Directory scan and runner -----------------------------

@dataclass(frozen=True)
class EventFolder:
    event_id: int
    path: Path
    is_button: bool


def find_event_folders(root: Path) -> list[EventFolder]:
    found: list[EventFolder] = []
    for d in root.rglob("*"):
        if not d.is_dir():
            continue
        if not d.name.isdigit():
            continue
        if not any(d.glob("*.trc")):
            continue
        found.append(EventFolder(event_id=int(d.name), path=d, is_button=(d / "button.trc").exists()))

    # Also support root itself being an event folder.
    if root.is_dir() and root.name.isdigit() and any(root.glob("*.trc")):
        folder = EventFolder(event_id=int(root.name), path=root, is_button=(root / "button.trc").exists())
        if folder not in found:
            found.append(folder)

    return sorted(found, key=lambda x: (x.event_id, str(x.path)))


def load_frames(path: Path, allow_dlc0: bool = False) -> list[CanFrame]:
    frames = parse_trc(path, allow_dlc0=allow_dlc0)
    if any(f.dlc > 8 for f in frames):
        raise ValueError(f"{path}: contains CAN-FD/DLC>8 frames; classic SLCAN cannot replay them")
    return frames


def frames_duration_s(frames: list[CanFrame], speed: float = 1.0) -> float:
    if not frames:
        return 0.0
    # Some CAN-Hacker/TRC exports reset timestamps inside a file or place a
    # final metadata-like row with an old timestamp. Using last-first can show
    # duration≈0.00s even when the file contains thousands of frames.
    t_values = [f.t_ms for f in frames]
    return max(0.0, max(t_values) - min(t_values)) / 1000.0 / max(speed, 1e-9)


def format_replay_timing(elapsed_s: float, trc_s: float) -> str:
    if trc_s > 0.001:
        return f"elapsed={elapsed_s:.2f}s, trc≈{trc_s:.2f}s, slow≈{elapsed_s / trc_s:.2f}x"
    return f"elapsed={elapsed_s:.2f}s, trc≈{trc_s:.2f}s"


def format_slcan_rx_stats(st: dict[str, int | str], context: str, elapsed_s: float, trc_s: float, warn_on_bel: bool = False) -> str:
    bel = int(st.get("rx_bel", 0) or 0)
    suffix = ""
    if bel:
        # LAWICEL SLCAN defines BEL as an error response. In this bench setup it
        # can happen for almost every transmitted frame if there is no CAN ACK
        # on the replay bus, while the alarm still receives enough frames to
        # trigger. So keep it visible but do not make every line look like a
        # catastrophic failure unless --warn-on-bel is explicitly requested.
        suffix = "  WARNING: BEL/error bytes" if warn_on_bel else "  NOTE: BEL bytes returned by adapter"
    return (
        f"    SLCAN RX during {context}: bytes={st.get('rx_bytes')} "
        f"lines={st.get('rx_lines')} bel={bel}; "
        f"{format_replay_timing(elapsed_s, trc_s)}{suffix}"
    )


def maybe_dump_first_frames(args: argparse.Namespace, path: Path, frames: list[CanFrame]) -> None:
    n = getattr(args, "dump_first_frames", 0)
    if n <= 0:
        return
    print(f"    first {min(n, len(frames))} parsed frames from {path}:", flush=True)
    for i, f in enumerate(frames[:n], start=1):
        data_hex = " ".join(f"{b:02X}" for b in f.data)
        print(
            f"      #{i:<3} t={f.t_ms:>10.3f}ms id=0x{f.can_id:X} dlc={f.dlc} data=[{data_hex}] slcan={frame_to_slcan_command(f)}",
            flush=True,
        )


def log_progress(args: argparse.Namespace, msg: str) -> None:
    if not getattr(args, "quiet", False):
        print(msg, flush=True)


def replay_optional_idle(
    folder: EventFolder,
    slcan: Optional[SlcanSender],
    args: argparse.Namespace,
) -> None:
    idle_path = folder.path / "idle.trc"
    if not idle_path.exists() or args.no_idle_reset:
        return
    frames = load_frames(idle_path, allow_dlc0=args.allow_dlc0)
    maybe_dump_first_frames(args, idle_path, frames)
    trc_s = frames_duration_s(frames, args.speed)
    log_progress(
        args,
        f"    idle reset: {idle_path} | frames={len(frames)} | duration≈{trc_s:.2f}s",
    )
    if slcan is not None:
        slcan.rx_stats(reset=True)
        t0 = time.perf_counter()
        slcan.replay(
            frames,
            speed=args.speed,
            min_gap_ms=args.min_gap_ms,
            auto_throttle=not args.no_auto_throttle,
            auto_throttle_margin=args.auto_throttle_margin,
            auto_throttle_min_ms=args.auto_throttle_min_ms,
            flush_every=args.flush_every,
        )
        elapsed_s = time.perf_counter() - t0
        st = slcan.rx_stats(reset=True)
        if st.get("rx_bytes", 0) or st.get("rx_lines", 0) or st.get("rx_bel", 0):
            log_progress(args, format_slcan_rx_stats(st, "idle", elapsed_s, trc_s, warn_on_bel=args.warn_on_bel))
        else:
            log_progress(args, f"    replay idle done: {format_replay_timing(elapsed_s, trc_s)}")
    if args.idle_wait > 0:
        log_progress(args, f"    idle wait: {args.idle_wait:.2f}s")
        time.sleep(args.idle_wait)


def run_button_test(
    folder: EventFolder,
    slcan: Optional[SlcanSender],
    log_reader: Optional[UartLogReader],
    args: argparse.Namespace,
) -> TestResult:
    path = folder.path / "button.trc"
    frames = load_frames(path, allow_dlc0=args.allow_dlc0)
    maybe_dump_first_frames(args, path, frames)
    log_progress(
        args,
        f">>> event {folder.event_id}: button | file={path} | frames={len(frames)} | duration≈{frames_duration_s(frames, args.speed):.2f}s",
    )

    frames_sent = 0
    replay_elapsed_s = 0.0
    rx_bytes = 0
    rx_bel = 0
    if not args.dry_run:
        assert slcan is not None and log_reader is not None
        replay_optional_idle(folder, slcan, args)
        log_reader.clear()
        log_progress(args, "    replay button.trc...")
        slcan.rx_stats(reset=True)
        trc_s = frames_duration_s(frames, args.speed)
        t0 = time.perf_counter()
        frames_sent = slcan.replay(
            frames,
            speed=args.speed,
            min_gap_ms=args.min_gap_ms,
            auto_throttle=not args.no_auto_throttle,
            auto_throttle_margin=args.auto_throttle_margin,
            auto_throttle_min_ms=args.auto_throttle_min_ms,
            flush_every=args.flush_every,
        )
        replay_elapsed_s = time.perf_counter() - t0
        st = slcan.rx_stats(reset=True)
        if st.get("rx_bytes", 0) or st.get("rx_lines", 0) or st.get("rx_bel", 0):
            log_progress(args, format_slcan_rx_stats(st, "replay", replay_elapsed_s, trc_s, warn_on_bel=args.warn_on_bel))
        else:
            log_progress(args, f"    replay done: {format_replay_timing(replay_elapsed_s, trc_s)}")
        rx_bytes = int(st.get("rx_bytes", 0) or 0)
        rx_bel = int(st.get("rx_bel", 0) or 0)
        log_progress(args, f"    wait logs: {args.post_wait:.2f}s")
        time.sleep(args.post_wait)
        lines = log_reader.drain()
    else:
        frames_sent = len(frames)
        replay_elapsed_s = 0.0
        rx_bytes = 0
        rx_bel = 0
        lines = []

    on_count, off_count, triggered_count, event_name, matched = analyze_event_logs(folder.event_id, lines)
    if args.dry_run:
        passed = True
        reason = "dry-run: parsed only"
    elif args.button_match == "exact":
        passed = triggered_count == args.button_expected
        reason = f"triggered={triggered_count}, expected exactly {args.button_expected}"
    else:
        passed = triggered_count >= args.button_expected
        reason = f"triggered={triggered_count}, expected at least {args.button_expected}"

    return TestResult(
        event_id=folder.event_id,
        event_type="button",
        trc_file=str(path),
        passed=passed,
        reason=reason,
        frames_sent=frames_sent,
        on_count=on_count,
        off_count=off_count,
        triggered_count=triggered_count,
        event_name=event_name,
        matched_lines=matched[-args.keep_log_lines :],
        replay_elapsed_s=replay_elapsed_s,
        rx_bytes=rx_bytes,
        rx_bel=rx_bel,
    )


def state_test_files(folder: EventFolder) -> list[Path]:
    # For state verification toggle.trc is the most useful recording because it
    # should contain several real state activations/deactivations. Keep open.trc
    # as a separate check: it should produce one ON activation.
    preferred = [folder.path / "toggle.trc", folder.path / "open.trc"]
    files = [p for p in preferred if p.exists()]
    if files:
        return files
    return sorted(
        p for p in folder.path.glob("*.trc")
        if p.name.lower() not in {"idle.trc", "button.trc"}
    )


def run_state_test(
    folder: EventFolder,
    trc_path: Path,
    slcan: Optional[SlcanSender],
    log_reader: Optional[UartLogReader],
    args: argparse.Namespace,
) -> TestResult:
    frames = load_frames(trc_path, allow_dlc0=args.allow_dlc0)
    maybe_dump_first_frames(args, trc_path, frames)
    log_progress(
        args,
        f">>> event {folder.event_id}: state  | file={trc_path} | frames={len(frames)} | duration≈{frames_duration_s(frames, args.speed):.2f}s",
    )

    frames_sent = 0
    replay_elapsed_s = 0.0
    rx_bytes = 0
    rx_bel = 0
    if not args.dry_run:
        assert slcan is not None and log_reader is not None
        replay_optional_idle(folder, slcan, args)
        log_reader.clear()
        log_progress(args, f"    replay {trc_path.name}...")
        slcan.rx_stats(reset=True)
        trc_s = frames_duration_s(frames, args.speed)
        t0 = time.perf_counter()
        frames_sent = slcan.replay(
            frames,
            speed=args.speed,
            min_gap_ms=args.min_gap_ms,
            auto_throttle=not args.no_auto_throttle,
            auto_throttle_margin=args.auto_throttle_margin,
            auto_throttle_min_ms=args.auto_throttle_min_ms,
            flush_every=args.flush_every,
        )
        replay_elapsed_s = time.perf_counter() - t0
        st = slcan.rx_stats(reset=True)
        if st.get("rx_bytes", 0) or st.get("rx_lines", 0) or st.get("rx_bel", 0):
            log_progress(args, format_slcan_rx_stats(st, "replay", replay_elapsed_s, trc_s, warn_on_bel=args.warn_on_bel))
        else:
            log_progress(args, f"    replay done: {format_replay_timing(replay_elapsed_s, trc_s)}")
        rx_bytes = int(st.get("rx_bytes", 0) or 0)
        rx_bel = int(st.get("rx_bel", 0) or 0)
        log_progress(args, f"    wait logs: {args.post_wait:.2f}s")
        time.sleep(args.post_wait)
        lines = log_reader.drain()
    else:
        frames_sent = len(frames)
        replay_elapsed_s = 0.0
        rx_bytes = 0
        rx_bel = 0
        lines = []

    on_count, off_count, triggered_count, event_name, matched = analyze_event_logs(folder.event_id, lines)
    file_name = trc_path.name.lower()

    if args.dry_run:
        passed = True
        reason = "dry-run: parsed only"
    elif file_name == "toggle.trc":
        # For state toggle recordings, count only ON transitions as real
        # activations. OFF transitions are useful diagnostics but are not
        # counted as separate state activations.
        passed = args.state_toggle_min <= on_count <= args.state_toggle_max
        reason = (
            f"ON={on_count}, OFF={off_count}, "
            f"expected ON in [{args.state_toggle_min}..{args.state_toggle_max}] for toggle.trc"
        )
        if args.state_require_off_for_toggle:
            passed = passed and off_count >= 1
            reason += ", and OFF >= 1"
    elif file_name == "open.trc":
        passed = on_count == args.state_open_expected
        reason = f"ON={on_count}, OFF={off_count}, expected ON exactly {args.state_open_expected} for open.trc"
    else:
        passed = on_count >= args.state_min_on
        reason = f"ON={on_count}, OFF={off_count}, expected ON >= {args.state_min_on}"

    return TestResult(
        event_id=folder.event_id,
        event_type="state",
        trc_file=str(trc_path),
        passed=passed,
        reason=reason,
        frames_sent=frames_sent,
        on_count=on_count,
        off_count=off_count,
        triggered_count=triggered_count,
        event_name=event_name,
        matched_lines=matched[-args.keep_log_lines :],
        replay_elapsed_s=replay_elapsed_s,
        rx_bytes=rx_bytes,
        rx_bel=rx_bel,
    )


def print_result(result: TestResult) -> None:
    mark = "PASS" if result.passed else "FAIL"
    rel_file = result.trc_file
    print(
        f"[{mark}] event={result.event_id:>2} type={result.event_type:<6} "
        f"file={rel_file} frames={result.frames_sent} "
        f"ON={result.on_count} OFF={result.off_count} TRIG={result.triggered_count} "
        f"elapsed={result.replay_elapsed_s:.2f}s bel={result.rx_bel} | {result.reason}",
        flush=True,
    )
    if result.matched_lines:
        for line in result.matched_lines:
            print(f"      log: {line}", flush=True)


def safe_report_folder_name(root: Path) -> str:
    """Return a filesystem-safe report folder name derived from --root.

    For --root kia -> kia
    For --root cars/kia -> kia
    For --root . -> current directory name
    """
    name = root.name or root.resolve().name or "root"
    # Windows-forbidden chars: < > : " / \ | ? * and control chars.
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", name).strip(" ._")
    return name or "root"


def configure_report_paths(args: argparse.Namespace, root: Path) -> None:
    """Fill report paths from --root when explicit report paths are absent."""
    if args.no_auto_report:
        return
    if args.report_json or args.report_csv:
        return

    report_dir = Path(args.report_base_dir) / safe_report_folder_name(root)
    args.report_json = str(report_dir / "report.json")
    args.report_csv = str(report_dir / "report.csv")


def write_reports(results: list[TestResult], args: argparse.Namespace) -> None:
    if args.report_json:
        out = Path(args.report_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps([asdict(r) for r in results], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"JSON report: {out}")

    if args.report_csv:
        out = Path(args.report_csv)
        out.parent.mkdir(parents=True, exist_ok=True)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "event_id",
                    "event_type",
                    "trc_file",
                    "passed",
                    "reason",
                    "frames_sent",
                    "on_count",
                    "off_count",
                    "triggered_count",
                    "event_name",
                    "replay_elapsed_s",
                    "rx_bytes",
                    "rx_bel",
                ],
            )
            writer.writeheader()
            for r in results:
                row = asdict(r)
                row.pop("matched_lines", None)
                writer.writerow(row)
        print(f"CSV report: {out}")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Replay TRC recordings via SLCAN and verify alarm CAN event logs via UART."
    )
    p.add_argument("--root", required=True, help="Root directory with numeric event folders, e.g. kia")

    p.add_argument("--can-port", help="SLCAN UART port, e.g. COM7 or /dev/ttyUSB0")
    p.add_argument("--log-port", help="Alarm log UART port, e.g. COM8 or /dev/ttyUSB1")
    p.add_argument("--slcan-baud", type=int, default=115200, help="SLCAN UART baudrate")
    p.add_argument("--log-baud", type=int, default=115200, help="Alarm log UART baudrate")
    p.add_argument("--can-bitrate", type=int, default=500000, help="CAN bitrate for SLCAN init")
    p.add_argument("--no-slcan-init", action="store_true", help="Do not send C/Sx/O commands to SLCAN adapter")
    p.add_argument("--allow-dlc0", action="store_true", help="Allow zero-length CAN frames from TRC. Disabled by default to avoid false tXXX0 frames from ambiguous TRC parsing")
    p.add_argument("--dump-first-frames", type=int, default=0, help="Print the first N parsed frames and SLCAN commands for every replayed file")

    p.add_argument("--speed", type=float, default=1.0, help="Replay speed multiplier. 1.0 preserves TRC timing")
    p.add_argument("--min-gap-ms", type=float, default=0.0, help="Minimum delay between transmitted frames")
    p.add_argument("--no-auto-throttle", action="store_true", help="Disable automatic SLCAN UART throttling. Not recommended at 115200 baud")
    p.add_argument("--auto-throttle-margin", type=float, default=1.6, help="UART time safety multiplier for automatic throttling")
    p.add_argument("--auto-throttle-min-ms", type=float, default=1.5, help="Minimum automatic delay between SLCAN frame commands")
    p.add_argument("--flush-every", type=int, default=1, help="Flush serial TX every N frames. 1 is safest; 0 disables periodic flush")
    p.add_argument("--write-timeout", type=float, default=5.0, help="Serial write timeout in seconds")
    p.add_argument("--no-slcan-drain", action="store_true", help="Disable background reading/draining of SLCAN adapter responses. Not recommended")
    p.add_argument("--dump-slcan-rx", action="store_true", help="Print raw data received from the SLCAN adapter. Very verbose")
    p.add_argument("--warn-on-bel", action="store_true", help="Show BEL responses from SLCAN adapter as WARNING instead of NOTE")
    p.add_argument("--reopen-attempts", type=int, default=8, help="How many times to reopen SLCAN adapter after serial write errors")
    p.add_argument("--reopen-delay", type=float, default=1.0, help="Delay between SLCAN reopen attempts")
    p.add_argument("--pause-after-reopen", type=float, default=0.5, help="Pause after successful SLCAN reopen")
    p.add_argument("--post-wait", type=float, default=2.0, help="Seconds to wait for logs after each replay")
    p.add_argument("--idle-wait", type=float, default=0.5, help="Seconds to wait after replaying idle.trc reset")
    p.add_argument("--no-idle-reset", action="store_true", help="Do not replay idle.trc before event test")

    p.add_argument("--state-min-on", type=int, default=1, help="Fallback for non-standard state files: pass if ON count is at least this value")
    p.add_argument("--state-open-expected", type=int, default=1, help="For open.trc, expected ON activation count. Default: exactly 1")
    p.add_argument("--state-toggle-min", type=int, default=1, help="For toggle.trc, minimum accepted ON activation count")
    p.add_argument("--state-toggle-max", type=int, default=5, help="For toggle.trc, maximum accepted ON activation count")
    p.add_argument(
        "--state-require-off-for-toggle",
        action="store_true",
        help="For toggle.trc, additionally require at least one OFF transition",
    )
    p.add_argument("--button-expected", type=int, default=3, help="Expected button trigger count")
    p.add_argument(
        "--button-match",
        choices=["exact", "at-least"],
        default="exact",
        help="Button trigger check mode",
    )

    p.add_argument("--event", action="append", type=int, help="Run only selected event ID. Can be used multiple times")
    p.add_argument("--skip-states", action="store_true", help="Skip state-event folders")
    p.add_argument("--skip-buttons", action="store_true", help="Skip button-event folders")
    p.add_argument("--quiet", action="store_true", help="Hide replay progress messages")
    p.add_argument("--stop-on-fail", action="store_true", help="Stop after first failed test")
    p.add_argument("--dry-run", action="store_true", help="Only parse folders/TRC files; do not open serial ports")
    p.add_argument("--report-json", help="Optional JSON report path. If omitted, auto path is reports/<root-name>/report.json")
    p.add_argument("--report-csv", help="Optional CSV report path. If omitted, auto path is reports/<root-name>/report.csv")
    p.add_argument("--report-base-dir", default="reports", help="Base directory for automatic reports. Default: reports")
    p.add_argument("--no-auto-report", action="store_true", help="Do not auto-create reports/<root-name>/report.json and report.csv when report paths are omitted")
    p.add_argument("--keep-log-lines", type=int, default=20, help="How many matched log lines to keep in report")
    return p



def maybe_reconnect_slcan(slcan: Optional[SlcanSender], args: argparse.Namespace, reason: BaseException) -> None:
    if slcan is None or args.dry_run:
        return
    print(f"    SLCAN write/replay error detected: {reason}", flush=True)
    print(
        f"    Trying to reopen {args.can_port} and reinitialize CAN bitrate {args.can_bitrate}...",
        flush=True,
    )
    ok = slcan.reopen(attempts=args.reopen_attempts, delay_s=args.reopen_delay)
    if ok:
        print("    SLCAN adapter reopened successfully; continuing with next test.", flush=True)
        time.sleep(args.pause_after_reopen)
    else:
        print("    SLCAN adapter was not reopened; following tests will likely fail.", flush=True)

def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    root = Path(args.root)
    if not root.exists():
        print(f"Root not found: {root}", file=sys.stderr)
        return 2
    if args.speed <= 0:
        print("--speed must be > 0", file=sys.stderr)
        return 2
    if args.flush_every < 0:
        print("--flush-every must be >= 0", file=sys.stderr)
        return 2
    if args.write_timeout <= 0:
        print("--write-timeout must be > 0", file=sys.stderr)
        return 2
    if args.reopen_attempts < 1:
        print("--reopen-attempts must be >= 1", file=sys.stderr)
        return 2
    if args.state_toggle_min < 0 or args.state_toggle_max < args.state_toggle_min:
        print("Invalid toggle state range: require 0 <= --state-toggle-min <= --state-toggle-max", file=sys.stderr)
        return 2
    if args.state_open_expected < 0:
        print("--state-open-expected must be >= 0", file=sys.stderr)
        return 2

    configure_report_paths(args, root)

    folders = find_event_folders(root)
    if args.event:
        selected = set(args.event)
        folders = [f for f in folders if f.event_id in selected]
    if args.skip_states:
        folders = [f for f in folders if f.is_button]
    if args.skip_buttons:
        folders = [f for f in folders if not f.is_button]

    if not folders:
        print(f"No matching numeric event folders with .trc files found under {root}", file=sys.stderr)
        return 2

    print(f"Found {len(folders)} event folders under {root}", flush=True)
    for f in folders:
        kind = "button" if f.is_button else "state"
        print(f"  event {f.event_id:>2}: {kind:6} {f.path}", flush=True)
    if args.report_json or args.report_csv:
        print(f"Reports directory: {Path(args.report_json or args.report_csv).parent}", flush=True)
        if args.report_json:
            print(f"  JSON: {args.report_json}", flush=True)
        if args.report_csv:
            print(f"  CSV:  {args.report_csv}", flush=True)
    print(flush=True)

    slcan: Optional[SlcanSender] = None
    log_reader: Optional[UartLogReader] = None
    results: list[TestResult] = []

    try:
        if not args.dry_run:
            if not args.can_port or not args.log_port:
                print("--can-port and --log-port are required unless --dry-run is used", file=sys.stderr)
                return 2
            log_progress(args, f"Opening SLCAN port {args.can_port} @ {args.slcan_baud}, CAN={args.can_bitrate}...")
            slcan = SlcanSender(
                port=args.can_port,
                baudrate=args.slcan_baud,
                can_bitrate=args.can_bitrate,
                init_adapter=not args.no_slcan_init,
                write_timeout=args.write_timeout,
                drain_readback=not args.no_slcan_drain,
                dump_slcan_rx=args.dump_slcan_rx,
            )
            log_progress(args, f"Opening alarm log port {args.log_port} @ {args.log_baud}...")
            log_reader = UartLogReader(port=args.log_port, baudrate=args.log_baud)
            log_progress(args, "Ports opened. Starting tests...")

        for folder in folders:
            try:
                if folder.is_button:
                    result = run_button_test(folder, slcan, log_reader, args)
                    results.append(result)
                    print_result(result)
                    if args.stop_on_fail and not result.passed:
                        break
                else:
                    files = state_test_files(folder)
                    if not files:
                        result = TestResult(
                            event_id=folder.event_id,
                            event_type="state",
                            trc_file=str(folder.path),
                            passed=False,
                            reason="no open.trc/toggle.trc/state .trc file found",
                            frames_sent=0,
                            on_count=0,
                            off_count=0,
                            triggered_count=0,
                            matched_lines=[],
                        )
                        results.append(result)
                        print_result(result)
                        if args.stop_on_fail:
                            break
                        continue

                    for trc_path in files:
                        result = run_state_test(folder, trc_path, slcan, log_reader, args)
                        results.append(result)
                        print_result(result)
                        if args.stop_on_fail and not result.passed:
                            break
                    if args.stop_on_fail and results and not results[-1].passed:
                        break
            except Exception as exc:
                result = TestResult(
                    event_id=folder.event_id,
                    event_type="button" if folder.is_button else "state",
                    trc_file=str(folder.path),
                    passed=False,
                    reason=f"exception: {exc}",
                    frames_sent=0,
                    on_count=0,
                    off_count=0,
                    triggered_count=0,
                    matched_lines=[],
                )
                results.append(result)
                print_result(result)
                if not args.dry_run and slcan is not None:
                    maybe_reconnect_slcan(slcan, args, exc)
                if args.stop_on_fail:
                    break

    finally:
        if log_reader is not None:
            log_reader.close()
        if slcan is not None:
            slcan.close()

    print(flush=True)
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = total - passed
    print(f"Summary: {passed}/{total} passed, {failed} failed", flush=True)
    if failed:
        print("Failed tests:", flush=True)
        for r in results:
            if not r.passed:
                print(
                    f"  event={r.event_id} type={r.event_type} file={r.trc_file} "
                    f"ON={r.on_count} OFF={r.off_count} TRIG={r.triggered_count} "
                    f"elapsed={r.replay_elapsed_s:.2f}s bel={r.rx_bel} | {r.reason}",
                    flush=True,
                )
    write_reports(results, args)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
