"""Standalone CLI recorder: writes NetworkTables value events to NDJSON.

Not an MCP tool. Connects to an NT4 server via ``NTManager``, subscribes
to a set of prefixes, and writes one JSON object per value event to a
timestamped ``.ndjson`` file. Status diagnostics go to stderr; NDJSON
goes only to the file.

Exit codes: 0 clean, 1 connection failure, 2 disk/IO error.

Usage::

    uv run nt-recorder --prefixes /SmartDashboard/ --output-dir ./recordings
    python -m nt_mcp_server.nt_recorder --team 254 --duration 30
    python -m nt_mcp_server.nt_recorder --duration 0.1 --prefixes /SmartDashboard/ --output-dir tests/tmp_recordings
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime
import fnmatch
import json
import os
import re
import signal
import sys
import time
from typing import Any, Iterator

from .nt_manager import NTConnectError, NTManager, StreamDecimator
from .nt_types import summarize

_CONNECT_TIMEOUT_SECONDS = 5.0
_CONNECT_POLL_SECONDS = 0.05


class Recorder:
    """Record NetworkTables value events to an NDJSON file.

    One JSON object per event: ``{"time": float, "topic": str, "value": jsonable}``.
    The file is flushed after every line and closed when ``run`` returns.
    When ``duration`` is None, runs until ``stop_event`` is set.

    Optional capture filters (all default None = unfiltered, uncapped):
    ``sample_interval`` keeps at most one sample per topic per interval,
    ``change_only`` drops samples whose value changed by <= the threshold,
    both via the same ``StreamDecimator`` used by ``NTManager.subscribe``.
    ``max_capture_rows`` stops capture once that many rows are written.
    After ``run()``: ``rows`` (int), ``topics`` (sorted unique names written),
    ``truncated`` (True only when the cap stopped capture early).
    """

    def __init__(
        self,
        manager: NTManager,
        output_dir: str,
        prefixes: list[str],
        duration: float | None = None,
        sample_interval: float | None = None,
        change_only: float | None = None,
        max_capture_rows: int | None = None,
        recording_id: str | None = None,
    ) -> None:
        self.manager = manager
        self.output_dir = output_dir
        self.prefixes = prefixes
        self.duration = duration
        self.stop_event = asyncio.Event()
        self._decimator = StreamDecimator(
            sample_interval=sample_interval, change_only=change_only
        )
        self._max_capture_rows = max_capture_rows
        self.recording_id = recording_id
        self.rows = 0
        self._topics: set[str] = set()
        self.topics: list[str] = []
        self.truncated = False

    async def run(self) -> str:
        """Subscribe, write events to NDJSON, return the output file path."""
        os.makedirs(self.output_dir, exist_ok=True)
        filename = self.recording_id or datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("nt-record-%Y-%m-%dT%H%M%SZ.ndjson")
        path = os.path.join(self.output_dir, filename)
        with open(path, "w", encoding="utf-8") as file:
            if self.duration is not None:
                # asyncio.timeout fires at the generator's await point even
                # when no events are flowing (bare time.monotonic checks
                # inside the async for never regain control between events).
                try:
                    async with asyncio.timeout(self.duration):
                        async for topic, value, time_seconds in (
                            self.manager.subscribe_stream(self.prefixes)
                        ):
                            if not self._write_filtered(
                                file, topic, value, time_seconds
                            ):
                                break
                except TimeoutError:
                    pass
            else:
                await self._run_until_stop(file)
        self.topics = sorted(self._topics)
        return path

    async def _run_until_stop(self, file) -> None:
        """Consume the stream until ``stop_event`` is set."""
        consume_task = asyncio.create_task(self._consume(file))
        stop_task = asyncio.create_task(self.stop_event.wait())
        try:
            await asyncio.wait(
                {consume_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (consume_task, stop_task):
                if not task.done():
                    task.cancel()
            # Drain: let the generator's finally close the subscriber and
            # poller; suppress only CancelledError, propagate real errors.
            for task in (consume_task, stop_task):
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _consume(self, file) -> None:
        async for topic, value, time_seconds in (
            self.manager.subscribe_stream(self.prefixes)
        ):
            if not self._write_filtered(file, topic, value, time_seconds):
                break

    def _write_filtered(self, file, topic: str, value: Any, time_seconds: float) -> bool:
        """Write one event through the decimator; update capture stats.

        Returns False only when ``max_capture_rows`` was reached and the
        caller must stop consuming (the file is already flushed per line).
        """
        if not self._decimator.keep(topic, value, time_seconds):
            return True
        _write_event(file, topic, value, time_seconds)
        self.rows += 1
        self._topics.add(topic)
        if self._max_capture_rows is not None and self.rows >= self._max_capture_rows:
            self.truncated = True
            return False
        return True


def _write_event(file, topic: str, value: Any, time_seconds: float) -> None:
    """Write one NDJSON line and flush."""
    line = {"time": time_seconds, "topic": topic, "value": value}
    file.write(json.dumps(line) + "\n")
    file.flush()


def _emit(quiet: bool, message: str) -> None:
    """Print a status line to stderr unless quiet."""
    if not quiet:
        print(message, file=sys.stderr, flush=True)


async def _wait_for_connection(manager: NTManager) -> bool:
    """Poll connection_info up to _CONNECT_TIMEOUT_SECONDS."""
    deadline = time.monotonic() + _CONNECT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if manager.connection_info()["connected"]:
            return True
        await asyncio.sleep(_CONNECT_POLL_SECONDS)
    return manager.connection_info()["connected"]


# --- Offline reader API -----------------------------------------------------
# Synchronous readers for NDJSON recording files. No live NT connection.
# Do NOT call these on a file currently being written by an active Recorder;
# the reader has no writer-detection — the caller must ensure the recording
# is closed first.


def _resolve_recording(
    recording_id: str | None,
    output_dir: str,
) -> str | None:
    """Return the path to the recording to read, or ``None`` if none exists.

    If ``recording_id`` is given, return ``output_dir / recording_id`` when
    that file exists, else ``None``. If ``recording_id`` is ``None``, return
    the path to the most recent ``.ndjson`` file in ``output_dir`` (or
    ``None`` if the directory is missing or empty).
    """
    if recording_id is not None:
        path = os.path.join(output_dir, recording_id)
        return path if os.path.isfile(path) else None
    recordings = list_recordings(output_dir)
    if not recordings:
        return None
    return recordings[0]["path"]


def _iter_events(path: str) -> Iterator[dict]:
    """Yield parsed event dicts from an NDJSON recording file.

    Skips blank lines and malformed JSON lines (warning to stderr). If the
    file does not exist, yields nothing.
    """
    if not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                yield json.loads(stripped)
            except json.JSONDecodeError as exc:
                print(
                    f"WARNING: skipping malformed line: {exc}",
                    file=sys.stderr,
                )


def _iter_timed_events(path: str) -> Iterator[tuple[float, dict]]:
    """Yield ``(time, event)`` for events with a valid numeric time."""
    for event in _iter_events(path):
        raw_time = event.get("time")
        if isinstance(raw_time, (int, float)):
            yield float(raw_time), event


def _time_bounds(path: str) -> tuple[float, float] | None:
    """Return ``(min_time, max_time)`` across all valid events, or ``None``.

    Zero or negative timestamps are ignored. NT's ``last_change()`` can
    return 0 for kImmediate/initial values, which would otherwise collapse
    the duration to the absolute end timestamp.
    """
    min_time: float | None = None
    max_time: float | None = None
    for t, _ in _iter_timed_events(path):
        if t <= 0:
            continue
        if min_time is None or t < min_time:
            min_time = t
        if max_time is None or t > max_time:
            max_time = t
    if min_time is None or max_time is None:
        return None
    return min_time, max_time


def _matches_filters(
    topic: str,
    prefix: str | None,
    regex: str | None,
    wildcard: str | None,
) -> bool:
    """Return True when ``topic`` matches all supplied filters."""
    if prefix is not None and not topic.startswith(prefix):
        return False
    if regex is not None and not re.search(regex, topic):
        return False
    if wildcard is not None and not fnmatch.fnmatch(topic, wildcard):
        return False
    return True


def _downsample(
    events: list[dict], sample_interval: float
) -> list[dict]:
    """Keep at most one event per ``sample_interval`` seconds."""
    if sample_interval is None or sample_interval <= 0:
        return events
    last_time: dict[str, float] = {}
    out: list[dict] = []
    for event in events:
        topic = event.get("topic", "")
        t = event["time"]
        if t - last_time.get(topic, -sample_interval) >= sample_interval:
            last_time[topic] = t
            out.append(event)
    return out


def _iso_timestamp(unix_time: float) -> str:
    """Return a UTC ISO-8601 string for a Unix timestamp."""
    return datetime.datetime.fromtimestamp(unix_time, tz=datetime.timezone.utc).isoformat()


def list_recordings(output_dir: str = "./recordings") -> list[dict]:
    """List ``.ndjson`` recordings in ``output_dir``, newest first.

    Returns a list of
    ``{"id": filename, "path": full_path, "size_bytes": int,
    "modified": float, "modified_iso": str}`` for each ``.ndjson`` file.
    If ``output_dir`` does not exist, returns ``[]``.

    Note: do not call reader functions on a recording currently being
    written by an active ``Recorder``.
    """
    if not os.path.isdir(output_dir):
        return []
    entries: list[dict] = []
    for name in os.listdir(output_dir):
        if not name.endswith(".ndjson"):
            continue
        full = os.path.join(output_dir, name)
        if not os.path.isfile(full):
            continue
        stat = os.stat(full)
        entries.append(
            {
                "id": name,
                "path": full,
                "size_bytes": stat.st_size,
                "modified": stat.st_mtime,
                "modified_iso": _iso_timestamp(stat.st_mtime),
            }
        )
    entries.sort(key=lambda r: r["modified"], reverse=True)
    return entries


def list_topics(
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    prefix: str | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
) -> list[str]:
    """List unique topic names in a recording, sorted.

    If ``recording_id`` is ``None``, reads the most recent recording in
    ``output_dir``. ``prefix``, ``regex`` and ``wildcard`` filters are
    applied together. Missing file or empty recording returns ``[]``.
    Malformed lines are skipped with a warning to stderr.

    Note: do not call on a recording currently being written.
    """
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return []
    topics: set[str] = set()
    for event in _iter_events(path):
        topic = event.get("topic")
        if not isinstance(topic, str):
            continue
        if not _matches_filters(topic, prefix, regex, wildcard):
            continue
        topics.add(topic)
    return sorted(topics)


def _apply_time_filter(
    path: str,
    start: float | None,
    end: float | None,
    last_seconds: float | None,
) -> tuple[float | None, float | None]:
    """Resolve ``last_seconds`` into an absolute ``start`` bound."""
    if last_seconds is not None:
        bounds = _time_bounds(path)
        if bounds is not None:
            computed_start = bounds[1] - last_seconds
            if start is None or computed_start > start:
                start = computed_start
    return start, end


def _meta_result(
    payload: Any, key: str, rows: int, total_rows: int
) -> dict[str, Any]:
    """Wrap a reader payload with visible-truncation metadata.

    ``payload`` is the per-topic dict (``subscribe_offline``) or the
    samples list (``get_history``); ``key`` names the field it is nested
    under (``"samples"``, ``"summary"`` or ``"topics"``).
    """
    return {
        key: payload,
        "rows": rows,
        "total_rows": total_rows,
        "truncated": total_rows > rows,
    }


def get_history(
    topic: str,
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    start: float | None = None,
    end: float | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    format: str = "samples",
    return_meta: bool = False,
) -> list[dict] | dict:
    """Return the event history for one topic.

    Each entry is ``{"time": float, "value": jsonable}`` — same shape as
    live ``nt_subscribe`` entries. ``start`` is inclusive (``time >= start``);
    ``end`` is exclusive (``time < end``). ``last_seconds`` selects the most
    recent N seconds. ``sample_interval`` decimates the samples, and
    ``format="summary"`` returns min/max/mean/last.

    Missing file returns ``[]``. Malformed lines are skipped with a warning
    to stderr.

    With ``return_meta=True`` the return value is
    ``{"samples": [...] | "summary": {...}, "rows": int, "total_rows": int,
    "truncated": bool}`` instead: ``rows`` is the number of entries actually
    returned, ``total_rows`` counts ALL matching events (counted even past
    the ``limit`` clip), and ``truncated`` is ``total_rows > rows``.
    """
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return [] if not return_meta else _meta_result([], "samples", 0, 0)
    start, end = _apply_time_filter(path, start, end, last_seconds)
    events: list[dict] = []
    total_rows = 0
    for t, event in _iter_timed_events(path):
        if event.get("topic") != topic:
            continue
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        total_rows += 1
        if limit is not None and len(events) >= limit:
            continue
        events.append({"time": t, "value": event.get("value")})
    if sample_interval is not None:
        events = _downsample(events, sample_interval)
    rows = len(events)
    if format == "summary":
        events = summarize(events)
    if return_meta:
        key = "summary" if format == "summary" else "samples"
        return _meta_result(events, key, rows, total_rows)
    return events


def get_recording_info(
    recording_id: str | None = None,
    output_dir: str = "./recordings",
) -> dict:
    """Return metadata for a recording.

    Includes ``duration_seconds``, ``total_samples``, ``topic_count``,
    ``id``, ``path``, ``size_bytes``, ``modified`` and ``modified_iso``.
    Missing file returns an ``error`` key.
    """
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {"error": "no recording found"}
    bounds = _time_bounds(path)
    duration = bounds[1] - bounds[0] if bounds else 0.0
    total = 0
    topics: set[str] = set()
    for event in _iter_events(path):
        total += 1
        topic = event.get("topic")
        if isinstance(topic, str):
            topics.add(topic)
    stat = os.stat(path)
    return {
        "id": os.path.basename(path),
        "path": path,
        "size_bytes": stat.st_size,
        "modified": stat.st_mtime,
        "modified_iso": _iso_timestamp(stat.st_mtime),
        "duration_seconds": duration,
        "total_samples": total,
        "topic_count": len(topics),
    }


def subscribe_offline(
    prefixes: list[str],
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    start: float | None = None,
    end: float | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    format: str = "samples",
    return_meta: bool = False,
) -> dict[str, Any]:
    """Return events for every topic under ``prefixes``.

    Returns ``{topic: [{"time": float, "value": jsonable}, ...] | summary}``.
    ``start`` is inclusive, ``end`` is exclusive. ``last_seconds`` selects
    the most recent N seconds. If ``limit`` is given, it is applied per-topic
    (each topic gets at most ``limit`` entries). ``sample_interval`` and
    ``format="summary"`` behave like live ``nt_subscribe``. Missing file
    returns ``{}``. Malformed lines are skipped with a warning to stderr.

    With ``return_meta=True`` the return value is
    ``{"topics": {topic: [...] | summary}, "rows": int, "total_rows": int,
    "truncated": bool}`` instead: ``rows`` is the number of entries actually
    returned, ``total_rows`` counts ALL matching events (counted even past
    the ``limit`` clip), and ``truncated`` is ``total_rows > rows``.

    Note: do not call on a recording currently being written.
    """
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {} if not return_meta else _meta_result({}, "topics", 0, 0)
    start, end = _apply_time_filter(path, start, end, last_seconds)
    out: dict[str, list[dict]] = {}
    total_rows = 0
    for t, event in _iter_timed_events(path):
        topic = event.get("topic")
        if not isinstance(topic, str):
            continue
        if not any(topic.startswith(p) for p in prefixes):
            continue
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        total_rows += 1
        if limit is not None and len(out.get(topic, ())) >= limit:
            continue
        out.setdefault(topic, []).append({"time": t, "value": event.get("value")})
    if sample_interval is not None:
        out = {topic: _downsample(samples, sample_interval) for topic, samples in out.items()}
    rows = sum(len(samples) for samples in out.values())
    if format == "summary":
        out = {topic: summarize(samples) for topic, samples in out.items()}
    if return_meta:
        key = "summary" if format == "summary" else "topics"
        return _meta_result(out, key, rows, total_rows)
    return out


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Record NetworkTables value events to an NDJSON file.",
    )
    parser.add_argument("--server-ip", default="127.0.0.1")
    parser.add_argument("--server-port", default=5810, type=int)
    parser.add_argument("--team", default=None, type=int)
    parser.add_argument("--identity", default="nt-recorder")
    parser.add_argument("--prefixes", nargs="+", default=["/"])
    parser.add_argument("--output-dir", default="./recordings")
    parser.add_argument("--duration", default=None, type=float)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    manager = NTManager()
    try:
        _emit(
            args.quiet,
            f"connecting to NT server at {args.server_ip}:{args.server_port}...",
        )
        try:
            manager.connect(
                server_ip=args.server_ip,
                server_port=args.server_port,
                team_number=args.team,
                identity=args.identity,
            )
        except NTConnectError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        if not await _wait_for_connection(manager):
            print(
                f"ERROR: failed to connect to NT server at "
                f"{args.server_ip}:{args.server_port} within "
                f"{_CONNECT_TIMEOUT_SECONDS:.0f}s",
                file=sys.stderr,
            )
            return 1
        _emit(args.quiet, "connected")

        recorder = Recorder(
            manager=manager,
            output_dir=args.output_dir,
            prefixes=args.prefixes,
            duration=args.duration,
        )

        # Signal handlers: best-effort. On Windows, loop.add_signal_handler
        # raises NotImplementedError; rely on KeyboardInterrupt instead.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, recorder.stop_event.set)
            except (NotImplementedError, RuntimeError):
                break

        _emit(
            args.quiet,
            f"recording prefixes={args.prefixes} to {args.output_dir}/",
        )
        try:
            path = await recorder.run()
        except (IOError, OSError) as exc:
            print(f"ERROR: disk/IO failure: {exc}", file=sys.stderr)
            return 2
        except KeyboardInterrupt:
            _emit(args.quiet, "interrupted")
            return 0
        _emit(args.quiet, f"stopped; wrote {path}")
    finally:
        manager.disconnect()

    return 0


def _sync_main() -> None:
    """Console-script entry point (sync wrapper around async main)."""
    raise SystemExit(asyncio.run(main()))


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(0)
