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
import bisect
import contextlib
import datetime
import fnmatch
import json
import math
import os
import re
import signal
import sys
import time
from typing import Any, Iterator

from .nt_manager import NTConnectError, NTManager, StreamDecimator
from .nt_types import TopicSelector, summarize

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
    ``truncated`` (True only when the cap stopped capture early), plus actual
    capture wall-clock and event-span metadata. Metadata is persisted in a
    sibling ``.meta.json`` file so offline readers do not confuse stale NT
    last-change timestamps with the capture duration.
    """

    def __init__(
        self,
        manager: NTManager,
        output_dir: str,
        prefixes: list[str] | None,
        duration: float | None = None,
        sample_interval: float | None = None,
        change_only: float | None = None,
        max_capture_rows: int | None = None,
        recording_id: str | None = None,
        topics: list[str] | None = None,
        regex: str | None = None,
        wildcard: str | None = None,
        label: str | None = None,
        idle_stop_seconds: float | None = None,
    ) -> None:
        self.manager = manager
        self.output_dir = output_dir
        self.selector = TopicSelector(prefixes, topics, regex, wildcard)
        if self.selector.empty:
            raise ValueError("at least one topic selector is required")
        self.prefixes = list(self.selector.prefixes)
        self.selected_topics = sorted(self.selector.topics)
        self.regex = regex
        self.wildcard = wildcard
        self.duration = duration
        self.stop_event = asyncio.Event()
        self._decimator = StreamDecimator(
            sample_interval=sample_interval, change_only=change_only
        )
        self._max_capture_rows = max_capture_rows
        self.sample_interval = sample_interval
        self.change_only = change_only
        self.recording_id = recording_id
        self.rows = 0
        self._topics: set[str] = set()
        self.topics: list[str] = []
        self.truncated = False
        self.label = label
        self.idle_stop_seconds = idle_stop_seconds
        self.idle_stopped = False
        self.last_activity_age_seconds: float | None = None
        self._last_event_monotonic: float | None = None
        self._capture_started_monotonic: float | None = None
        self.capture_started_at: float | None = None
        self.capture_ended_at: float | None = None
        self.capture_duration_seconds: float | None = None
        self.event_started_at: float | None = None
        self.event_ended_at: float | None = None
        self.event_span_seconds = 0.0

    async def run(self) -> str:
        """Subscribe, write events to NDJSON, return the output file path."""
        os.makedirs(self.output_dir, exist_ok=True)
        filename = self.recording_id or datetime.datetime.now(
            datetime.timezone.utc
        ).strftime("nt-record-%Y-%m-%dT%H%M%SZ.ndjson")
        path = os.path.join(self.output_dir, filename)
        started_monotonic: float | None = None
        try:
            with open(path, "w", encoding="utf-8") as file:
                self.capture_started_at = time.time()
                started_monotonic = time.monotonic()
                self._capture_started_monotonic = started_monotonic
                if self.duration is not None:
                    # asyncio.timeout fires at the generator's await point even
                    # when no events are flowing (bare time.monotonic checks
                    # inside the async for never regain control between events).
                    try:
                        async with asyncio.timeout(self.duration):
                            async for (
                                topic,
                                value,
                                time_seconds,
                            ) in self.manager.subscribe_stream(
                                self.prefixes,
                                self.selected_topics,
                                self.regex,
                                self.wildcard,
                            ):
                                if not self._write_filtered(
                                    file, topic, value, time_seconds
                                ):
                                    break
                    except TimeoutError:
                        pass
                else:
                    await self._run_until_stop(file)
        finally:
            if started_monotonic is not None:
                ended_monotonic = time.monotonic()
                self.capture_ended_at = time.time()
                self.capture_duration_seconds = max(
                    0.0, ended_monotonic - started_monotonic
                )
                activity_anchor = self._last_event_monotonic or started_monotonic
                self.last_activity_age_seconds = max(
                    0.0, ended_monotonic - activity_anchor
                )
            self.topics = sorted(self._topics)
            if self.event_started_at is not None and self.event_ended_at is not None:
                self.event_span_seconds = max(
                    0.0, self.event_ended_at - self.event_started_at
                )
            if self.capture_started_at is not None:
                self._write_metadata(path)
        return path

    def _write_metadata(self, path: str) -> None:
        """Persist capture-clock metadata separately from the NDJSON events."""
        metadata = {
            "schema_version": 1,
            "event_time_fields": ["time", "capture_time"],
            "recording_id": os.path.basename(path),
            "capture_started_at": self.capture_started_at,
            "capture_started_iso": _iso_timestamp(self.capture_started_at),
            "capture_ended_at": self.capture_ended_at,
            "capture_ended_iso": _iso_timestamp(self.capture_ended_at),
            "capture_duration_seconds": self.capture_duration_seconds,
            "event_started_at": self.event_started_at,
            "event_ended_at": self.event_ended_at,
            "event_span_seconds": self.event_span_seconds,
            "prefixes": self.prefixes,
            "topics": self.selected_topics,
            "regex": self.regex,
            "wildcard": self.wildcard,
            "sample_interval": self.sample_interval,
            "change_only": self.change_only,
            "max_rows": self._max_capture_rows,
            "rows": self.rows,
            "topic_count": len(self.topics),
            "truncated": self.truncated,
            "label": self.label,
            "idle_stop_seconds": self.idle_stop_seconds,
            "idle_stopped": self.idle_stopped,
            "last_activity_age_seconds": self.last_activity_age_seconds,
        }
        with open(_metadata_path(path), "w", encoding="utf-8") as file:
            json.dump(metadata, file, separators=(",", ":"))

    async def _run_until_stop(self, file) -> None:
        """Consume the stream until ``stop_event`` or the idle timeout."""
        consume_task = asyncio.create_task(self._consume(file))
        stop_task = asyncio.create_task(self.stop_event.wait())
        tasks = {consume_task, stop_task}
        if self.idle_stop_seconds is not None:
            tasks.add(asyncio.create_task(self._idle_watchdog()))
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            # Drain: let the generator's finally close the subscriber and
            # poller; suppress only CancelledError, propagate real errors.
            for task in tasks:
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _idle_watchdog(self) -> None:
        """Stop capture after ``idle_stop_seconds`` without a written event."""
        assert self.idle_stop_seconds is not None
        interval = min(0.1, self.idle_stop_seconds)
        while True:
            await asyncio.sleep(interval)
            started = self._capture_started_monotonic or time.monotonic()
            last = self._last_event_monotonic or started
            if time.monotonic() - last >= self.idle_stop_seconds:
                self.idle_stopped = True
                return

    async def _consume(self, file) -> None:
        async for topic, value, time_seconds in self.manager.subscribe_stream(
            self.prefixes,
            self.selected_topics,
            self.regex,
            self.wildcard,
        ):
            if not self._write_filtered(file, topic, value, time_seconds):
                break

    def _write_filtered(
        self, file, topic: str, value: Any, time_seconds: float
    ) -> bool:
        """Write one event through the decimator; update capture stats.

        Returns False only when ``max_capture_rows`` was reached and the
        caller must stop consuming (the file is already flushed per line).
        """
        if not self._decimator.keep(topic, value, time_seconds):
            return True
        _write_event(file, topic, value, time_seconds, capture_time=time.time())
        self._last_event_monotonic = time.monotonic()
        self.rows += 1
        self._topics.add(topic)
        if time_seconds > 0:
            if self.event_started_at is None or time_seconds < self.event_started_at:
                self.event_started_at = time_seconds
            if self.event_ended_at is None or time_seconds > self.event_ended_at:
                self.event_ended_at = time_seconds
        if self._max_capture_rows is not None and self.rows >= self._max_capture_rows:
            self.truncated = True
            return False
        return True


def _write_event(
    file,
    topic: str,
    value: Any,
    time_seconds: float,
    capture_time: float | None = None,
) -> None:
    """Write one NDJSON line and flush.

    ``capture_time`` is the local wall-clock write time; it is additive so
    legacy readers that only use ``time`` remain compatible.
    """
    line = {"time": time_seconds, "topic": topic, "value": value}
    if capture_time is not None:
        line["capture_time"] = capture_time
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


def _iter_timed_events(
    path: str, time_basis: str = "event"
) -> Iterator[tuple[float, dict]]:
    """Yield ``(time, event)`` for events with a valid numeric time.

    ``time_basis="event"`` (the default) uses the NT last-change ``time``
    field. ``time_basis="capture"`` uses the local wall-clock
    ``capture_time`` field written by newer recorders; legacy events without
    that field are skipped.
    """
    key = "capture_time" if time_basis == "capture" else "time"
    for event in _iter_events(path):
        raw_time = event.get(key)
        if isinstance(raw_time, (int, float)) and not isinstance(raw_time, bool):
            yield float(raw_time), event


def _validate_time_basis(time_basis: str) -> None:
    """Refuse unknown time bases before any file scan."""
    if time_basis not in ("event", "capture"):
        raise ValueError("time_basis must be 'event' or 'capture'")


def _time_bounds(path: str, time_basis: str = "event") -> tuple[float, float] | None:
    """Return ``(min_time, max_time)`` across all valid events, or ``None``.

    Zero or negative timestamps are ignored. NT's ``last_change()`` can
    return 0 for kImmediate/initial values, which would otherwise collapse
    the duration to the absolute end timestamp.
    """
    min_time: float | None = None
    max_time: float | None = None
    for t, _ in _iter_timed_events(path, time_basis):
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


def _iso_timestamp(unix_time: float | None) -> str | None:
    """Return a UTC ISO-8601 string for a Unix timestamp."""
    if unix_time is None:
        return None
    return datetime.datetime.fromtimestamp(
        unix_time, tz=datetime.timezone.utc
    ).isoformat()


def _metadata_path(recording_path: str) -> str:
    """Return the sibling metadata path for an NDJSON recording."""
    return os.path.splitext(recording_path)[0] + ".meta.json"


def _read_capture_metadata(recording_path: str) -> dict[str, Any] | None:
    """Read persisted capture metadata, returning None for legacy recordings."""
    path = _metadata_path(recording_path)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as file:
            value = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        print(
            f"WARNING: could not read recording metadata {path}: {exc}",
            file=sys.stderr,
        )
        return None
    return value if isinstance(value, dict) else None


def list_recordings(output_dir: str = "./recordings") -> list[dict]:
    """List ``.ndjson`` recordings in ``output_dir``, newest first.

    Returns a list of
    ``{"id": filename, "path": full_path, "size_bytes": int,
    "modified": float, "modified_iso": str}`` for each ``.ndjson`` file,
    plus capture-clock and event-span fields when a metadata sidecar exists.
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
        entry = {
            "id": name,
            "path": full,
            "size_bytes": stat.st_size,
            "modified": stat.st_mtime,
            "modified_iso": _iso_timestamp(stat.st_mtime),
        }
        metadata = _read_capture_metadata(full)
        if metadata is not None:
            entry.update(
                {
                    key: metadata.get(key)
                    for key in (
                        "capture_started_at",
                        "capture_started_iso",
                        "capture_ended_at",
                        "capture_ended_iso",
                        "capture_duration_seconds",
                        "event_span_seconds",
                        "label",
                        "idle_stop_seconds",
                        "idle_stopped",
                        "last_activity_age_seconds",
                    )
                }
            )
        entries.append(entry)
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


def _require_capture_metadata(path: str) -> dict[str, Any]:
    """Return capture metadata, refusing legacy files for capture-clock reads."""
    metadata = _read_capture_metadata(path)
    if metadata is None or metadata.get("capture_started_at") is None:
        raise ValueError(
            "time_basis='capture' requires recording capture metadata "
            "(the .meta.json sidecar with capture_started_at); legacy "
            "recordings support only time_basis='event'"
        )
    return metadata


def _parse_time_bound(
    value: float | str | None,
    time_basis: str,
    capture_started_at: float | None,
) -> float | None:
    """Resolve a numeric, ISO-8601 or ``+seconds`` time bound.

    Numbers are absolute in the selected clock. Strings are allowed only
    with ``time_basis="capture"``: ``+12.5`` / ``+12.5s`` is relative to
    capture start, and ISO-8601 is an absolute wall-clock time (naive
    timestamps are treated as UTC).
    """
    if value is None:
        return None
    if isinstance(value, str):
        if time_basis != "capture":
            raise ValueError(
                "ISO-8601 and '+seconds' time bounds require "
                "time_basis='capture'"
            )
        text = value.strip()
        if not text:
            raise ValueError("time bound must not be empty")
        if text.startswith("+"):
            number = text[1:].rstrip("sS")
            try:
                delta = float(number)
            except ValueError as exc:
                raise ValueError(
                    f"invalid relative time bound {value!r}; use '+12.5' "
                    "or '+12.5s'"
                ) from exc
            if not math.isfinite(delta) or delta < 0:
                raise ValueError("relative time bounds must be finite and non-negative")
            assert capture_started_at is not None
            return capture_started_at + delta
        try:
            parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"invalid ISO-8601 time bound {value!r}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.timestamp()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("time bounds must be numbers, ISO-8601 strings, or '+seconds'")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise ValueError("numeric time bounds must be finite and non-negative")
    return number


def _apply_time_filter(
    path: str,
    start: float | str | None,
    end: float | str | None,
    last_seconds: float | None,
    time_basis: str = "event",
) -> tuple[float | None, float | None]:
    """Resolve relative/ISO bounds and ``last_seconds`` into absolute bounds."""
    _validate_time_basis(time_basis)
    capture_started_at = None
    if time_basis == "capture":
        capture_started_at = float(
            _require_capture_metadata(path)["capture_started_at"]
        )
    resolved_start = _parse_time_bound(start, time_basis, capture_started_at)
    resolved_end = _parse_time_bound(end, time_basis, capture_started_at)
    if last_seconds is not None:
        bounds = _time_bounds(path, time_basis)
        if bounds is not None:
            computed_start = bounds[1] - last_seconds
            if resolved_start is None or computed_start > resolved_start:
                resolved_start = computed_start
    return resolved_start, resolved_end


def _window_payload(
    path: str,
    time_basis: str,
    start: float | None,
    end: float | None,
) -> dict[str, Any]:
    """Describe the resolved query window and the recording's actual bounds."""
    bounds = _time_bounds(path, time_basis)
    metadata = _read_capture_metadata(path)
    return {
        "basis": time_basis,
        "start": start,
        "end": end,
        "start_iso": (
            _iso_timestamp(start) if time_basis == "capture" else None
        ),
        "end_iso": (_iso_timestamp(end) if time_basis == "capture" else None),
        "data_start": bounds[0] if bounds is not None else None,
        "data_end": bounds[1] if bounds is not None else None,
        "capture_started_at": (
            metadata.get("capture_started_at") if metadata is not None else None
        ),
        "capture_started_iso": (
            metadata.get("capture_started_iso") if metadata is not None else None
        ),
        "capture_ended_at": (
            metadata.get("capture_ended_at") if metadata is not None else None
        ),
        "capture_ended_iso": (
            metadata.get("capture_ended_iso") if metadata is not None else None
        ),
    }


def describe_window(
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    start: float | str | None = None,
    end: float | str | None = None,
    last_seconds: float | None = None,
    time_basis: str = "event",
) -> dict[str, Any]:
    """Return the resolved query window for a recording.

    Missing files return a window description with ``start``/``end`` as
    given and ``data_start``/``data_end`` null.
    """
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {
            "basis": time_basis,
            "start": start,
            "end": end,
            "start_iso": None,
            "end_iso": None,
            "data_start": None,
            "data_end": None,
            "capture_started_at": None,
            "capture_started_iso": None,
            "capture_ended_at": None,
            "capture_ended_iso": None,
        }
    resolved_start, resolved_end = _apply_time_filter(
        path, start, end, last_seconds, time_basis
    )
    return _window_payload(path, time_basis, resolved_start, resolved_end)


def _meta_result(payload: Any, key: str, rows: int, total_rows: int) -> dict[str, Any]:
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
    start: float | str | None = None,
    end: float | str | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    format: str = "samples",
    return_meta: bool = False,
    time_basis: str = "event",
) -> list[dict] | dict:
    """Return the event history for one topic.

    Each entry is ``{"time": float, "value": jsonable}`` — same shape as
    live ``nt_subscribe`` entries. ``start`` is inclusive (``time >= start``);
    ``end`` is exclusive (``time < end``). ``last_seconds`` selects the most
    recent N seconds. ``sample_interval`` decimates before ``limit`` caps
    the retained samples, and
    ``format="summary"`` returns min/max/mean/last.

    ``time_basis="event"`` (default) filters on NT last-change timestamps.
    ``time_basis="capture"`` uses each event's wall-clock ``capture_time``
    and accepts ISO-8601 or ``+seconds`` bounds relative to capture start;
    legacy recordings without capture timestamps are refused.

    Missing file returns ``[]``. Malformed lines are skipped with a warning
    to stderr.

    With ``return_meta=True`` the return value is
    ``{"samples": [...] | "summary": {...}, "rows": int, "total_rows": int,
    "truncated": bool, "window": {...}}`` instead: ``rows`` is the number of
    entries actually returned, ``total_rows`` counts ALL matching events
    (counted even past the ``limit`` clip), and ``truncated`` is
    ``total_rows > rows``.
    """
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return [] if not return_meta else _meta_result([], "samples", 0, 0)
    start, end = _apply_time_filter(path, start, end, last_seconds, time_basis)
    events: list[dict] = []
    total_rows = 0
    decimator = StreamDecimator(
        sample_interval=sample_interval if sample_interval and sample_interval > 0 else None
    )
    for t, event in _iter_timed_events(path, time_basis):
        if event.get("topic") != topic:
            continue
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        total_rows += 1
        if not decimator.keep(topic, event.get("value"), t):
            continue
        if limit is not None and len(events) >= limit:
            continue
        events.append({"time": t, "value": event.get("value")})
    rows = len(events)
    if format == "summary":
        events = summarize(events)
    if return_meta:
        key = "summary" if format == "summary" else "samples"
        result = _meta_result(events, key, rows, total_rows)
        result["window"] = _window_payload(path, time_basis, start, end)
        return result
    return events


def get_recording_info(
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    prefix: str | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
    topic_limit: int = 100,
    include_activity: bool = False,
) -> dict:
    """Return metadata for a recording.

    New recordings report actual ``capture_duration_seconds`` separately
    from ``event_span_seconds`` (the span of NT last-change timestamps), and
    use capture time for the backward-compatible ``duration_seconds`` field.
    Legacy recordings without a metadata sidecar use event span and report
    ``duration_source="event_span"``. Also includes sample/topic counts and
    ordinary file metadata. Missing files return an ``error`` key.

    ``include_activity=True`` adds a bounded ``activity`` map for the first
    ``topic_limit`` matching topics, with ``first_time``, ``last_time``,
    ``span_seconds``, ``mean_interval_seconds``, ``effective_hz``,
    ``max_gap_seconds`` and ``last_change_age_seconds`` (relative to the
    recording's latest event timestamp). ``activity_truncated`` reports
    whether more matching topics exist.
    """
    if not 1 <= topic_limit <= 1000:
        raise ValueError("topic_limit must be between 1 and 1000")
    selector = TopicSelector(
        [prefix] if prefix is not None else None, regex=regex, wildcard=wildcard
    )
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {"error": "no recording found"}
    min_time = None
    max_time = None
    total = 0
    topics: set[str] = set()
    times_by_topic: dict[str, list[float]] = {}
    for event in _iter_events(path):
        total += 1
        timestamp = event.get("time")
        if isinstance(timestamp, (int, float)) and timestamp > 0:
            min_time = timestamp if min_time is None else min(min_time, timestamp)
            max_time = timestamp if max_time is None else max(max_time, timestamp)
        topic = event.get("topic")
        if isinstance(topic, str):
            topics.add(topic)
            if (
                include_activity
                and isinstance(timestamp, (int, float))
                and timestamp > 0
            ):
                times_by_topic.setdefault(topic, []).append(timestamp)
    event_span = max_time - min_time if min_time is not None else 0.0
    matching_topics = sorted(topic for topic in topics if selector.matches(topic))
    stat = os.stat(path)
    metadata = _read_capture_metadata(path)
    capture_duration = (
        metadata.get("capture_duration_seconds") if metadata is not None else None
    )
    result = {
        "id": os.path.basename(path),
        "path": path,
        "size_bytes": stat.st_size,
        "modified": stat.st_mtime,
        "modified_iso": _iso_timestamp(stat.st_mtime),
        # Backward-compatible name: new recordings use actual capture time;
        # legacy recordings fall back to their event timestamp span.
        "duration_seconds": (
            capture_duration if capture_duration is not None else event_span
        ),
        "capture_duration_seconds": capture_duration,
        "event_span_seconds": event_span,
        "duration_source": (
            "capture" if capture_duration is not None else "event_span"
        ),
        "total_samples": total,
        "topic_count": len(topics),
        "matching_topic_count": len(matching_topics),
        "topics": matching_topics[:topic_limit],
        "topics_returned": min(len(matching_topics), topic_limit),
        "topics_truncated": len(matching_topics) > topic_limit,
    }
    if include_activity:
        activity = {
            topic: _activity_from_times(times_by_topic.get(topic, ()), max_time)
            for topic in matching_topics[:topic_limit]
        }
        result["activity"] = activity
        result["activity_returned"] = len(activity)
        result["activity_truncated"] = len(matching_topics) > topic_limit
    if metadata is not None:
        result.update(
            {
                key: metadata.get(key)
                for key in (
                    "capture_started_at",
                    "capture_started_iso",
                    "capture_ended_at",
                    "capture_ended_iso",
                    "event_started_at",
                    "event_ended_at",
                    "label",
                    "idle_stop_seconds",
                    "idle_stopped",
                    "last_activity_age_seconds",
                )
            }
        )
    return result


def _activity_from_times(
    times: Iterator[float] | list[float],
    global_last: float | None,
) -> dict[str, Any]:
    """Summarize one topic's event timing for recording discovery."""
    positive = sorted(t for t in times if t > 0)
    if not positive:
        return {"count": 0}
    first = positive[0]
    last = positive[-1]
    span = last - first
    gaps = [later - earlier for earlier, later in zip(positive, positive[1:])]
    return {
        "count": len(positive),
        "first_time": first,
        "last_time": last,
        "span_seconds": span,
        "mean_interval_seconds": (
            span / (len(positive) - 1) if len(positive) > 1 else None
        ),
        "effective_hz": ((len(positive) - 1) / span) if span > 0 else None,
        "max_gap_seconds": max(gaps) if gaps else 0.0,
        "last_change_age_seconds": (
            global_last - last if global_last is not None else None
        ),
    }


def get_windows(
    prefixes: list[str] | None = None,
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    start: float | str | None = None,
    end: float | str | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
    gap_seconds: float = 1.0,
    time_basis: str = "event",
) -> dict[str, list[dict[str, Any]]]:
    """Return contiguous activity windows per topic.

    A window starts when a topic has no event for more than ``gap_seconds``
    and ends at its last event in the burst. Each window reports ``start``,
    ``end``, ``duration_seconds``, ``samples`` and
    ``mean_interval_seconds``. ``start`` is inclusive and ``end`` is
    exclusive for the query window. ``limit`` caps windows per topic.
    """
    if gap_seconds <= 0:
        raise ValueError("gap_seconds must be greater than 0")
    selector = TopicSelector(prefixes, topics, regex, wildcard)
    if selector.empty:
        raise ValueError("at least one topic selector is required")
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {}
    start, end = _apply_time_filter(path, start, end, last_seconds, time_basis)
    times: dict[str, list[float]] = {}
    for t, event in _iter_timed_events(path, time_basis):
        topic = event.get("topic")
        if not isinstance(topic, str) or not selector.matches(topic):
            continue
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        if t <= 0:
            continue
        times.setdefault(topic, []).append(t)

    out: dict[str, list[dict[str, Any]]] = {}
    for topic, topic_times in times.items():
        topic_times.sort()
        windows: list[dict[str, Any]] = []
        window_start = topic_times[0]
        window_last = topic_times[0]
        samples = 1
        for timestamp in topic_times[1:]:
            if timestamp - window_last > gap_seconds:
                windows.append(_window(window_start, window_last, samples))
                window_start = timestamp
                samples = 1
            else:
                samples += 1
            window_last = timestamp
        windows.append(_window(window_start, window_last, samples))
        if limit is not None:
            windows = windows[:limit]
        out[topic] = windows
    return out


def _window(start: float, end: float, samples: int) -> dict[str, Any]:
    """Build one activity-window record."""
    span = end - start
    return {
        "start": start,
        "end": end,
        "duration_seconds": span,
        "samples": samples,
        "mean_interval_seconds": (span / (samples - 1) if samples > 1 else None),
    }


def build_table(
    prefixes: list[str] | None = None,
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    start: float | str | None = None,
    end: float | str | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
    interval_seconds: float = 0.1,
    max_gap_seconds: float = 1.0,
    max_rows: int = 5000,
    time_basis: str = "event",
) -> dict[str, Any]:
    """Resample selected topics onto one common time grid.

    Each grid row is ``{"time": t, "values": {topic: value | null}}``. A
    topic's value is the last sample at or before ``t`` when that sample is
    no more than ``max_gap_seconds`` old; otherwise null. ``max_rows`` caps
    the number of built grid rows, while ``total_rows`` reports the
    uncapped count so clipping is visible. ``start``/``end`` follow the
    selected ``time_basis``.
    """
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be greater than 0")
    if max_gap_seconds < 0:
        raise ValueError("max_gap_seconds must be non-negative")
    if max_rows <= 0:
        raise ValueError("max_rows must be greater than 0")
    selector = TopicSelector(prefixes, topics, regex, wildcard)
    if selector.empty:
        raise ValueError("at least one topic selector is required")
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {
            "columns": [],
            "table": [],
            "total_rows": 0,
            "truncated": False,
            "interval_seconds": interval_seconds,
            "max_gap_seconds": max_gap_seconds,
            "window": describe_window(
                recording_id=recording_id,
                output_dir=output_dir,
                start=start,
                end=end,
                last_seconds=last_seconds,
                time_basis=time_basis,
            ),
        }
    start, end = _apply_time_filter(path, start, end, last_seconds, time_basis)
    series: dict[str, dict[str, list[Any]]] = {}
    decimator = StreamDecimator(
        sample_interval=(
            sample_interval if sample_interval and sample_interval > 0 else None
        )
    )
    for t, event in _iter_timed_events(path, time_basis):
        topic = event.get("topic")
        if not isinstance(topic, str) or not selector.matches(topic):
            continue
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        if not decimator.keep(topic, event.get("value"), t):
            continue
        bucket = series.setdefault(topic, {"times": [], "values": []})
        if limit is not None and len(bucket["times"]) >= limit:
            continue
        bucket["times"].append(t)
        bucket["values"].append(event.get("value"))

    if not series:
        return {
            "columns": [],
            "table": [],
            "total_rows": 0,
            "truncated": False,
            "interval_seconds": interval_seconds,
            "max_gap_seconds": max_gap_seconds,
            "window": _window_payload(path, time_basis, start, end),
        }

    columns = sorted(series)
    for bucket in series.values():
        pairs = sorted(zip(bucket["times"], bucket["values"]), key=lambda p: p[0])
        bucket["times"] = [pair[0] for pair in pairs]
        bucket["values"] = [pair[1] for pair in pairs]

    data_start = min(bucket["times"][0] for bucket in series.values())
    data_end = max(bucket["times"][-1] for bucket in series.values())
    grid_start = start if start is not None else data_start
    grid_end = end if end is not None else data_end
    if grid_end < grid_start:
        total_points = 0
    elif end is None:
        total_points = int(math.floor((grid_end - grid_start) / interval_seconds)) + 1
    else:
        total_points = int(
            math.ceil((grid_end - grid_start) / interval_seconds - 1e-9)
        )
        total_points = max(0, total_points)
    build_count = min(total_points, max_rows)
    rows: list[dict[str, Any]] = []
    for index in range(build_count):
        timestamp = grid_start + index * interval_seconds
        values: dict[str, Any] = {}
        for topic in columns:
            bucket = series[topic]
            position = bisect.bisect_right(bucket["times"], timestamp) - 1
            if position >= 0 and timestamp - bucket["times"][position] <= max_gap_seconds:
                values[topic] = bucket["values"][position]
            else:
                values[topic] = None
        rows.append({"time": timestamp, "values": values})
    return {
        "columns": columns,
        "table": rows,
        "total_rows": total_points,
        "truncated": total_points > len(rows),
        "interval_seconds": interval_seconds,
        "max_gap_seconds": max_gap_seconds,
        "window": _window_payload(path, time_basis, start, end),
    }



def subscribe_offline(
    prefixes: list[str] | None = None,
    recording_id: str | None = None,
    output_dir: str = "./recordings",
    start: float | str | None = None,
    end: float | str | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    format: str = "samples",
    return_meta: bool = False,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
    time_basis: str = "event",
) -> dict[str, Any]:
    """Return events matching topic selectors.

    Returns ``{topic: [{"time": float, "value": jsonable}, ...] | summary}``.
    Prefix and exact-topic lists are ORed internally; different selector
    kinds are ANDed. At least one selector kind is required.
    ``start`` is inclusive, ``end`` is exclusive. ``last_seconds`` selects
    the most recent N seconds. If ``limit`` is given, it is applied per-topic
    after decimation (each topic gets at most ``limit`` retained entries).
    ``sample_interval`` and
    ``format="summary"`` behave like live ``nt_subscribe``. Missing file
    returns ``{}``. Malformed lines are skipped with a warning to stderr.

    ``time_basis="event"`` (default) filters on NT last-change timestamps.
    ``time_basis="capture"`` uses each event's wall-clock ``capture_time``
    and accepts ISO-8601 or ``+seconds`` bounds relative to capture start;
    legacy recordings without capture timestamps are refused.

    With ``return_meta=True`` the return value is
    ``{"topics": {topic: [...] | summary}, "rows": int, "total_rows": int,
    "truncated": bool, "window": {...}}`` instead: ``rows`` is the number of
    entries actually returned, ``total_rows`` counts ALL matching events
    (counted even past the ``limit`` clip), and ``truncated`` is
    ``total_rows > rows``.

    Note: do not call on a recording currently being written.
    """
    selector = TopicSelector(prefixes, topics, regex, wildcard)
    if selector.empty:
        raise ValueError("at least one topic selector is required")
    path = _resolve_recording(recording_id, output_dir)
    if path is None:
        return {} if not return_meta else _meta_result({}, "topics", 0, 0)
    start, end = _apply_time_filter(path, start, end, last_seconds, time_basis)
    out: dict[str, list[dict]] = {}
    total_rows = 0
    decimator = StreamDecimator(
        sample_interval=sample_interval if sample_interval and sample_interval > 0 else None
    )
    for t, event in _iter_timed_events(path, time_basis):
        topic = event.get("topic")
        if not isinstance(topic, str):
            continue
        if not selector.matches(topic):
            continue
        if start is not None and t < start:
            continue
        if end is not None and t >= end:
            continue
        total_rows += 1
        if not decimator.keep(topic, event.get("value"), t):
            continue
        if limit is not None and len(out.get(topic, ())) >= limit:
            continue
        out.setdefault(topic, []).append({"time": t, "value": event.get("value")})
    rows = sum(len(samples) for samples in out.values())
    if format == "summary":
        out = {topic: summarize(samples) for topic, samples in out.items()}
    if return_meta:
        key = "summary" if format == "summary" else "topics"
        result = _meta_result(out, key, rows, total_rows)
        result["window"] = _window_payload(path, time_basis, start, end)
        return result
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
