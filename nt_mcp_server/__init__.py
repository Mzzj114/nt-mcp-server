"""FastMCP server exposing FRC NetworkTables to AI agents.

Thin MCP layer: every tool delegates to the module-level ``NTManager``
from ``nt_manager``. No ntcore imports here — all NetworkTables logic
lives in the manager. Run with ``uv run nt-mcp-server`` or
``python -m nt_mcp_server`` (stdio transport, the default for ``mcp.run()``).
"""

import asyncio
import contextlib
import datetime
import json
import os
import time
import uuid
from typing import Any

from fastmcp import FastMCP

from .nt_manager import NTConnectError, NTManager
from .nt_types import TopicSelector, enforce_max_chars, summarize
from . import nt_recorder

mcp = FastMCP("NetworkTables")
manager = NTManager()

_CONNECT_TIMEOUT_SECONDS = 5.0
_CONNECT_POLL_INTERVAL_SECONDS = 0.05

# Outer safety guard: no tool response may serialize past this ceiling,
# no matter what the row budgets allowed. See nt_types.enforce_max_chars.
_MAX_RESPONSE_CHARS = 60_000

# The file-mode receipt lists at most this many topic names.
_MAX_RECEIPT_TOPICS = 50

# Persistent subscription state. Each subscription is a background asyncio
# Task consuming ``manager.subscribe_stream`` and appending to a list buffer.
_subscription_tasks: dict[str, asyncio.Task] = {}
_subscription_buffers: dict[str, list[dict[str, Any]]] = {}

# Nonblocking file recordings. The recording id is both the public handle
# and the NDJSON filename, so callers never need to rediscover the file.
_recording_tasks: dict[str, asyncio.Task[str]] = {}
_recorders: dict[str, nt_recorder.Recorder] = {}


async def _consume_subscription(
    subscription_id: str,
    prefixes: list[str] | None,
    topics: list[str] | None,
    regex: str | None,
    wildcard: str | None,
    buffer_size: int,
) -> None:
    """Consume the stream for ``subscription_id`` into its buffer."""
    buffer: list[dict[str, Any]] = []
    _subscription_buffers[subscription_id] = buffer
    try:
        async for topic, value, time_seconds in manager.subscribe_stream(
            prefixes, topics, regex, wildcard
        ):
            buffer.append({"topic": topic, "time": time_seconds, "value": value})
            if len(buffer) > buffer_size:
                buffer.pop(0)
    finally:
        _subscription_buffers.pop(subscription_id, None)


def _stop_all_subscriptions() -> None:
    """Cancel every persistent subscription."""
    for task in _subscription_tasks.values():
        if not task.done():
            task.cancel()
    _subscription_tasks.clear()
    _subscription_buffers.clear()


def _request_stop_all_recordings() -> None:
    """Ask active recorders to close without blocking this sync callback."""
    for recorder in _recorders.values():
        recorder.stop_event.set()


def _new_recording_id() -> str:
    """Return a Windows-safe id unique even for starts in the same second."""
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H%M%SZ"
    )
    return f"nt-record-{timestamp}-{uuid.uuid4().hex[:8]}.ndjson"


def _recording_receipt(
    recorder: nt_recorder.Recorder,
    path: str,
) -> dict:
    """Build the compact, value-free receipt shared by recording tools."""
    topics = recorder.topics
    return {
        "output": "file",
        "recording_id": os.path.basename(path),
        "path": path,
        "duration_seconds": recorder.capture_duration_seconds,
        "capture_started_at": recorder.capture_started_at,
        "capture_started_iso": _iso_timestamp(recorder.capture_started_at),
        "capture_ended_at": recorder.capture_ended_at,
        "capture_ended_iso": _iso_timestamp(recorder.capture_ended_at),
        "capture_duration_seconds": recorder.capture_duration_seconds,
        "event_span_seconds": recorder.event_span_seconds,
        "rows": recorder.rows,
        "topic_count": len(topics),
        "topics": topics[:_MAX_RECEIPT_TOPICS],
        "topics_truncated": len(topics) > _MAX_RECEIPT_TOPICS,
        "truncated": recorder.truncated,
    }


def _iso_timestamp(unix_time: float | None) -> str | None:
    """Return a UTC ISO timestamp for a capture-clock value."""
    if unix_time is None:
        return None
    return datetime.datetime.fromtimestamp(
        unix_time, tz=datetime.timezone.utc
    ).isoformat()


def _topic_selector(
    prefixes: list[str] | None,
    topics: list[str] | None,
    regex: str | None,
    wildcard: str | None,
) -> tuple[TopicSelector | None, str | None]:
    """Validate a tool's topic selectors and return a structured error."""
    try:
        selector = TopicSelector(prefixes, topics, regex, wildcard)
    except ValueError as exc:
        return None, str(exc)
    if selector.empty:
        return None, (
            "at least one topic selector is required: prefixes, topics, "
            "regex, or wildcard"
        )
    return selector, None


def _closed_recording_id(
    recording_id: str | None, output_dir: str
) -> tuple[str | None, str | None]:
    """Resolve the latest file and reject reads of an active recording."""
    if recording_id is None:
        recordings = nt_recorder.list_recordings(output_dir)
        if not recordings:
            return None, "no recording found"
        recording_id = recordings[0]["id"]
    path = os.path.normcase(os.path.abspath(os.path.join(output_dir, recording_id)))
    for active_id, recorder in _recorders.items():
        active_path = os.path.normcase(os.path.abspath(os.path.join(recorder.output_dir, active_id)))
        if path == active_path and not _recording_tasks[active_id].done():
            return recording_id, "recording is active; nt_stop_recording closes it before offline queries"
    if not os.path.isfile(path):
        return recording_id, f"recording {recording_id!r} not found; nt_list_recordings lists available files"
    return recording_id, None


def _clip_topic_rows(
    topics: dict[str, list[Any]], max_rows: int
) -> tuple[dict[str, list[Any]], int]:
    """Clip per-topic row lists until their total fits ``max_rows``.

    Returns the clipped container (topic keys preserved, even when their
    list empties) and the number of rows kept.
    """
    kept = 0
    clipped: dict[str, list[Any]] = {}
    for topic, rows in topics.items():
        if kept >= max_rows:
            clipped[topic] = rows[:0]
            continue
        room = max_rows - kept
        if len(rows) > room:
            clipped[topic] = rows[:room]
            kept += room
        else:
            clipped[topic] = rows
            kept += len(rows)
    return clipped, kept


def _bounded_events(response: dict, key: str, output: str, max_rows: int) -> dict:
    """Apply row and character budgets before reducing selected rows to summaries."""
    rows = sum(len(items) for items in response[key].values())
    if rows > max_rows:
        response[key], rows = _clip_topic_rows(response[key], max_rows)
    response["rows"] = rows
    response["truncated"] = rows < response["total_rows"]
    response = enforce_max_chars(response, key, _MAX_RESPONSE_CHARS)
    if output == "summary":
        response[key] = {topic: summarize(items) for topic, items in response[key].items()}
        # A last_value can itself be large; summaries also need the ceiling.
        while response[key] and len(json.dumps(response)) > _MAX_RESPONSE_CHARS:
            topic = next(reversed(response[key]))
            response["rows"] -= response[key].pop(topic)["count"]
            response["truncated"] = True
    return response


@mcp.tool(
    annotations={
        "title": "Connect to NetworkTables",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def nt_connect(
    server_ip: str = "127.0.0.1",
    server_port: int = 5810,
    team_number: int | None = None,
    identity: str = "nt-mcp",
    timeout_seconds: float = _CONNECT_TIMEOUT_SECONDS,
) -> dict:
    """Start the NT4 client and wait for a live connection.

    Pass exactly one target: ``team_number``, or ``server_ip`` /
    ``server_port`` with the default ``server_ip``. Conflicting targets
    and re-targeting a running client are refused with a structured
    error (``connected: false`` plus ``error``); call ``nt_disconnect``
    before connecting elsewhere. On timeout the response carries
    ``connections`` diagnostics and a routing ``hint``.
    """
    try:
        manager.connect(
            server_ip=server_ip,
            server_port=server_port,
            team_number=team_number,
            identity=identity,
        )
    except NTConnectError as exc:
        return {"connected": False, "status": "not connected", "error": str(exc)}
    target = manager.target
    start = time.monotonic()
    deadline = start + timeout_seconds
    while time.monotonic() < deadline:
        if manager.is_connected():
            return {"connected": True, "status": "connected", "target": target}
        await asyncio.sleep(_CONNECT_POLL_INTERVAL_SECONDS)
    return {
        "connected": False,
        "status": "not connected",
        "target": target,
        "connections": manager.connection_info()["connections"],
        "elapsed_seconds": time.monotonic() - start,
        "hint": (
            "check the target address/port, that the robot's NT4 server "
            "(5810) is listening, and the host routing"
        ),
    }


@mcp.tool(
    annotations={
        "title": "Disconnect from NetworkTables",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def nt_disconnect() -> dict:
    """Stop the NT4 client and tear down any persistent subscriptions."""
    _stop_all_subscriptions()
    _request_stop_all_recordings()
    manager.disconnect()
    return {"connected": False, "status": "disconnected"}


@mcp.tool(
    annotations={
        "title": "Inspect NetworkTables Connection",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
def nt_connection_info() -> dict:
    """Return ``{"connected": bool, "connections": [...]}``."""
    return manager.connection_info()


@mcp.tool(
    annotations={
        "title": "Read NetworkTables Topics",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def nt_read_topics(topics: list[str]) -> dict:
    """Return values for every requested topic.

    Response: ``{"connected": bool, "values": {topic: value}}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    return {"connected": True, "values": await manager.get_multiple(topics)}


@mcp.tool(
    annotations={
        "title": "Inspect NetworkTables Topic",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def nt_get_topic_info(topic: str) -> dict:
    """Return metadata for ``topic``.

    Response: ``{"connected": bool, "info": {...} | null}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    return {"connected": True, "info": await manager.get_info(topic)}


@mcp.tool(
    annotations={
        "title": "Write NetworkTables Topics",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
def nt_write_topics(updates: dict[str, Any], strict_type_check: bool = False) -> dict:
    """Write every ``{topic: value}`` pair.

    Response:
    ``{"connected": bool, "results": {topic: bool}, "warnings": {topic: str | null}}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    res = manager.set_multiple(updates, strict_type_check=strict_type_check)
    return {
        "connected": True,
        "results": res["results"],
        "warnings": res["warnings"],
    }


@mcp.tool(
    annotations={
        "title": "List NetworkTables Topics",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def nt_list_topics(
    prefix: str | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
) -> dict:
    """Return topic names, filtered by prefix, regex, and/or wildcard.

    Response: ``{"connected": bool, "topics": [...]}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    topics = await manager.list_topics(prefix=prefix, regex=regex, wildcard=wildcard)
    return {"connected": True, "topics": topics}


@mcp.tool(
    annotations={
        "title": "Sample NetworkTables Updates",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
async def nt_sample(
    prefixes: list[str] | None = None,
    duration: float = 10.0,
    sample_interval: float | None = None,
    change_only: float | None = None,
    output: str = "summary",
    limit: int | None = None,
    max_rows: int = 5000,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
) -> dict:
    """Sample selected live updates for a fixed window and return bounded results.

    Returns ``samples`` keyed by topic, ``rows``, ``total_rows`` and
    ``truncated``. Summary mode returns statistics over retained samples;
    samples mode returns timestamp/value rows. Selectors are ANDed across
    kinds and ORed within lists. A bare "/" prefix is refused.
    ``nt_start_recording`` captures a human-controlled window to a file;
    ``nt_start_subscription`` buffers updates for later polling.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    selector, selector_error = _topic_selector(
        prefixes, topics, regex, wildcard
    )
    if selector_error is not None:
        return {
            "connected": True,
            "output": output,
            "error": selector_error,
        }
    assert selector is not None
    if output not in ("summary", "samples"):
        return {
            "connected": True,
            "output": output,
            "error": (
                f"unknown output mode {output!r}; valid modes: "
                "'summary', 'samples'"
            ),
        }
    if selector.bare_root_only:
        return {
            "connected": True,
            "output": output,
            "error": (
                "prefix '/' matches every topic; narrow it "
                "(e.g. '/SmartDashboard/') or use nt_start_recording"
            ),
        }
    # Inline modes: collect raw samples, enforce the total row budget,
    # then reduce summaries from what survived the clip.
    samples = await manager.subscribe(
        prefixes,
        duration=duration,
        sample_interval=sample_interval,
        change_only=change_only,
        format="samples",
        limit=None,
        topics=topics,
        regex=regex,
        wildcard=wildcard,
    )
    total_rows = sum(len(rows) for rows in samples.values())
    if limit is not None:
        samples = {topic: rows[:limit] for topic, rows in samples.items()}
    return _bounded_events(
        {"connected": True, "samples": samples, "total_rows": total_rows},
        "samples", output, max_rows,
    )


@mcp.tool(
    annotations={
        "title": "Start Persistent NetworkTables Subscription",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
async def nt_start_subscription(
    prefixes: list[str] | None = None,
    subscription_id: str | None = None,
    buffer_size: int = 1000,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
) -> dict:
    """Open a persistent subscription and start collecting samples.

    Response:
    ``{"connected": bool, "subscription_id": str, "started": bool}``.
    Use ``nt_poll_subscription`` to read the buffered samples in later
    turns, and ``nt_stop_subscription`` to tear it down.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    _, selector_error = _topic_selector(prefixes, topics, regex, wildcard)
    if selector_error is not None:
        return {
            "connected": True,
            "started": False,
            "error": selector_error,
        }
    if subscription_id is None:
        subscription_id = str(uuid.uuid4())
    if subscription_id in _subscription_tasks:
        return {
            "connected": True,
            "subscription_id": subscription_id,
            "started": False,
            "error": "subscription already exists",
        }
    task = asyncio.create_task(
        _consume_subscription(
            subscription_id,
            prefixes,
            topics,
            regex,
            wildcard,
            buffer_size,
        )
    )
    _subscription_tasks[subscription_id] = task
    return {
        "connected": True,
        "subscription_id": subscription_id,
        "started": True,
    }


@mcp.tool(
    annotations={
        "title": "Poll NetworkTables Subscription",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
def nt_poll_subscription(subscription_id: str, clear: bool = True) -> dict:
    """Return buffered samples for a persistent subscription.

    Response:
    ``{"connected": bool, "samples": {topic: [{"time", "value"}, ...]}}``.
    ``clear=True`` empties the buffer after reading.
    """
    connected = manager.is_connected()
    if subscription_id not in _subscription_buffers:
        return {
            "connected": connected,
            "error": f"no subscription with id {subscription_id}",
        }
    buffer = _subscription_buffers[subscription_id]
    samples: dict[str, list[dict]] = {}
    for item in buffer:
        samples.setdefault(item["topic"], []).append(
            {"time": item["time"], "value": item["value"]}
        )
    if clear:
        buffer.clear()
    return {"connected": connected, "samples": samples}


@mcp.tool(
    annotations={
        "title": "Stop NetworkTables Subscription",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    }
)
async def nt_stop_subscription(subscription_id: str) -> dict:
    """Stop a persistent subscription and free its resources."""
    task = _subscription_tasks.pop(subscription_id, None)
    _subscription_buffers.pop(subscription_id, None)
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return {"connected": manager.is_connected(), "stopped": True}


@mcp.tool(
    annotations={
        "title": "Start NetworkTables Recording",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
async def nt_start_recording(
    prefixes: list[str] | None = None,
    sample_interval: float | None = None,
    change_only: float | None = None,
    max_rows: int = 5000,
    output_dir: str | None = None,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
) -> dict:
    """Start an NDJSON recording and return its id immediately.

    Use ``nt_stop_recording(recording_id)`` after the human-controlled test
    window. The stop call closes the file and returns the compact receipt;
    do not use offline readers before it reports ``stopped: true``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    _, selector_error = _topic_selector(prefixes, topics, regex, wildcard)
    if selector_error is not None:
        return {
            "connected": True,
            "started": False,
            "error": selector_error,
        }
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    recording_id = _new_recording_id()
    recorder = nt_recorder.Recorder(
        manager,
        output_dir=dir_,
        prefixes=prefixes,
        duration=None,
        sample_interval=sample_interval,
        change_only=change_only,
        max_capture_rows=max_rows,
        recording_id=recording_id,
        topics=topics,
        regex=regex,
        wildcard=wildcard,
    )
    task = asyncio.create_task(recorder.run())
    _recorders[recording_id] = recorder
    _recording_tasks[recording_id] = task
    # Give run() a chance to create the file and surface immediate I/O errors.
    await asyncio.sleep(0)
    if task.done() and (error := task.exception()) is not None:
        _recorders.pop(recording_id, None)
        _recording_tasks.pop(recording_id, None)
        return {
            "connected": manager.is_connected(),
            "recording_id": recording_id,
            "started": False,
            "error": str(error),
        }
    response = {
        "connected": True,
        "recording_id": recording_id,
        "path": os.path.join(dir_, recording_id),
        "started": True,
    }
    if recorder.capture_started_at is not None:
        response["capture_started_at"] = recorder.capture_started_at
        response["capture_started_iso"] = _iso_timestamp(
            recorder.capture_started_at
        )
    return response


@mcp.tool(
    annotations={
        "title": "Stop NetworkTables Recording",
        "readOnlyHint": False,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    }
)
async def nt_stop_recording(recording_id: str) -> dict:
    """Stop a nonblocking recording and return its compact file receipt."""
    recorder = _recorders.get(recording_id)
    task = _recording_tasks.get(recording_id)
    if recorder is None or task is None:
        return {
            "connected": manager.is_connected(),
            "recording_id": recording_id,
            "stopped": False,
            "error": f"no active recording with id {recording_id}",
        }
    recorder.stop_event.set()
    try:
        path = await task
    except (IOError, OSError) as exc:
        return {
            "connected": manager.is_connected(),
            "recording_id": recording_id,
            "stopped": False,
            "error": str(exc),
        }
    finally:
        _recorders.pop(recording_id, None)
        _recording_tasks.pop(recording_id, None)
    return {
        "connected": manager.is_connected(),
        "stopped": True,
        **_recording_receipt(recorder, path),
    }


@mcp.tool(
    annotations={
        "title": "List NetworkTables Recordings",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
def nt_list_recordings(output_dir: str | None = None) -> list[dict]:
    """List available NDJSON recordings in the output directory.

    Each entry includes ``id``, ``path``, ``size_bytes``, ``modified``
    (Unix float) and ``modified_iso`` (UTC ISO-8601).
    """
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    return nt_recorder.list_recordings(dir_)


@mcp.tool(
    annotations={
        "title": "Inspect NetworkTables Recording",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
def nt_get_recording_info(
    recording_id: str | None = None,
    output_dir: str | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
    topic_limit: int = 100,
) -> dict:
    """Return metadata and a bounded, filterable topic list for a recording.

    Includes actual capture duration, NT event timestamp span, total sample
    count, topic count, file size and timestamps. Legacy recordings without
    a metadata sidecar explicitly fall back to their event span.
    """
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    recording_id, error = _closed_recording_id(recording_id, dir_)
    if error:
        return {"recording_id": recording_id, "error": error}
    try:
        result = nt_recorder.get_recording_info(
            recording_id, dir_, prefix, regex, wildcard, topic_limit
        )
    except ValueError as exc:
        return {"error": str(exc)}
    while result["topics"] and len(json.dumps(result)) > _MAX_RESPONSE_CHARS:
        result["topics"].pop()
        result["topics_returned"] = len(result["topics"])
        result["topics_truncated"] = True
    return result


@mcp.tool(
    annotations={
        "title": "Read NetworkTables Recording Events",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    }
)
def nt_query_recording(
    prefixes: list[str] | None = None,
    recording_id: str | None = None,
    start: float | None = None,
    end: float | None = None,
    last_seconds: float | None = None,
    limit: int = 1000,
    max_rows: int = 5000,
    sample_interval: float | None = None,
    output: str = "samples",
    output_dir: str | None = None,
    topics: list[str] | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
) -> dict:
    """Return selected events from a recording.

    ``prefixes`` and exact ``topics`` are ORed within their lists; selector
    kinds are ANDed. ``regex`` and ``wildcard`` match full topic names.

    ``last_seconds`` selects the most recent N seconds. ``sample_interval``
    decimates per-topic samples, and ``output="summary"`` returns
    min/max/mean/last per topic. ``limit`` caps entries per topic and
    ``max_rows`` caps the total across topics; the response always states
    ``rows``, ``total_rows`` and ``truncated`` so clipping is never silent.
    A bare ``"/"`` prefix is refused — narrow it (e.g. ``/SmartDashboard/``)
    or use the ``nt-recorder`` CLI to dump a whole recording. A final
    character ceiling keeps the whole response under 60,000 serialized
    characters; any rows it drops set ``truncated: True``.
    """
    selector, selector_error = _topic_selector(
        prefixes, topics, regex, wildcard
    )
    if selector_error is not None:
        return {
            "recording_id": recording_id,
            "error": selector_error,
            "rows": 0,
            "total_rows": 0,
            "truncated": False,
        }
    assert selector is not None
    if selector.bare_root_only:
        return {
            "recording_id": recording_id,
            "error": (
                "prefix '/' matches every topic in the recording; narrow it "
                "(e.g. '/SmartDashboard/') or use the nt-recorder CLI to "
                "dump the whole recording to a file"
            ),
            "rows": 0,
            "total_rows": 0,
            "truncated": False,
        }
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    recording_id, error = _closed_recording_id(recording_id, dir_)
    if error:
        return {"recording_id": recording_id, "error": error, "topics": {},
                "rows": 0, "total_rows": 0, "truncated": False}
    result = nt_recorder.subscribe_offline(
        prefixes=prefixes,
        recording_id=recording_id,
        output_dir=dir_,
        start=start,
        end=end,
        last_seconds=last_seconds,
        limit=limit,
        sample_interval=sample_interval,
        format="samples",
        return_meta=True,
        topics=topics,
        regex=regex,
        wildcard=wildcard,
    )
    return _bounded_events(
        {"recording_id": recording_id, "topics": result["topics"],
         "total_rows": result["total_rows"]},
        "topics", output, max_rows,
    )


def main() -> None:
    mcp.run()
