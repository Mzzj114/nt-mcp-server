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
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from pydantic import Field

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

# Bound both the number and serialized size of topic names in file receipts.
_MAX_RECEIPT_TOPICS = 5
_MAX_RECEIPT_TOPIC_CHARS = 500

# Shared parameter contracts are included in the MCP input schemas.
TopicName = Annotated[
    str,
    Field(
        min_length=1,
        max_length=2048,
        pattern=r"^/",
        description="Absolute NT topic name, such as /SmartDashboard/Speed.",
    ),
]
TopicNames = Annotated[
    list[TopicName],
    Field(
        min_length=1,
        max_length=100,
        description="Exact topic names to read; a one-element list reads one topic.",
    ),
]
SelectedTopics = Annotated[
    list[TopicName] | None,
    Field(
        min_length=1,
        max_length=100,
        description="Exact topic names, ORed within the list and ANDed with other selectors; omitted means no exact-name filter.",
    ),
]
SelectedPrefixes = Annotated[
    list[TopicName] | None,
    Field(
        min_length=1,
        max_length=100,
        description="Topic prefixes, ORed within the list and ANDed with other selectors; omitted means no prefix filter.",
    ),
]
PrefixFilter = Annotated[
    str | None,
    Field(
        max_length=2048, description="Topic name prefix; omitted includes all prefixes."
    ),
]
RegexFilter = Annotated[
    str | None,
    Field(
        max_length=2048,
        description="Python regex searched anywhere in the topic name; omitted disables this filter.",
    ),
]
WildcardFilter = Annotated[
    str | None,
    Field(
        max_length=2048,
        description="Shell-style topic glob, for example /SmartDashboard/*/Speed; omitted disables this filter.",
    ),
]
RecordingDirectory = Annotated[
    str | None,
    Field(
        min_length=1,
        description="Server-side recording directory; defaults to NT_RECORDINGS_DIR or ./recordings.",
    ),
]
RecordingId = Annotated[
    str,
    Field(
        pattern=r"^[^/\\]+\.ndjson$",
        description="Recording filename returned by nt_start_recording or nt_list_recordings, without directory components.",
    ),
]
SelectedRecording = Annotated[
    RecordingId | None,
    Field(
        description="Recording filename; omitted selects the most recently modified recording in the directory."
    ),
]
SubscriptionId = Annotated[
    str,
    Field(
        min_length=1,
        max_length=128,
        description="Handle returned by nt_start_subscription.",
    ),
]
NewSubscriptionId = Annotated[
    str | None,
    Field(
        min_length=1,
        max_length=128,
        description="Optional subscription handle; omitted generates a UUID.",
    ),
]
SampleInterval = Annotated[
    float | None,
    Field(
        gt=0,
        allow_inf_nan=False,
        description="Minimum NT timestamp spacing between retained events per topic; filters received events before row limits, without generating periodic readings. Omitted disables interval filtering.",
    ),
]
ChangeThreshold = Annotated[
    float | None,
    Field(
        ge=0,
        allow_inf_nan=False,
        description="Skip numeric changes at or below this threshold, or unchanged non-numeric values; omitted disables change filtering.",
    ),
]
OutputMode = Annotated[
    Literal["summary", "samples"],
    Field(
        description="summary returns statistics over rows retained after filters and output limits; last_value is the last retained event, not necessarily the value at the end of the window. samples returns timestamp/value rows."
    ),
]
RowLimit = Annotated[
    int,
    Field(
        ge=1, le=5000, description="Maximum retained events per topic, from 1 to 5000."
    ),
]
OptionalRowLimit = Annotated[
    int | None,
    Field(
        ge=1,
        le=5000,
        description="Maximum retained events per topic; omitted uses only the total row budget.",
    ),
]
RowBudget = Annotated[
    int,
    Field(
        ge=1,
        le=5000,
        description="Maximum total retained events across topics, from 1 to 5000; clipped results report truncated.",
    ),
]
CaptureBudget = Annotated[
    int,
    Field(
        ge=1,
        le=1_000_000,
        description="Maximum events written to the recording; reaching the cap ends capture and the receipt reports truncated.",
    ),
]
StartTime = Annotated[
    float | None,
    Field(
        ge=0,
        allow_inf_nan=False,
        description="Inclusive event timestamp bound in seconds, using the recording's NT time clock; omitted is unbounded.",
    ),
]
EndTime = Annotated[
    float | None,
    Field(
        ge=0,
        allow_inf_nan=False,
        description="Exclusive event timestamp bound in seconds, using the recording's NT time clock; omitted is unbounded.",
    ),
]
LastSeconds = Annotated[
    float | None,
    Field(
        gt=0,
        allow_inf_nan=False,
        description="Most recent N seconds relative to the recording's latest NT event timestamp, not its wall-clock capture end; omitted selects the full time range.",
    ),
]

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
    topic_preview: list[str] = []
    for topic in topics[:_MAX_RECEIPT_TOPICS]:
        if len(json.dumps(topic_preview + [topic])) > _MAX_RECEIPT_TOPIC_CHARS:
            break
        topic_preview.append(topic)
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
        "topics": topic_preview,
        "topics_truncated": len(topics) > len(topic_preview),
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
        active_path = os.path.normcase(
            os.path.abspath(os.path.join(recorder.output_dir, active_id))
        )
        if path == active_path and not _recording_tasks[active_id].done():
            return (
                recording_id,
                "recording is active; nt_stop_recording closes it before offline queries",
            )
    if not os.path.isfile(path):
        return (
            recording_id,
            f"recording {recording_id!r} not found; nt_list_recordings lists available files",
        )
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
        response[key] = {
            topic: summarize(items) for topic, items in response[key].items()
        }
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
    server_ip: Annotated[
        str,
        Field(
            min_length=1,
            description="NT4 server IP or hostname; defaults to the local sim. Use team_number for a team target.",
        ),
    ] = "127.0.0.1",
    server_port: Annotated[
        int, Field(ge=1, le=65535, description="NT4 TCP port; defaults to 5810.")
    ] = 5810,
    team_number: Annotated[
        int | None,
        Field(
            ge=1,
            le=99999,
            description="FRC team number for robot address lookup; omitted uses server_ip/server_port.",
        ),
    ] = None,
    identity: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            description="NT client name visible to the server.",
        ),
    ] = "nt-mcp",
    timeout_seconds: Annotated[
        float,
        Field(
            gt=0,
            le=60,
            allow_inf_nan=False,
            description="Maximum seconds to wait for a live connection, up to 60.",
        ),
    ] = _CONNECT_TIMEOUT_SECONDS,
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
    """Return connection state, remote peers, resolved target and package version."""
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
async def nt_read_topics(
    topics: TopicNames,
    include_metadata: Annotated[
        bool,
        Field(description=(
            "Add per-topic NT value type, validity, local_time_us and server_time_us "
            "from the same value snapshot. Timestamps are integer microseconds in "
            "the client's local NT clock and NT server clock, not assumed wall-clock "
            "or robot FPGA time; unavailable timestamps (including the local "
            "value's server-time placeholder 1) are null. Topics are read "
            "sequentially, not atomically as a batch. Defaults to values only."
        )),
    ] = False,
) -> dict:
    """Read current values for one or more exact topic names.

    Returns ``{"connected": bool, "values": {topic: value | null}}``;
    unknown topics have null values. Opt in with ``include_metadata=True``
    to add ``metadata: {topic: {type, valid, local_time_us, server_time_us}}``.
    Each value and its metadata come from one atomic NT value snapshot;
    the batch is not atomic across topics. Type is the NT storage type,
    not inferred from JSON. Invalid values have null type/timestamps;
    nonpositive/unavailable timestamps and the server-time placeholder 1
    are null even for valid values.
    Timestamps are microseconds in the local client and server NT clocks,
    not assumed wall-clock or robot FPGA time. A value timestamp is not
    acknowledgment of a command on another topic; robot-side clock
    comparisons may require robot-side instrumentation.
    ``nt_get_topic_info`` returns topic type labels/properties, and
    ``nt_sample`` observes changes over a time window.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    if include_metadata:
        return {"connected": True, **await manager.get_multiple_with_metadata(topics)}
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
async def nt_get_topic_info(topic: TopicName) -> dict:
    """Return a topic's name, NT type and properties, or null if unknown.

    Returns ``{"connected": bool, "info": {...} | null}``.
    ``nt_read_topics`` returns current values.
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
def nt_write_topics(
    updates: Annotated[
        dict[TopicName, Any],
        Field(
            min_length=1,
            max_length=100,
            description="Map of absolute topic names to values; one entry writes one topic. Results are per topic, and writes are not atomic.",
        ),
    ],
    strict_type_check: Annotated[
        bool,
        Field(
            description="True refuses known topic type mismatches; False attempts the write and returns warnings."
        ),
    ] = False,
) -> dict:
    """Publish one or more topic/value pairs and return per-topic results.

    Response:
    ``{"connected": bool, "results": {topic: bool}, "warnings": {topic: str | null}}``.
    Writes are not atomic; partial success is reported per topic.
    ``nt_get_topic_info`` exposes the existing NT type for mismatch checks.
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
    prefix: PrefixFilter = None,
    regex: RegexFilter = None,
    wildcard: WildcardFilter = None,
) -> dict:
    """Return topic names, filtered by prefix, regex, and/or wildcard.

    Response: ``{"connected": bool, "topics": [...]}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    try:
        TopicSelector(regex=regex)
    except ValueError as exc:
        return {"connected": True, "error": str(exc)}
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
    prefixes: SelectedPrefixes = None,
    duration: Annotated[
        float,
        Field(
            gt=0,
            le=60,
            allow_inf_nan=False,
            description="Fixed sampling window in seconds, up to 60; blocks until it ends.",
        ),
    ] = 10.0,
    sample_interval: SampleInterval = None,
    change_only: ChangeThreshold = None,
    output: OutputMode = "summary",
    limit: OptionalRowLimit = None,
    max_rows: RowBudget = 5000,
    topics: SelectedTopics = None,
    regex: RegexFilter = None,
    wildcard: WildcardFilter = None,
) -> dict:
    """Sample selected live updates for a fixed window and return bounded results.

    Receives available initial values followed by incoming value updates;
    it does not periodically read unchanged values. A quiet topic may have
    only one sample. Initial NT timestamps can predate this sampling window.
    ``sample_interval`` filters received events before row limits; it does
    not set a polling rate or create samples.

    Returns ``samples`` keyed by topic, ``rows``, ``total_rows`` and
    ``truncated``. Summary mode returns statistics over retained samples;
    ``last_value`` is the last event retained after filters and output
    limits, which may precede the end of the window. Samples mode returns
    timestamp/value rows. Selectors are ANDed across
    kinds and ORed within lists. A bare "/" prefix is refused.
    ``nt_start_recording`` captures a human-controlled window to a file;
    ``nt_start_subscription`` buffers updates for later polling.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    selector, selector_error = _topic_selector(prefixes, topics, regex, wildcard)
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
                f"unknown output mode {output!r}; valid modes: 'summary', 'samples'"
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
        "samples",
        output,
        max_rows,
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
    prefixes: SelectedPrefixes = None,
    subscription_id: NewSubscriptionId = None,
    buffer_size: Annotated[
        int,
        Field(
            ge=1,
            le=5000,
            description="Maximum buffered events; oldest events are discarded when full.",
        ),
    ] = 1000,
    topics: SelectedTopics = None,
    regex: RegexFilter = None,
    wildcard: WildcardFilter = None,
) -> dict:
    """Open a persistent subscription and start collecting samples.

    Response:
    ``{"connected": bool, "subscription_id": str, "started": bool}``.
    Use ``nt_poll_subscription`` to read the buffered samples in later
    turns, and ``nt_stop_subscription`` to tear it down.
    ``nt_start_recording`` captures to a file instead; ``nt_sample`` reads
    a fixed window in one call. Selector kinds are ANDed; list entries ORed.
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
def nt_poll_subscription(
    subscription_id: SubscriptionId,
    clear: Annotated[
        bool,
        Field(
            description="True clears buffered events after polling; False keeps them for another poll."
        ),
    ] = True,
) -> dict:
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
async def nt_stop_subscription(subscription_id: SubscriptionId) -> dict:
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
    prefixes: SelectedPrefixes = None,
    sample_interval: SampleInterval = None,
    change_only: ChangeThreshold = None,
    max_rows: CaptureBudget = 5000,
    output_dir: RecordingDirectory = None,
    topics: SelectedTopics = None,
    regex: RegexFilter = None,
    wildcard: WildcardFilter = None,
) -> dict:
    """Start an NDJSON recording and return its id immediately.

    Use ``nt_stop_recording(recording_id)`` after the human-controlled test
    window. The stop call closes the file and returns the compact receipt;
    do not use offline readers before it reports ``stopped: true``.
    Records the same initial values and incoming updates as ``nt_sample``;
    it does not periodically read unchanged values. ``sample_interval``
    filters received events rather than generating periodic readings.
    Selector kinds are ANDed; list entries ORed. ``nt_sample`` returns a
    fixed window inline; ``nt_start_subscription`` buffers events for polling.
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
        response["capture_started_iso"] = _iso_timestamp(recorder.capture_started_at)
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
async def nt_stop_recording(recording_id: RecordingId) -> dict:
    """Stop a nonblocking recording and return its compact file receipt.

    Timing and counts describe the full capture. ``topics`` previews at most
    five names within 500 serialized characters; ``topics_truncated`` marks
    omitted names, independently of the captured-row ``truncated`` flag.
    Use ``nt_get_recording_info`` with this ``recording_id`` and optional
    topic filters for bounded discovery of the recorded topics.
    """
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
def nt_list_recordings(output_dir: RecordingDirectory = None) -> list[dict]:
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
    recording_id: SelectedRecording = None,
    output_dir: RecordingDirectory = None,
    prefix: PrefixFilter = None,
    regex: RegexFilter = None,
    wildcard: WildcardFilter = None,
    topic_limit: Annotated[
        int,
        Field(
            ge=1,
            le=1000,
            description="Maximum topic names returned, from 1 to 1000; metadata counts describe the full recording.",
        ),
    ] = 100,
) -> dict:
    """Return metadata and a bounded, filterable topic list for a recording.

    Includes actual capture duration, NT event timestamp span, total sample
    count, topic count, file size and timestamps. Legacy recordings without
    a metadata sidecar explicitly fall back to their event span.
    ``topic_count`` describes the whole file; ``matching_topic_count``
    describes names matching all filters. ``topics_returned`` and
    ``topics_truncated`` report clipping of the sorted topic list.
    ``nt_query_recording`` returns values; ``nt_list_recordings`` finds files.
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
    prefixes: SelectedPrefixes = None,
    recording_id: SelectedRecording = None,
    start: StartTime = None,
    end: EndTime = None,
    last_seconds: LastSeconds = None,
    limit: RowLimit = 1000,
    max_rows: RowBudget = 5000,
    sample_interval: SampleInterval = None,
    output: OutputMode = "samples",
    output_dir: RecordingDirectory = None,
    topics: SelectedTopics = None,
    regex: RegexFilter = None,
    wildcard: WildcardFilter = None,
) -> dict:
    """Return selected events from a recording.

    ``prefixes`` and exact ``topics`` are ORed within their lists; selector
    kinds are ANDed. ``regex`` and ``wildcard`` match full topic names.

    ``last_seconds`` selects the most recent N seconds. ``sample_interval``
    decimates per-topic events before row limits; it does not generate
    regularly spaced samples. ``output="summary"`` returns min/max/mean/last
    over rows retained after filtering and output limits. ``last_value``
    is the last retained event, which may precede the end of the selected
    window. ``limit`` caps entries per topic and
    ``max_rows`` caps the total across topics; the response always states
    ``rows``, ``total_rows`` and ``truncated`` so clipping is never silent.
    A bare ``"/"`` prefix is refused; ``nt_get_recording_info`` discovers
    topic names for a narrower query. A final
    character ceiling keeps the whole response under 60,000 serialized
    characters; any rows it drops set ``truncated: True``.
    """
    selector, selector_error = _topic_selector(prefixes, topics, regex, wildcard)
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
                "(e.g. '/SmartDashboard/'); nt_get_recording_info lists "
                "topic names for a narrower query"
            ),
            "rows": 0,
            "total_rows": 0,
            "truncated": False,
        }
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    recording_id, error = _closed_recording_id(recording_id, dir_)
    if error:
        return {
            "recording_id": recording_id,
            "error": error,
            "topics": {},
            "rows": 0,
            "total_rows": 0,
            "truncated": False,
        }
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
        {
            "recording_id": recording_id,
            "topics": result["topics"],
            "total_rows": result["total_rows"],
        },
        "topics",
        output,
        max_rows,
    )


def main() -> None:
    mcp.run()
