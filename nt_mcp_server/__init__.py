"""FastMCP server exposing FRC NetworkTables to AI agents.

Thin MCP layer: every tool delegates to the module-level ``NTManager``
from ``nt_manager``. No ntcore imports here — all NetworkTables logic
lives in the manager. Run with ``uv run nt-mcp-server`` or
``python -m nt_mcp_server`` (stdio transport, the default for ``mcp.run()``).
"""

import asyncio
import contextlib
import os
import time
import uuid
from typing import Any

from fastmcp import FastMCP

from .nt_manager import NTManager
from . import nt_recorder

mcp = FastMCP("NetworkTables")
manager = NTManager()

_CONNECT_TIMEOUT_SECONDS = 2.0
_CONNECT_POLL_INTERVAL_SECONDS = 0.05

# Persistent subscription state. Each subscription is a background asyncio
# Task consuming ``manager.subscribe_stream`` and appending to a list buffer.
_subscription_tasks: dict[str, asyncio.Task] = {}
_subscription_buffers: dict[str, list[dict[str, Any]]] = {}


async def _consume_subscription(subscription_id: str, prefixes: list[str], buffer_size: int) -> None:
    """Consume the stream for ``subscription_id`` into its buffer."""
    buffer: list[dict[str, Any]] = []
    _subscription_buffers[subscription_id] = buffer
    try:
        async for topic, value, time_seconds in manager.subscribe_stream(prefixes):
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


@mcp.tool()
async def nt_connect(
    server_ip: str = "127.0.0.1",
    server_port: int = 5810,
    team_number: int | None = None,
    identity: str = "nt-mcp",
) -> dict:
    """Start the NT4 client and wait for a live connection."""
    manager.connect(
        server_ip=server_ip,
        server_port=server_port,
        team_number=team_number,
        identity=identity,
    )
    deadline = time.monotonic() + _CONNECT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if manager.is_connected():
            return {"connected": True, "status": "connected"}
        await asyncio.sleep(_CONNECT_POLL_INTERVAL_SECONDS)
    return {"connected": False, "status": "not connected"}


@mcp.tool()
def nt_disconnect() -> dict:
    """Stop the NT4 client and tear down any persistent subscriptions."""
    _stop_all_subscriptions()
    manager.disconnect()
    return {"connected": False, "status": "disconnected"}


@mcp.tool()
def nt_connection_info() -> dict:
    """Return ``{"connected": bool, "connections": [...]}``."""
    return manager.connection_info()


@mcp.tool()
async def nt_get(topic: str) -> dict:
    """Return the JSON-normalized value of ``topic``.

    Response: ``{"connected": bool, "value": jsonable | null}`` when
    connected, or ``{"connected": false, "error": "..."}`` when
    disconnected.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    return {"connected": True, "value": await manager.get(topic)}


@mcp.tool()
async def nt_get_multiple(topics: list[str]) -> dict:
    """Return values for every requested topic.

    Response: ``{"connected": bool, "values": {topic: value}}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    return {"connected": True, "values": await manager.get_multiple(topics)}


@mcp.tool()
async def nt_get_info(topic: str) -> dict:
    """Return metadata for ``topic``.

    Response: ``{"connected": bool, "info": {...} | null}``.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    return {"connected": True, "info": await manager.get_info(topic)}


@mcp.tool()
def nt_set(topic: str, value: Any, strict_type_check: bool = False) -> dict:
    """Publish ``value`` to ``topic``.

    Response: ``{"connected": bool, "ok": bool, "warning": str | null}``.
    ``strict_type_check=True`` refuses mismatched writes instead of only
    warning about them.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    res = manager.set(topic, value, strict_type_check=strict_type_check)
    return {
        "connected": True,
        "ok": res["ok"],
        "warning": res["warning"],
    }


@mcp.tool()
def nt_set_multiple(updates: dict[str, Any], strict_type_check: bool = False) -> dict:
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


@mcp.tool()
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


@mcp.tool()
async def nt_subscribe(
    prefixes: list[str],
    duration: float = 10.0,
    sample_interval: float | None = None,
    change_only: float | None = None,
    format: str = "samples",
) -> dict:
    """Sample value updates under ``prefixes`` for ``duration`` seconds.

    Response: ``{"connected": bool, "samples": {topic: [...] | summary}}``.
    ``sample_interval`` decimates events to one per topic per interval.
    ``change_only`` skips numeric changes at or below the threshold.
    ``format="summary"`` returns min/max/mean/last per topic.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
    samples = await manager.subscribe(
        prefixes,
        duration=duration,
        sample_interval=sample_interval,
        change_only=change_only,
        format=format,
    )
    return {"connected": True, "samples": samples}


@mcp.tool()
async def nt_start_subscription(
    prefixes: list[str],
    subscription_id: str | None = None,
    buffer_size: int = 1000,
) -> dict:
    """Open a persistent subscription and start collecting samples.

    Response:
    ``{"connected": bool, "subscription_id": str, "started": bool}``.
    Use ``nt_poll_subscription`` to read the buffered samples in later
    turns, and ``nt_stop_subscription`` to tear it down.
    """
    if not manager.is_connected():
        return {"connected": False, "error": "NT server disconnected"}
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
        _consume_subscription(subscription_id, prefixes, buffer_size)
    )
    _subscription_tasks[subscription_id] = task
    return {
        "connected": True,
        "subscription_id": subscription_id,
        "started": True,
    }


@mcp.tool()
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


@mcp.tool()
async def nt_stop_subscription(subscription_id: str) -> dict:
    """Stop a persistent subscription and free its resources."""
    task = _subscription_tasks.pop(subscription_id, None)
    _subscription_buffers.pop(subscription_id, None)
    if task is not None and not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return {"connected": manager.is_connected(), "stopped": True}


@mcp.tool()
def nt_list_recordings(output_dir: str | None = None) -> list[dict]:
    """List available NDJSON recordings in the output directory.

    Each entry includes ``id``, ``path``, ``size_bytes``, ``modified``
    (Unix float) and ``modified_iso`` (UTC ISO-8601).
    """
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    return nt_recorder.list_recordings(dir_)


@mcp.tool()
def nt_get_history(
    topic: str,
    recording_id: str | None = None,
    start: float | None = None,
    end: float | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    format: str = "samples",
    output_dir: str | None = None,
) -> list[dict]:
    """Return event history for one topic from a recording.

    ``last_seconds`` selects the most recent N seconds of the recording.
    ``sample_interval`` and ``format="summary"`` behave like live
    ``nt_subscribe``.
    """
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    return nt_recorder.get_history(
        topic,
        recording_id,
        dir_,
        start,
        end,
        last_seconds,
        limit,
        sample_interval,
        format,
    )


@mcp.tool()
def nt_get_recording_info(
    recording_id: str | None = None,
    output_dir: str | None = None,
) -> dict:
    """Return metadata about a recording.

    Includes duration, total sample count, topic count, file size and
    timestamps.
    """
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    return nt_recorder.get_recording_info(recording_id, dir_)


@mcp.tool()
def nt_list_topics_offline(
    recording_id: str | None = None,
    prefix: str | None = None,
    regex: str | None = None,
    wildcard: str | None = None,
    output_dir: str | None = None,
) -> list[str]:
    """List unique topic names in a recording, optionally filtered."""
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    return nt_recorder.list_topics(recording_id, dir_, prefix, regex, wildcard)


@mcp.tool()
def nt_subscribe_offline(
    prefixes: list[str],
    recording_id: str | None = None,
    start: float | None = None,
    end: float | None = None,
    last_seconds: float | None = None,
    limit: int | None = None,
    sample_interval: float | None = None,
    format: str = "samples",
    output_dir: str | None = None,
) -> dict[str, list[dict]]:
    """Return events for every topic under ``prefixes`` from a recording.

    ``last_seconds`` selects the most recent N seconds. ``sample_interval``
    decimates per-topic samples, and ``format="summary"`` returns
    min/max/mean/last per topic.
    """
    dir_ = output_dir or os.environ.get("NT_RECORDINGS_DIR", "./recordings")
    return nt_recorder.subscribe_offline(
        prefixes,
        recording_id,
        dir_,
        start,
        end,
        last_seconds,
        limit,
        sample_interval,
        format,
    )


def main() -> None:
    mcp.run()
