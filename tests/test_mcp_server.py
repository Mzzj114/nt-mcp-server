"""Tool-level tests for the FastMCP server (:mod:`nt_mcp_server`).

Each run spins up a real pyntcore NT4 server in-process
(``NetworkTableInstance.create()``) on port 5810 — the same loopback
recipe as ``tests/test_nt_manager.py`` — and drives the 10 MCP tools as
plain functions from :mod:`nt_mcp_server` (``nt_mcp_server.nt_get(...)``
etc.), never through the stdio transport.

The module-level ``nt_mcp_server.manager`` uses
``NetworkTableInstance.getDefault()`` as its client, a different
instance than the fixture server, so this is genuine cross-instance NT4
traffic. The module-scoped ``mcp_synced`` fixture connects with
``nt_connect`` and waits for the fixture topics to sync once per module;
every test then reuses the same connected manager. Sync tools
(``nt_set``, ``nt_set_multiple``, ``nt_disconnect``,
``nt_connection_info``) are called directly; async tools are awaited
under ``pytest.mark.anyio``.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

import ntcore
import pytest

import nt_mcp_server

PORT = 5810

# (topic name, entry setter, value) published by the server fixture.
FIXTURE_TOPICS: list[tuple[str, str, object]] = [
    ("/SmartDashboard/test_num", "setDouble", 42.0),
    ("/SmartDashboard/test_str", "setString", "hi"),
    ("/SmartDashboard/test_bool", "setBoolean", True),
    ("/SmartDashboard/test_arr", "setIntegerArray", [1, 2, 3]),
]
FIXTURE_NAMES = [name for name, _, _ in FIXTURE_TOPICS]


async def wait_until(
    condition: Callable[[], Awaitable[bool]],
    timeout: float = 10.0,
    interval: float = 0.05,
) -> bool:
    """Poll the async ``condition`` until True or ``timeout`` elapses."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await condition():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.fixture(scope="module")
def nt_server() -> Iterator[ntcore.NetworkTableInstance]:
    """In-process NT4 server publishing the fixture topics on port 5810."""
    inst = ntcore.NetworkTableInstance.create()
    inst.startServer()
    for name, setter, value in FIXTURE_TOPICS:
        getattr(inst.getEntry(name), setter)(value)
    yield inst
    inst.stopServer()
    ntcore.NetworkTableInstance.destroy(inst)


@pytest.fixture(scope="module")
async def mcp_synced(nt_server: ntcore.NetworkTableInstance) -> AsyncIterator[None]:
    """Tool client connected via ``nt_connect`` and synced with fixtures."""
    status = await nt_mcp_server.nt_connect(
        server_ip="127.0.0.1", server_port=PORT, identity="mcp-test"
    )
    assert status == "connected", "nt_connect never reached the fixture server"

    async def synced() -> bool:
        names = await nt_mcp_server.nt_list_topics()
        return all(name in names for name in FIXTURE_NAMES) and (
            await nt_mcp_server.nt_get("/SmartDashboard/test_num") is not None
        )

    assert await wait_until(synced, timeout=15.0), "tools never synced fixture topics"
    yield
    nt_mcp_server.nt_disconnect()


@pytest.mark.anyio
async def test_nt_connect_returns_connected(mcp_synced: None) -> None:
    """Given the synced fixture, nt_connect is idempotent and reports connected."""
    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status == "connected"


@pytest.mark.anyio
async def test_nt_connection_info_is_json_serializable(mcp_synced: None) -> None:
    """Given a live connection, nt_connection_info() dumps as JSON."""
    info = nt_mcp_server.nt_connection_info()
    assert info["connected"] is True
    json.dumps(info)


@pytest.mark.anyio
async def test_nt_get_returns_bare_value(mcp_synced: None) -> None:
    """Given a published double, nt_get returns a bare float, not metadata."""
    value = await nt_mcp_server.nt_get("/SmartDashboard/test_num")
    assert value == 42.0
    assert type(value) is float


@pytest.mark.anyio
async def test_nt_get_info_returns_type_str(mcp_synced: None) -> None:
    """Given a published topic, nt_get_info returns metadata with type_str."""
    info = await nt_mcp_server.nt_get_info("/SmartDashboard/test_num")
    assert info is not None
    assert info["name"] == "/SmartDashboard/test_num"
    assert info["type_str"] == "double"


@pytest.mark.anyio
async def test_nt_set_int_roundtrip_preserves_type(mcp_synced: None) -> None:
    """Given nt_set(7), nt_get returns the same int."""
    assert nt_mcp_server.nt_set("/SmartDashboard/mcp_int", 7) is True
    value = await nt_mcp_server.nt_get("/SmartDashboard/mcp_int")
    assert value == 7
    assert type(value) is int


@pytest.mark.anyio
async def test_nt_set_float_roundtrip_preserves_type(mcp_synced: None) -> None:
    """Given nt_set(2.5), nt_get returns the same float."""
    assert nt_mcp_server.nt_set("/SmartDashboard/mcp_double", 2.5) is True
    value = await nt_mcp_server.nt_get("/SmartDashboard/mcp_double")
    assert value == 2.5
    assert type(value) is float


@pytest.mark.anyio
async def test_nt_set_bool_roundtrip_preserves_type(mcp_synced: None) -> None:
    """Given nt_set(False), nt_get returns the same bool."""
    assert nt_mcp_server.nt_set("/SmartDashboard/mcp_bool", False) is True
    value = await nt_mcp_server.nt_get("/SmartDashboard/mcp_bool")
    assert value is False
    assert type(value) is bool


@pytest.mark.anyio
async def test_nt_set_multiple_returns_dict_of_bools(mcp_synced: None) -> None:
    """Given two topics, nt_set_multiple returns per-topic bool results."""
    result = nt_mcp_server.nt_set_multiple(
        {"/SmartDashboard/mcp_multi_int": 11, "/SmartDashboard/mcp_multi_str": "abc"}
    )
    assert result == {
        "/SmartDashboard/mcp_multi_int": True,
        "/SmartDashboard/mcp_multi_str": True,
    }
    assert all(type(ok) is bool for ok in result.values())


@pytest.mark.anyio
async def test_nt_get_multiple_returns_flat_dict(mcp_synced: None) -> None:
    """Given three topics, nt_get_multiple returns a flat {topic: value} dict."""
    result = await nt_mcp_server.nt_get_multiple(
        ["/SmartDashboard/test_num", "/SmartDashboard/test_str", "/SmartDashboard/nope"]
    )
    assert result == {
        "/SmartDashboard/test_num": 42.0,
        "/SmartDashboard/test_str": "hi",
        "/SmartDashboard/nope": None,
    }


@pytest.mark.anyio
async def test_nt_list_topics_prefix_filter(mcp_synced: None) -> None:
    """Given a prefix, nt_list_topics narrows to matching topics."""
    names = await nt_mcp_server.nt_list_topics("/SmartDashboard/test_")
    assert set(FIXTURE_NAMES) <= set(names)
    assert await nt_mcp_server.nt_list_topics("/does/not/match/") == []


@pytest.mark.anyio
async def test_nt_subscribe_returns_json_samples(mcp_synced: None) -> None:
    """Given the fixture prefix, nt_subscribe samples every topic as JSON."""
    samples = await nt_mcp_server.nt_subscribe(["/SmartDashboard/"], duration=1.0)
    for name in FIXTURE_NAMES:
        assert name in samples, f"no samples for {name}"
        assert len(samples[name]) >= 1
    json.dumps(samples)


@pytest.mark.anyio
async def test_nt_disconnect_returns_status_and_reconnects(mcp_synced: None) -> None:
    """Given a live client, nt_disconnect stops it; nt_connect restores it."""
    assert nt_mcp_server.nt_disconnect() == "disconnected"

    async def disconnected() -> bool:
        return nt_mcp_server.nt_connection_info()["connected"] is False

    assert await wait_until(
        disconnected, timeout=2.0
    ), "client still connected after nt_disconnect"
    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status == "connected"
