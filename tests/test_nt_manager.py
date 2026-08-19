"""Loopback NT4 integration tests for :mod:`nt_manager`.

Each run spins up a real pyntcore NT4 server in-process
(``NetworkTableInstance.create()``), publishes fixture topics under
``/SmartDashboard/``, and drives ``NTManager`` as a client against
``127.0.0.1:5810`` — the same transport a real RobotPy sim uses.

NT4 gotchas this suite encodes:

- ``NetworkTableInstance``'s constructor is not callable; a separate
  server instance must come from ``NetworkTableInstance.create()``.
- A client never sees server topics/values without a subscription, so
  the fixtures (and the manager) must wait/poll for the sync to finish.

All tests are async (``pytest.mark.anyio``) because the manager's read
methods are coroutines.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import ntcore
import pytest

from nt_manager import NTManager

PORT = 5810

# (topic name, entry setter, value) published by the server fixture.
FIXTURE_TOPICS: list[tuple[str, str, object]] = [
    ("/SmartDashboard/test_num", "setDouble", 42.0),
    ("/SmartDashboard/test_str", "setString", "hi"),
    ("/SmartDashboard/test_bool", "setBoolean", True),
    ("/SmartDashboard/test_arr", "setIntegerArray", [1, 2, 3]),
]
FIXTURE_NAMES = [name for name, _, _ in FIXTURE_TOPICS]
FIXTURE_VALUES = {name: value for name, _, value in FIXTURE_TOPICS}


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
async def manager(nt_server: ntcore.NetworkTableInstance) -> AsyncIterator[NTManager]:
    """Connected NTManager client, fully synced with the fixture server."""
    mgr = NTManager()
    mgr.connect("127.0.0.1", PORT, identity="test")

    async def connected() -> bool:
        return mgr.connection_info()["connected"]

    assert await wait_until(connected), "client never connected to the fixture server"

    async def synced() -> bool:
        names = await mgr.list_topics()
        return all(name in names for name in FIXTURE_NAMES) and (
            await mgr.get("/SmartDashboard/test_num") is not None
        )

    assert await wait_until(synced, timeout=15.0), "client never synced fixture topics"
    yield mgr
    mgr.disconnect()


@pytest.mark.anyio
async def test_connection_info_reports_connected(manager: NTManager) -> None:
    """Given a connected manager, connection_info()['connected'] is True."""
    assert manager.connection_info()["connected"] is True


@pytest.mark.anyio
async def test_list_topics_contains_fixture_topics(manager: NTManager) -> None:
    """Given a synced client, list_topics() includes every fixture topic."""
    names = await manager.list_topics()
    for name in FIXTURE_NAMES:
        assert name in names


@pytest.mark.anyio
async def test_list_topics_prefix_filter(manager: NTManager) -> None:
    """Given a prefix, list_topics() narrows to matching topics."""
    names = await manager.list_topics("/SmartDashboard/test_")
    assert set(FIXTURE_NAMES) <= set(names)
    assert await manager.list_topics("/does/not/match/") == []


@pytest.mark.anyio
async def test_get_returns_bare_typed_values(manager: NTManager) -> None:
    """get() returns raw values, never metadata wrappers."""
    num = await manager.get("/SmartDashboard/test_num")
    assert num == 42.0
    assert type(num) is float
    assert await manager.get("/SmartDashboard/test_str") == "hi"
    assert await manager.get("/SmartDashboard/test_bool") is True
    assert await manager.get("/SmartDashboard/test_arr") == [1, 2, 3]


@pytest.mark.anyio
async def test_get_unpublished_topic_is_none(manager: NTManager) -> None:
    """Given an unpublished topic, get() returns None."""
    assert await manager.get("/SmartDashboard/never_published") is None


@pytest.mark.anyio
async def test_get_multiple_flat_dict_with_none(manager: NTManager) -> None:
    """get_multiple() returns every requested key; unpublished -> None."""
    result = await manager.get_multiple(
        ["/SmartDashboard/test_num", "/SmartDashboard/test_str", "/SmartDashboard/nope"]
    )
    assert result == {
        "/SmartDashboard/test_num": 42.0,
        "/SmartDashboard/test_str": "hi",
        "/SmartDashboard/nope": None,
    }


@pytest.mark.anyio
async def test_set_integer_roundtrip_preserves_int(manager: NTManager) -> None:
    assert manager.set("/SmartDashboard/client_int", 7) is True
    value = await manager.get("/SmartDashboard/client_int")
    assert value == 7
    assert type(value) is int


@pytest.mark.anyio
async def test_set_double_roundtrip_preserves_float(manager: NTManager) -> None:
    assert manager.set("/SmartDashboard/client_double", 2.5) is True
    value = await manager.get("/SmartDashboard/client_double")
    assert value == 2.5
    assert type(value) is float


@pytest.mark.anyio
async def test_set_boolean_roundtrip_preserves_bool(manager: NTManager) -> None:
    assert manager.set("/SmartDashboard/client_bool", False) is True
    value = await manager.get("/SmartDashboard/client_bool")
    assert value is False
    assert type(value) is bool


@pytest.mark.anyio
async def test_set_string_roundtrip(manager: NTManager) -> None:
    assert manager.set("/SmartDashboard/client_str", "hello") is True
    assert await manager.get("/SmartDashboard/client_str") == "hello"


@pytest.mark.anyio
async def test_set_integer_array_roundtrip(manager: NTManager) -> None:
    assert manager.set("/SmartDashboard/client_iarr", [4, 5, 6]) is True
    assert await manager.get("/SmartDashboard/client_iarr") == [4, 5, 6]


@pytest.mark.anyio
async def test_set_double_array_roundtrip(manager: NTManager) -> None:
    assert manager.set("/SmartDashboard/client_darr", [0.5, 1.5]) is True
    assert await manager.get("/SmartDashboard/client_darr") == [0.5, 1.5]


@pytest.mark.anyio
async def test_set_invalid_type_returns_false(manager: NTManager) -> None:
    """set() on unsupported types returns False without raising."""
    assert manager.set("/SmartDashboard/client_bad", {"not": "a scalar"}) is False
    assert manager.set("/SmartDashboard/client_mixed", [1, "two"]) is False


@pytest.mark.anyio
async def test_subscribe_samples_every_fixture_topic(manager: NTManager) -> None:
    """Async subscribe on the fixture prefix for 1.0s samples every topic."""
    samples = await manager.subscribe(["/SmartDashboard/"], duration=1.0)
    for name in FIXTURE_NAMES:
        assert name in samples, f"no samples for {name}"
        assert len(samples[name]) >= 1
    for name, events in samples.items():
        for event in events:
            assert sorted(event) == ["time", "value"]
            assert isinstance(event["time"], float)
        if name in FIXTURE_VALUES:
            assert any(event["value"] == FIXTURE_VALUES[name] for event in events)


@pytest.mark.anyio
async def test_subscribe_stream_yields_fixture_events(manager: NTManager) -> None:
    """subscribe_stream yields (topic, value, time) tuples for every fixture topic.

    Consumes the generator for 1.0s via ``asyncio.timeout`` (the generator
    only yields on value updates, so a bare ``time.monotonic`` check inside
    the ``async for`` would never fire between events). Each yielded tuple
    must have a str topic, a float time, and a bare value (not wrapped in
    metadata like ``{"value": ...}``).
    """
    seen: dict[str, list[tuple[Any, float]]] = {}
    try:
        async with asyncio.timeout(1.0):
            async for topic, value, time_seconds in manager.subscribe_stream(
                ["/SmartDashboard/"]
            ):
                assert isinstance(topic, str)
                assert isinstance(time_seconds, float)
                # bare value, not wrapped in metadata like {"value": ...} or {"time": ...}
                assert not (
                    isinstance(value, dict) and ("value" in value or "time" in value)
                )
                seen.setdefault(topic, []).append((value, time_seconds))
    except TimeoutError:
        pass
    for name in FIXTURE_NAMES:
        assert name in seen, f"no event for {name}"
        assert len(seen[name]) >= 1
    for name, events in seen.items():
        if name in FIXTURE_VALUES:
            assert any(value == FIXTURE_VALUES[name] for value, _ in events)
