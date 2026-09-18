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
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any

import ntcore
import pytest

from nt_mcp_server.nt_manager import (
    DEFAULT_SERVER_IP,
    DEFAULT_SERVER_PORT,
    NTManager,
    AmbiguousTargetError,
    StreamDecimator,
    TargetMismatchError,
    _package_version,
)

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


@pytest.fixture()
def fresh_manager(nt_server: ntcore.NetworkTableInstance) -> Iterator[NTManager]:
    """Unconnected NTManager for target-handling tests; torn down after."""
    mgr = NTManager()
    yield mgr
    mgr.disconnect()


# --- connect target handling (plan checkboxes 1 and 2) ---
# NOTE: the ``fresh_manager`` tests below MUST run before any test that uses
# the module-scoped ``manager`` fixture: every NTManager shares the default
# NetworkTableInstance, so a second manager's ``disconnect()`` stops the
# shared client and would kill the module fixture's loopback connection.


@pytest.mark.anyio
async def test_connect_ambiguous_target_raises_without_state_change(
    fresh_manager: NTManager,
) -> None:
    """Given both team_number and a non-default server_ip, connect raises
    AmbiguousTargetError before touching any instance state."""
    with pytest.raises(AmbiguousTargetError, match="8326"):
        fresh_manager.connect(server_ip="10.83.26.2", team_number=8326)
    assert fresh_manager._client_running is False
    assert fresh_manager.target is None


@pytest.mark.anyio
async def test_connect_server_records_target(fresh_manager: NTManager) -> None:
    """Given a server target, mgr.target matches the requested target."""
    fresh_manager.connect("127.0.0.1", PORT, identity="test")
    assert fresh_manager.target == {
        "kind": "server",
        "server_ip": "127.0.0.1",
        "server_port": PORT,
    }


@pytest.mark.anyio
async def test_connect_same_target_is_idempotent(fresh_manager: NTManager) -> None:
    """A second connect() with identical args does not raise."""
    fresh_manager.connect("127.0.0.1", PORT, identity="test")
    fresh_manager.connect("127.0.0.1", PORT, identity="test")
    assert fresh_manager._client_running is True


@pytest.mark.anyio
async def test_disconnect_resets_target(fresh_manager: NTManager) -> None:
    """After disconnect(), target is None."""
    fresh_manager.connect("127.0.0.1", PORT, identity="test")
    fresh_manager.disconnect()
    assert fresh_manager.target is None
    assert fresh_manager._client_running is False


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
async def test_list_topics_regex_filter(manager: NTManager) -> None:
    """Regex filters topic names."""
    names = await manager.list_topics(regex=r"test_(num|str)$")
    assert "/SmartDashboard/test_num" in names
    assert "/SmartDashboard/test_str" in names
    assert "/SmartDashboard/test_bool" not in names


@pytest.mark.anyio
async def test_list_topics_wildcard_filter(manager: NTManager) -> None:
    """Shell-style wildcards filter topic names."""
    names = await manager.list_topics(wildcard="/SmartDashboard/test_*")
    assert set(FIXTURE_NAMES) <= set(names)
    assert await manager.list_topics(wildcard="/SmartDashboard/*.does.not.match") == []


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
    res = manager.set("/SmartDashboard/client_int", 7)
    assert res == {"ok": True, "warning": None}
    value = await manager.get("/SmartDashboard/client_int")
    assert value == 7
    assert type(value) is int


@pytest.mark.anyio
async def test_set_double_roundtrip_preserves_float(manager: NTManager) -> None:
    res = manager.set("/SmartDashboard/client_double", 2.5)
    assert res == {"ok": True, "warning": None}
    value = await manager.get("/SmartDashboard/client_double")
    assert value == 2.5
    assert type(value) is float


@pytest.mark.anyio
async def test_set_boolean_roundtrip_preserves_bool(manager: NTManager) -> None:
    res = manager.set("/SmartDashboard/client_bool", False)
    assert res == {"ok": True, "warning": None}
    value = await manager.get("/SmartDashboard/client_bool")
    assert value is False
    assert type(value) is bool


@pytest.mark.anyio
async def test_set_string_roundtrip(manager: NTManager) -> None:
    res = manager.set("/SmartDashboard/client_str", "hello")
    assert res == {"ok": True, "warning": None}
    assert await manager.get("/SmartDashboard/client_str") == "hello"


@pytest.mark.anyio
async def test_set_integer_array_roundtrip(manager: NTManager) -> None:
    res = manager.set("/SmartDashboard/client_iarr", [4, 5, 6])
    assert res == {"ok": True, "warning": None}
    assert await manager.get("/SmartDashboard/client_iarr") == [4, 5, 6]


@pytest.mark.anyio
async def test_set_double_array_roundtrip(manager: NTManager) -> None:
    res = manager.set("/SmartDashboard/client_darr", [0.5, 1.5])
    assert res == {"ok": True, "warning": None}
    assert await manager.get("/SmartDashboard/client_darr") == [0.5, 1.5]


@pytest.mark.anyio
async def test_set_invalid_type_returns_false(manager: NTManager) -> None:
    """set() on unsupported types returns ok=False without raising."""
    assert manager.set("/SmartDashboard/client_bad", {"not": "a scalar"}) == {
        "ok": False,
        "warning": None,
    }
    assert manager.set("/SmartDashboard/client_mixed", [1, "two"]) == {
        "ok": False,
        "warning": None,
    }


@pytest.mark.anyio
async def test_set_type_warning_for_known_topic(manager: NTManager) -> None:
    """Writing the wrong type to a known topic produces a warning."""
    manager.set("/SmartDashboard/warn_num", 1.0)
    res = manager.set("/SmartDashboard/warn_num", "not a number")
    assert res["warning"] is not None
    assert "expected double" in res["warning"]


@pytest.mark.anyio
async def test_set_strict_type_check_refuses_mismatch(manager: NTManager) -> None:
    """strict_type_check=True refuses mismatched writes."""
    manager.set("/SmartDashboard/strict_num", 1.0)
    res = manager.set("/SmartDashboard/strict_num", "not a number", strict_type_check=True)
    assert res["ok"] is False
    assert res["warning"] is not None


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
async def test_subscribe_summary_format(manager: NTManager) -> None:
    """format='summary' reduces per-topic samples to min/max/mean/last."""
    summary = await manager.subscribe(["/SmartDashboard/"], duration=1.0, format="summary")
    assert "/SmartDashboard/test_num" in summary
    num_summary = summary["/SmartDashboard/test_num"]
    assert num_summary["count"] >= 1
    assert num_summary["last_value"] == 42.0
    assert "min" in num_summary
    assert "max" in num_summary
    assert "mean" in num_summary


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


@pytest.mark.anyio
async def test_subscribe_stream_all_topics(manager: NTManager) -> None:
    """subscribe_stream(["/"]) — the root prefix — yields every fixture topic.

    Validates that the default recording prefix captures all topics that
    list_topics() reports. Consumes the generator for 1.0s via
    ``asyncio.timeout`` (the generator only yields on value updates, so
    a bare ``time.monotonic`` check inside the ``async for`` would never
    fire between events).
    """
    topics_seen: set[str] = set()
    try:
        async with asyncio.timeout(1.0):
            async for topic, _, _ in manager.subscribe_stream(["/"]):
                topics_seen.add(topic)
    except TimeoutError:
        pass
    assert set(topics_seen) >= set(FIXTURE_NAMES)


def test_stream_decimator_change_only_drops_small_numeric_changes() -> None:
    """Given change_only=0.5, near-identical numerics drop; bools and
    non-numerics never drop via the numeric-diff path (bools are excluded
    from it and the equality fallback only applies to non-numeric pairs)."""
    dec = StreamDecimator(change_only=0.5)
    assert dec.keep("/t", 1.0, 0.0) is True
    assert dec.keep("/t", 1.2, 0.1) is False  # |1.2 - 1.0| <= 0.5
    assert dec.keep("/t", 2.0, 0.2) is True
    assert dec.keep("/t", True, 0.3) is True  # bool vs numeric: kept
    assert dec.keep("/t", True, 0.4) is True  # equal bool: kept (bool path)
    assert dec.keep("/t", "hi", 0.5) is True
    assert dec.keep("/t", "hi", 0.6) is False  # equal non-numeric dropped


def test_stream_decimator_change_only_zero_keeps_distinct_values() -> None:
    """Given change_only=0 and a strictly-changing sequence, nothing drops."""
    dec = StreamDecimator(change_only=0.0)
    assert dec.keep("/t", 1.0, 0.0) is True
    assert dec.keep("/t", 2.0, 0.1) is True
    assert dec.keep("/t", 3.0, 0.2) is True


def test_stream_decimator_sample_interval_one_per_topic_per_interval() -> None:
    """Given sample_interval=0.1, at most one sample per topic per 0.1s,
    tracked independently per topic."""
    dec = StreamDecimator(sample_interval=0.1)
    assert dec.keep("/t", 1.0, 0.0) is True
    assert dec.keep("/t", 2.0, 0.05) is False
    assert dec.keep("/t", 3.0, 0.2) is True
    assert dec.keep("/other", 9.0, 0.01) is True


@pytest.mark.anyio
async def test_subscribe_limit_caps_samples_per_topic(manager: NTManager) -> None:
    """subscribe(..., limit=1) returns at most one sample per topic."""
    samples = await manager.subscribe(["/SmartDashboard/"], duration=1.0, limit=1)
    for name in FIXTURE_NAMES:
        assert name in samples, f"no samples for {name}"
    for events in samples.values():
        assert len(events) <= 1


@pytest.mark.anyio
async def test_connect_different_port_raises_target_mismatch(
    manager: NTManager,
) -> None:
    """Given a running client, connect() with a different server_port raises
    TargetMismatchError naming both targets."""
    with pytest.raises(TargetMismatchError, match="5811"):
        manager.connect(server_ip="127.0.0.1", server_port=5811)
    assert manager._client_running is True
    assert manager.target == {
        "kind": "server",
        "server_ip": "127.0.0.1",
        "server_port": PORT,
    }


@pytest.mark.anyio
async def test_connection_info_carries_target_and_version(
    manager: NTManager,
) -> None:
    """connection_info() reports the resolved target and a version string."""
    info = manager.connection_info()
    assert info["target"] == {
        "kind": "server",
        "server_ip": "127.0.0.1",
        "server_port": PORT,
    }
    assert isinstance(info["version"], str)
    assert info["version"]
    assert info["connected"] is True
    assert "connections" in info


@pytest.mark.anyio
async def test_package_version_falls_back_when_metadata_absent(
    manager: NTManager, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Given package metadata is absent, _package_version() returns
    "unknown" instead of raising, and connection_info()["version"] stays a
    JSON-serializable non-empty string."""
    from importlib.metadata import PackageNotFoundError

    assert isinstance(_package_version(), str)
    assert _package_version()

    def raise_package_not_found(name: str) -> str:
        raise PackageNotFoundError(name)

    # Patch the module-level name nt_manager actually calls (it did
    # ``from importlib.metadata import ... version`` at import time).
    monkeypatch.setattr(
        "nt_mcp_server.nt_manager.version", raise_package_not_found
    )
    assert _package_version() == "unknown"
    info = manager.connection_info()
    assert isinstance(info["version"], str)
    assert info["version"]
    json.dumps(info)


# NOTE: this test must run LAST in this module. ``setServerTeam`` replaces
# the whole server list on the shared default NetworkTableInstance, which
# would break the module-scoped ``manager`` fixture's loopback connection
# for any test running after it.
@pytest.mark.anyio
async def test_connect_team_records_target_with_requested_port(
    fresh_manager: NTManager,
) -> None:
    """Given team_number with the default server_ip and a NON-default port,
    connect does not raise and records a team target carrying the requested
    port (proves the port is no longer dropped by setServerTeam)."""
    fresh_manager.connect(
        server_ip=DEFAULT_SERVER_IP, server_port=5910, team_number=8326
    )
    assert fresh_manager.target == {
        "kind": "team",
        "team_number": 8326,
        "server_port": 5910,
    }
