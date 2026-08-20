"""Tool-level tests for the FastMCP server (:mod:`nt_mcp_server`).

Each run spins up a real pyntcore NT4 server in-process
(``NetworkTableInstance.create()``) on port 5810 — the same loopback
recipe as ``tests/test_nt_manager.py`` — and drives the MCP tools as
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
from nt_mcp_server import nt_recorder

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
    assert status == {"connected": True, "status": "connected"}

    async def synced() -> bool:
        result = await nt_mcp_server.nt_list_topics()
        names = result["topics"]
        return all(name in names for name in FIXTURE_NAMES) and (
            (await nt_mcp_server.nt_get("/SmartDashboard/test_num"))["value"] is not None
        )

    assert await wait_until(synced, timeout=15.0), "tools never synced fixture topics"
    yield
    nt_mcp_server.nt_disconnect()


@pytest.mark.anyio
async def test_nt_connect_returns_connected(mcp_synced: None) -> None:
    """Given the synced fixture, nt_connect is idempotent and reports connected."""
    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status == {"connected": True, "status": "connected"}


@pytest.mark.anyio
async def test_nt_connection_info_is_json_serializable(mcp_synced: None) -> None:
    """Given a live connection, nt_connection_info() dumps as JSON."""
    info = nt_mcp_server.nt_connection_info()
    assert info["connected"] is True
    json.dumps(info)


@pytest.mark.anyio
async def test_nt_get_returns_bare_value(mcp_synced: None) -> None:
    """Given a published double, nt_get returns a wrapped value with connected flag."""
    response = await nt_mcp_server.nt_get("/SmartDashboard/test_num")
    assert response["connected"] is True
    assert response["value"] == 42.0
    assert type(response["value"]) is float


@pytest.mark.anyio
async def test_nt_get_info_returns_type_str(mcp_synced: None) -> None:
    """Given a published topic, nt_get_info returns metadata with type_str."""
    response = await nt_mcp_server.nt_get_info("/SmartDashboard/test_num")
    assert response["connected"] is True
    info = response["info"]
    assert info is not None
    assert info["name"] == "/SmartDashboard/test_num"
    assert info["type_str"] == "double"


@pytest.mark.anyio
async def test_nt_get_info_unknown_topic(mcp_synced: None) -> None:
    """Unknown topics return connected=True and info=None (no schema error)."""
    response = await nt_mcp_server.nt_get_info("/SmartDashboard/does_not_exist")
    assert response["connected"] is True
    assert response["info"] is None
    json.dumps(response)


@pytest.mark.anyio
async def test_nt_set_int_roundtrip_preserves_type(mcp_synced: None) -> None:
    """Given nt_set(7), nt_get returns the same int."""
    response = nt_mcp_server.nt_set("/SmartDashboard/mcp_int", 7)
    assert response == {"connected": True, "ok": True, "warning": None}
    value = (await nt_mcp_server.nt_get("/SmartDashboard/mcp_int"))["value"]
    assert value == 7
    assert type(value) is int


@pytest.mark.anyio
async def test_nt_set_float_roundtrip_preserves_type(mcp_synced: None) -> None:
    """Given nt_set(2.5), nt_get returns the same float."""
    response = nt_mcp_server.nt_set("/SmartDashboard/mcp_double", 2.5)
    assert response == {"connected": True, "ok": True, "warning": None}
    value = (await nt_mcp_server.nt_get("/SmartDashboard/mcp_double"))["value"]
    assert value == 2.5
    assert type(value) is float


@pytest.mark.anyio
async def test_nt_set_bool_roundtrip_preserves_type(mcp_synced: None) -> None:
    """Given nt_set(False), nt_get returns the same bool."""
    response = nt_mcp_server.nt_set("/SmartDashboard/mcp_bool", False)
    assert response == {"connected": True, "ok": True, "warning": None}
    value = (await nt_mcp_server.nt_get("/SmartDashboard/mcp_bool"))["value"]
    assert value is False
    assert type(value) is bool


@pytest.mark.anyio
async def test_nt_set_multiple_returns_dict_of_bools(mcp_synced: None) -> None:
    """Given two topics, nt_set_multiple returns per-topic bool results."""
    response = nt_mcp_server.nt_set_multiple(
        {"/SmartDashboard/mcp_multi_int": 11, "/SmartDashboard/mcp_multi_str": "abc"}
    )
    assert response["connected"] is True
    assert response["results"] == {
        "/SmartDashboard/mcp_multi_int": True,
        "/SmartDashboard/mcp_multi_str": True,
    }
    assert all(type(ok) is bool for ok in response["results"].values())
    assert response["warnings"] == {
        "/SmartDashboard/mcp_multi_int": None,
        "/SmartDashboard/mcp_multi_str": None,
    }


@pytest.mark.anyio
async def test_nt_get_multiple_returns_flat_dict(mcp_synced: None) -> None:
    """Given three topics, nt_get_multiple returns a wrapped {topic: value} dict."""
    response = await nt_mcp_server.nt_get_multiple(
        ["/SmartDashboard/test_num", "/SmartDashboard/test_str", "/SmartDashboard/nope"]
    )
    assert response["connected"] is True
    assert response["values"] == {
        "/SmartDashboard/test_num": 42.0,
        "/SmartDashboard/test_str": "hi",
        "/SmartDashboard/nope": None,
    }


@pytest.mark.anyio
async def test_nt_list_topics_prefix_filter(mcp_synced: None) -> None:
    """Given a prefix, nt_list_topics narrows to matching topics."""
    response = await nt_mcp_server.nt_list_topics("/SmartDashboard/test_")
    assert response["connected"] is True
    assert set(FIXTURE_NAMES) <= set(response["topics"])
    empty = await nt_mcp_server.nt_list_topics("/does/not/match/")
    assert empty == {"connected": True, "topics": []}


@pytest.mark.anyio
async def test_nt_list_topics_regex_filter(mcp_synced: None) -> None:
    """Regex filter works through the MCP wrapper."""
    response = await nt_mcp_server.nt_list_topics(regex=r"test_(num|str)$")
    assert response["connected"] is True
    assert "/SmartDashboard/test_num" in response["topics"]
    assert "/SmartDashboard/test_bool" not in response["topics"]


@pytest.mark.anyio
async def test_nt_subscribe_returns_json_samples(mcp_synced: None) -> None:
    """Given the fixture prefix, nt_subscribe samples every topic as JSON."""
    response = await nt_mcp_server.nt_subscribe(["/SmartDashboard/"], duration=1.0)
    assert response["connected"] is True
    samples = response["samples"]
    for name in FIXTURE_NAMES:
        assert name in samples, f"no samples for {name}"
        assert len(samples[name]) >= 1
    json.dumps(response)


@pytest.mark.anyio
async def test_nt_subscribe_summary_format(mcp_synced: None) -> None:
    """format='summary' returns min/max/mean/last per topic."""
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"], duration=1.0, format="summary"
    )
    assert response["connected"] is True
    summary = response["samples"]["/SmartDashboard/test_num"]
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary


@pytest.mark.anyio
async def test_nt_set_type_warning(mcp_synced: None) -> None:
    """Writing wrong type to a known topic produces a warning.

    NT itself refuses cross-type writes, so ``ok`` may be False even in
    warn mode; the important behavior is that a warning is surfaced.
    """
    # Use a fresh topic so the mismatch does not corrupt fixture topics.
    nt_mcp_server.nt_set("/SmartDashboard/mcp_warn_num", 1.0)
    response = nt_mcp_server.nt_set("/SmartDashboard/mcp_warn_num", "not a number")
    assert response["connected"] is True
    assert response["warning"] is not None
    assert "expected double" in response["warning"]


@pytest.mark.anyio
async def test_nt_set_strict_type_check(mcp_synced: None) -> None:
    """strict_type_check=True refuses mismatched writes."""
    nt_mcp_server.nt_set("/SmartDashboard/mcp_strict_num", 1.0)
    response = nt_mcp_server.nt_set(
        "/SmartDashboard/mcp_strict_num", "not a number", strict_type_check=True
    )
    assert response["connected"] is True
    assert response["ok"] is False
    assert response["warning"] is not None


@pytest.mark.anyio
async def test_nt_disconnect_returns_status_and_reconnects(mcp_synced: None) -> None:
    """Given a live client, nt_disconnect stops it; nt_connect restores it."""
    assert nt_mcp_server.nt_disconnect() == {"connected": False, "status": "disconnected"}

    async def disconnected() -> bool:
        return nt_mcp_server.nt_connection_info()["connected"] is False

    assert await wait_until(
        disconnected, timeout=2.0
    ), "client still connected after nt_disconnect"

    # After disconnect, read tools report the disconnected state clearly.
    response = await nt_mcp_server.nt_get_info("/SmartDashboard/test_num")
    assert response["connected"] is False
    assert "error" in response

    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status == {"connected": True, "status": "connected"}


@pytest.mark.anyio
async def test_nt_persistent_subscription(mcp_synced: None) -> None:
    """Persistent subscriptions can be started, polled, and stopped."""
    start = await nt_mcp_server.nt_start_subscription(["/SmartDashboard/"])
    assert start["connected"] is True
    assert start["started"] is True
    sub_id = start["subscription_id"]

    # Give the background task a moment to collect the immediate values.
    await asyncio.sleep(0.5)

    poll = nt_mcp_server.nt_poll_subscription(sub_id)
    assert poll["connected"] is True
    assert "/SmartDashboard/test_num" in poll["samples"]

    stop = await nt_mcp_server.nt_stop_subscription(sub_id)
    assert stop["connected"] is True
    assert stop["stopped"] is True

    missing = nt_mcp_server.nt_poll_subscription(sub_id)
    assert "error" in missing


# --- Offline MCP tool tests -------------------------------------------------


@pytest.mark.anyio
async def test_nt_list_recordings_returns_metadata(
    mcp_synced: None, tmp_path
) -> None:
    """Given a recording, nt_list_recordings returns metadata entries."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_list_recordings(output_dir=str(tmp_path))
    assert isinstance(result, list)
    assert len(result) >= 1
    for entry in result:
        assert {"id", "path", "size_bytes", "modified", "modified_iso"} <= set(entry)
    json.dumps(result)


@pytest.mark.anyio
async def test_nt_get_history_returns_events(mcp_synced: None, tmp_path) -> None:
    """Given a recording, nt_get_history returns {time, value} events."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path)
    )
    assert isinstance(result, list)
    assert len(result) >= 1
    for entry in result:
        assert sorted(entry) == ["time", "value"]
        assert isinstance(entry["time"], float)
    assert any(entry["value"] == 42.0 for entry in result)
    json.dumps(result)


@pytest.mark.anyio
async def test_nt_get_history_summary(mcp_synced: None, tmp_path) -> None:
    """Given a recording, nt_get_history with format=summary returns stats."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path), format="summary"
    )
    assert isinstance(result, dict)
    assert result["count"] >= 1
    assert result["last_value"] == 42.0
    assert "min" in result


@pytest.mark.anyio
async def test_nt_get_recording_info(mcp_synced: None, tmp_path) -> None:
    """Given a recording, nt_get_recording_info returns metadata."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    info = nt_mcp_server.nt_get_recording_info(output_dir=str(tmp_path))
    assert "error" not in info
    assert info["total_samples"] >= len(FIXTURE_NAMES)
    assert info["topic_count"] >= len(FIXTURE_NAMES)
    assert info["duration_seconds"] >= 0.0
    assert "modified_iso" in info


@pytest.mark.anyio
async def test_nt_list_topics_offline_returns_topics(
    mcp_synced: None, tmp_path
) -> None:
    """Given a recording, nt_list_topics_offline returns topics; prefix narrows."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    all_topics = nt_mcp_server.nt_list_topics_offline(output_dir=str(tmp_path))
    assert set(FIXTURE_NAMES) <= set(all_topics)
    prefixed = nt_mcp_server.nt_list_topics_offline(
        output_dir=str(tmp_path), prefix="/SmartDashboard/"
    )
    assert all(topic.startswith("/SmartDashboard/") for topic in prefixed)
    assert set(FIXTURE_NAMES) <= set(prefixed)
    json.dumps(prefixed)


@pytest.mark.anyio
async def test_nt_subscribe_offline_returns_dict(
    mcp_synced: None, tmp_path
) -> None:
    """Given a recording, nt_subscribe_offline buckets events by topic."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_subscribe_offline(
        ["/SmartDashboard/"], output_dir=str(tmp_path)
    )
    assert isinstance(result, dict)
    for name in FIXTURE_NAMES:
        assert name in result, f"missing topic {name}"
        for entry in result[name]:
            assert sorted(entry) == ["time", "value"]
            assert isinstance(entry["time"], float)
    assert (
        nt_mcp_server.nt_subscribe_offline(
            ["/does/not/exist/"], output_dir=str(tmp_path)
        )
        == {}
    )
    json.dumps(result)


@pytest.mark.anyio
async def test_nt_subscribe_offline_summary(
    mcp_synced: None, tmp_path
) -> None:
    """subscribe_offline with format=summary returns per-topic summaries."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_subscribe_offline(
        ["/SmartDashboard/"], output_dir=str(tmp_path), format="summary"
    )
    assert isinstance(result, dict)
    summary = result["/SmartDashboard/test_num"]
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary


def test_nt_get_history_missing_recording_returns_empty() -> None:
    """Given a missing recording_id, nt_get_history returns [] (never raises)."""
    result = nt_mcp_server.nt_get_history(
        "/x", recording_id="nonexistent.ndjson", output_dir="."
    )
    assert result == []
    json.dumps(result)
