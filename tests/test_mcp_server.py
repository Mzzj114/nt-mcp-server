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
import contextlib
import json
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

import ntcore
import pytest

import nt_mcp_server
from nt_mcp_server import nt_recorder
from nt_mcp_server.nt_types import enforce_max_chars

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
    assert status == {
        "connected": True,
        "status": "connected",
        "target": {"kind": "server", "server_ip": "127.0.0.1", "server_port": PORT},
    }

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
async def test_nt_connect_rejects_ambiguous_target() -> None:
    """Given both team_number and a non-default server_ip, connect is refused.

    Runs before any mcp_synced test so the shared manager has never
    connected: the refusal must leave no client running and no target
    recorded.
    """
    response = await nt_mcp_server.nt_connect(
        server_ip="10.0.0.5", team_number=359, timeout_seconds=0.2
    )
    assert response["connected"] is False
    assert response["status"] == "not connected"
    assert response["error"]

    info = nt_mcp_server.nt_connection_info()
    assert info["connected"] is False
    assert info["target"] is None


@pytest.mark.anyio
async def test_nt_connect_returns_connected(mcp_synced: None) -> None:
    """Given the synced fixture, nt_connect is idempotent and reports connected."""
    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status == {
        "connected": True,
        "status": "connected",
        "target": {"kind": "server", "server_ip": "127.0.0.1", "server_port": PORT},
    }


@pytest.mark.anyio
async def test_nt_connect_refuses_retarget_when_running(mcp_synced: None) -> None:
    """Given a running client, re-targeting to another port is refused.

    The structured error is returned and the original target stays
    intact (the client keeps running against port 5810).
    """
    response = await nt_mcp_server.nt_connect(
        server_ip="127.0.0.1", server_port=5811, timeout_seconds=0.2
    )
    assert response["connected"] is False
    assert response["status"] == "not connected"
    assert response["error"]

    info = nt_mcp_server.nt_connection_info()
    assert info["connected"] is True
    assert info["target"] == {
        "kind": "server",
        "server_ip": "127.0.0.1",
        "server_port": PORT,
    }

    # The refusal must not have disturbed the live connection: a REAL
    # read of a fixture topic still returns its value (not just
    # bookkeeping — the actual NT4 traffic survived).
    read = await nt_mcp_server.nt_get("/SmartDashboard/test_num")
    assert read["connected"] is True
    assert read["value"] == 42.0


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
async def test_nt_subscribe_default_writes_file_receipt(
    mcp_synced: None, tmp_path
) -> None:
    """Default output='file' writes an NDJSON recording and a receipt.

    The receipt carries recording metadata only — no sample values —
    and the whole response stays well under the budget ceiling.
    """
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"], duration=1.0, output_dir=str(tmp_path)
    )
    assert response["connected"] is True
    assert response["output"] == "file"
    assert response["recording_id"].startswith("nt-record-")
    assert response["recording_id"].endswith(".ndjson")
    assert response["path"] == str(tmp_path / response["recording_id"])
    assert response["rows"] >= len(FIXTURE_NAMES)
    assert response["topic_count"] >= len(FIXTURE_NAMES)
    for name in FIXTURE_NAMES:
        assert name in response["topics"], f"missing topic {name}"
    assert response["topics_truncated"] is False
    assert response["truncated"] is False
    assert "samples" not in response, "receipt must not contain sample values"
    assert len(json.dumps(response)) < 20_000


@pytest.mark.anyio
async def test_nt_subscribe_summary_mode_reports_rows(mcp_synced: None) -> None:
    """output='summary' returns per-topic stats plus rows/truncated."""
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"], duration=1.0, output="summary"
    )
    assert response["connected"] is True
    assert response["truncated"] is False
    assert response["rows"] >= 1
    assert response["rows"] == sum(
        summary["count"] for summary in response["samples"].values()
    )
    summary = response["samples"]["/SmartDashboard/test_num"]
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary
    assert len(json.dumps(response)) <= 60_000


@pytest.mark.anyio
async def test_nt_subscribe_samples_mode_enforces_budget(mcp_synced: None) -> None:
    """output='samples' on a churned prefix hits max_rows and reports it."""
    topic = "/SmartDashboard/sub_churn_num"

    async def churn() -> None:
        for i in range(20):
            nt_mcp_server.nt_set(topic, float(i))
            await asyncio.sleep(0.05)

    churn_task = asyncio.create_task(churn())
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"],
        duration=1.0,
        output="samples",
        limit=30,
        max_rows=10,
    )
    await churn_task
    assert response["connected"] is True
    assert response["truncated"] is True
    assert response["rows"] <= 10
    assert sum(len(v) for v in response["samples"].values()) == response["rows"]
    assert all(len(v) <= 30 for v in response["samples"].values())
    assert len(json.dumps(response)) <= 60_000


@pytest.mark.anyio
async def test_nt_subscribe_inline_rejects_bare_root_prefix(mcp_synced: None) -> None:
    """A bare '/' prefix is refused on inline modes with an actionable error."""
    for output in ("samples", "summary"):
        response = await nt_mcp_server.nt_subscribe(
            ["/"], output=output, duration=0.5
        )
        assert response["connected"] is True
        assert "error" in response
        assert "prefix '/'" in response["error"]
        assert "output='file'" in response["error"]


@pytest.mark.anyio
async def test_nt_subscribe_file_mode_allows_bare_root_prefix(
    mcp_synced: None, tmp_path
) -> None:
    """output='file' accepts '/': spilling to disk is the sanctioned escape."""
    response = await nt_mcp_server.nt_subscribe(
        ["/"], duration=1.0, output_dir=str(tmp_path)
    )
    assert response["connected"] is True
    assert response["output"] == "file"
    assert "error" not in response
    assert response["rows"] >= 1


@pytest.mark.anyio
async def test_nt_subscribe_unknown_output_rejected(mcp_synced: None) -> None:
    """An unknown output mode returns a structured error listing modes."""
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"], duration=0.5, output="bogus"
    )
    assert response["connected"] is True
    assert "error" in response
    assert "bogus" in response["error"]
    for mode in ("file", "summary", "samples"):
        assert mode in response["error"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("selectors", "expected"),
    [
        (
            {"topics": ["/SmartDashboard/test_num"]},
            {"/SmartDashboard/test_num"},
        ),
        (
            {"regex": r"test_(num|str)$"},
            {"/SmartDashboard/test_num", "/SmartDashboard/test_str"},
        ),
        (
            {"wildcard": "/SmartDashboard/test_arr"},
            {"/SmartDashboard/test_arr"},
        ),
    ],
)
async def test_nt_subscribe_rich_topic_selectors(
    mcp_synced: None, selectors: dict, expected: set[str]
) -> None:
    """Live subscriptions accept exact topics, regex, and wildcard filters."""
    response = await nt_mcp_server.nt_subscribe(
        duration=0.5,
        output="summary",
        **selectors,
    )
    assert response["connected"] is True
    assert "error" not in response
    assert set(response["samples"]) == expected


@pytest.mark.anyio
async def test_nt_subscribe_selector_validation(mcp_synced: None) -> None:
    """Missing and malformed selectors return compact structured errors."""
    missing = await nt_mcp_server.nt_subscribe(
        duration=0.1, output="summary"
    )
    assert "at least one topic selector" in missing["error"]

    invalid = await nt_mcp_server.nt_subscribe(
        regex="[", duration=0.1, output="summary"
    )
    assert "invalid topic regex" in invalid["error"]

    narrowed_root = await nt_mcp_server.nt_subscribe(
        prefixes=["/"],
        regex=r"/SmartDashboard/test_num$",
        duration=0.5,
        output="summary",
    )
    assert set(narrowed_root["samples"]) == {"/SmartDashboard/test_num"}


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
    assert status == {
        "connected": True,
        "status": "connected",
        "target": {"kind": "server", "server_ip": "127.0.0.1", "server_port": PORT},
    }


@pytest.mark.anyio
async def test_nt_disconnect_then_reconnect_to_different_target(
    mcp_synced: None,
) -> None:
    """After nt_disconnect, connecting to a DIFFERENT target succeeds.

    Regression guard for the old sticky no-op: ``connect()`` used to
    return early while the client was running, so a disconnect was the
    only way to re-target. Proves the new target is recorded and the
    live connection actually serves reads, then restores the module
    fixture's target for the tests that follow.
    """
    assert nt_mcp_server.nt_disconnect() == {
        "connected": False,
        "status": "disconnected",
    }

    second_server = ntcore.NetworkTableInstance.create()
    second_server.startServer(
        persist_filename="networktables.json",
        listen_address="",
        port3=1735,
        port4=5812,
    )
    second_server.getEntry("/SmartDashboard/second_num").setDouble(7.0)
    try:
        status = await nt_mcp_server.nt_connect(
            server_ip="127.0.0.1", server_port=5812, timeout_seconds=5.0
        )
        assert status == {
            "connected": True,
            "status": "connected",
            "target": {
                "kind": "server",
                "server_ip": "127.0.0.1",
                "server_port": 5812,
            },
        }

        async def synced() -> bool:
            response = await nt_mcp_server.nt_get("/SmartDashboard/second_num")
            return response["connected"] is True and response["value"] is not None

        assert await wait_until(
            synced, timeout=10.0
        ), "reconnected client never synced the second server's topic"
    finally:
        nt_mcp_server.nt_disconnect()
        second_server.stopServer()
        ntcore.NetworkTableInstance.destroy(second_server)

    # Restore the module fixture's target for the remaining tests.
    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status["connected"] is True
    assert status["target"] == {
        "kind": "server",
        "server_ip": "127.0.0.1",
        "server_port": PORT,
    }


@pytest.mark.anyio
async def test_nt_connect_timeout_reports_diagnostics(mcp_synced: None) -> None:
    """Given a port with no NT4 server, connect times out with diagnostics.

    The timeout branch reports ``hint``, ``connections`` and a float
    ``elapsed_seconds``; the client is then disconnected and restored
    to the fixture target for the tests that follow.
    """
    assert nt_mcp_server.nt_disconnect() == {
        "connected": False,
        "status": "disconnected",
    }
    response = await nt_mcp_server.nt_connect(
        server_ip="127.0.0.1", server_port=5811, timeout_seconds=0.2
    )
    assert response["connected"] is False
    assert response["status"] == "not connected"
    assert response["target"] == {
        "kind": "server",
        "server_ip": "127.0.0.1",
        "server_port": 5811,
    }
    assert isinstance(response["connections"], list)
    assert isinstance(response["elapsed_seconds"], float)
    assert "hint" in response

    # The timed-out client is still running against 5811; disconnect
    # before restoring the fixture target.
    nt_mcp_server.nt_disconnect()
    status = await nt_mcp_server.nt_connect(server_ip="127.0.0.1", server_port=PORT)
    assert status["connected"] is True


@pytest.mark.anyio
async def test_nt_persistent_subscription(mcp_synced: None) -> None:
    """Persistent subscriptions can be started, polled, and stopped."""
    start = await nt_mcp_server.nt_start_subscription(
        topics=["/SmartDashboard/test_num"]
    )
    assert start["connected"] is True
    assert start["started"] is True
    sub_id = start["subscription_id"]

    # Give the background task a moment to collect the immediate values.
    await asyncio.sleep(0.5)

    poll = nt_mcp_server.nt_poll_subscription(sub_id)
    assert poll["connected"] is True
    assert set(poll["samples"]) == {"/SmartDashboard/test_num"}

    stop = await nt_mcp_server.nt_stop_subscription(sub_id)
    assert stop["connected"] is True
    assert stop["stopped"] is True

    missing = nt_mcp_server.nt_poll_subscription(sub_id)
    assert "error" in missing


@pytest.mark.anyio
async def test_nt_nonblocking_recording_lifecycle(
    mcp_synced: None, tmp_path
) -> None:
    """A recording can span user turns and returns a receipt when stopped."""
    start = await nt_mcp_server.nt_start_recording(
        topics=["/SmartDashboard/test_num"], output_dir=str(tmp_path)
    )
    assert start["connected"] is True
    assert start["started"] is True
    recording_id = start["recording_id"]
    assert start["path"] == str(tmp_path / recording_id)

    await asyncio.sleep(0.5)

    stop = await nt_mcp_server.nt_stop_recording(recording_id)
    assert stop["connected"] is True
    assert stop["stopped"] is True
    assert stop["recording_id"] == recording_id
    assert stop["output"] == "file"
    assert stop["rows"] >= 1
    assert stop["topic_count"] == 1
    assert stop["topics"] == ["/SmartDashboard/test_num"]
    assert stop["duration_seconds"] >= 0.0
    assert stop["duration_seconds"] == stop["capture_duration_seconds"]
    assert stop["capture_started_at"] <= stop["capture_ended_at"]
    assert stop["event_span_seconds"] >= 0.0
    assert "samples" not in stop
    assert os.path.isfile(stop["path"])

    missing = await nt_mcp_server.nt_stop_recording(recording_id)
    assert missing["stopped"] is False
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
    """Given a recording, nt_get_history returns bounded samples with meta."""
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
    assert isinstance(result, dict)
    assert result["topic"] == "/SmartDashboard/test_num"
    assert result["rows"] >= 1
    assert result["total_rows"] >= result["rows"]
    assert result["truncated"] is False
    samples = result["samples"]
    assert isinstance(samples, list)
    assert len(samples) == result["rows"]
    for entry in samples:
        assert sorted(entry) == ["time", "value"]
        assert isinstance(entry["time"], float)
    assert any(entry["value"] == 42.0 for entry in samples)
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
    assert result["truncated"] is False
    summary = result["summary"]
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary


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
    assert info["duration_seconds"] == info["capture_duration_seconds"]
    assert info["duration_source"] == "capture"
    assert info["event_span_seconds"] >= 0.0
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
    assert result["recording_id"] is None  # defaults to most recent
    assert result["rows"] >= 1
    assert result["total_rows"] >= result["rows"]
    assert result["truncated"] is False
    topics = result["topics"]
    assert sum(len(v) for v in topics.values()) == result["rows"]
    for name in FIXTURE_NAMES:
        assert name in topics, f"missing topic {name}"
        for entry in topics[name]:
            assert sorted(entry) == ["time", "value"]
            assert isinstance(entry["time"], float)
    empty = nt_mcp_server.nt_subscribe_offline(
        ["/does/not/exist/"], output_dir=str(tmp_path)
    )
    assert empty["rows"] == 0
    assert empty["total_rows"] == 0
    assert empty["truncated"] is False
    assert empty["topics"] == {}
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
    assert result["truncated"] is False
    summary = result["topics"]["/SmartDashboard/test_num"]
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary


@pytest.mark.anyio
async def test_nt_subscribe_offline_rich_topic_selectors(
    mcp_synced: None, tmp_path
) -> None:
    """Offline reads accept exact topics, regex, and wildcard filters."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()

    exact = nt_mcp_server.nt_subscribe_offline(
        topics=["/SmartDashboard/test_num"], output_dir=str(tmp_path)
    )
    assert set(exact["topics"]) == {"/SmartDashboard/test_num"}

    regex = nt_mcp_server.nt_subscribe_offline(
        regex=r"test_(num|str)$", output_dir=str(tmp_path)
    )
    assert set(regex["topics"]) == {
        "/SmartDashboard/test_num",
        "/SmartDashboard/test_str",
    }

    wildcard = nt_mcp_server.nt_subscribe_offline(
        wildcard="/SmartDashboard/test_arr", output_dir=str(tmp_path)
    )
    assert set(wildcard["topics"]) == {"/SmartDashboard/test_arr"}


def test_nt_get_history_missing_recording_returns_empty() -> None:
    """Given a missing recording_id, nt_get_history returns empty samples."""
    result = nt_mcp_server.nt_get_history(
        "/x", recording_id="nonexistent.ndjson", output_dir="."
    )
    assert result["rows"] == 0
    assert result["total_rows"] == 0
    assert result["truncated"] is False
    assert result["samples"] == []
    json.dumps(result)


@pytest.mark.anyio
async def test_nt_subscribe_offline_rejects_bare_root_prefix(
    mcp_synced: None, tmp_path
) -> None:
    """A bare '/' prefix is refused with an actionable structured error."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_subscribe_offline(["/"], output_dir=str(tmp_path))
    assert "error" in result
    assert "/" in result["error"]
    assert "nt-recorder" in result["error"]
    assert result["rows"] == 0
    assert result["truncated"] is False
    json.dumps(result)


@pytest.mark.anyio
async def test_nt_get_history_reports_truncation(
    mcp_synced: None, tmp_path
) -> None:
    """limit below the match count sets truncated with a true total_rows."""
    topic = "/SmartDashboard/trunc_num"

    async def churn() -> None:
        for i in range(10):
            nt_mcp_server.nt_set(topic, float(i))
            await asyncio.sleep(0.05)

    churn_task = asyncio.create_task(churn())
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    await churn_task
    result = nt_mcp_server.nt_get_history(
        topic, output_dir=str(tmp_path), limit=3
    )
    assert result["rows"] == 3
    assert result["total_rows"] > 3
    assert result["truncated"] is True
    assert len(result["samples"]) == 3


@pytest.mark.anyio
async def test_nt_subscribe_offline_max_rows_enforces_budget(
    mcp_synced: None, tmp_path
) -> None:
    """max_rows clips the total across topics and flags truncated."""
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    result = nt_mcp_server.nt_subscribe_offline(
        ["/SmartDashboard/"], output_dir=str(tmp_path), max_rows=2
    )
    assert result["rows"] <= 2
    assert result["total_rows"] > result["rows"]
    assert result["truncated"] is True
    assert sum(len(v) for v in result["topics"].values()) == result["rows"]
    assert len(json.dumps(result)) < 60_000


# --- Character-ceiling guard (nt_types.enforce_max_chars) --------------------


def test_enforce_max_chars_trims_rows_and_recomputes_meta() -> None:
    """Rows that overflow the ceiling are dropped; truncated forced True."""
    big_row = {"time": 1.0, "value": "x" * 100}
    response = {
        "rows": 4,
        "total_rows": 4,
        "truncated": False,
        "topics": {
            "/a": [dict(big_row), dict(big_row)],
            "/b": [dict(big_row), {"time": 2.0, "value": 1}],
        },
    }
    result = enforce_max_chars(response, "topics", 400)
    assert len(json.dumps(result)) <= 400
    assert result["truncated"] is True
    assert result["rows"] == sum(len(v) for v in result["topics"].values())
    assert result["rows"] < 4
    assert result["total_rows"] == 4
    json.dumps(result)


def test_enforce_max_chars_visits_topics_in_sorted_order() -> None:
    """With a tight budget the alphabetically-first topic keeps its rows."""
    big_row = {"time": 1.0, "value": "y" * 100}
    response = {
        "rows": 2,
        "total_rows": 2,
        "truncated": False,
        "topics": {"/z": [dict(big_row)], "/a": [dict(big_row)]},
    }
    result = enforce_max_chars(response, "topics", 300)
    assert len(result["topics"]["/a"]) == 1
    assert result["topics"]["/z"] == []
    assert result["rows"] == 1
    assert result["truncated"] is True
    assert len(json.dumps(result)) <= 300


def test_enforce_max_chars_skips_oversized_row_keeps_later_smaller() -> None:
    """One huge row does not starve the rest: it is skipped, scan continues."""
    huge = {"time": 2.0, "value": "h" * 500}
    small = {"time": 1.0, "value": 1}
    response = {
        "rows": 2,
        "total_rows": 2,
        "truncated": False,
        "samples": [huge, small],
    }
    result = enforce_max_chars(response, "samples", 300)
    assert result["samples"] == [small]
    assert result["rows"] == 1
    assert result["truncated"] is True
    assert result["total_rows"] == 2
    assert len(json.dumps(result)) <= 300


def test_enforce_max_chars_small_response_untouched() -> None:
    """Converse: a response under the ceiling passes through unmodified."""
    response = {
        "rows": 1,
        "total_rows": 1,
        "truncated": False,
        "topics": {"/a": [{"time": 1.0, "value": 1}]},
    }
    result = enforce_max_chars(response, "topics", 60_000)
    assert result == response
    assert result["truncated"] is False
    assert response["rows"] == 1  # input envelope not mutated


def test_enforce_max_chars_leaves_summary_containers_alone() -> None:
    """A container with no row lists (summaries) is returned unchanged."""
    response = {
        "rows": 3,
        "total_rows": 3,
        "truncated": False,
        "topics": {"/a": {"count": 3, "min": 0.0}},
    }
    assert enforce_max_chars(response, "topics", 10) == response


def test_enforce_max_chars_forces_truncated_without_rows_key() -> None:
    """Envelopes without a rows field still get a truthful truncated flag."""
    response = {"samples": [{"time": 2.0, "value": "h" * 500}, {"time": 1.0, "value": 1}]}
    result = enforce_max_chars(response, "samples", 200)
    assert "rows" not in result
    assert result["truncated"] is True
    assert len(json.dumps(result)) <= 200


def test_nt_subscribe_offline_char_ceiling_worst_case(tmp_path) -> None:
    """Worst case: 5000 dense rows would far exceed 60,000 chars.

    10 topics x 700 events = 7000 matches; the max_rows=5000 row budget
    clips first, and the char guard must then drop further rows so the
    whole envelope serializes under the ceiling.
    """
    recording = tmp_path / "dense.ndjson"
    with open(recording, "w", encoding="utf-8") as file:
        for topic_i in range(10):
            topic = f"/SmartDashboard/dense_{topic_i}"
            for i in range(700):
                file.write(
                    json.dumps(
                        {"time": 1000.0 + i, "topic": topic, "value": 1234.5678}
                    )
                    + "\n"
                )
    result = nt_mcp_server.nt_subscribe_offline(
        ["/SmartDashboard/"], recording_id="dense.ndjson", output_dir=str(tmp_path)
    )
    dumped = json.dumps(result)
    row_count = sum(len(v) for v in result["topics"].values())
    assert len(dumped) <= 60_000
    assert result["truncated"] is True
    assert result["rows"] == row_count
    assert row_count < 5000  # the char guard dropped rows the row budget kept
    assert result["total_rows"] == 7000


def test_nt_get_history_char_ceiling_worst_case(tmp_path) -> None:
    """nt_get_history holds the same ceiling on a dense single-topic file."""
    recording = tmp_path / "single_dense.ndjson"
    with open(recording, "w", encoding="utf-8") as file:
        for i in range(6000):
            file.write(
                json.dumps(
                    {
                        "time": 1000.0 + i,
                        "topic": "/SmartDashboard/dense",
                        "value": 1234.5678,
                    }
                )
                + "\n"
            )
    result = nt_mcp_server.nt_get_history(
        "/SmartDashboard/dense",
        recording_id="single_dense.ndjson",
        output_dir=str(tmp_path),
    )
    dumped = json.dumps(result)
    assert len(dumped) <= 60_000
    assert result["truncated"] is True
    assert result["rows"] == len(result["samples"])
    assert result["rows"] < 5000  # char guard dropped rows the limit kept
    assert result["total_rows"] == 6000


# --- Output-budget regression tests (plan checkbox 10) -----------------------
#
# These lock the budget invariant end to end through the MCP tools so the
# unbounded-samples defect class cannot return. The authoritative worst case
# (f) publishes its own churning topic so the assertion does not depend on
# how many topics the module fixture happens to publish.


RECEIPT_KEYS = {
    "connected",
    "output",
    "recording_id",
    "path",
    "duration_seconds",
    "capture_started_at",
    "capture_started_iso",
    "capture_ended_at",
    "capture_ended_iso",
    "capture_duration_seconds",
    "event_span_seconds",
    "rows",
    "topic_count",
    "topics",
    "topics_truncated",
    "truncated",
}


@pytest.mark.anyio
async def test_budget_default_subscribe_receipt_is_bounded(
    mcp_synced: None, tmp_path
) -> None:
    """(a) A broad default nt_subscribe returns a small value-free receipt.

    Given the synced fixture, When nt_subscribe runs with default
    arguments on the whole /SmartDashboard/ prefix, Then the response is
    exactly the receipt contract (no "samples" key, so no sample values
    can leak) and serializes far under the 20,000-char receipt budget.
    """
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"], duration=1.0, output_dir=str(tmp_path)
    )
    assert response["connected"] is True
    assert set(response) == RECEIPT_KEYS
    assert "samples" not in response
    receipt = json.dumps(response)
    assert len(receipt) < 20_000, f"receipt serialized to {len(receipt)} chars"


@pytest.mark.anyio
async def test_budget_samples_mode_max_rows_caps_total_rows(
    mcp_synced: None,
) -> None:
    """(b) output='samples' with max_rows=10 returns at most 10 total rows.

    Given a churning topic guaranteeing more than 10 events on the
    prefix, When nt_subscribe collects samples with max_rows=10, Then
    truncated is True and the summed per-topic rows are exactly the
    reported rows, capped at 10.
    """
    topic = "/SmartDashboard/budget_churn_num"

    async def churn() -> None:
        for i in range(20):
            nt_mcp_server.nt_set(topic, float(i))
            await asyncio.sleep(0.05)

    churn_task = asyncio.create_task(churn())
    response = await nt_mcp_server.nt_subscribe(
        ["/SmartDashboard/"],
        duration=1.0,
        output="samples",
        max_rows=10,
    )
    await churn_task
    assert response["connected"] is True
    assert response["truncated"] is True
    total_rows = sum(len(v) for v in response["samples"].values())
    assert total_rows <= 10
    assert response["rows"] == total_rows


@pytest.mark.anyio
async def test_budget_bare_root_summary_returns_structured_error(
    mcp_synced: None,
) -> None:
    """(c) nt_subscribe(prefixes=['/'], output='summary') refuses inline.

    Given the synced fixture, When a bare '/' prefix is requested with an
    inline mode, Then the response is a structured error naming the
    prefix and the sanctioned output='file' escape — never samples.
    """
    response = await nt_mcp_server.nt_subscribe(
        ["/"], output="summary", duration=0.5
    )
    assert response["connected"] is True
    assert "error" in response
    assert "prefix '/'" in response["error"]
    assert "output='file'" in response["error"]
    assert "samples" not in response


@pytest.mark.anyio
async def test_budget_subscribe_offline_max_rows_truncates(
    mcp_synced: None, tmp_path
) -> None:
    """(d) nt_subscribe_offline(max_rows=10) sets truncated on a dense read.

    Given a recording with more matching events than the budget, When the
    offline tool reads with max_rows=10, Then truncated is True, the
    summed per-topic rows equal the reported rows, and the envelope stays
    under the 60,000-char ceiling.
    """
    topic = "/SmartDashboard/budget_offline_num"

    async def churn() -> None:
        for i in range(20):
            nt_mcp_server.nt_set(topic, float(i))
            await asyncio.sleep(0.05)

    churn_task = asyncio.create_task(churn())
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    await recorder.run()
    await churn_task
    assert recorder.rows > 10, "fixture must produce more than 10 events"

    result = nt_mcp_server.nt_subscribe_offline(
        ["/SmartDashboard/"], output_dir=str(tmp_path), max_rows=10
    )
    assert result["truncated"] is True
    total_rows = sum(len(v) for v in result["topics"].values())
    assert total_rows <= 10
    assert result["rows"] == total_rows
    assert result["total_rows"] == recorder.rows
    assert len(json.dumps(result)) <= 60_000


@pytest.mark.anyio
async def test_budget_get_history_limit_reports_true_total(
    mcp_synced: None, tmp_path
) -> None:
    """(e) nt_get_history(limit=2) reports truncated truthfully.

    Given a fixture recording whose topic holds exactly ``recorded``
    events, When nt_get_history reads with limit=2, Then rows == 2,
    truncated is True iff the true count exceeds 2, and total_rows equals
    the real number of events in the recording.
    """
    topic = "/SmartDashboard/budget_hist_num"
    recorded = 5

    async def churn() -> None:
        for i in range(recorded):
            nt_mcp_server.nt_set(topic, float(i))
            await asyncio.sleep(0.05)

    churn_task = asyncio.create_task(churn())
    recorder = nt_recorder.Recorder(
        nt_mcp_server.manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
    )
    recording_path = await recorder.run()
    await churn_task
    assert recorder.rows >= recorded, "fixture must record at least 5 events"

    # The true count for THIS topic, straight from the NDJSON file.
    with open(recording_path, encoding="utf-8") as file:
        topic_events = sum(
            1 for line in file if json.loads(line)["topic"] == topic
        )
    assert topic_events >= recorded

    result = nt_mcp_server.nt_get_history(
        topic, output_dir=str(tmp_path), limit=2
    )
    assert result["rows"] == 2
    assert result["total_rows"] == topic_events
    assert result["truncated"] is (topic_events > 2)
    assert len(result["samples"]) == 2


@pytest.mark.anyio
async def test_budget_worst_case_broad_prefix_stays_under_60k_chars(
    mcp_synced: None,
) -> None:
    """(f) Authoritative worst case: a long broad-prefix window stays <60k.

    Given a churning topic flooding the /SmartDashboard/ prefix with
    dense rows for the whole window (the churn, not the fixture's static
    topics, creates the load), When nt_subscribe samples with the default
    max_rows=5000 row budget, Then the char guard holds the serialized
    response under the 60,000-character ceiling with truncated=True.
    """
    topic = "/SmartDashboard/budget_flood_num"

    async def flood() -> None:
        i = 0
        while True:
            # 200-element double arrays: each row serializes to ~1.3 KB, so
            # ~45 rows fill the 60,000-char ceiling. A 0.01s flood reliably
            # produces far more rows than that inside the 5s window.
            nt_mcp_server.nt_set(topic, [float(i + j) for j in range(200)])
            i += 1
            await asyncio.sleep(0.01)

    churn_task = asyncio.create_task(flood())
    try:
        response = await nt_mcp_server.nt_subscribe(
            ["/SmartDashboard/"],
            duration=5.0,
            output="samples",
        )
    finally:
        churn_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await churn_task

    dumped = json.dumps(response)
    assert response["connected"] is True
    assert response["truncated"] is True
    assert response["rows"] == sum(len(v) for v in response["samples"].values())
    # The flood must genuinely threaten the ceiling: each row is a
    # ~1.3 KB array sample, so the row budget alone (5000 rows) would
    # serialize to megabytes. This assertion only passes because the
    # char guard fired and dropped rows.
    assert response["rows"] > 10
    assert len(dumped) <= 60_000, (
        f"worst-case response serialized to {len(dumped)} chars"
    )
