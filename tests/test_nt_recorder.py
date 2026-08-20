"""Loopback NT4 integration tests for :mod:`nt_recorder`.

Exercises the ``Recorder`` writer and the synchronous offline reader API
(``list_recordings``, ``list_topics``, ``get_history``,
``subscribe_offline``) against the same in-process NT4 loopback fixture
used by ``tests/test_nt_manager.py``. The fixture code is duplicated
here on purpose — pytest fixtures do not share across modules without a
``conftest.py``, and the project deliberately has none.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

import ntcore
import pytest

import nt_recorder
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
async def test_recorder_writes_fixture_topics(
    manager: NTManager, tmp_path
) -> None:
    """Recorder.run() writes an .ndjson file with every fixture topic."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    path = await recorder.run()
    assert path.endswith(".ndjson")
    assert os.path.isfile(path)
    topics_seen: set[str] = set()
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue
            event = json.loads(stripped)
            assert isinstance(event["time"], float)
            assert isinstance(event["topic"], str)
            assert "value" in event
            topics_seen.add(event["topic"])
    for name in FIXTURE_NAMES:
        assert name in topics_seen, f"missing topic {name} in recording"


@pytest.mark.anyio
async def test_reader_get_history(manager: NTManager, tmp_path) -> None:
    """get_history returns {time, value} entries with the fixture value."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    history = nt_recorder.get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path)
    )
    assert isinstance(history, list)
    assert len(history) >= 1
    for entry in history:
        assert sorted(entry) == ["time", "value"]
        assert isinstance(entry["time"], float)
    assert any(entry["value"] == 42.0 for entry in history)


@pytest.mark.anyio
async def test_reader_get_history_summary(manager: NTManager, tmp_path) -> None:
    """get_history with format=summary returns stats for a topic."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    summary = nt_recorder.get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path), format="summary"
    )
    assert isinstance(summary, dict)
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary


@pytest.mark.anyio
async def test_reader_get_history_last_seconds(manager: NTManager, tmp_path) -> None:
    """last_seconds filters to the most recent portion of the recording."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    full = nt_recorder.get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path)
    )
    recent = nt_recorder.get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path), last_seconds=0.1
    )
    assert len(recent) <= len(full)
    if recent:
        assert all(
            entry["time"] >= full[-1]["time"] - 0.1 for entry in recent
        )


@pytest.mark.anyio
async def test_reader_list_topics_with_prefix(
    manager: NTManager, tmp_path
) -> None:
    """list_topics returns all topics, narrows by prefix, empty on miss."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    all_topics = nt_recorder.list_topics(output_dir=str(tmp_path))
    for name in FIXTURE_NAMES:
        assert name in all_topics
    prefixed = nt_recorder.list_topics(
        output_dir=str(tmp_path), prefix="/SmartDashboard/"
    )
    # Prefix filter narrows correctly: every returned topic matches the
    # prefix and every fixture topic is present. We do NOT assert the
    # result contains ONLY fixture topics — NetworkTableInstance.getDefault()
    # is a process-wide singleton, so topics published by client-SET tests
    # in other modules (e.g. /SmartDashboard/client_int) persist in the
    # default instance's local store and get recorded alongside the
    # fixture topics when this suite runs as part of the full test run.
    assert all(topic.startswith("/SmartDashboard/") for topic in prefixed)
    assert set(FIXTURE_NAMES) <= set(prefixed)
    assert nt_recorder.list_topics(
        output_dir=str(tmp_path), prefix="/nonexistent/"
    ) == []


@pytest.mark.anyio
async def test_reader_list_topics_regex_and_wildcard(
    manager: NTManager, tmp_path
) -> None:
    """Regex and wildcard filters work on offline topic lists."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    regex_topics = nt_recorder.list_topics(
        output_dir=str(tmp_path), regex=r"test_(num|str)$"
    )
    assert "/SmartDashboard/test_num" in regex_topics
    assert "/SmartDashboard/test_bool" not in regex_topics

    wildcard_topics = nt_recorder.list_topics(
        output_dir=str(tmp_path), wildcard="/SmartDashboard/test_*"
    )
    assert set(FIXTURE_NAMES) <= set(wildcard_topics)


@pytest.mark.anyio
async def test_reader_subscribe_offline(
    manager: NTManager, tmp_path
) -> None:
    """subscribe_offline buckets events by topic; miss prefix returns {}."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    result = nt_recorder.subscribe_offline(
        ["/SmartDashboard/"], output_dir=str(tmp_path)
    )
    assert isinstance(result, dict)
    for name in FIXTURE_NAMES:
        assert name in result, f"missing topic {name}"
        for entry in result[name]:
            assert sorted(entry) == ["time", "value"]
            assert isinstance(entry["time"], float)
    assert (
        nt_recorder.subscribe_offline(
            ["/does/not/exist/"], output_dir=str(tmp_path)
        )
        == {}
    )


@pytest.mark.anyio
async def test_reader_subscribe_offline_summary(
    manager: NTManager, tmp_path
) -> None:
    """subscribe_offline with format=summary returns per-topic summaries."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    result = nt_recorder.subscribe_offline(
        ["/SmartDashboard/"], output_dir=str(tmp_path), format="summary"
    )
    assert isinstance(result, dict)
    summary = result["/SmartDashboard/test_num"]
    assert summary["count"] >= 1
    assert summary["last_value"] == 42.0
    assert "min" in summary


@pytest.mark.anyio
async def test_reader_get_recording_info(
    manager: NTManager, tmp_path
) -> None:
    """get_recording_info returns duration, sample count and topic count."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    info = nt_recorder.get_recording_info(output_dir=str(tmp_path))
    assert "error" not in info
    assert info["total_samples"] >= len(FIXTURE_NAMES)
    assert info["topic_count"] >= len(FIXTURE_NAMES)
    assert info["duration_seconds"] >= 0.0
    assert "modified_iso" in info


@pytest.mark.anyio
async def test_reader_list_recordings_and_most_recent_default(
    manager: NTManager, tmp_path
) -> None:
    """list_recordings returns metadata; get_history defaults to most recent."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    recordings = nt_recorder.list_recordings(output_dir=str(tmp_path))
    assert len(recordings) >= 1
    for entry in recordings:
        assert {"id", "path", "size_bytes", "modified", "modified_iso"} <= set(entry)
    history = nt_recorder.get_history(
        "/SmartDashboard/test_num", output_dir=str(tmp_path)
    )
    assert len(history) >= 1


def test_reader_missing_recording_returns_empty() -> None:
    """Missing recording_id yields empty results, never raises."""
    assert (
        nt_recorder.get_history(
            "/x", recording_id="nonexistent.ndjson", output_dir="."
        )
        == []
    )
    assert (
        nt_recorder.subscribe_offline(
            ["/x"], recording_id="nonexistent.ndjson", output_dir="."
        )
        == {}
    )
    assert (
        nt_recorder.list_topics(recording_id="nonexistent.ndjson", output_dir=".")
        == []
    )
    info = nt_recorder.get_recording_info(
        recording_id="nonexistent.ndjson", output_dir="."
    )
    assert "error" in info
