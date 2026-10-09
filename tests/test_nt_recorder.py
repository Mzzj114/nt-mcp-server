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
import sys
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator

import ntcore
import pytest

from nt_mcp_server import nt_recorder
from nt_mcp_server.nt_manager import NTManager

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
    """get_recording_info separates capture time from NT event time."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    await recorder.run()
    info = nt_recorder.get_recording_info(output_dir=str(tmp_path))
    assert "error" not in info
    assert info["total_samples"] >= len(FIXTURE_NAMES)
    assert info["topic_count"] >= len(FIXTURE_NAMES)
    assert info["duration_seconds"] >= 0.0
    assert info["duration_seconds"] == info["capture_duration_seconds"]
    assert info["duration_source"] == "capture"
    assert info["capture_started_at"] <= info["capture_ended_at"]
    assert info["event_span_seconds"] >= 0.0
    assert (tmp_path / info["id"].replace(".ndjson", ".meta.json")).is_file()
    assert "modified_iso" in info


def test_reader_get_recording_info_ignores_zero_timestamps(tmp_path) -> None:
    """Zero/negative timestamps from kImmediate values must not corrupt duration."""
    recording = tmp_path / "zero-time.ndjson"
    with open(recording, "w", encoding="utf-8") as file:
        file.write(json.dumps({"time": 0, "topic": "/x", "value": 1}) + "\n")
        file.write(json.dumps({"time": 1000.0, "topic": "/x", "value": 2}) + "\n")
        file.write(json.dumps({"time": 1017.5, "topic": "/x", "value": 3}) + "\n")
    info = nt_recorder.get_recording_info(
        recording_id=recording.name, output_dir=str(tmp_path)
    )
    assert info["duration_seconds"] == pytest.approx(17.5, abs=0.01)
    assert info["capture_duration_seconds"] is None
    assert info["event_span_seconds"] == pytest.approx(17.5, abs=0.01)
    assert info["duration_source"] == "event_span"
    assert info["total_samples"] == 3
    assert info["topic_count"] == 1


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
        assert entry["capture_duration_seconds"] >= 0.0
        assert entry["event_span_seconds"] >= 0.0
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


def test_reader_missing_recording_return_meta_empty() -> None:
    """return_meta=True on a missing recording returns zeroed metadata."""
    result = nt_recorder.get_history(
        "/x",
        recording_id="nonexistent.ndjson",
        output_dir=".",
        return_meta=True,
    )
    assert result == {"samples": [], "rows": 0, "total_rows": 0, "truncated": False}
    result = nt_recorder.subscribe_offline(
        ["/x"],
        recording_id="nonexistent.ndjson",
        output_dir=".",
        return_meta=True,
    )
    assert result == {"topics": {}, "rows": 0, "total_rows": 0, "truncated": False}


def _write_synthetic_recording(tmp_path, topic: str, count: int) -> str:
    """Write an NDJSON recording with ``count`` events for one topic."""
    for i in range(count):
        event = {"time": 1000.0 + i, "topic": topic, "value": float(i)}
        (tmp_path / "synthetic.ndjson").open("a", encoding="utf-8").write(
            json.dumps(event) + "\n"
        )
    return "synthetic.ndjson"


def test_reader_get_history_return_meta_counts_past_limit(tmp_path) -> None:
    """Given 10 events and limit=3, return_meta reports rows=3, total_rows=10."""
    recording_id = _write_synthetic_recording(
        tmp_path, "/SmartDashboard/synth", 10
    )
    result = nt_recorder.get_history(
        "/SmartDashboard/synth",
        recording_id=recording_id,
        output_dir=str(tmp_path),
        limit=3,
        return_meta=True,
    )
    assert result["rows"] == 3
    assert result["total_rows"] == 10
    assert result["truncated"] is True
    assert len(result["samples"]) == 3
    assert [entry["value"] for entry in result["samples"]] == [0.0, 1.0, 2.0]


def test_reader_get_history_return_meta_not_truncated(tmp_path) -> None:
    """Given limit >= match count, truncated is False and totals agree."""
    recording_id = _write_synthetic_recording(
        tmp_path, "/SmartDashboard/synth", 10
    )
    result = nt_recorder.get_history(
        "/SmartDashboard/synth",
        recording_id=recording_id,
        output_dir=str(tmp_path),
        limit=10,
        return_meta=True,
    )
    assert result["rows"] == 10
    assert result["total_rows"] == 10
    assert result["truncated"] is False


def test_reader_get_history_return_meta_summary(tmp_path) -> None:
    """return_meta with format=summary nests the summary under 'summary'."""
    recording_id = _write_synthetic_recording(
        tmp_path, "/SmartDashboard/synth", 10
    )
    result = nt_recorder.get_history(
        "/SmartDashboard/synth",
        recording_id=recording_id,
        output_dir=str(tmp_path),
        limit=3,
        format="summary",
        return_meta=True,
    )
    assert result["rows"] == 3
    assert result["total_rows"] == 10
    assert result["truncated"] is True
    assert result["summary"]["count"] == 3
    assert result["summary"]["last_value"] == 2.0


def test_reader_subscribe_offline_return_meta_counts_past_limit(tmp_path) -> None:
    """subscribe_offline return_meta counts all matches across topics."""
    for i in range(10):
        event = {"time": 1000.0 + i, "topic": "/SmartDashboard/synth", "value": float(i)}
        (tmp_path / "synthetic.ndjson").open("a", encoding="utf-8").write(
            json.dumps(event) + "\n"
        )
    for i in range(4):
        event = {"time": 1000.0 + i, "topic": "/SmartDashboard/other", "value": i}
        (tmp_path / "synthetic.ndjson").open("a", encoding="utf-8").write(
            json.dumps(event) + "\n"
        )
    result = nt_recorder.subscribe_offline(
        ["/SmartDashboard/"],
        recording_id="synthetic.ndjson",
        output_dir=str(tmp_path),
        limit=3,
        return_meta=True,
    )
    assert result["rows"] == 6  # 3 per topic x 2 topics
    assert result["total_rows"] == 14  # 10 + 4, counted past the clip
    assert result["truncated"] is True
    assert len(result["topics"]["/SmartDashboard/synth"]) == 3
    assert len(result["topics"]["/SmartDashboard/other"]) == 3


def test_reader_default_path_has_no_meta_keys(tmp_path) -> None:
    """return_meta=False (default) keeps the exact legacy return shapes."""
    recording_id = _write_synthetic_recording(
        tmp_path, "/SmartDashboard/synth", 10
    )
    history = nt_recorder.get_history(
        "/SmartDashboard/synth",
        recording_id=recording_id,
        output_dir=str(tmp_path),
        limit=3,
    )
    assert isinstance(history, list)
    assert len(history) == 3
    for entry in history:
        assert sorted(entry) == ["time", "value"]
    result = nt_recorder.subscribe_offline(
        ["/SmartDashboard/"],
        recording_id=recording_id,
        output_dir=str(tmp_path),
        limit=3,
    )
    assert set(result) == {"/SmartDashboard/synth"}
    for entry in result["/SmartDashboard/synth"]:
        assert sorted(entry) == ["time", "value"]


def _count_lines(path: str) -> int:
    """Count non-blank NDJSON lines in a recording file."""
    with open(path, "r", encoding="utf-8") as file:
        return sum(1 for line in file if line.strip())


@pytest.mark.anyio
async def test_recorder_max_capture_rows_stops_capture(
    manager: NTManager, tmp_path
) -> None:
    """max_capture_rows=3 writes exactly 3 lines and flags truncated."""
    recorder = nt_recorder.Recorder(
        manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=5.0,
        max_capture_rows=3,
    )
    path = await recorder.run()
    assert isinstance(path, str)
    assert _count_lines(path) == 3
    assert recorder.rows == 3
    assert recorder.truncated is True


@pytest.mark.anyio
async def test_recorder_sample_interval_reduces_rows(
    manager: NTManager, tmp_path
) -> None:
    """sample_interval decimation writes fewer lines than an unfiltered run."""
    topic = "/SmartDashboard/deci_num"

    async def churn() -> None:
        value = 0.0
        for _ in range(20):
            value = 1.0 if value == 0.0 else 0.0
            manager.set(topic, value)
            await asyncio.sleep(0.05)

    async def record(subdir: str, **kwargs) -> int:
        recorder = nt_recorder.Recorder(
            manager,
            output_dir=str(tmp_path / subdir),
            prefixes=["/SmartDashboard/"],
            duration=1.0,
            **kwargs,
        )
        path = await recorder.run()
        return _count_lines(path)

    churn_task = asyncio.create_task(churn())
    unfiltered = await record("unfiltered")
    filtered = await record("filtered", sample_interval=0.2)
    await churn_task
    assert filtered < unfiltered


@pytest.mark.anyio
async def test_recorder_change_only_drops_small_changes(
    manager: NTManager, tmp_path
) -> None:
    """change_only keeps only the first sample of a small-oscillation topic."""
    topic = "/SmartDashboard/change_num"

    async def churn() -> None:
        for _ in range(20):
            manager.set(topic, 1.0)
            await asyncio.sleep(0.05)
            manager.set(topic, 2.0)
            await asyncio.sleep(0.05)

    churn_task = asyncio.create_task(churn())
    recorder = nt_recorder.Recorder(
        manager,
        output_dir=str(tmp_path),
        prefixes=["/SmartDashboard/"],
        duration=1.0,
        change_only=1.5,
    )
    path = await recorder.run()
    await churn_task
    history = nt_recorder.get_history(topic, output_dir=str(tmp_path))
    assert len(history) == 1
    assert _count_lines(path) >= len(FIXTURE_NAMES)


@pytest.mark.anyio
async def test_recorder_stats_topics_and_not_truncated(
    manager: NTManager, tmp_path
) -> None:
    """Unfiltered run: truncated is False and topics matches the file."""
    recorder = nt_recorder.Recorder(
        manager, output_dir=str(tmp_path), prefixes=["/SmartDashboard/"], duration=1.0
    )
    path = await recorder.run()
    assert recorder.truncated is False
    topics_in_file: set[str] = set()
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if stripped:
                topics_in_file.add(json.loads(stripped)["topic"])
    assert recorder.topics == sorted(topics_in_file)
    assert set(FIXTURE_NAMES) <= set(recorder.topics)


@pytest.mark.anyio
async def test_main_ambiguous_target_exits_1_without_traceback(
    monkeypatch, capsys
) -> None:
    """--team with a non-default --server-ip exits 1 with an error line.

    Drives the real ``main()`` entry point with ``sys.argv`` patched.
    ``AmbiguousTargetError`` is raised before any NT state changes, so no
    loopback server is needed for this failure path.
    """
    monkeypatch.setattr(
        sys,
        "argv",
        ["nt-recorder", "--team", "8326", "--server-ip", "10.83.26.2"],
    )
    exit_code = await nt_recorder.main()
    assert exit_code == 1
    captured = capsys.readouterr()
    assert "Traceback" not in captured.err
    assert "ERROR:" in captured.err
    assert "8326" in captured.err
    assert "10.83.26.2" in captured.err
