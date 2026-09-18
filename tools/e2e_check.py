"""E2E check for nt-mcp-server response contracts.

Self-contained by default: starts an in-process NT4 loopback server on
``127.0.0.1:5810`` (the same ``NetworkTableInstance.create()`` +
``startServer()`` pattern as the pytest fixtures) and publishes its own
topics through the client, so a bare ``uv run python tools\\e2e_check.py``
succeeds without a sim. Pass ``--external`` to skip the loopback server
and run against an already-running sim/NT4 server on 5810 instead.

Drives the real tool functions from ``nt_mcp_server`` directly — no
stdio/JSON-RPC — and asserts the shipped response contracts: connect
reports the resolved ``target``, the default ``nt_subscribe`` file
capture returns a receipt with ``rows >= 1`` and no sample values, and
an offline read of the produced recording exposes ``truncated`` /
``total_rows``. Prints PASS/FAIL per assertion and exits 0 only when
every assertion passes.

Usage: ``uv run python tools\\e2e_check.py`` (add ``--external`` for a live sim)
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
import time

import ntcore

from nt_mcp_server import (
    manager,
    nt_connect,
    nt_connection_info,
    nt_get,
    nt_get_history,
    nt_list_topics,
    nt_set,
    nt_subscribe,
)

NUM_TOPIC = "/SmartDashboard/MCP_E2E_num"
NUM_VALUE = 3.5
MARKER_TOPIC = "/SmartDashboard/MCP_E2E"
MARKER_VALUE = 12345
CHURN_TOPIC = "/SmartDashboard/MCP_E2E_churn"
SUBSCRIBE_PREFIX = ["/SmartDashboard/"]
SUBSCRIBE_DURATION_SECONDS = 2.0
EXPECTED_TARGET = {"kind": "server", "server_ip": "127.0.0.1", "server_port": 5810}

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str) -> None:
    """Record one assertion and print its PASS/FAIL line."""
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name} ({detail})")


def _is_bare_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


async def _churn(topic: str, seconds: float) -> None:
    """Alternate a boolean on ``topic`` every 50 ms so captures see updates."""
    end = time.monotonic() + seconds
    value = True
    while time.monotonic() < end:
        nt_set(topic, value)
        value = not value
        await asyncio.sleep(0.05)


async def main() -> int:
    external = "--external" in sys.argv
    loopback: ntcore.NetworkTableInstance | None = None
    tmpdir = tempfile.mkdtemp(prefix="e2e-recordings-")
    try:
        if not external:
            loopback = ntcore.NetworkTableInstance.create()
            loopback.startServer()

        # A1: nt_connect waits (default 5 s timeout) and reports connected.
        connect_result = await nt_connect()
        info = nt_connection_info()
        print(f"INFO: connection_info = {info}")
        check(
            "nt_connect reports a live connection within its 5 s timeout",
            connect_result.get("connected") is True,
            f"connect={connect_result}, connections={len(info['connections'])}",
        )

        # Setup for A2/A3: publish a harness-owned numeric topic through
        # the client so the assertions never depend on sim fixture content.
        nt_set(NUM_TOPIC, NUM_VALUE)

        # A2: non-empty topic list containing the harness-owned topic.
        # The server's announcement of a client-published topic can lag
        # the first poll, so retry briefly (same wait_until pattern as
        # the pytest fixtures) instead of depending on instant sync.
        topics: list[str] = []
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            topics_response = await nt_list_topics()
            topics = topics_response.get("topics", [])
            if NUM_TOPIC in topics:
                break
            await asyncio.sleep(0.1)
        own_topics = [t for t in topics if t == NUM_TOPIC]
        check(
            f"topic list non-empty and contains {NUM_TOPIC!r}",
            bool(topics) and bool(own_topics),
            f"{len(topics)} topics; own-topic matches: {own_topics}",
        )

        # A3: bare numeric value from nt_get.
        get_result = await nt_get(NUM_TOPIC)
        value = get_result.get("value") if isinstance(get_result, dict) else None
        check(
            f"nt_get({NUM_TOPIC!r}) returns bare numeric value",
            _is_bare_number(value),
            f"value={value!r} type={type(value).__name__}",
        )

        # A4: set marker and read it back.
        set_result = nt_set(MARKER_TOPIC, MARKER_VALUE)
        roundtrip = await nt_get(MARKER_TOPIC)
        roundtrip_value = (
            roundtrip.get("value") if isinstance(roundtrip, dict) else None
        )
        check(
            "nt_set marker topic and read it back",
            set_result.get("ok") is True and roundtrip_value == MARKER_VALUE,
            f"set->{set_result}, get->{roundtrip_value!r} (expected {MARKER_VALUE})",
        )

        # A5: subscribe samples at least once under /SmartDashboard/.
        samples = await manager.subscribe(SUBSCRIBE_PREFIX, SUBSCRIBE_DURATION_SECONDS)
        sample_count = sum(len(v) for v in samples.values())
        check(
            "subscribe captures at least 1 sample",
            sample_count >= 1,
            f"{sample_count} samples across {len(samples)} topics",
        )

        # A6 (new contract a): the resolved target is reported and matches
        # the requested server/port, in both the connect response and
        # connection_info.
        check(
            "connect reports resolved target 127.0.0.1:5810",
            connect_result.get("connected") is True
            and connect_result.get("target") == EXPECTED_TARGET
            and info.get("target") == EXPECTED_TARGET,
            f"connect target={connect_result.get('target')}, "
            f"info target={info.get('target')}",
        )

        # A7 (new contract b): default file-mode capture writes a real
        # .ndjson file and returns a receipt with rows >= 1, no samples.
        churn = asyncio.create_task(
            _churn(CHURN_TOPIC, SUBSCRIBE_DURATION_SECONDS + 0.5)
        )
        receipt = await nt_subscribe(
            SUBSCRIBE_PREFIX,
            duration=SUBSCRIBE_DURATION_SECONDS,
            output_dir=tmpdir,
        )
        await churn
        path = receipt.get("path")
        check(
            "default subscribe capture writes file receipt (rows >= 1, no samples)",
            receipt.get("connected") is True
            and receipt.get("output") == "file"
            and isinstance(receipt.get("rows"), int)
            and receipt["rows"] >= 1
            and isinstance(path, str)
            and path.endswith(".ndjson")
            and os.path.isfile(path)
            and "samples" not in receipt,
            f"receipt={receipt}",
        )

        # A8 (new contract c): an offline read of the produced recording
        # exposes truncated (bool) and total_rows (int).
        recording_id = receipt.get("recording_id")
        if receipt.get("connected") and isinstance(recording_id, str):
            history = nt_get_history(
                CHURN_TOPIC, recording_id=recording_id, output_dir=tmpdir
            )
            check(
                "offline read of the capture exposes truncated/total_rows",
                isinstance(history.get("truncated"), bool)
                and isinstance(history.get("total_rows"), int)
                and history.get("rows", 0) >= 1,
                f"rows={history.get('rows')}, total_rows={history.get('total_rows')}, "
                f"truncated={history.get('truncated')}",
            )
        else:
            check(
                "offline read of the capture exposes truncated/total_rows",
                False,
                "skipped: no recording receipt from the file capture",
            )
    finally:
        manager.disconnect()
        if loopback is not None:
            loopback.stopServer()
            ntcore.NetworkTableInstance.destroy(loopback)
        shutil.rmtree(tmpdir, ignore_errors=True)

    print()
    failed = [r for r in results if not r[1]]
    print(f"SUMMARY: {len(results) - len(failed)}/{len(results)} assertions passed")
    if failed:
        print(f"FAILED: {', '.join(name for name, _, _ in failed)}")
        print(
            "E2E FAILURE: one or more assertions failed. "
            "Is an NT4 server reachable on 127.0.0.1:5810? The harness "
            "starts its own loopback server by default; with --external, "
            "start the RobotPy sim (`python -m robotpy sim --nogui`)."
        )
        return 1
    print("E2E SUCCESS: nt-mcp-server response contracts hold end to end.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
