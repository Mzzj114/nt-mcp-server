"""E2E check for nt-mcp-server against the live RobotPy sim.

Run while ``python -m robotpy sim --nogui`` is serving NT4 on
``127.0.0.1:5810`` (start it from ``D:\\Program\\Hardware\\try-robotpy``
with its own venv). Exercises the real ``NTManager`` against the real
sim: connect, list topics, read the Swerve pose, write + read back a
marker, and sample a subscription. Prints PASS/FAIL per assertion and
exits 0 only when every assertion passes.

Usage: ``.venv\\Scripts\\python.exe tools\\e2e_check.py``
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from nt_manager import NTManager  # noqa: E402

CONNECT_TIMEOUT_SECONDS = 5.0
POSE_TOPIC = "/SmartDashboard/Swerve/Pose X"
MARKER_TOPIC = "/SmartDashboard/MCP_E2E"
MARKER_VALUE = 12345
SUBSCRIBE_PREFIX = ["/SmartDashboard/"]
SUBSCRIBE_DURATION_SECONDS = 3.0

results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str) -> None:
    """Record one assertion and print its PASS/FAIL line."""
    results.append((name, ok, detail))
    print(f"{'PASS' if ok else 'FAIL'}: {name} ({detail})")


def _is_bare_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


async def main() -> int:
    manager = NTManager()
    try:
        manager.connect()

        # A1: connected within CONNECT_TIMEOUT_SECONDS.
        deadline = time.monotonic() + CONNECT_TIMEOUT_SECONDS
        connected = False
        while time.monotonic() < deadline:
            if manager.connection_info()["connected"]:
                connected = True
                break
            await asyncio.sleep(0.05)
        info = manager.connection_info()
        print(f"INFO: connection_info = {info}")
        check(
            "connected to NT server within 5 s",
            connected,
            f"isConnected={info['connected']}, connections={len(info['connections'])}",
        )

        # A2: non-empty topic list containing Swerve/Pose X.
        topics = await manager.list_topics()
        pose_topics = [t for t in topics if "Swerve/Pose X" in t]
        check(
            "topic list non-empty and contains Swerve/Pose X",
            bool(topics) and bool(pose_topics),
            f"{len(topics)} topics; pose matches: {pose_topics}",
        )

        # A3: bare numeric value from get(POSE_TOPIC).
        pose_value = await manager.get(POSE_TOPIC)
        check(
            f"get({POSE_TOPIC!r}) returns bare numeric value",
            _is_bare_number(pose_value),
            f"value={pose_value!r} type={type(pose_value).__name__}",
        )

        # A4: set marker and read it back.
        set_result = manager.set(MARKER_TOPIC, MARKER_VALUE)
        roundtrip_value = await manager.get(MARKER_TOPIC)
        check(
            "set marker topic and read it back",
            set_result["ok"] and roundtrip_value == MARKER_VALUE,
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
    finally:
        manager.disconnect()

    print()
    failed = [r for r in results if not r[1]]
    print(f"SUMMARY: {len(results) - len(failed)}/{len(results)} assertions passed")
    if failed:
        print(f"FAILED: {', '.join(name for name, _, _ in failed)}")
        print(
            "E2E FAILURE: one or more assertions failed. "
            "Is the RobotPy sim running (`python -m robotpy sim --nogui` "
            "on 127.0.0.1:5810)?"
        )
        return 1
    print("E2E SUCCESS: nt-mcp-server works against the live RobotPy sim.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
