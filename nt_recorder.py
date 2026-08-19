"""Standalone CLI recorder: writes NetworkTables value events to NDJSON.

Not an MCP tool. Connects to an NT4 server via ``NTManager``, subscribes
to a set of prefixes, and writes one JSON object per value event to a
timestamped ``.ndjson`` file. Status diagnostics go to stderr; NDJSON
goes only to the file.

Exit codes: 0 clean, 1 connection failure, 2 disk/IO error.

Usage::

    python nt_recorder.py --prefixes /SmartDashboard/ --output-dir ./recordings
    python nt_recorder.py --team 254 --duration 30
    python nt_recorder.py --duration 0.1 --prefixes /SmartDashboard/ --output-dir tests/tmp_recordings
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import datetime
import json
import os
import signal
import sys
import time
from typing import Any

from nt_manager import NTManager

_CONNECT_TIMEOUT_SECONDS = 5.0
_CONNECT_POLL_SECONDS = 0.05


class Recorder:
    """Record NetworkTables value events to an NDJSON file.

    One JSON object per event: ``{"time": float, "topic": str, "value": jsonable}``.
    The file is flushed after every line and closed when ``run`` returns.
    When ``duration`` is None, runs until ``stop_event`` is set.
    """

    def __init__(
        self,
        manager: NTManager,
        output_dir: str,
        prefixes: list[str],
        duration: float | None = None,
    ) -> None:
        self.manager = manager
        self.output_dir = output_dir
        self.prefixes = prefixes
        self.duration = duration
        self.stop_event = asyncio.Event()

    async def run(self) -> str:
        """Subscribe, write events to NDJSON, return the output file path."""
        os.makedirs(self.output_dir, exist_ok=True)
        filename = datetime.datetime.now(datetime.timezone.utc).strftime(
            "nt-record-%Y-%m-%dT%H%M%SZ.ndjson"
        )
        path = os.path.join(self.output_dir, filename)
        with open(path, "w", encoding="utf-8") as file:
            if self.duration is not None:
                # asyncio.timeout fires at the generator's await point even
                # when no events are flowing (bare time.monotonic checks
                # inside the async for never regain control between events).
                try:
                    async with asyncio.timeout(self.duration):
                        async for topic, value, time_seconds in (
                            self.manager.subscribe_stream(self.prefixes)
                        ):
                            _write_event(file, topic, value, time_seconds)
                except TimeoutError:
                    pass
            else:
                await self._run_until_stop(file)
        return path

    async def _run_until_stop(self, file) -> None:
        """Consume the stream until ``stop_event`` is set."""
        consume_task = asyncio.create_task(self._consume(file))
        stop_task = asyncio.create_task(self.stop_event.wait())
        try:
            await asyncio.wait(
                {consume_task, stop_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (consume_task, stop_task):
                if not task.done():
                    task.cancel()
            # Drain: let the generator's finally close the subscriber and
            # poller; suppress only CancelledError, propagate real errors.
            for task in (consume_task, stop_task):
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _consume(self, file) -> None:
        async for topic, value, time_seconds in (
            self.manager.subscribe_stream(self.prefixes)
        ):
            _write_event(file, topic, value, time_seconds)


def _write_event(file, topic: str, value: Any, time_seconds: float) -> None:
    """Write one NDJSON line and flush."""
    line = {"time": time_seconds, "topic": topic, "value": value}
    file.write(json.dumps(line) + "\n")
    file.flush()


def _emit(quiet: bool, message: str) -> None:
    """Print a status line to stderr unless quiet."""
    if not quiet:
        print(message, file=sys.stderr, flush=True)


async def _wait_for_connection(manager: NTManager) -> bool:
    """Poll connection_info up to _CONNECT_TIMEOUT_SECONDS."""
    deadline = time.monotonic() + _CONNECT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if manager.connection_info()["connected"]:
            return True
        await asyncio.sleep(_CONNECT_POLL_SECONDS)
    return manager.connection_info()["connected"]


async def main() -> int:
    parser = argparse.ArgumentParser(
        description="Record NetworkTables value events to an NDJSON file.",
    )
    parser.add_argument("--server-ip", default="127.0.0.1")
    parser.add_argument("--server-port", default=5810, type=int)
    parser.add_argument("--team", default=None, type=int)
    parser.add_argument("--identity", default="nt-recorder")
    parser.add_argument("--prefixes", nargs="+", default=["/"])
    parser.add_argument("--output-dir", default="./recordings")
    parser.add_argument("--duration", default=None, type=float)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    manager = NTManager()
    try:
        _emit(
            args.quiet,
            f"connecting to NT server at {args.server_ip}:{args.server_port}...",
        )
        manager.connect(
            server_ip=args.server_ip,
            server_port=args.server_port,
            team_number=args.team,
            identity=args.identity,
        )
        if not await _wait_for_connection(manager):
            print(
                f"ERROR: failed to connect to NT server at "
                f"{args.server_ip}:{args.server_port} within "
                f"{_CONNECT_TIMEOUT_SECONDS:.0f}s",
                file=sys.stderr,
            )
            return 1
        _emit(args.quiet, "connected")

        recorder = Recorder(
            manager=manager,
            output_dir=args.output_dir,
            prefixes=args.prefixes,
            duration=args.duration,
        )

        # Signal handlers: best-effort. On Windows, loop.add_signal_handler
        # raises NotImplementedError; rely on KeyboardInterrupt instead.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, recorder.stop_event.set)
            except (NotImplementedError, RuntimeError):
                break

        _emit(
            args.quiet,
            f"recording prefixes={args.prefixes} to {args.output_dir}/",
        )
        try:
            path = await recorder.run()
        except (IOError, OSError) as exc:
            print(f"ERROR: disk/IO failure: {exc}", file=sys.stderr)
            return 2
        except KeyboardInterrupt:
            _emit(args.quiet, "interrupted")
            return 0
        _emit(args.quiet, f"stopped; wrote {path}")
    finally:
        manager.disconnect()

    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(0)
