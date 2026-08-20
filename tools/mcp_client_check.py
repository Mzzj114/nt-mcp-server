"""Headless stdio MCP client smoke check for nt_mcp_server.

Spawns ``nt_mcp_server.py`` as a subprocess and speaks newline-delimited
JSON-RPC 2.0 over stdio: ``initialize`` -> ``notifications/initialized``
-> ``tools/call`` for ``nt_connect``, ``nt_list_topics`` and ``nt_get``.
Exits 0 only when ``nt_get`` on the Swerve pose topic returns a bare
float. Run against the live sim or a loopback NT4 server on 5810.

Usage: ``.venv\\Scripts\\python.exe tools\\mcp_client_check.py``
"""

from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
SERVER_SCRIPT = ROOT / "nt_mcp_server.py"
PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"
POSE_TOPIC = "/SmartDashboard/Swerve/Pose X"

REQUEST_TIMEOUT_SECONDS = 30.0


class MCPServerDied(RuntimeError):
    """The server process exited before answering a request."""


class MCPClient:
    """Minimal newline-delimited JSON-RPC 2.0 client over stdio."""

    def __init__(self, proc: subprocess.Popen[str]) -> None:
        self.proc = proc
        self._next_id = 1
        self._queue: queue.Queue[dict[str, Any] | None] = queue.Queue()
        self._stderr_lines: list[str] = []
        self._stdout_thread = threading.Thread(
            target=self._read_stdout, name="mcp-stdout-reader", daemon=True
        )
        self._stderr_thread = threading.Thread(
            target=self._read_stderr, name="mcp-stderr-reader", daemon=True
        )
        self._stdout_thread.start()
        self._stderr_thread.start()

    def _read_stdout(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.strip()
            if line:
                try:
                    self._queue.put(json.loads(line))
                except json.JSONDecodeError as exc:
                    print(f"WARN: non-JSON line on server stdout: {line!r} ({exc})")
        self._queue.put(None)  # EOF sentinel

    def _read_stderr(self) -> None:
        assert self.proc.stderr is not None
        for line in self.proc.stderr:
            self._stderr_lines.append(line.rstrip("\n"))

    def notify(self, method: str) -> None:
        self._send({"jsonrpc": "2.0", "method": method})

    def _send(self, message: dict[str, Any]) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(message) + "\n")
        self.proc.stdin.flush()

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Send a request and return its result, matched by id."""
        request_id = self._next_id
        self._next_id += 1
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self._send(message)
        deadline = time.monotonic() + REQUEST_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                response = self._queue.get(timeout=0.1)
            except queue.Empty:
                if self.proc.poll() is not None:
                    raise MCPServerDied(
                        f"server exited with code {self.proc.returncode} while "
                        f"awaiting {method}"
                    ) from None
                continue
            if response is None:
                raise MCPServerDied(f"server stdout closed while awaiting {method}")
            if response.get("id") == request_id:
                if "error" in response:
                    raise RuntimeError(
                        f"JSON-RPC error for {method}: {response['error']}"
                    )
                return response["result"]
        raise TimeoutError(f"no response for {method} within {REQUEST_TIMEOUT_SECONDS}s")

    def stderr_tail(self, n: int = 20) -> str:
        return "\n".join(self._stderr_lines[-n:])


def _text_of(result: dict[str, Any]) -> str:
    """Extract the text payload of a tools/call result."""
    content = result.get("content", [])
    if not content:
        return ""
    block = content[0]
    return block.get("text", "") if isinstance(block, dict) else ""


def main() -> int:
    proc = subprocess.Popen(
        [str(PYTHON), str(SERVER_SCRIPT)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    client = MCPClient(proc)
    failures: list[str] = []
    try:
        init_result = client.call(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "mcp-client-check", "version": "0.1.0"},
            },
        )
        print(f"PASS: initialize negotiated protocolVersion={init_result.get('protocolVersion')!r}")
        client.notify("notifications/initialized")

        connect_text = _text_of(
            client.call("tools/call", {"name": "nt_connect", "arguments": {}})
        )
        print(f"INFO: nt_connect -> {connect_text!r}")
        if "connected" not in connect_text:
            failures.append(f"nt_connect returned {connect_text!r}")

        topics_text = _text_of(
            client.call("tools/call", {"name": "nt_list_topics", "arguments": {}})
        )
        topics_result = json.loads(topics_text)
        topics = topics_result.get("topics", []) if isinstance(topics_result, dict) else topics_result
        pose_topics = [t for t in topics if "Swerve/Pose X" in t]
        print(
            f"PASS: nt_list_topics returned {len(topics)} topics; "
            f"pose matches: {pose_topics}"
        )
        if not pose_topics:
            failures.append("nt_list_topics did not contain Swerve/Pose X")

        get_text = _text_of(
            client.call(
                "tools/call",
                {"name": "nt_get", "arguments": {"topic": POSE_TOPIC}},
            )
        )
        get_result = json.loads(get_text)
        value = get_result.get("value") if isinstance(get_result, dict) else get_result
        is_float = isinstance(value, float) and not isinstance(value, bool)
        print(f"{'PASS' if is_float else 'FAIL'}: nt_get({POSE_TOPIC!r}) -> {value!r} (bare float)")
        if not is_float:
            failures.append(f"nt_get returned {get_result!r}, expected a wrapped bare float")
    finally:
        client._send({"jsonrpc": "2.0", "method": "shutdown"})
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.terminate()
            proc.wait(timeout=5)

    print()
    if failures:
        print("FAILED:")
        for failure in failures:
            print(f"  - {failure}")
        print("--- server stderr tail ---")
        print(client.stderr_tail())
        print("MCP CLIENT E2E FAILURE: see failures above.")
        return 1
    print("MCP CLIENT SUCCESS: stdio JSON-RPC roundtrip against nt_mcp_server works.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
