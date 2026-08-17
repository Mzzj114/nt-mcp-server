"""NetworkTables connection and state management for nt-mcp-server.

Pure NetworkTables layer: knows about ntcore, knows nothing about MCP,
FastMCP, or tool definitions. The server layer calls into NTManager to
control the client lifecycle and read connection state.
"""

from __future__ import annotations

from typing import Any

import ntcore

try:  # numpy is optional — normalize numpy values only when it is present
    import numpy as np
except ImportError:  # pragma: no cover - venv without numpy
    np = None  # type: ignore[assignment]


def _to_jsonable(value: Any) -> Any:
    """Normalize a value into a JSON-serializable primitive.

    - ``bytes``/``bytearray``/``memoryview`` -> ``None`` (raw NT topics are
      unsupported; bytes must never leak into tool output).
    - numpy scalars -> ``float``/``int``/``bool``/``str`` via ``item`` casts.
    - numpy arrays -> ``list`` (recursively normalized).
    - ``list``/``tuple``/``dict`` recurse; anything else passes through.
    """
    if isinstance(value, (bytes, bytearray, memoryview)):
        return None
    if np is not None:
        if isinstance(value, np.bool_):
            return bool(value)
        if isinstance(value, np.integer):
            return int(value)
        if isinstance(value, np.floating):
            return float(value)
        if isinstance(value, np.str_):
            return str(value)
        if isinstance(value, np.ndarray):
            return [_to_jsonable(item) for item in value.tolist()]
    if isinstance(value, (list, tuple)):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    return value


class NTManager:
    """Owns the ntcore client lifecycle for the MCP server.

    The default ``NetworkTableInstance`` is created lazily via
    ``ntcore.NetworkTableInstance.getDefault()``, so importing or
    instantiating ``NTManager`` has no side effects. ``connect`` and
    ``disconnect`` are idempotent and safe to call in any order;
    connection state is exposed as a JSON-serializable dict through
    ``connection_info``.
    """

    def __init__(self) -> None:
        self._inst: ntcore.NetworkTableInstance | None = None
        self._client_running: bool = False

    def _instance(self) -> ntcore.NetworkTableInstance:
        """Return the lazily created default NetworkTableInstance."""
        if self._inst is None:
            self._inst = ntcore.NetworkTableInstance.getDefault()
        return self._inst

    def connect(
        self,
        server_ip: str = "127.0.0.1",
        server_port: int = 5810,
        team_number: int | None = None,
        identity: str = "nt-mcp",
    ) -> None:
        """Start the NT4 client against a server or team number.

        Uses ``setServerTeam`` when ``team_number`` is given, otherwise
        ``setServer(server_ip, server_port)``. Idempotent: calling again
        while the client is running is a no-op.
        """
        if self._client_running:
            return
        inst = self._instance()
        if team_number is not None:
            inst.setServerTeam(team_number)
        else:
            inst.setServer(server_ip, server_port)
        inst.startClient4(identity)
        self._client_running = True

    def disconnect(self) -> None:
        """Stop the NT4 client. Idempotent; safe before any connect."""
        if self._client_running and self._inst is not None:
            self._inst.stopClient()
        self._client_running = False

    def connection_info(self) -> dict[str, Any]:
        """Return JSON-serializable connection state.

        ``{"connected": bool, "connections": [{"remote_id", "remote_ip",
        "last_update"}]}``.
        """
        inst = self._instance()
        info: dict[str, Any] = {
            "connected": inst.isConnected(),
            "connections": [
                {
                    "remote_id": conn.remote_id,
                    "remote_ip": conn.remote_ip,
                    "last_update": conn.last_update,
                }
                for conn in inst.getConnections()
            ],
        }
        return _to_jsonable(info)
