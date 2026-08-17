"""FastMCP server exposing FRC NetworkTables to AI agents.

Thin MCP layer: every tool delegates to the module-level ``NTManager``
from ``nt_manager``. No ntcore imports here — all NetworkTables logic
lives in the manager. Run with ``.venv\\Scripts\\python.exe
nt_mcp_server.py`` (stdio transport, the default for ``mcp.run()``).
"""

import asyncio
import time
from typing import Any

from fastmcp import FastMCP

from nt_manager import NTManager

mcp = FastMCP("NetworkTables")
manager = NTManager()

_CONNECT_TIMEOUT_SECONDS = 2.0
_CONNECT_POLL_INTERVAL_SECONDS = 0.05


@mcp.tool()
async def nt_connect(
    server_ip: str = "127.0.0.1",
    server_port: int = 5810,
    team_number: int | None = None,
    identity: str = "nt-mcp",
) -> str:
    """Start the NT4 client and wait for a live connection.

    Delegates to ``manager.connect``, then polls ``connection_info()``
    for up to ~2 s — yielding with ``await asyncio.sleep`` — and returns
    ``"connected"`` or ``"not connected"``.
    """
    manager.connect(
        server_ip=server_ip,
        server_port=server_port,
        team_number=team_number,
        identity=identity,
    )
    deadline = time.monotonic() + _CONNECT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if manager.connection_info()["connected"]:
            return "connected"
        await asyncio.sleep(_CONNECT_POLL_INTERVAL_SECONDS)
    return "not connected"


@mcp.tool()
def nt_disconnect() -> str:
    """Stop the NT4 client. Safe to call before any connect."""
    manager.disconnect()
    return "disconnected"


@mcp.tool()
def nt_connection_info() -> dict:
    """Return ``{"connected": bool, "connections": [...]}``."""
    return manager.connection_info()


@mcp.tool()
async def nt_get(topic: str) -> Any:
    """Return the JSON-normalized value of ``topic`` or ``None``."""
    return await manager.get(topic)


@mcp.tool()
async def nt_get_multiple(topics: list[str]) -> dict:
    """Return ``{topic: value}`` for every requested topic.

    Unpublished topics map to ``None``; every requested key is present.
    """
    return await manager.get_multiple(topics)


@mcp.tool()
async def nt_get_info(topic: str) -> dict:
    """Return metadata (name/type_str/properties) for ``topic``."""
    return await manager.get_info(topic)


@mcp.tool()
def nt_set(topic: str, value: Any) -> bool:
    """Publish ``value`` to ``topic``; return the setter's bool result."""
    return manager.set(topic, value)


@mcp.tool()
def nt_set_multiple(updates: dict[str, Any]) -> dict[str, bool]:
    """Write every ``{topic: value}`` pair; return per-topic results."""
    return manager.set_multiple(updates)


@mcp.tool()
async def nt_list_topics(prefix: str | None = None) -> list[str]:
    """Return all topic names, optionally filtered by ``prefix``."""
    return await manager.list_topics(prefix)


@mcp.tool()
async def nt_subscribe(prefixes: list[str], duration: float = 10.0) -> dict:
    """Sample value updates under ``prefixes`` for ``duration`` seconds.

    Returns ``{topic: [{"time": float_s, "value": jsonable}, ...]}``.
    """
    return await manager.subscribe(prefixes, duration)


if __name__ == "__main__":
    mcp.run()
