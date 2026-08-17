# nt-mcp-server

A standalone MCP server for reading, writing, and monitoring FRC NetworkTables data. It lets an AI agent talk to a robot's network tables over the NetworkTables protocol. The primary target is the local RobotPy sim (`python -m robotpy sim` on `127.0.0.1:5810`).

This project lives as a sibling to `try-robotpy` and never touches it. It has its own venv and pinned dependencies (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).

## Run the server

```powershell
.venv\Scripts\python.exe nt_mcp_server.py
```

The server runs on stdio, which is how MCP clients (like opencode) talk to it.

## Run the RobotPy sim

The sim must be running before the server can read or write anything useful. Start it from its own project and venv:

```powershell
cd D:\Program\Hardware\try-robotpy && .venv\Scripts\activate && python -m robotpy sim
```

The server connects to `127.0.0.1:5810` by default, which is where the sim listens.

## Tools

TODO (T4.3): fill in the table of the 10 tools (`nt_connect`, `nt_disconnect`, `nt_connection_info`, `nt_get`, `nt_get_multiple`, `nt_get_info`, `nt_set`, `nt_set_multiple`, `nt_list_topics`, `nt_subscribe`).

## Register in opencode

TODO (T4.3): fill in the local MCP registration snippet for `opencode.jsonc`.

## Development

- Python 3.14 venv in `.venv`, deps installed from `requirements.txt`.
- Tests: `.venv\Scripts\python.exe -m pytest tests/ -v`
