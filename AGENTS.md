# AGENTS.md

MCP server exposing FRC NetworkTables (NT4) to AI agents. Reads/writes/monitors a robot's network tables; primary target is a local RobotPy sim (`python -m robotpy sim` on `127.0.0.1:5810`).

## Run

```powershell
uv run nt-mcp-server          # stdio MCP server (equiv: python -m nt_mcp_server)
uv run nt-recorder --prefixes /SmartDashboard/ --output-dir recordings   # standalone CLI, not an MCP tool
```

Server runs on **stdio** — MCP clients (opencode) launch it via `opencode.jsonc` (`uv run nt-mcp-server`). It connects to `127.0.0.1:5810` by default. Live NT is optional; offline tools work without any connection.

## Architecture

Package `nt_mcp_server/` (4 modules, strict layering — each lower layer knows nothing about the one above):

- `__init__.py` — FastMCP `mcp` instance + all 17 tool definitions. **Imports no ntcore.** Tools delegate to the module-level `manager = NTManager()`. Sync tools (`nt_set`, `nt_disconnect`, etc.) are plain functions; read tools are `async`.
- `nt_manager.py` — `NTManager`: pure NT4 client lifecycle + reads/writes/subscribe streams. No MCP/FastMCP imports. Lazy default instance via `ntcore.NetworkTableInstance.getDefault()`.
- `nt_recorder.py` — two unrelated pieces: the `nt-recorder` CLI (argparse `main`), and the **synchronous offline NDJSON reader API** (`list_recordings`, `get_history`, `subscribe_offline`, …) used by the offline MCP tools.
- `nt_types.py` — value normalization (`to_jsonable`), homogeneous-list classification, summaries. Pure, no connection state.

17 tools = 13 live + 4 offline (`nt_list_recordings`, `nt_get_recording_info`, `nt_get_history`/`nt_subscribe_offline`/`nt_list_topics_offline`). **Every live tool response carries a top-level `connected` flag; disconnected reads return `{"connected": false, "error": ...}` instead.**

## NT4 gotchas (cost real debugging time)

- An NT4 client only receives a topic/value once it has a matching subscription. `NTManager` keeps a topics-only subscription on `"/"` so the topic store stays populated; first `list_topics`/`get` after connect polls briefly for the server to announce topics. Don't assume topics exist before syncing.
- `nt_set` dispatches on Python type (bool before int). Writes to an already-typed topic warn on mismatch; `strict_type_check=True` refuses the write (`ok=False`). NT itself also refuses cross-type writes.
- Offline tools read from `NT_RECORDINGS_DIR` env var (default `./recordings`). Recordings are plain NDJSON: `{"time": float, "topic": str, "value": jsonable}`; filenames are Windows-safe (`nt-record-<YYYY-MM-DDTHHMMSSZ>.ndjson`). Never call reader functions on a recording an active `Recorder` is still writing.

## Tests

```powershell
uv run pytest tests/ -v
```

- Real **integration** tests, not mocks: each module starts an in-process `ntcore` NT4 server (`NetworkTableInstance.create()` + `startServer()`) publishing fixture topics on `127.0.0.1:5810`, then drives the tools as plain functions. They need a free port 5810 and take ~seconds (use module-scoped `mcp_synced` fixture so the client connects once).
- Async tests use `@pytest.mark.anyio` (asyncio backend, auto-configured). Run the whole `tests/` dir; a single file is fine too.
- **Deliberately no `conftest.py`** — the loopback fixture is duplicated in `test_nt_manager.py`, `test_mcp_server.py`, and `test_nt_recorder.py` on purpose. Keep that duplication if adding a new test module; don't introduce a shared conftest expecting cross-module fixtures.

## Dependencies / env

- Pinned: `fastmcp==3.4.7`, `pyntcore==2026.2.2` (`pyproject.toml` + `requirements.txt`). `numpy` is optional (only normalized when present).
- `requires-python = ">=3.10"`; dev `.venv` is Python 3.14. Use `uv run` for everything.
