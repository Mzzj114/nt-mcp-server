# nt-mcp-server

A standalone MCP server for reading, writing, and monitoring FRC NetworkTables data. It lets an AI agent talk to a robot's network tables over the NetworkTables protocol. The primary target is the local RobotPy sim (`python -m robotpy sim` on `127.0.0.1:5810`).

This project lives as a sibling to `try-robotpy` and never touches it. It has its own venv and pinned dependencies (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).

## try-robotpy untouched

`nt-mcp-server` is a sibling project to `try-robotpy` (both live side-by-side in the same parent directory), and nothing is added to the robot repo. The server has its own venv, its own pinned dependencies, and its own git history. `try-robotpy`'s `git status` stays clean, so nothing new can reach the roboRIO deploy surface. The only interaction is the sim process itself, which the server connects to over NT4 at `127.0.0.1:5810`.

## Run the server

```powershell
uv run nt-mcp-server
```

Or, equivalently, from a checkout without uv's shim:

```powershell
python -m nt_mcp_server
```

The server runs on stdio, which is how MCP clients (like opencode) talk to it.

## Run the RobotPy sim

The sim must be running before the server can read or write anything useful. Start it from its own project and venv:

```powershell
cd <path-to-try-robotpy> && .venv\Scripts\activate && python -m robotpy sim
```

The server connects to `127.0.0.1:5810` by default, which is where the sim listens.

## Record NetworkTables to NDJSON (offline)

The `nt-recorder` CLI connects to a live NT4 server and writes value events to a timestamped `.ndjson` file. It runs on the dev laptop and reads NT from the sim or robot; no robot-side changes are needed.

```powershell
uv run nt-recorder --prefixes /SmartDashboard/ --output-dir recordings
```

Options:
- `--prefixes` — topic prefixes to subscribe to (default: `/`)
- `--output-dir` — directory for output files (default: `./recordings`)
- `--duration` — record for N seconds, then exit (default: run until Ctrl+C)
- `--team` — connect via team number instead of server IP/port
- `--server-ip` / `--server-port` — NT4 server address (default: `127.0.0.1:5810`)
- `--identity` — client identity string (default: `nt-recorder`)
- `--quiet` — suppress status output to stderr

Output files are named `nt-record-<YYYY-MM-DDTHHMMSSZ>.ndjson` (no colons, Windows-safe). Each line is `{"time": float, "topic": str, "value": jsonable}`. Exit codes: 0 clean, 1 connection failure, 2 disk/IO error.

## Tools

The server exposes 17 tools (13 live + 4 offline). Every live tool response includes a `connected` flag. If the NT server is disconnected, live tools return `{"connected": false, "error": "NT server disconnected"}` instead of empty or schema-invalid output.

### Live tools

| Tool | Description |
| --- | --- |
| `nt_connect` | Start the NT4 client and wait for a live connection. Returns `{"connected": bool, "status": "connected" \| "not connected"}`. |
| `nt_disconnect` | Stop the NT4 client and tear down persistent subscriptions. Returns `{"connected": false, "status": "disconnected"}`. |
| `nt_connection_info` | Return connection state: `{"connected": bool, "connections": [...]}`. |
| `nt_get` | Return the JSON-normalized value of one topic. Response: `{"connected": bool, "value": jsonable \| null}`. |
| `nt_get_multiple` | Return every requested topic. Response: `{"connected": bool, "values": {topic: value}}`. |
| `nt_get_info` | Return topic metadata. Response: `{"connected": bool, "info": {name, type_str, properties} \| null}`. |
| `nt_set` | Publish a value. Response: `{"connected": bool, "ok": bool, "warning": str \| null}`. Add `strict_type_check=True` to refuse type mismatches. |
| `nt_set_multiple` | Write every `{topic: value}` pair. Response: `{"connected": bool, "results": {...}, "warnings": {...}}`. |
| `nt_list_topics` | List topic names, filtered by `prefix`, `regex`, and/or `wildcard`. Response: `{"connected": bool, "topics": [...]}`. |
| `nt_subscribe` | Sample updates under prefixes for `duration` seconds. Response: `{"connected": bool, "samples": {topic: [...] \| summary}}`. Supports `sample_interval`, `change_only`, and `format="summary"`. |
| `nt_start_subscription` | Open a persistent subscription. Response: `{"connected": bool, "subscription_id": str, "started": bool}`. |
| `nt_poll_subscription` | Read buffered samples from a persistent subscription. Response: `{"connected": bool, "samples": {topic: [...]}}`. |
| `nt_stop_subscription` | Stop a persistent subscription. Response: `{"connected": bool, "stopped": bool}`. |

### Offline recording tools

| Tool | Description |
| --- | --- |
| `nt_list_recordings` | List recordings. Each entry includes `id`, `path`, `size_bytes`, `modified` (Unix float), and `modified_iso` (UTC ISO-8601). |
| `nt_get_recording_info` | Return duration, total sample count, topic count, and file metadata for a recording. |
| `nt_get_history` | Return event history for one topic. Supports `last_seconds`, `sample_interval`, and `format="summary"`. |
| `nt_list_topics_offline` | List unique topic names in a recording, filtered by `prefix`, `regex`, and/or `wildcard`. |
| `nt_subscribe_offline` | Return events for every topic under prefixes. Supports `last_seconds`, `sample_interval`, and `format="summary"`. |

The offline tools read from the directory set by the `NT_RECORDINGS_DIR` environment variable (defaults to `./recordings`). Recordings are local-only — the recorder runs on the dev laptop and reads NT from the sim/robot; no robot-side changes are needed.

## Register in any agent

The server is a uvx-runnable package from git, so any MCP client can launch it without a local checkout or a pre-built venv. Run it on demand:

```powershell
uvx --from git+https://github.com/<your-repo>/nt-mcp-server.git nt-mcp-server
```

Or install the console script once and run it anywhere:

```powershell
uv tool install "git+https://github.com/<your-repo>/nt-mcp-server.git"
uvx nt-mcp-server
```

Add this entry to your agent's MCP config (shown here as opencode's project-level `opencode.jsonc` at the repo root):

```jsonc
{
  "mcp": {
    "nt": {
      "type": "local",
      "command": [
        "uvx",
        "--from",
        "git+https://github.com/<your-repo>/nt-mcp-server.git",
        "nt-mcp-server"
      ],
      "enabled": true
    }
  }
}
```

Verify with `opencode mcp list` from the project root: the `nt` server should show as connected.

## Development

- Python 3.14.0 venv in `.venv`; deps installed from `requirements.txt` (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).
- Tests: `uv run pytest tests/ -v`
