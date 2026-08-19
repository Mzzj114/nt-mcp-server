# nt-mcp-server

A standalone MCP server for reading, writing, and monitoring FRC NetworkTables data. It lets an AI agent talk to a robot's network tables over the NetworkTables protocol. The primary target is the local RobotPy sim (`python -m robotpy sim` on `127.0.0.1:5810`).

This project lives as a sibling to `try-robotpy` and never touches it. It has its own venv and pinned dependencies (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).

## try-robotpy untouched

`nt-mcp-server` is a sibling project to `try-robotpy` (both live under `D:\Program\Hardware\`), and nothing is added to the robot repo. The server has its own venv, its own pinned dependencies, and its own git history. `try-robotpy`'s `git status` stays clean, so nothing new can reach the roboRIO deploy surface. The only interaction is the sim process itself, which the server connects to over NT4 at `127.0.0.1:5810`.

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

## Record NetworkTables to NDJSON (offline)

The `nt_recorder.py` CLI connects to a live NT4 server and writes value events to a timestamped `.ndjson` file. It runs on the dev laptop and reads NT from the sim or robot; no robot-side changes are needed.

```powershell
.venv\Scripts\python.exe nt_recorder.py --prefixes /SmartDashboard/ --output-dir recordings
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

The server exposes 14 tools (10 live + 4 offline). Values are returned in JSON-normalized (raw) form, never wrapped in metadata.

| Tool | Description |
| --- | --- |
| `nt_connect` | Start the NT4 client and wait for a live connection (default `127.0.0.1:5810`). |
| `nt_disconnect` | Stop the NT4 client; safe to call before any connect. |
| `nt_connection_info` | Return connection state: `{"connected": bool, "connections": [...]}`. |
| `nt_get` | Return the JSON-normalized value of one topic, or `None` if unpublished. |
| `nt_get_multiple` | Return `{topic: value}` for every requested topic (`None` for unpublished ones). |
| `nt_get_info` | Return topic metadata: name, type, properties. |
| `nt_set` | Publish a value to a topic; returns the setter's boolean result. |
| `nt_set_multiple` | Write every `{topic: value}` pair; returns per-topic results. |
| `nt_list_topics` | List all topic names, optionally filtered by a prefix. |
| `nt_subscribe` | Sample value updates under topic prefixes for a given duration. |

### Offline recording tools

| Tool | Description |
| --- | --- |
| `nt_list_recordings` | List available NDJSON recordings in the output directory. |
| `nt_get_history` | Return event history for one topic from a recording. |
| `nt_list_topics_offline` | List unique topic names in a recording, optionally filtered by prefix. |
| `nt_subscribe_offline` | Return events for every topic under prefixes from a recording. |

The offline tools read from the directory set by the `NT_RECORDINGS_DIR` environment variable (defaults to `./recordings`). Recordings are local-only — the recorder runs on the dev laptop and reads NT from the sim/robot; no robot-side changes are needed.

## Register in opencode

Add this entry to the project-level `opencode.jsonc` at the repo root:

```jsonc
{
  "mcp": {
    "nt": {
      "type": "local",
      "command": [
        "D:\\Program\\Hardware\\nt-mcp-server\\.venv\\Scripts\\python.exe",
        "D:\\Program\\Hardware\\nt-mcp-server\\nt_mcp_server.py"
      ],
      "enabled": true
    }
  }
}
```

Verify with `opencode mcp list` from the project root: the `nt` server should show as connected.

## Development

- Python 3.14.0 venv in `.venv`; deps installed from `requirements.txt` (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).
- Tests: `.venv\Scripts\python.exe -m pytest tests/ -v`

## Acceptance checklist

- [ ] Standalone git repo with its own venv; `import fastmcp, ntcore` works.
- [ ] All pytest suites green: loopback NT4 tests + tool-level tests (JSON-serializable, raw-value contract).
- [ ] E2E against `python -m robotpy sim --nogui` passes: connect / list / get / set / subscribe against `127.0.0.1:5810` (with the sim-alive pre-check).
- [ ] `opencode mcp` lists and starts the `nt` server; a tool call through it returns a bare value.
- [ ] `try-robotpy` is byte-for-byte untouched (`git status` clean there); nothing new reaches the roboRIO deploy surface.
- [ ] README documents run commands, registration snippet, tool table, and acceptance checklist.
- [ ] Final verification wave (F1-F4) all approve.
