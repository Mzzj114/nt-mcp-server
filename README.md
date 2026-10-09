# nt-mcp-server

A standalone MCP server for reading, writing, and monitoring FRC NetworkTables data. It lets an AI agent talk to a robot's network tables over the NetworkTables protocol. The primary target is the local RobotPy sim (`python -m robotpy sim` on `127.0.0.1:5810`).

pinned dependencies (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).

## Run the server

```powershell
uv run nt-mcp-server
```

Or, equivalently, from a checkout without uv's shim:

```powershell
python -m nt_mcp_server
```

The server runs on stdio, which is how MCP clients (like opencode) talk to it.

## Connect to RobotPy sim NetworkTables

The sim must be running before the server can read or write anything useful. Start it from its own project and venv:

```powershell
cd <path-to-try-robotpy> && .venv\Scripts\activate && python -m robotpy sim
```

The server connects to `127.0.0.1:5810` by default, which is where the sim listens.

## Connect to Real Robot NetworkTables

For most cases, just record NetworkTables and use the mcp to analyze the recordings.

If you want to connect to a real robot's NetworkTables in real time, you need to have 2 network adapters on you device: one for Internet and the other for robot communication. One approach is to use your wireless adapter to connect to the wifi and use a ethernet cable to connect to the robot. Alternatively, you get a USB Network Adapter as the second adapter. In ethier cases, you may need to configure your device's network routing.

## Register in any agent

The server is a uvx-runnable package from git, so any MCP client can launch it without a local checkout or a pre-built venv. Run it on demand:

```powershell
uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-mcp-server
```

Or install the console script once and run it anywhere:

```powershell
uv tool install "git+https://github.com/Mzzj114/nt-mcp-server.git"
uvx nt-mcp-server
```

Add this entry to your agent's MCP config (shown here as opencode's project-level `opencode.jsonc`):

```json
{
  "mcp": {
    "nt": {
      "type": "local",
      "command": [
        "uvx",
        "--from",
        "git+https://github.com/Mzzj114/nt-mcp-server.git",
        "nt-mcp-server"
      ],
      "enabled": true
    }
  }
}
```

Verify with `opencode mcp list` from the project root: the `nt` server should show as connected.

### Load the nt-mcp-workflow skill

The repo ships an agent skill at `skills/nt-mcp-workflow/` that teaches an agent how to run a NetworkTables investigation (live, offline replay, and recording). opencode does **not** scan a project-local `.agents/skills` directory — it only auto-loads `~/.agents/skills`, `~/.claude/skills`, project `.opencode/skill(s)/`, and directories listed under explicit `skills.paths`. Add this block to your `opencode.jsonc` (same file as the `mcp` entry above):

```json
{
  "skills": {
    "paths": ["../nt-mcp-server/skills"]
  }
}
```

The relative path resolves against the directory containing the config file, so adjust it to wherever your checkout of this repo sits relative to that config. opencode scans `skills.paths` recursively for `**/SKILL.md`, so pointing at the repo's `skills` directory exposes `nt-mcp-workflow`.

> opencode loads its config once at startup and does **not** hot-reload — restart opencode after editing the config for the skill to appear.

## Record NetworkTables to NDJSON (offline)

The `nt-recorder` CLI connects to a live NT4 server and writes value events to a timestamped `.ndjson` file. It runs on the dev laptop and reads NT from the sim or robot; no robot-side changes are needed.

```powershell
uv run nt-recorder --prefixes /SmartDashboard/ --output-dir recordings
```

Or, with no local checkout, run it straight from git via uvx (same source as the server):

```powershell
uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-recorder --prefixes /SmartDashboard/ --output-dir recordings
```

Or install the tool once so both `nt-mcp-server` and `nt-recorder` are on PATH, then run either without re-fetching:

```powershell
uv tool install "git+https://github.com/Mzzj114/nt-mcp-server.git"
nt-recorder --prefixes /SmartDashboard/ --output-dir recordings
```

The recorder is a standalone console script, independent of the MCP server — running the server via uvx does not start it, and the agent does not need to be connected to record.

Options:
- `--prefixes` — topic prefixes to subscribe to (default: `/`)
- `--output-dir` — directory for output files (default: `./recordings`)
- `--duration` — record for N seconds, then exit (default: run until Ctrl+C)
- `--team` — connect via team number instead of server IP/port
- `--server-ip` / `--server-port` — NT4 server address (default: `127.0.0.1:5810`)
- `--identity` — client identity string (default: `nt-recorder`)
- `--quiet` — suppress status output to stderr

Output files use Windows-safe names such as `nt-record-<YYYY-MM-DDTHHMMSSZ>.ndjson`; nonblocking lifecycle captures add a short uniqueness suffix. Each NDJSON has a sibling `.meta.json` file containing actual capture-clock timing and the NT event timestamp span. Every NDJSON line is `{"time": float, "topic": str, "value": jsonable}`. Exit codes: 0 clean, 1 connection failure, 2 disk/IO error.

> Run `uv cache prune` or `uv cache clear` if you don't want cache files to stay on your device after `uvx`.

## Tools

The server exposes 20 tools (15 live + 5 offline). Every live tool response includes a `connected` flag.

### Live tools

| Tool | Description |
| --- | --- |
| `nt_connect` | Start the NT4 client and wait for a live connection. Pass exactly one target: `team_number`, or `server_ip`/`server_port` with the default `server_ip`. Parameters: `server_ip="127.0.0.1"`, `server_port=5810`, `team_number=None`, `identity="nt-mcp"`, `timeout_seconds=5.0`. Success returns `{"connected": true, "status": "connected", "target": {...}}` where `target` reports the resolved target (e.g. `{"kind": "server", "server_ip": "127.0.0.1", "server_port": 5810}` or `{"kind": "team", "team_number": 8326, "server_port": 5810}`). Two refusals return `{"connected": false, "error": ...}`: an ambiguous target (both `team_number` and a non-default `server_ip`) is rejected before any state change, and re-targeting a running client is rejected — call `nt_disconnect` first. On timeout the response adds `connections` diagnostics, `elapsed_seconds`, and a routing `hint`. The `team_number` path resolves robot addresses via pyntcore's team-number lookup; the exact address list is not yet verified against real hardware, so the resolved `target` is reported rather than assumed. |
| `nt_disconnect` | Stop the NT4 client and tear down persistent subscriptions. Returns `{"connected": false, "status": "disconnected"}`. |
| `nt_connection_info` | Return connection state: `{"connected": bool, "connections": [{"remote_id", "remote_ip", "last_update"}], "target": resolved_target \| null, "version": str}`. `target` is the resolved connect target (null before the first connect); `version` is the installed package version (`"unknown"` when metadata is absent). |
| `nt_get` | Return the JSON-normalized value of one topic. Response: `{"connected": bool, "value": jsonable \| null}`. |
| `nt_get_multiple` | Return every requested topic. Response: `{"connected": bool, "values": {topic: value}}`. |
| `nt_get_info` | Return topic metadata. Response: `{"connected": bool, "info": {name, type_str, properties} \| null}`. |
| `nt_set` | Publish a value. Response: `{"connected": bool, "ok": bool, "warning": str \| null}`. Add `strict_type_check=True` to refuse type mismatches. |
| `nt_set_multiple` | Write every `{topic: value}` pair. Response: `{"connected": bool, "results": {...}, "warnings": {...}}`. |
| `nt_list_topics` | List topic names, filtered by `prefix`, `regex`, and/or `wildcard`. Response: `{"connected": bool, "topics": [...]}`. |
| `nt_subscribe` | Sample selected updates for `duration` seconds. Select with `prefixes`, exact `topics`, `regex`, and/or `wildcard`; values within a list are ORed and selector kinds are ANDed. `output` defaults to `"file"`, which writes NDJSON and returns a compact receipt with **no sample values**. Inline `"summary"` / `"samples"` modes are bounded by per-topic `limit`, total `max_rows` (default 5000), and a 60,000-character ceiling. An unqualified bare `"/"` is refused inline, while `"/"` combined with another narrowing selector is allowed. `sample_interval` and `change_only` decimate events. |

Default `nt_subscribe` receipt (no sample values, a few hundred serialized characters, far under the 20,000-character budget):

```json
{
  "connected": true,
  "output": "file",
  "recording_id": "nt-record-2026-09-18T120000Z.ndjson",
  "path": "recordings\\nt-record-2026-09-18T120000Z.ndjson",
  "duration_seconds": 10.0,
  "capture_started_at": 1789732800.0,
  "capture_started_iso": "2026-09-18T12:00:00+00:00",
  "capture_ended_at": 1789732810.0,
  "capture_ended_iso": "2026-09-18T12:00:10+00:00",
  "capture_duration_seconds": 10.0,
  "event_span_seconds": 35.4,
  "rows": 214,
  "topic_count": 6,
  "topics": ["/SmartDashboard/gyro_angle", "/SmartDashboard/left_speed"],
  "topics_truncated": false,
  "truncated": false
}
```

`topics` lists at most 50 names (`topics_truncated: true` when more exist); `rows` is the number of events written and `truncated` is true when the `max_rows` capture cap stopped recording early. `duration_seconds` is the actual wall-clock capture duration (also named explicitly as `capture_duration_seconds`); `event_span_seconds` is the span of NT last-change timestamps and can be longer when immediate values predate the capture.
| `nt_start_subscription` | Open a persistent subscription using `prefixes`, exact `topics`, `regex`, and/or `wildcard`. Response: `{"connected": bool, "subscription_id": str, "started": bool}`. |
| `nt_poll_subscription` | Read buffered samples from a persistent subscription. Response: `{"connected": bool, "samples": {topic: [...]}}`. |
| `nt_stop_subscription` | Stop a persistent subscription. Response: `{"connected": bool, "stopped": bool}`. |
| `nt_start_recording` | Start a nonblocking NDJSON recording with the same topic selectors and immediately return its `recording_id`; intended for human-controlled test windows. |
| `nt_stop_recording` | Close a nonblocking recording and return the same compact, value-free receipt as file-mode `nt_subscribe`. |

### Offline recording tools

| Tool | Description |
| --- | --- |
| `nt_list_recordings` | List recordings. Each entry includes file metadata plus persisted capture-clock and NT event-span timing when available. |
| `nt_get_recording_info` | Return capture duration, event timestamp span, total sample count, topic count, and file metadata. Legacy recordings without a metadata sidecar report `duration_source="event_span"`. |
| `nt_get_history` | Return event history for one topic. Supports `last_seconds`, `sample_interval`, and `format="summary"`. The response always states `rows` (entries returned), `total_rows` (all matching events, counted even past the clip), and `truncated` (`total_rows > rows`, or the 60,000-character ceiling dropped rows) so clipping is never silent. |
| `nt_list_topics_offline` | List unique topic names in a recording, filtered by `prefix`, `regex`, and/or `wildcard`. |
| `nt_subscribe_offline` | Return recorded events selected by `prefixes`, exact `topics`, `regex`, and/or `wildcard`. Supports `last_seconds`, `sample_interval`, and `format="summary"`. `limit` (default 1000 per topic), `max_rows` (default 5000 total), and the character ceiling bound the payload; `rows` / `total_rows` / `truncated` make clipping visible. |

The offline tools read from the directory set by the `NT_RECORDINGS_DIR` environment variable (defaults to `./recordings`). Recordings are local-only — the recorder runs on the dev laptop and reads NT from the sim/robot; no robot-side changes are needed.

The `nt-recorder` CLI also supports `--team N` to connect via team number; the MCP `nt_connect` tool exposes the same choice via `team_number`.

## Development

- Python 3.14.0 venv in `.venv`; deps installed from `requirements.txt` (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).
- Tests: `uv run pytest tests/ -v`

## Release notes

### 0.2.0 — breaking changes

- **`nt_subscribe` output rework:** the `format` parameter was removed and replaced by `output`, which defaults to `"file"`. Callers that passed `format=` must switch to `output=`. In `"file"` mode the response is a compact receipt (recording path, row count, topic list) with no sample values; inline modes (`"summary"`, `"samples"`) are bounded by `limit`, `max_rows`, and a 60,000-character ceiling.
- **Offline tools return receipts:** `nt_get_history` now includes `rows` / `total_rows` / `truncated`; `nt_subscribe_offline` returns `{recording_id, topics, rows, total_rows, truncated}`. Clipping is never silent.
- **`nt_connect` target guards:** an ambiguous target (both `team_number` and a non-default `server_ip`) is rejected before any state change, and re-targeting a running client is refused — call `nt_disconnect` first.
- **`nt_connection_info` gained `target` and `version`:** `target` is the resolved connect target (`null` before the first connect); `version` is the installed package version.

## License
This project is under MIT License.
