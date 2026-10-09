# nt-mcp-server

> **What is a MCP?**
> The Model Context Protocol (MCP) is a standard way for AI agents to use external tools and data. The concept is established by Anthropic researchers in the end of 2024.
> For more information, see [https://modelcontextprotocol.io/](https://modelcontextprotocol.io/).

This server exposes FIRST Robotics Competition (FRC) NetworkTables through MCP, so a coding agent can inspect telemetry, diagnose robot behavior, and test values without manually copying data between the robot, dashboards, and code.

It works with a local RobotPy simulation by default and can also connect to a real robot.

> **How do I use AI while connecting to the Robot Wifi?**
> Obviously Robot Wifi doesn't provide Internet access, so you can't call LLM. The key is to use TWO network adapters: one for the Robot Wifi, another for Internet access. 
> The first network adapter is usually the one inside your device that you always use to go online. For the second one, your laptop may have a Ethernet port, you can therefore connect a Ethernet cable to the `DS` port of the robot radio. In other cases, you may want a USB-to-Ethernet adapter or a USB wireless network adapter.
> The connections should be ready to go now. Rarely you need to configure the routing table of your device to explicitly guide traffic targeting the robot IP to the correct network adapter.

## Register with an agent

The package can run directly from Git with `uvx`, so no checkout or pre-built virtual environment is required. Install [uv](https://docs.astral.sh/uv/) first, then use the setup for your agent.

### Codex

```powershell
codex mcp add nt -- uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-mcp-server
codex mcp list
```

### Claude Code

```powershell
claude mcp add --scope project nt -- uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-mcp-server
claude mcp list
```

### OpenCode

Add the server to the project's `opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
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

Verify the connection with `opencode mcp list`.

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

To run the mcp server by yourself from a local checkout, start the stdio MCP server with:

```powershell
uv run nt-mcp-server
```

Equivalent Python entry point:

```powershell
python -m nt_mcp_server
```

For RobotPy simulation, start the simulator from the robot project:

```powershell
python -m robotpy sim
```

Then use `nt_connect` with its defaults to connect to `127.0.0.1:5810`.

For a real robot, connect the development machine to the robot network and call `nt_connect` with `team_number`. If the agent also needs Internet access, use separate network adapters—for example, Wi-Fi for Internet and Ethernet or a USB network adapter for the robot—and configure routing if needed.


## Release notes

### 0.2.0 — breaking changes

- **`nt_subscribe` output rework:** the `format` parameter was removed and replaced by `output`, which defaults to `"file"`. Callers that passed `format=` must switch to `output=`. In `"file"` mode the response is a compact receipt (recording path, row count, topic list) with no sample values; inline modes (`"summary"`, `"samples"`) are bounded by `limit`, `max_rows`, and a 60,000-character ceiling.
- **Offline tools return receipts:** `nt_get_history` now includes `rows` / `total_rows` / `truncated`; `nt_subscribe_offline` returns `{recording_id, topics, rows, total_rows, truncated}`. Clipping is never silent.
- **`nt_connect` target guards:** an ambiguous target (both `team_number` and a non-default `server_ip`) is rejected before any state change, and re-targeting a running client is refused — call `nt_disconnect` first.
- **`nt_connection_info` gained `target` and `version`:** `target` is the resolved connect target (`null` before the first connect); `version` is the installed package version.

## License
This project is under MIT License.
