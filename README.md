# nt-mcp-server

[![Listed on mcpservers.org](https://mcpservers.org/badge.svg)](https://mcpservers.org/servers/mzzj114/nt-mcp-server)

> ### What is a MCP?
> 
> **The Model Context Protocol (MCP)** is a standard way for AI agents to use external tools and data. The concept is established by Anthropic researchers in the end of 2024.
> For more information, see [https://modelcontextprotocol.io/](https://modelcontextprotocol.io/).

This server exposes FIRST Robotics Competition (FRC) NetworkTables through MCP, so a coding agent can inspect telemetry, diagnose robot behavior, and test values without manually copying data between the robot, dashboards, and code.

It works with a local RobotPy simulation by default and can also connect to a real robot.

> ### How do I use AI while connecting to the Robot Wifi?
> 
> Obviously Robot Wifi doesn't provide Internet access, so you can't call LLM. The key is to use **TWO network adapters: one for the Robot Wifi, another for Internet access**. 
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

The server exposes 16 tools (13 live + 3 offline). Every live response includes a `connected` flag. Reads and writes have separate tools.

### Live tools

| Tool                    | Description                                                                                                                                                                                                                                                               |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `nt_connect`            | Start the NT4 client and wait for a connection. Defaults to `127.0.0.1:5810`; choose `team_number` or `server_ip`/`server_port`. Conflicting targets and re-targeting a running client are refused; disconnect first. Returns the resolved target or timeout diagnostics. |
| `nt_disconnect`         | Stop the client, tear down persistent subscriptions and request active recordings to close.                                                                                                                                                                               |
| `nt_connection_info`    | Return connection state, remote peers, resolved target and package version.                                                                                                                                                                                               |
| `nt_read_topics`        | Read one or more exact `topics`. Returns `{connected, values: {topic: value}}`; unknown topics have null values.                                                                                                                                                          |
| `nt_get_topic_info`     | Return one topic's name, NT type and properties, or null if unknown.                                                                                                                                                                                                      |
| `nt_write_topics`       | Publish an `updates` map, including a single-entry map. Returns per-topic `results` and `warnings`. `strict_type_check=True` refuses known type mismatches. Writes are not atomic.                                                                                        |
| `nt_list_topics`        | Discover live topic names with optional `prefix`, `regex` and `wildcard` filters.                                                                                                                                                                                         |
| `nt_sample`             | Observe selected updates for `duration` seconds (up to 60). Defaults to `output="summary"`; `output="samples"` returns timestamp/value rows. Returns `samples` keyed by topic, `rows`, `total_rows` and `truncated`.                                                      |
| `nt_start_subscription` | Start a persistent in-memory subscription and return its `subscription_id`. `buffer_size` bounds retained events; oldest events are discarded when full.                                                                                                                  |
| `nt_poll_subscription`  | Read buffered events; `clear=True` clears them after polling.                                                                                                                                                                                                             |
| `nt_stop_subscription`  | Stop the subscription and free its resources.                                                                                                                                                                                                                             |
| `nt_start_recording`    | Start an NDJSON recording and immediately return its `recording_id` and path for a human-controlled test window.                                                                                                                                                          |
| `nt_stop_recording`     | Close a recording and return a compact receipt with timing, row count, topic names and truncation flags, without sample values.                                                                                                                                           |

Live sampling, subscriptions, recordings and recording queries share `prefixes`, exact `topics`, `regex` and `wildcard` selectors. Entries within a list are ORed; different selector kinds are ANDed. At least one selector is required. A bare `prefixes=["/"]` is allowed for recordings, but refused for inline sampling and recording queries unless another selector narrows it.

`nt_sample` blocks for its fixed window and returns inline results. For an interactive test, call `nt_start_recording`, perform the test, then call `nt_stop_recording`. Use the returned ID for offline analysis after the file closes.

### Offline recording tools

| Tool                    | Description                                                                                                                                                                                                                                                                                                                      |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `nt_list_recordings`    | List available files, newest first, with file metadata and persisted capture timing when available.                                                                                                                                                                                                                              |
| `nt_get_recording_info` | Return capture duration, NT event span, sample count, file metadata and a sorted topic list. `prefix`, `regex` and `wildcard` filter topic discovery; `topic_limit` defaults to 100 (maximum 1000). `topic_count` describes the whole file; `matching_topic_count`, `topics_returned` and `topics_truncated` describe discovery. |
| `nt_query_recording`    | Query selected topics. Returns `{recording_id, topics, rows, total_rows, truncated}`. Defaults to `output="samples"`; `output="summary"` returns statistics under the same `topics` key. Supports `start` (inclusive), `end` (exclusive), `last_seconds` and `sample_interval`.                                                  |

The offline directory defaults to `NT_RECORDINGS_DIR` or `./recordings`; `output_dir` overrides it. Omitting `recording_id` selects the most recently modified recording. Queries return the resolved ID. The MCP tools refuse to inspect a recording still being written by this server; a separate CLI recorder must also be stopped before analysis.

For one topic's history:

```python
nt_query_recording(
    topics=["/SmartDashboard/Swerve/Pose X"],
    recording_id="nt-record-...ndjson",
    last_seconds=5,
    output="summary",
)
```

Inline sampling and offline queries are bounded by per-topic `limit`, total `max_rows` (default 5000, maximum 5000) and a 60,000-character ceiling. Summary statistics describe retained rows; `rows` / `total_rows` / `truncated` report reduction explicitly. Offline `total_rows` counts matching events before limits and decimation; live `total_rows` counts events after sampling/change filters and before output clipping. `last_seconds` uses the latest NT event timestamp, which can differ from the capture clock.

Each new recording has a sibling `.meta.json` sidecar. `capture_duration_seconds` measures capture time; `event_span_seconds` measures the range of NT last-change timestamps, including older immediate values. Legacy files without sidecars report `duration_source="event_span"`.

The `nt-recorder` CLI remains available independently of MCP, including `--team N` for robot address lookup. Recordings are local to the server host; no robot-side changes are needed.

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

### 0.3.0 — consolidated tool surface

The exposed MCP surface changes from 20 to 16 tools. Restart the MCP server/client to refresh tool discovery. These are breaking name and argument changes:

| Previous call                                          | Replacement                                                                           |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------- |
| `nt_get(topic)` / `nt_get_multiple(topics)`            | `nt_read_topics(topics=[...])`; values are always keyed by topic                      |
| `nt_set(topic, value)` / `nt_set_multiple(updates)`    | `nt_write_topics(updates={topic: value})`; results/warnings are always keyed by topic |
| `nt_get_info(topic)`                                   | `nt_get_topic_info(topic)`                                                            |
| `nt_subscribe(output="summary")` or `output="samples"` | `nt_sample(output=...)`; defaults to summary and has no file mode                     |
| `nt_subscribe(output="file")`                          | `nt_start_recording(...)`, then `nt_stop_recording(recording_id)`                     |
| `nt_get_history(topic, ...)`                           | `nt_query_recording(topics=[topic], ...)`; read `topics[topic]`                       |
| `nt_subscribe_offline(format=...)`                     | `nt_query_recording(output=...)`                                                      |
| `nt_list_topics_offline(...)`                          | `nt_get_recording_info(...)`; read `topics` and discovery counts                      |

Schemas describe every parameter and enforce output enums, absolute topic names, non-empty batches, positive sampling intervals, row budgets, port ranges and filename-only recording IDs. Offline reader functions in `nt_recorder` retain their Python API.

### 0.2.0 — breaking changes

- **`nt_subscribe` output rework:** the `format` parameter was removed and replaced by `output`, which defaults to `"file"`. Callers that passed `format=` must switch to `output=`. In `"file"` mode the response is a compact receipt (recording path, row count, topic list) with no sample values; inline modes (`"summary"`, `"samples"`) are bounded by `limit`, `max_rows`, and a 60,000-character ceiling.
- **Offline tools return receipts:** `nt_get_history` now includes `rows` / `total_rows` / `truncated`; `nt_subscribe_offline` returns `{recording_id, topics, rows, total_rows, truncated}`. Clipping is never silent.
- **`nt_connect` target guards:** an ambiguous target (both `team_number` and a non-default `server_ip`) is rejected before any state change, and re-targeting a running client is refused — call `nt_disconnect` first.
- **`nt_connection_info` gained `target` and `version`:** `target` is the resolved connect target (`null` before the first connect); `version` is the installed package version.

## License

This project is under MIT License.
