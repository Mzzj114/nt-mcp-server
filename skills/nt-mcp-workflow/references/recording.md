# Recording NetworkTables

Use `nt-recorder` to capture live NetworkTables data to NDJSON files for offline analysis. The recorder is a standalone CLI, independent of the MCP server. You do not need the MCP server running to record.

## CLI usage

```powershell
uv run nt-recorder --prefixes /SmartDashboard/ --output-dir recordings
```

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--prefixes` | `/` | Topic prefixes to subscribe to. Pass multiple: `--prefixes /SmartDashboard/ /FMSInfo/` |
| `--output-dir` | `./recordings` | Directory for output files. Created automatically if it does not exist. |
| `--duration` | None (run forever) | Record for N seconds, then exit cleanly. |
| `--team` | None | Connect via FRC team number instead of server IP. Mutually exclusive with a non-default `--server-ip`. |
| `--server-ip` | `127.0.0.1` | NT4 server address. |
| `--server-port` | `5810` | NT4 server port. |
| `--identity` | `nt-recorder` | Client identity string reported to the NT4 server. |
| `--quiet` | off | Suppress status output to stderr. |

If both `--team` and a non-default `--server-ip` are passed, the recorder exits with code 1 and prints an error to stderr. The two targeting methods are mutually exclusive.

### Output format

Each line in the `.ndjson` file is a JSON object:

```json
{"time": 1.234, "topic": "/SmartDashboard/Pose X", "value": 1.5}
```

Three fields, always present: `time` (float, NT4 timestamp seconds), `topic` (string), `value` (JSON-serializable).

### Filename scheme

Files are named `nt-record-<YYYY-MM-DDTHHMMSSZ>.ndjson`. The timestamp is UTC, colons are stripped so the name is Windows-safe.

### Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Clean exit (duration elapsed, or Ctrl+C / SIGINT) |
| 1 | Connection failure (could not reach the NT4 server within the timeout) |
| 2 | Disk or I/O error (write failed, directory not writable, etc.) |

### Environment variable

`NT_RECORDINGS_DIR` sets the default directory for MCP recording and offline tools. When unset they default to `./recordings`. The CLI uses `--output-dir` (default `./recordings`).

## Running detached on Windows

To record in the background without blocking the agent turn, start the recorder as a hidden process:

```powershell
Start-Process -FilePath ".venv\Scripts\python.exe" `
  -ArgumentList "-m", "nt_mcp_server.nt_recorder", "--prefixes", "/SmartDashboard/", "--output-dir", "recordings" `
  -PassThru -WindowStyle Hidden `
  -RedirectStandardOutput "recordings\nt_recorder.out" `
  -RedirectStandardError  "recordings\nt_recorder.err"
```

The `-PassThru` flag returns a process object you can capture for later reference.

## Stopping a detached recorder

Match the command line to find the right process, then kill it:

```powershell
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like '*nt-recorder*' } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

This finds any `python.exe` whose command line contains `nt-recorder` and force-stops it. The recorder flushes each NDJSON line as it is written, so partial files are still valid up to the last flushed line.

## Stop the recorder between test sessions

A live recorder left running keeps growing the output file and mixes data from different test runs (enabled windows, disabled windows, different tuning configurations). Always stop the recorder before starting a new session. If you are using the detached-on-Windows pattern, run the stop command above before launching a new recorder.

## MCP recording lifecycle

`nt_start_recording` starts an NDJSON recording and immediately returns its ID and path. Select with `prefixes`, exact `topics`, `regex` or `wildcard`; list entries are ORed, selector kinds are ANDed. A bare `"/"` prefix is allowed for file capture.

```python
nt_start_recording(prefixes=["/SmartDashboard/"], max_rows=100000)
# Perform the human-controlled test.
nt_stop_recording(recording_id="nt-record-...ndjson")
```

The start call returns `{connected, recording_id, path, started}` plus capture start time. The stop call closes the file and returns a value-free receipt: `stopped`, `recording_id`, `path`, capture start/end, `capture_duration_seconds`, `event_span_seconds`, `rows`, `topic_count`, bounded `topics`, `topics_truncated` and `truncated`. `duration_seconds` is the actual capture duration. The row cap defaults to 5000 (maximum 1,000,000); reaching it ends capture early and the receipt reports truncation. The stop call still retrieves the final receipt.

MCP recording filenames include a short unique suffix so starts within the same second do not overwrite each other. New recordings also have a sibling `.meta.json` sidecar. Capture duration is measured by the recorder clock; NT event timestamps may include older immediate values, so event span is reported separately.

After `nt_stop_recording` reports `stopped: true`, pass its ID to the offline tools. The MCP tools refuse to inspect active recordings managed by the server. The separate CLI recorder must be stopped before its file is read, too.

For a short observation that should return values inline, `nt_sample(duration=3, topics=[...])` returns summary statistics by default. It has no file mode.

## Offline analysis

1. `nt_list_recordings` finds available files when you do not already have an ID.
2. `nt_get_recording_info(recording_id=...)` returns timing, sample counts and bounded topic discovery. Filter names with `prefix`, `regex` or `wildcard`; `topic_limit` defaults to 100.
3. `nt_query_recording(topics=[...], recording_id=..., output="summary")` queries one or multiple exact topics. Prefix, regex and wildcard queries are supported too. `output="samples"` returns raw rows and is the default.

Both recording paths write the same NDJSON shape. See `references/replay-recording.md` for windowing and bounds.
