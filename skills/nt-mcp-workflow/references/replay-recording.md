# Offline recording workflow

Use this workflow for a saved `.ndjson` recording, including transient issues or post-test analysis. Live NT is not required.

Recordings are stored on the MCP server host in `NT_RECORDINGS_DIR` (default `./recordings`). Each line is `{"time": float, "topic": str, "value": jsonable}`. New files have a sibling `.meta.json` timing sidecar.

## 0. Produce a recording

- `nt-recorder` CLI records until stopped or `--duration` elapses.
- `nt_start_recording(prefixes=[...])` returns an ID immediately. Perform the test, then `nt_stop_recording(recording_id)` closes the file and returns its receipt without sample values.

Stop the recording before analysis. Reuse the receipt's ID; discovery is unnecessary when the ID is already known. See `references/recording.md`.

## 1. Discover telemetry

Search robot code for `SmartDashboard`, `NetworkTables` and `Field2d` to understand topic meanings and the expected behavior. Topic names published through SmartDashboard include `/SmartDashboard/`.

## 2. Inspect the recording

1. `nt_list_recordings` finds available files, newest first, when the ID is unknown.
2. `nt_get_recording_info(recording_id=...)` returns duration, `total_samples`, `topic_count` and sorted `topics`. Optional `prefix`, `regex` and `wildcard` filters are ANDed for topic discovery; `topic_limit` defaults to 100 (maximum 1000). The overview still describes the whole file. `matching_topic_count`, `topics_returned` and `topics_truncated` report filtered discovery and clipping.

`capture_duration_seconds` is actual capture time. `event_span_seconds` is the span of NT timestamps, possibly including older immediate values. Legacy files without sidecars report `duration_source="event_span"`.

## 3. Query samples or summaries

`nt_query_recording` handles both single-topic history and multi-topic reads:

```python
nt_query_recording(
    recording_id="nt-record-...ndjson",
    topics=["/SmartDashboard/Swerve/Pose X", "/SmartDashboard/Swerve/Pose Y"],
    last_seconds=5,
    output="summary",
)
```

Select with exact `topics`, `prefixes`, `regex` or `wildcard`. Entries within a list are ORed; selector kinds are ANDed. At least one selector is required. A bare `prefixes=["/"]` is refused unless another selector narrows it.

Both output modes return `{recording_id, topics, rows, total_rows, truncated}`. `topics` is keyed by topic name:
- `output="samples"` (default) returns timestamp/value rows.
- `output="summary"` returns statistics over retained rows, including count and last value.

Window and bound the query with:
- `start` (inclusive) and `end` (exclusive), using the recording's NT event timestamps.
- `last_seconds`, relative to the latest NT event timestamp, not wall-clock capture end.
- `sample_interval`, keeping at most one sample per N seconds per topic.
- `limit`, retaining at most N events per topic (default 1000, maximum 5000).
- `max_rows`, retaining at most N events across all topics (default/maximum 5000).

The 60,000-character ceiling applies to samples and summaries. `total_rows` counts matching events before limits and decimation; `rows` reports retained events. If `truncated` is true, the results are a subset. Narrow the selectors or time window before drawing conclusions. See `references/analysis-gotchas.md`.

Omitting the recording ID selects the most recently modified file and returns its resolved ID. Pin that ID for subsequent queries so a newer recording does not change the dataset.

## 4. Wrap up

Summarize findings with specific time windows or topic values and propose any follow-up test supported by the evidence.
