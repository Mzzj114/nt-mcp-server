## 2. Discover what is on the telemetry

Grep the codebase for `SmartDashboard` (and `NetworkTables` / `Field2d`) to
learn which topics the robot publishes or subscribes to. Example targets:

- `subsystems/swerve_drive.py` publishing `/SmartDashboard/Swerve/Pose X`,
  `/SmartDashboard/Swerve/Pose Y`, `/SmartDashboard/Swerve/Pose Theta`, and a
  `Field2d` at `/SmartDashboard/Field`.
- Other subsystems may publish encoder counts, voltages, setpoints, etc.


## 5. Offline (recording) calls

Use these when you have a saved `.ndjson` recording and the robot is not running.

1. `nt_nt_list_recordings` — lists files with size, `modified_iso`, and path.
2. `nt_nt_get_recording_info(recording_id=...)` — duration, `total_samples`,
   and `topic_count` for a quick overview before opening it.
3. `nt_nt_list_topics_offline(recording_id=...)` — the topics present in the
   recording.
4. `nt_nt_subscribe_offline(prefixes=[...], recording_id=...)` — replays
   samples. Same token-saving knobs as live:
   - `sample_interval` — decimate.
   - `limit` — cap sample count.
   - `format` — `"samples"` (default) or `"summary"` (min/max/mean/last).
5. `nt_nt_get_history(topic=..., recording_id=...)` — pull one topic's history
   with windowing:
   - `last_seconds` — the most recent N seconds.
   - `start` / `end` — an explicit time window (NT4 timestamps).

Example: `nt_nt_get_history(topic="/SmartDashboard/Swerve/Pose X",
recording_id="nt-record-...ndjson", last_seconds=5)`.