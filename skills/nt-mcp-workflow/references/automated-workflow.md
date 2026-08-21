## 2. Discover what is on the telemetry

Grep the codebase for `SmartDashboard` (and `NetworkTables` / `Field2d`) to
learn which topics the robot publishes or subscribes to. Example targets:

- `subsystems/swerve_drive.py` publishing `/SmartDashboard/Swerve/Pose X`,
  `/SmartDashboard/Swerve/Pose Y`, `/SmartDashboard/Swerve/Pose Theta`, and a
  `Field2d` at `/SmartDashboard/Field`.
- Other subsystems may publish encoder counts, voltages, setpoints, etc.

It's likely you need extra input/output fields to drive the test:

- Create a dedicated testing branch.
- Add the new output fields (e.g. extra `putNumber`/`putBoolean` calls) so the
  behavior you are debugging is visible.
- Add **writable** fields you can use to operate the robot from the MCP. Put
  these under a `/MCP` topic prefix (e.g. `/SmartDashboard/MCP/TestMode`,
  `/SmartDashboard/MCP/Enable`) so they are easy to find and clearly separated
  from production telemetry.

## 4. Online (live) calls

1. Connect with `nt_nt_connect` (default `127.0.0.1:5810` for the sim).
2. Use `nt_nt_connection_info` to confirm the remote IP and that the link is up.
3. List topics with `nt_nt_list_topics` (optionally a `prefix`).
4. Inspect a topic's type/properties with `nt_nt_get_info` before writing, so
   you know whether it is a `double`, `string`, `double[]`, etc.
5. Read values with `nt_nt_get` / `nt_nt_get_multiple`.
6. Drive inputs with `nt_nt_set` / `nt_nt_set_multiple` (e.g. publish a
   `/SmartDashboard/MCP/*` setpoint the robot reads).
7. Watch telemetry with `nt_nt_subscribe`. To keep responses small and avoid
   burning context tokens:
   - `sample_interval` — decimate to one sample per N seconds.
   - `change_only` — skip samples that barely changed (great for mostly-static
     values; spinning pose still streams).
   - `limit` — cap the number of returned samples.
   - `duration` — how long to collect.

   Example: `nt_nt_subscribe(prefixes=["/SmartDashboard/Swerve"],
   duration=3, sample_interval=0.5, change_only=true)`.

Use `nt_nt_disconnect` when finished.

Example: Tuning a single motor shooter PID
First, modify code
Create a tune_shooter_pid command, in robot periodics, run or stop the command based on `/SmartDashboard/MCP/tuneShooterPID/run` (boolean)
In the command, spin shooter motor with
- `/SmartDashboard/MCP/tuneShooterPID/target` for target velocity
- The P,I,D value (e.g.`/SmartDashboard/MCP/tuneShooterPID/P`)
And add actual velocity measurement to telemetry (and optionally the difference between target and actual)
Once code is ready and the robot is ready, connected to the robot and
- set new a P,I,D value and set a target
- start subscribing actual velocity
- start the command (by setting `/SmartDashboard/MCP/tuneShooterPID/run`)
- after a few seconds, stop
- poll subscribed value
- if still needs tuning, back to the first step, otherwise stop

## Wrap Up
briefly summrize the results, ask user to confirm, and end your response

if user replied ok, you may leave the extra files you created (e.g. `commands/tune_shooter_pid.py`) for future references, but restore the core files like `robot.py`
Finally, commit and merge branch if necessary