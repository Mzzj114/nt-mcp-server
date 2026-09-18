# nt-mcp-server notes and current workflow

Date: 2026-09-17
Context: debugging the MK4i swerve steering while connected to the real robot
(team 8326, roboRIO at `10.83.26.2`).

## TL;DR

`nt-mcp-server` (the `nt` MCP server) **cannot connect to the robot or the sim
from this machine**. Every tool call reports disconnected even though the
NetworkTables server is up and a plain `ntcore` client in the project venv
connects instantly. I work around it by recording NT4 directly with a small
`ntcore` script (located in `C:\Users\mzzj\AppData\Local\Temp\opencode\`) in the venv and analyzing the CSV offline.

## Environment

- MCP config, `opencode.jsonc`:
  - `type: local`
  - `command: uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-mcp-server`
  - `enabled: true`
- Robot: roboRIO `10.83.26.2` (team 8326). Laptop Ethernet `10.83.26.200`.
- Open ports on the roboRIO: `22` (SSH), `80`, `1735` (NT3), `1741`, `5810` (NT4).
- Project deps: RobotPy 2026.2.2, `robotpy-rev`, `phoenix6`, `robotpy-navx`.

## The issue

### Symptom

All `nt_*` MCP tools fail, regardless of arguments:

- `nt_nt_connect` with `server_ip=10.83.26.2`, `server_port=5810`,
  `team_number=8326`, `identity=opencode-nt`
  -> `{"connected": false, "status": "not connected"}`
- Same before/after the robot was enabled.
- `nt_nt_list_topics` -> `{"connected": false, "error": "NT server disconnected"}`

### Evidence the network/server is fine

- `Test-NetConnection 10.83.26.2 -Port 5810` -> `True` (NT4 server listening).
- Raw `ntcore` from the venv connects:

  ```python
  import ntcore
  inst = ntcore.NetworkTableInstance.getDefault()
  inst.startClient4("opencode-dump")
  inst.setServer("10.83.26.2", ntcore.NetworkTableInstance.kDefaultPort4)
  # isConnected() -> True; 119 topics discovered
  ```

So this is a problem **inside the MCP server process**, not the robot, not the
network, not `ntcore`.

### Root-cause hypotheses (not yet confirmed)

1. The MCP server's NT instance is bound to a different default
   server/port (or an internal client that never started), so `connect` is a
   no-op or targets the wrong endpoint.
2. It may hold a stale/failed client from an earlier connect attempt and never
   retry.
3. It might require the robot to be **enabled** at connect time (untested: the
   calls failed both before and after enable).
4. Tool-level bug: `status`/`connected` reporting is wrong while the client is
   actually connected (but `nt_list_topics` independently errors, so probably
   not).

The MCP is a personal/in-progress package (`notes.txt` has a "complete mcp
skill / package mcp" todo), so this is likely still rough.

## Current handling (workaround)

Bypass the MCP. Use the venv's `ntcore` to record and analyze directly.

### 1. Discover topics

`getTopics()` returns only locally-known topics; NT4 announces remote topics
only after a subscription/listener matches. Discover with a catch-all listener:

```python
inst.addListener([""], ntcore.EventFlags.kTopic | ntcore.EventFlags.kValueAll,
                 lambda e: None)
```

This revealed the layout (note the leading slash and the `SmartDashboard`
prefix):

- `/SmartDashboard/Driver/Left X|Left Y|Right X|Right Y`, `/Driver/A Button`
- `/SmartDashboard/Diagnostics/Battery Voltage (V)`
- `/SmartDashboard/Swerve/<Front Left|Front Right|Rear Left|Rear Right>/<field>`
  where `<field>` is `Angle (deg)`, `Angle Desired (deg)`, `Angle Error (deg)`,
  `Speed (m/s)`, `Speed Desired (m/s)`, `Turn Encoder (rad)`, `Turn Output`,
  `Turn Command (V)`, `Turn Current (A)`, `Turn Bus Voltage (V)`,
  `Drive Output`, `Drive Current (A)`, `CANcoder Connected`,
  `CANcoder Magnet Health`, `Turn Fault Bits`, ...
- `/FMSInfo/*`, `/limelight-shoot/*`, `/LiveWindow/*`, `/Shuffleboard/*`

### 2. Record to CSV

`nt_monitor.py` (in `%TEMP%\opencode`) connects, subscribes to the fields of
interest via `getDoubleTopic(name).subscribe(0.0)`, and appends a row every
50 ms to `%TEMP%\opencode\nt_log.csv` with a header, flushing each row.

Start it detached so the agent turn is not blocked:

```powershell
Start-Process -FilePath ".venv\Scripts\python.exe" `
  -ArgumentList "C:\Users\mzzj\AppData\Local\Temp\opencode\nt_monitor.py" `
  -PassThru -WindowStyle Hidden `
  -RedirectStandardOutput "...\nt_monitor.out" `
  -RedirectStandardError  "...\nt_monitor.err"
```

Stop it by matching the command line:

```powershell
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
  Where-Object { $_.CommandLine -like '*nt_monitor*' } |
  ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
```

### 3. Analyze offline

Standalone Python scripts read the CSV and compute min/max/mean, sign
correlations, enabled-window segmentation, and per-window error trends. This
is how every finding in the steering investigation was made.

## NT4 / recording gotchas that have bitten us

- **`getTopics()` shows nothing remote** until something subscribes or a
  listener with the `""` prefix fires. Use the listener to enumerate.
- **Keys carry the `/SmartDashboard/` prefix** when published through
  `wpilib.SmartDashboard`.
- **Both NT3 (`1735`) and NT4 (`5810`) are open**; use NT4.
- **A live recorder must be stopped between test sessions.** Otherwise the CSV
  keeps growing and mixes enabled and disabled windows.
- **Disabled windows are misleading.** When disabled, the default command does
  not run, `stop()` zeroes outputs and the encoders freeze. Any "the wheels
  settled at X" conclusion must be checked against `Turn Output != 0`.
- **20 Hz recording aliases the 50 Hz control loop.** Derivative/sign tests
  (`sign(err) vs sign(dEncoder)`) are unreliable under aliasing; prefer
  magnitude and one-way-travel evidence (does the wheel reach the target once,
  or sweep full revolutions?).

## Workflow used in this investigation (with the small `ntcore` script)

1. Added telemetry: battery voltage, driver axes, and per-module steer
   angle/output/current plus fault/identity keys.
2. User enabled; recorded NT to CSV (agent did not drive).
3. Analysis: turn motors had `Turn Output = 0.00` and a constant `0.0000`
   encoder for the whole run -> the Spark MAX had no absolute encoder, so the
   on-board position loop never ran (forward/back worked because it needs no
   steering).
4. User confirmed **CTRE CANcoders** (`13, 23, 43, 33`). Rewired steering to
   read the CANcoder on the roboRIO and drive the Spark MAX by voltage with a
   `PIDController` (continuous input).
5. Recorded: encoders now moved but stalled short of target (gain too low).
6. Raised gains; user tried `kTurnP = 0.1`; still broken.
7. User googled empirical PID constants for MK4i swerve. Issue fixed.

