---
name: nt-mcp-workflow
description: Use when investigating an issue/bug using nt-mcp-server.
license: MIT
metadata:
  author: Randy Z
  version: "1.0.0"
---

# Suggested Workflow

This skill guides you through investigating an FRC robot issue with the `nt`
(NetworkTables) MCP server. The server speaks NT4 and can operate both **live**
(connected to a running sim or real robot) and **offline** (replaying a saved
`.ndjson` recording).

## 1. Decide the workflow

If you feel like things are controlable through network table once robot starts use the automated-workflow.md
- **Tricky / specific issue** (one value is wrong, a single subsystem
  misbehaves, you need to drive inputs in real time): use the **live online
  tools** against a real robot or the RobotPy sim.

if you are analyzing a recording, refer to replay-recording.md
- **Broad / unclear issue** (what happened during a whole match, a transient
  you can't reproduce, post-event forensics): use the **offline recording
  tools** instead.

Otherwise, you can make judgement calls utilizing any nt tools you need.

## 3. Bring up the NT source

- **Sim:** start it detached so the process does not block your tool calls. For example, in
  PowerShell, use `Start-Process` rather than a foreground launch:

  ```powershell
  .venv\Scripts\activate
  Start-Process -FilePath "python" -ArgumentList "-m", "robotpy", "sim" `
      -WorkingDirectory "D:\Program\Hardware\try-robotpy"
  ```
- **Real robot:** the user need to connect the cable / Wi-Fi so you can
  reach the roboRIO. Confirm the team number / IP, then connect.

Wait a few seconds for the robot to boot, then proceed.



