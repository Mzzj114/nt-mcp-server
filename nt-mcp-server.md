# nt-mcp-server - Work Plan

## TL;DR (For humans)

**What you'll get:** A standalone NetworkTables MCP server project at `D:\Program\Hardware\nt-mcp-server\` (sibling to `try-robotpy`), so an AI agent can read/write/monitor FRC robot data over NetworkTables — primarily against the local RobotPy sim (`python -m robotpy sim` on `127.0.0.1:5810`). It exposes 10 tools (`nt_connect`, `nt_disconnect`, `nt_connection_info`, `nt_get`, `nt_get_multiple`, `nt_get_info`, `nt_set`, `nt_set_multiple`, `nt_list_topics`, `nt_subscribe`) with **token-efficient raw-value returns**, and is registered as a local MCP server in the opencode config.

**Why this approach:** `robotpy deploy` walks the *entire* robot project directory and uploads every non-excluded file/folder to the roboRIO — it skips only dot-dirs, `venv`/`__pycache__`/`ctre_sim`, and `.pyc/.whl/.ipk/.zip/.gz/.wpilog` (`robotpy_installer/cli_deploy.py:_copy_to_tmpdir`, lines 715–740; no `deploycfg.json` exclusion exists in 2026.0.2). Putting the MCP server in `try-robotpy`'s root would ship dev-tooling to the robot. The MCP server is generic tooling with zero dependency on this robot's code, so it lives in its own project with its own venv and deps (`fastmcp==3.4.7`, `pyntcore==2026.2.2`).

**What it will NOT do:** touch `try-robotpy` at all (no files added/edited there); no auth/authorization; no robot-safety write guards; no MCP `resource://` URIs; nothing gets deployed to the roboRIO.

**Effort:** ~4 waves, 12 todos (11 commits; the opencode-config todo is not committed). Two real source files (`nt_manager.py` + `nt_mcp_server.py`), a loopback test suite, an E2E check against the running sim, and one config registration.

**Risk:** `fastmcp` stable 3.4.7 declares `>=3.10` but has no verified Python 3.14 classifier; the documented contingency is creating the venv with Python 3.12/3.13 (`py -3.12 -m venv`). Main correctness risk is the subscribe sampler (`MultiSubscriber` + `NetworkTableListenerPoller` drain loop) — covered by loopback tests. pyntcore API details (write path, timestamp accessor, event topic type) have been verified against the installed 2026.2.2 stub and pinned in this plan.

**Decisions:** Separate project (user-approved). Pinned exact versions. Raw-value contract for `nt_get`. Dot-folder avoided entirely by living outside the robot repo.

---

## Scope

**In scope (the standalone project only):**
- `D:\Program\Hardware\nt-mcp-server\` as a new git repo containing:
  - `pyproject.toml` + `requirements.txt` (pins: `fastmcp==3.4.7`, `pyntcore==2026.2.2`)
  - `nt_manager.py` — NetworkTables connection + state management, NO MCP knowledge, async-capable subscribe
  - `nt_mcp_server.py` — FastMCP entry point exposing `nt_manager` as the 10 tools, `mcp.run()` under `__main__`
  - `tests/test_nt_manager.py` + `tests/test_mcp_server.py` — loopback NT4-server unit tests (happy + failure)
  - `tools/e2e_check.py` + `tools/mcp_client_check.py` — E2E and headless MCP-client harnesses
  - `README.md` — usage, registration snippet, tool table, acceptance checklist
  - `.gitignore` (`.venv/`, `__pycache__/`, `*.pyc`, `.pytest_cache/`, `.omo/`)
  - own `.venv` created from the machine's Python (3.14); fallback to 3.12/3.13 if `fastmcp` 3.4.7 fails there
- Registration of the server as a **local** MCP entry in `C:\Users\mzzj\.config\opencode\opencode.jsonc` (absolute paths to the new venv python + script)
- E2E validation against the live sim: `python -m robotpy sim --nogui` run from `try-robotpy` (its own venv) while the MCP server connects to `127.0.0.1:5810`

**Out of scope / Must-NOT-Have:**
- No files added to or edited inside `D:\Program\Hardware\try-robotpy` (reference `nt_mcp_plan.md` there as documentation only)
- No `fastmcp` or `pyntcore` added to `try-robotpy`'s `pyproject.toml` / `[tool.robotpy].requires` / `requirements.txt`
- No authentication/authorization on the server
- No write-protection or robot-safety guards (sim focus; revisit before real hardware)
- No MCP `resource://` URI surface — tools only
- No changes to the opencode global config beyond the one `mcp` entry (do not touch `plugin`/`provider` sections)
- No `time.sleep()` anywhere in async code; no dataclasses with mismatched `__init__`; raw (`bytes`) NT topics are unsupported (returned as-is is forbidden — see T2.1 `_to_jsonable`)

---

## Verification strategy

Every todo carries agent-executable acceptance criteria + happy/failure QA with an evidence path (command run, artifact produced, assertion checked). No criterion needs a human.

- **Loopback unit tests** (`tests/test_nt_manager.py`): a second `ntcore.NetworkTableInstance()` starts an in-process NT4 server (`startServer()`), fixtures publish known topics, and an `NTManager` client connects via `setServer("127.0.0.1", 5810)` + `startClient4`. Covers get/list/set/set_multiple/get_info/subscribe roundtrips plus failure paths (unpublished topic → `None`, invalid write type → `False`).
- **Server smoke test**: `python nt_mcp_server.py` starts, stays alive on stdio, and exits cleanly on interrupt; tool registry introspectable via `fastmcp`.
- **E2E against sim**: launch `python -m robotpy sim --nogui` (try-robotpy venv), first confirm the sim stays alive ≥10s and the `/SmartDashboard/Swerve/Pose X` topic actually appears (pre-check, since `SwerveDrive.__init__` constructs `navx.AHRS`), then run the MCP server path (or `tools/e2e_check.py` importing `nt_manager`) against defaults `127.0.0.1:5810`; assert `connected: true`, non-empty topic list including `Swerve/Pose X`, raw numeric value from `nt_get`, set→get roundtrip, and a clean 3s subscribe.
- **Config validation**: `opencode.jsonc` parses and `opencode mcp` lists the new server; a tool call through the registered server (via `tools/mcp_client_check.py`, a headless stdio MCP client) returns a raw value.
- **Final verification wave** (after all todos): F1 plan-compliance audit, F2 code-quality review, F3 real manual QA against the running sim, F4 scope fidelity (try-robotpy untouched).

---

## Execution strategy

Four waves, each with a single git commit at its end. The dependency matrix is linear: Wave 1 (project) → Wave 2 (`nt_manager.py`+tests) → Wave 3 (`nt_mcp_server.py`+tests) → Wave 4 (E2E + registration + docs). No parallelism needed; a slow `fastmcp` install in Wave 1 blocks nothing else since Wave 2 only needs `pyntcore`.

`nt_mcp_plan.md` (at `D:\Program\Hardware\try-robotpy\nt_mcp_plan.md`) is the design reference. **This plan SUPERSEDES its §Deliverables (files move to the sibling project) and §Environment facts "add fastmcp as dev dependency" (deps are isolated to the new venv).** All other sections — "Design decisions (non-negotiable)", "MCP tool surface", and "Implementation notes (must-do)" — are binding. Key binding rules repeated here for the executor:

- `nt_get` returns the **raw value only** via `Value.value()` — never a metadata wrapper. Guard unpublished topics with `Value.isValid()` → return `None`.
- Metadata is opt-in (`nt_get_info`); `nt_list_topics` supports a `prefix` filter.
- `nt_set` dispatches by Python type with `bool` checked **before** `int`; **write via `inst.getEntry(topic).setValue(value)`** (NOT `setEntryValue` — that method does not exist in pyntcore 2026.2.2). For int-preserving writes use the type-specific `setInteger`; float → `setDouble`; bool → `setBoolean`; str → `setString`; homogeneous list → `setIntegerArray`/`setDoubleArray`/`setBooleanArray`/`setStringArray`. Return the boolean result of the setter.
- Subscribe uses `ntcore.MultiSubscriber(inst, prefixes, options)` + `ntcore.NetworkTableListenerPoller(inst)` + `addListener(subscriber, EventFlags.kValueAll | EventFlags.kImmediate)`, drained via `poller.readQueue()`. **Timestamp via `event.data.value.last_change()`** (snake_case; `lastChange` does not exist). **Topic name via `event.data.topic.getName()`** — `ValueEventData.topic` is already a `Topic` object, so do NOT call `inst.getTopic(event.data.topic)` (that expects a `str`).
- Connection: `inst.setServer(ip, port)` or `inst.setServerTeam(team)`, then `inst.startClient4(identity)`; `inst.stopClient()` to disconnect; health via `inst.isConnected()` / `inst.getConnections()` (`ConnectionInfo` fields: `remote_id`, `remote_ip`, `last_update`). The design ref's `addConnectionListener`/`addTimeSyncListener` are intentionally NOT required by the specced tools.
- All tool outputs JSON-serializable primitives only. `_to_jsonable` normalizes numpy scalars/arrays AND must raise/translate on `bytes` (raw NT type) — raw topics are unsupported: return `None` for `bytes` values rather than failing `json.dumps`.
- The design ref's `fastmcp dev` command (line 95) is stale (removed in fastmcp 2.x) — use `python nt_mcp_server.py` or `fastmcp run`.

---

## Todos

### Wave 1 — Standalone project scaffold

**T1.1 — Create the standalone project skeleton**
- WHERE: `D:\Program\Hardware\nt-mcp-server\`
- WHAT: `git init` the directory; write `pyproject.toml` (`[project] name = "nt-mcp-server"`, `requires-python = ">=3.10"`, `dependencies = ["fastmcp==3.4.7", "pyntcore==2026.2.2"]`), `requirements.txt` with the same two exact pins, `.gitignore` (`.venv/`, `__pycache__/`, `*.pyc`, `.pytest_cache/`, `.omo/`).
- REFERENCES: `nt_mcp_plan.md` §Environment facts; fastmcp PyPI (stable `3.4.7`, `requires-python >=3.10`); pyntcore 2026.2.2 already proven on this machine (3.14/win) in `try-robotpy\.venv`.
- ACCEPTANCE: `git status` shows a clean repo with exactly the 4 files tracked; both files contain the exact pins `fastmcp==3.4.7` and `pyntcore==2026.2.2`.
- QA happy: `git log --oneline` shows the initial commit with the 4 files.
- QA failure: pins typo'd (e.g. `fastmcp==3.4.7 ` with trailing space) → `pip install -r requirements.txt` in T1.2 must fail loudly; verify the failure is surfaced, then fix the pin and re-run.
- EVIDENCE: `.venv\Scripts\pip.exe list` (after T1.2), `git log --oneline`.
- COMMIT: `scaffold: nt-mcp-server project skeleton (pyproject, requirements, gitignore)`

**T1.2 — Create venv and install pinned deps**
- WHERE: `D:\Program\Hardware\nt-mcp-server\`
- WHAT: `python -m venv .venv`, then `.venv\Scripts\pip.exe install -r requirements.txt`.
- REFERENCES: machine Python is 3.14.0 (AGENTS.md); fastmcp 3.4.7 declares `>=3.10` but has no verified 3.14 classifier; contingency below.
- ACCEPTANCE: `.venv\Scripts\python.exe -c "import fastmcp, ntcore; print(fastmcp.__version__)"` prints a version; `pip list` shows exactly the two pins.
- QA happy: the import line above exits 0 and prints `3.4.7`.
- QA failure (documented contingency): if `fastmcp` 3.4.7 fails to install/import on Python 3.14, **prefer** creating the venv with Python 3.12/3.13 (`py -3.12 -m venv .venv` or `uv venv --python 3.12`); only as a secondary attempt pin `fastmcp==4.0.0b3` (no verified 3.14 advantage over 3.4.7 — same declared `>=3.10`/classifiers). Record whichever interpreter/pin was used in the commit message and README.
- EVIDENCE: `.venv\Scripts\pip.exe list` output; the `import fastmcp, ntcore` line.
- COMMIT: `chore: add venv with fastmcp==3.4.7 and pyntcore==2026.2.2` (or the contingency pin)

**T1.3 — README stub**
- WHERE: `D:\Program\Hardware\nt-mcp-server\README.md`
- WHAT: create README with: purpose (NT MCP server for reading/writing/monitoring FRC NT data, primary target = local RobotPy sim), run instructions (`.venv\Scripts\python.exe nt_mcp_server.py`), the sim command (`cd D:\Program\Hardware\try-robotpy && .venv\Scripts\activate && python -m robotpy sim`), and a placeholder for the tool table + registration snippet (filled in T4.3).
- REFERENCES: `nt_mcp_plan.md`; `AGENTS.md` sim command.
- ACCEPTANCE: README exists and contains the two run commands verbatim.
- QA happy: `git diff --stat HEAD` shows README.md added.
- QA failure: README references a wrong sim path → grep shows the exact `try-robotpy` path used.
- EVIDENCE: file content; commit.
- COMMIT: `docs: README stub`

### Wave 2 — `nt_manager.py` + loopback tests

**T2.1 — Implement `nt_manager.py` core: connection + state**
- WHERE: `D:\Program\Hardware\nt-mcp-server\nt_manager.py`
- WHAT: `class NTManager` holding `self._inst: ntcore.NetworkTableInstance` (created lazily, `ntcore.NetworkTableInstance.getDefault()`), with:
  - `connect(server_ip="127.0.0.1", server_port=5810, team_number=None, identity="nt-mcp")` — `setServerTeam` if team_number else `setServer`; `startClient4(identity)`
  - `disconnect()` — `stopClient()`
  - `connection_info() -> dict` — `{connected: inst.isConnected(), connections: [{remote_id, remote_ip, last_update} for c in inst.getConnections()]}` (fields from `ConnectionInfo`)
  - module-level `_to_jsonable(value: Any) -> Any` normalizing numpy scalars (`float()`/`int()`/`bool()`) and numpy arrays → list; **`bytes` values return `None`** (raw NT topics unsupported — never leak bytes into tool output)
- REFERENCES: `nt_mcp_plan.md` §Implementation notes (API list is binding); pyntcore API docs `https://robotpy.readthedocs.io/projects/pyntcore/en/stable/`.
- ACCEPTANCE: module imports without MCP imports (`import ntcore` only); class exposes `connect/disconnect/connection_info`; `connection_info()` returns only JSON-serializable primitives.
- QA happy (unit, in T2.4): connect → `connection_info()["connected"]` is `True` against loopback server.
- QA failure: calling `connect` twice / `disconnect` before `connect` must not raise (idempotent guards `self._inst`).
- EVIDENCE: pytest output (T2.4); `git diff --stat`.
- COMMIT: `feat: nt_manager connection core`

**T2.2 — Implement read path: `list_topics`, `get`, `get_multiple`, `get_info`**
- WHERE: `D:\Program\Hardware\nt-mcp-server\nt_manager.py`
- WHAT:
  - `list_topics(prefix: str | None = None) -> list[str]` — `inst.getTopicInfo()`, filter `.name.startswith(prefix)` when given
  - `get(topic: str) -> Any` — `inst.getEntry(topic).getValue()`; return `None` if not `Value.isValid()` else `_to_jsonable(Value.value())`
  - `get_multiple(topics: list[str]) -> dict[str, Any]` — per-topic `get`, `None` for unpublished
  - `get_info(topic: str) -> dict` — match `TopicInfo.name`, return `{name, type_str, properties}` (note: `type_str` is a string, `properties` is a JSON string) or `None`
- REFERENCES: `nt_mcp_plan.md` tool surface + correctness notes (raw value only, no wrapper). `TopicInfo` exposes `name`/`type_str`/`properties` (verified in installed stub).
- ACCEPTANCE: `get` returns bare `int/float/bool/str/list` — never a dict with `valid/last_change/type/size` keys.
- QA happy: `get("/SmartDashboard/test_num")` returns `42.0` (a float), `get_multiple` returns a flat dict.
- QA failure: `get("/unpublished")` returns `None` without raising; `get_info("/unpublished")` returns `None`.
- EVIDENCE: pytest (T2.4); `git diff --stat`.
- COMMIT: `feat: nt_manager read path (list/get/get_multiple/get_info)`

**T2.3 — Implement write path + subscribe sampler**
- WHERE: `D:\Program\Hardware\nt-mcp-server\nt_manager.py`
- WHAT:
  - `set(topic: str, value: Any) -> bool` — type dispatch, `bool` before `int`: `bool`→`getEntry(topic).setBoolean(v)`; `int`→`getEntry(topic).setInteger(v)`; `float`→`setDouble(v)`; `str`→`setString(v)`; homogeneous list→`setIntegerArray`/`setDoubleArray`/`setBooleanArray`/`setStringArray` by element type. Return the boolean result of the setter. **Never call `inst.setEntryValue` (does not exist).**
  - `set_multiple(updates: dict[str, Any]) -> dict[str, bool]`
  - `async subscribe(prefixes: list[str], duration: float = 10.0) -> dict[str, list[dict]]` — `MultiSubscriber` + `NetworkTableListenerPoller` + `addListener(... kValueAll | kImmediate)`; loop `await asyncio.sleep(0.02)` draining `poller.readQueue()` until `duration` elapses; each sample `{"time": event.data.value.last_change() / 1_000_000.0, "value": _to_jsonable(event.data.value.value())}` keyed by `event.data.topic.getName()`; `close()` subscriber+poller in a `finally`
- REFERENCES: `nt_mcp_plan.md` §Implementation notes (subscribe API is binding) + "no blocking `time.sleep()` in async" rule. API names verified in installed stub (`last_change`, `ValueEventData.topic: Topic`).
- ACCEPTANCE: `set` roundtrips via `get`; `set(False)` writes a bool not an int 0; `set(42)` preserves int via `setInteger`; `subscribe` returns per-topic `{time, value}` lists and never raises on disconnect.
- QA happy: set `42` (int) → `get` returns `42` (int); set `42.5` → `get` returns `42.5`; set `True` → `get` returns `True` (type preserved).
- QA failure: `set("/x", object())` returns `False` (invalid type) without raising; `subscribe` with zero published topics returns `{}` cleanly.
- EVIDENCE: pytest (T2.4); `git diff --stat`.
- COMMIT: `feat: nt_manager write path and async subscribe sampler`

**T2.4 — Loopback unit tests**
- WHERE: `D:\Program\Hardware\nt-mcp-server\tests\test_nt_manager.py` (+ `tests/__init__.py`)
- WHAT: fixture starts a server instance (`server = ntcore.NetworkTableInstance()`, `server.startServer()` on default NT4 port 5810), publishes `/SmartDashboard/test_num=42.0`, `/SmartDashboard/test_str="hi"`, `/SmartDashboard/test_bool=True`, `/SmartDashboard/test_arr=[1,2,3]`; client `NTManager` connects via `setServer("127.0.0.1", 5810)` + `startClient4("test")`. Tests: connection_info connected; list_topics contains fixtures + prefix filter; get raw values (bare float, no wrapper); get unpublished → None; get_multiple mixed; set roundtrip incl. bool-vs-int dispatch (`setInteger` path) and list write; set invalid → False; async subscribe on a prefix for 1.0s returns ≥1 sample with `time`+`value` keys and exits cleanly.
- REFERENCES: pyntcore server API (`NetworkTableInstance.startServer`); pytest.
- ACCEPTANCE: `.venv\Scripts\python.exe -m pytest tests/ -v` → all tests pass; run with the venv python (if `pytest` is missing in the venv, install it and record in the README dev section).
- QA happy: full green run; at least one test asserts a **bare** value type (`type(value) is float`).
- QA failure: subscribe test times out or returns `{}` when it should have samples → fix sampler (listener flags / poller drain timing), re-run.
- EVIDENCE: pytest console output file saved to `tests/.pytest-report.txt` in the commit; `git diff --stat`.
- COMMIT: `test: loopback NT4 unit tests for nt_manager`

### Wave 3 — `nt_mcp_server.py` + tool tests

**T3.1 — Implement the FastMCP server (all 10 tools)**
- WHERE: `D:\Program\Hardware\nt-mcp-server\nt_mcp_server.py`
- WHAT: `from fastmcp import FastMCP`; `mcp = FastMCP("NetworkTables")`; one module-level `NTManager()`. Tools (each `@mcp.tool()`, async where blocking):
  - `async def nt_connect(server_ip="127.0.0.1", server_port=5810, team_number=None, identity="nt-mcp") -> str` — calls connect, then `await asyncio.sleep` loop up to ~2s waiting `isConnected()`; returns a status string
  - `nt_disconnect() -> str`
  - `nt_connection_info() -> dict`
  - `nt_get(topic: str) -> Any` (raw value)
  - `nt_get_multiple(topics: list[str]) -> dict`
  - `nt_get_info(topic: str) -> dict`
  - `nt_set(topic: str, value: Any) -> bool`
  - `nt_set_multiple(updates: dict[str, Any]) -> dict[str, bool]`
  - `nt_list_topics(prefix: str | None = None) -> list[str]`
  - `async def nt_subscribe(prefixes: list[str], duration: float = 10.0) -> dict`
  - `if __name__ == "__main__": mcp.run()`
- REFERENCES: `nt_mcp_plan.md` §MCP tool surface (names/signatures binding); fastmcp docs `https://gofastmcp.com/` (FastMCP + `@mcp.tool()` + `mcp.run()` stdio default).
- ACCEPTANCE: file imports with the venv python (`python -c "import nt_mcp_server"` from the project dir); `python nt_mcp_server.py` starts and stays alive on stdio (no exception for ≥5s); no `time.sleep` in any function body (grep check).
- QA happy: `fastmcp run nt_mcp_server.py` (or `python nt_mcp_server.py`) starts and the tool registry contains the 10 names.
- QA failure: server crashes at startup → traceback must show the import/registration line; fix and re-run smoke test.
- EVIDENCE: smoke-test console capture saved to `tests/.server-smoke.txt`; `git diff --stat`.
- COMMIT: `feat: fastmcp NT server with 10 tools`

**T3.2 — Tool-level tests (server against loopback server)**
- WHERE: `D:\Program\Hardware\nt-mcp-server\tests\test_mcp_server.py` (design ref: `D:\Program\Hardware\try-robotpy\nt_mcp_plan.md`)
- WHAT: reuse the T2.4 loopback server fixture; invoke the 10 tools directly (import the module, call each function) asserting: `nt_get` returns a bare value; `nt_get_info` returns metadata with `type_str`; `nt_set`→`nt_get` roundtrip (incl. `setInteger` int preservation); `nt_list_topics(prefix)` filters; `nt_connection_info` is JSON-serializable (`json.dumps` succeeds); `nt_subscribe` (async) returns serializable samples.
- REFERENCES: `nt_mcp_plan.md` §Validation (JSON-serializability is an acceptance item).
- ACCEPTANCE: `.venv\Scripts\python.exe -m pytest tests/ -v` all green (both test files).
- QA happy: every tool return passes `json.dumps` without error.
- QA failure: any tool returns a non-primitive (numpy scalar/`Value`/`bytes`) → `_to_jsonable` gap; fix and re-run.
- EVIDENCE: pytest console saved to `tests/.pytest-report.txt`; `git diff --stat`.
- COMMIT: `test: tool-level tests for MCP server`

### Wave 4 — E2E, registration, docs

**T4.1 — E2E validation against the live sim**
- WHERE: run from `D:\Program\Hardware\nt-mcp-server` while sim runs from `D:\Program\Hardware\try-robotpy`
- WHAT: start sim in background (`.venv\Scripts\python.exe -m robotpy sim --nogui` from try-robotpy, its own venv). **Pre-check:** let it run ≥10s and confirm it did not crash (the `SwerveDrive.__init__` constructs `navx.AHRS` — if the sim dies on that, the E2E target topic never appears; investigate/note before proceeding). Then run `tools/e2e_check.py` in nt-mcp-server that uses `NTManager` with defaults (`127.0.0.1:5810`) to: connect (wait up to 5s for `isConnected`), print `connection_info`, list topics, `get` the `/SmartDashboard/Swerve/Pose X` topic (published by `SwerveDrive.periodic`), `set` a marker topic `/SmartDashboard/MCP_E2E` and `get` it back, and `await subscribe(["/SmartDashboard/"], 3.0)`.
- REFERENCES: `subsystems/swerve_drive.py` (publishes `Swerve/Pose X`, line 76); `robot.py` (instantiates `SwerveDrive` in `robotInit`, runs scheduler in `robotPeriodic`); `nt_mcp_plan.md` §Validation items 2–6.
- ACCEPTANCE: harness prints `connected: True`, non-empty topic list containing `Swerve/Pose X`, a bare numeric value, `True` roundtrip, and ≥1 subscribe sample; exits 0.
- QA happy: all five assertions print PASS.
- QA failure: sim not running / port wrong → harness prints a clear error (timeout, not a hang); verify `nt_connect` returns an error string rather than blocking forever. If the sim crashes on `navx.AHRS` in `--nogui`, record the crash and confirm the topic never appears (this is a pre-existing robot-sim issue, out of scope for this plan — do NOT edit `try-robotpy` to fix it).
- EVIDENCE: `tools/.e2e-output.txt` saved in the commit; also run once more after T4.2 via the registered server.
- COMMIT: `test: e2e harness against robotpy sim + output evidence`

**T4.2 — Register the local MCP server in opencode config**
- WHERE: `C:\Users\mzzj\.config\opencode\opencode.jsonc`
- WHAT: add to the `mcp` object (preserving `context7`):
  ```jsonc
  "nt": {
    "type": "local",
    "command": ["D:\\Program\\Hardware\\nt-mcp-server\\.venv\\Scripts\\python.exe", "D:\\Program\\Hardware\\nt-mcp-server\\nt_mcp_server.py"],
    "enabled": true
  }
  ```
- REFERENCES: current config content (mcp has only `context7`); opencode MCP config docs — local `command` is **Array-only** (there is no `command`-string + `args` alternative).
- ACCEPTANCE: config file is valid (parse check); `opencode mcp` lists an `nt` server; opencode can start it and a tool call returns a raw value (verified via `tools/mcp_client_check.py`).
- QA happy: `opencode mcp list` shows `nt` with status running/available.
- QA failure: server fails to start from the config → error names the bad path; fix the absolute path and re-verify. If the `command` array form is rejected by this opencode version, verify the array is `[python.exe, script.py]` exactly (array-only format) — do NOT invent a string+args form.
- EVIDENCE: `opencode mcp list` output saved to `tools/.mcp-registration.txt`; the config diff.
- COMMIT: not committed (config is outside the repo) — record the exact change in the plan's commit-strategy notes and in README §Registration.

**T4.3 — Finalize README + final commit**
- WHERE: `D:\Program\Hardware\nt-mcp-server\README.md`
- WHAT: fill the tool table (10 tools, one-line description each), the opencode registration snippet (as registered in T4.2), the venv/Python note (3.14 or the contingency interpreter), the acceptance checklist (mirror `nt_mcp_plan.md` §Validation), and a "try-robotpy untouched" note. Ensure the whole repo has no stray files (run `git status`).
- REFERENCES: `nt_mcp_plan.md` §Validation; everything above.
- ACCEPTANCE: README contains the tool table, registration snippet, both run commands, and the acceptance checklist; `git status` clean.
- QA happy: `git log --oneline` shows the 11 commits in order.
- QA failure: README registration snippet differs from actual config → grep both and align.
- EVIDENCE: README content; `git log --oneline`.
- COMMIT: `docs: finalize README (tools, registration, acceptance)`

---

## Final verification wave

Runs in parallel after all todos; ALL must APPROVE before handoff:

- **F1 Plan-compliance audit** — every todo's commit exists; `git log` matches the commit strategy (11 commits); no stray untracked files; pins exactly `fastmcp==3.4.7`(+contingency) and `pyntcore==2026.2.2`; acceptance criteria re-runnable.
- **F2 Code-quality review** — `nt_manager.py` has no MCP imports; no `time.sleep`; `_to_jsonable` covers numpy and translates `bytes`→`None`; no dataclass with mismatched `__init__`; async tools use `await asyncio.sleep`; type annotations on all signatures (AGENTS.md style); no call to the nonexistent `setEntryValue` or `lastChange` (grep must return zero hits for both).
- **F3 Real manual QA** — run `tools/mcp_client_check.py`, a headless stdio MCP client that spawns `[python.exe, nt_mcp_server.py]` (or connects to the registered server) and calls `nt_list_topics` + `nt_get("/SmartDashboard/Swerve/Pose X")`, asserting a bare float return; evidence saved to `tools/.f3-output.txt`. This runs with the sim up (per T4.1).
- **F4 Scope fidelity** — `git status` in `try-robotpy` shows no changes (dirty tree risk check); `grep` confirms no `nt_`/`fastmcp`/`nt_mcp` additions in `try-robotpy`; `[tool.robotpy].requires` unchanged.

---

## Commit strategy

One atomic commit per todo (11 commits) in the new `nt-mcp-server` repo, messages exactly as written per todo. The global `opencode.jsonc` change (T4.2) is intentionally NOT committed anywhere (it lives outside both repos); its content is preserved in the README and in this plan. No commit ever touches `try-robotpy`.

---

## Success criteria

1. `D:\Program\Hardware\nt-mcp-server\` is a standalone git repo with own venv; `import fastmcp, ntcore` works.
2. All pytest suites green: loopback NT4 tests + tool-level tests (JSON-serializable, raw-value contract).
3. E2E against `python -m robotpy sim --nogui`: connect/list/get/set/subscribe all PASS against `127.0.0.1:5810` (with the sim-alive pre-check noted).
4. `opencode mcp` lists and starts the `nt` server; `tools/mcp_client_check.py` returns a bare value through it.
5. `try-robotpy` is byte-for-byte untouched (`git status` clean there); nothing new can reach the roboRIO deploy surface.
6. README documents run commands, registration snippet, tool table, and acceptance checklist.
7. Final verification wave (F1–F4) all APPROVE.
