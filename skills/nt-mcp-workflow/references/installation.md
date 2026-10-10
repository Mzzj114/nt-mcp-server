# Install the NetworkTables MCP server

The server uses stdio and is normally started by the agent client. 

## Prerequisites

- Install [uv](https://docs.astral.sh/uv/).

## OpenCode: install from Git

Add this to the project's `opencode.jsonc`:

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

Then verify the server:

```powershell
opencode mcp list
```

OpenCode prefixes the model-facing tool names with the configured server name.
For example, the server tool `nt_connect` appears as `nt_nt_connect`. This is
only a client-side namespace; the MCP server still receives `nt_connect`.

## Local checkout

To run the checked-out source instead of downloading it from Git, configure
the server from the repository directory:

```jsonc
{
  "mcp": {
    "nt": {
      "type": "local",
      "command": ["uv", "run", "nt-mcp-server"],
      "enabled": true
    }
  }
}
```

The equivalent manual command is:

```powershell
uv run nt-mcp-server
```

Keep this process attached to the MCP client; it communicates over stdin/stdout.
Do not print diagnostic output to stdout.

## Other clients

The server can also be registered with clients that launch stdio MCP servers. Details may vary due to different clients, but the overall idea is the same.
For example:

```powershell
codex mcp add nt -- uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-mcp-server
claude mcp add --scope project nt -- uvx --from git+https://github.com/Mzzj114/nt-mcp-server.git nt-mcp-server
```

Tool naming and configuration syntax are client-specific, so use the name
shown by the client after it discovers the server.

## First live connection

After the server is registered, connect to the simulator or robot with the
client's exposed `nt_connect` tool. The default simulator target is:

```text
server_ip=127.0.0.1
server_port=5810
```

For a real robot, use either `team_number` or `server_ip`/`server_port`, not
both. On a robot network, a second network adapter may be needed to retain
Internet access; see `network-setup.md`.
