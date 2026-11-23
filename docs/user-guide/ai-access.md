---
title: "AI access"
parent: "User guide"
nav_order: 10
---
# AI access

This page shows how to let an AI agent work with your databases. There are three ways: read-only MCP servers that an agent such as Claude Code, Codex or Cursor queries directly, the query scheduler, where a human approves each query the agent submits, and the built-in chat assistant.

| Way | Best for | Set up with |
|-----|----------|-------------|
| [Read-only MCP servers](#read-only-mcp-servers) | Short, ad-hoc questions: exploring the dataset and schema, and helping the agent craft complex analysis queries | `python sdp.py db setup-mcp` |
| [Query scheduler](#human-approved-queries-query-scheduler) | Human-approved queries that produce result files, keeping long aggregations out of the agent's context | `python sdp.py db setup-jobs` |
| [Chat](#the-built-in-assistant-chat) | Asking the built-in assistant about the data from the web UI | `python sdp.py chat setup` |

`python sdp.py db setup-llm` runs `setup-mcp` followed by `setup-jobs`.

## Read-only MCP servers

Each database can have an MCP (Model Context Protocol) server that exposes it to AI tools. They are optional and start together with their database.

| Database | Server | Transport | Default port | Endpoint |
|----------|--------|-----------|--------------|----------|
| PostgreSQL | `crystaldba/postgres-mcp` | SSE | 8000 (`POSTGRES_MCP_PORT`) | `/sse` |
| MongoDB | Built from `Dockerfile.mcp-mongo` | Streamable HTTP | 3000 (`MONGO_MCP_PORT`) | `/mcp` |
| StarRocks | Built from `Dockerfile.mcp-starrocks` | Streamable HTTP | 9000 (`STARROCKS_MCP_PORT`) | `/mcp` |

**1. Configure the databases with a read-only user.** Run `python sdp.py db setup` with authentication on and a read-only user. When auth is enabled, the MCP servers connect as that read-only user; without auth they use the default credentials.

**2. Configure the MCP servers.** The setup asks, for each configured database, whether to enable its server, its port, and (for PostgreSQL and MongoDB) whether to allow write access. Read-only is the default.

```bash
python sdp.py db setup-mcp           # Configure MCP servers (interactive)
```

It writes `config/db/mcp.yaml` and the MCP settings in `.env`. It needs an existing read-only user when auth is on, and refuses with the recovery command when one is missing.

The same questions can be answered in the web UI (Databases → MCP servers → Change…, with the management password); see [Setting up the MCP servers and the scheduler](webui.md#setting-up-the-mcp-servers-and-the-scheduler). Neither restarts a running server: `python sdp.py db restart-needed` names the services whose settings changed after they started.

**3. Start them.** The servers start with their databases:

```bash
python sdp.py db start               # Starts databases + MCP servers together
python sdp.py db start postgres-mcp  # Or one server: postgres-mcp, mongo-mcp, starrocks-mcp
python sdp.py db status              # Shows the MCP servers and their state
```

**4. Connect your client.** Add each server's URL to your MCP client (Claude Code, Codex, Cursor, VS Code, Claude Desktop), with `<host>` the machine running the pipeline. `setup-mcp` prints these at the end. The exact config format varies between clients; for VS Code's `mcp.json`:

```json
{
    "servers": {
        "postgres": {"url": "http://<host>:8000/sse", "type": "sse"},
        "mongodb": {"url": "http://<host>:3000/mcp", "type": "http"},
        "starrocks": {"url": "http://<host>:9000/mcp", "type": "http"}
    }
}
```

For Claude Code, the same URLs go in with `claude mcp add`, using the SSE transport for PostgreSQL and HTTP for the other two.

> The MCP servers have no login of their own: anyone who can reach their ports can query the database, with the server's database user. Without database authentication, the StarRocks server runs as `root` and can write. Enable auth with a read-only user before you open these ports beyond your machine.

To write query results to the host instead of returning them to the agent, use `/export` in SQL `COPY` or `mongoexport` (see [Connecting and querying](databases.md#connecting-and-querying)).

To remove the MCP configuration and stop the MCP containers, run `python sdp.py db unsetup-mcp`. How the servers are built, which credentials they use and where read-only is enforced is in [MCP servers](../architecture/mcp.md).

## Human-approved queries (query scheduler)

The query scheduler is for queries that should run only after a human approves them, and that produce result files. Agents submit queries over the jobs MCP at `http://localhost:8050/mcp`. The web page at `http://localhost:8050/` lets you approve, reject or kill pending jobs, EXPLAIN them against the live database, and preview completed results inline with a one-click download. Result files (Parquet, CSV or NDJSON) land in `JOBS_RESULT_ROOT/<job_id>/`, which keeps long-running aggregations out of the agent's context. An opt-in per-target auto-accept switch (with a FIFO cap) is available for trusted workloads.

Configure it with `python sdp.py db setup-jobs` after `python sdp.py db setup-mcp`. See [Query scheduler](query-scheduler.md).

## The built-in assistant (chat)

The chat is an AI assistant in a sandbox container whose only network peer is a gateway container that holds the model's credential and reaches the model and the databases' MCP servers for it. `python sdp.py chat start` attaches the running MCP server of each database that is on a read-only user. You ask it questions in the web UI's Chat window. See [Chat](chat.md).
