---
title: "MCP servers"
parent: "Architecture"
nav_order: 7
---
# MCP servers

Three optional MCP (Model Context Protocol) servers expose the databases to AI tools over HTTP, one per database. `python sdp.py db setup-mcp` writes `config/db/mcp.yaml` and the `*_MCP_*` keys in `.env`; `python sdp.py db start` starts each enabled server's `<db>_mcp` profile once its database is healthy. This page describes how the servers are built, which credentials they use, and where read-only access is enforced. Connecting a client is on [AI access](../user-guide/ai-access.md).

## How the servers are built

| Profile / service | Built from | Entrypoint | Port (container, host variable, default) | Transport, endpoint |
|-------------------|------------|------------|------------------------------------------|---------------------|
| `postgres_mcp` / `postgres-mcp` | Image `crystaldba/postgres-mcp`, pulled | `config/mcp/entrypoint-postgres.sh`, bind-mounted at `/mcp-config` | 8000, `POSTGRES_MCP_PORT`, 8000 | SSE, `/sse` |
| `mongo_mcp` / `mongo-mcp` | `Dockerfile.mcp-mongo`: `kiliczsh/mcp-mongo-server` at a pinned commit plus three patches, built with bun, run on `node:22-slim` | `config/mcp/entrypoint-mongo.sh`, copied into the image | 3000, `MONGO_MCP_PORT`, 3000 | Streamable HTTP, `/mcp` |
| `starrocks_mcp` / `starrocks-mcp` | `Dockerfile.mcp-starrocks`: `mcp-server-starrocks==0.3.0` from PyPI | `config/mcp/entrypoint-starrocks.sh`, copied into the image | 9000, `STARROCKS_MCP_PORT`, 9000 | Streamable HTTP, `/mcp` |

The container port is fixed; the `*_MCP_PORT` variables change only the host side of the mapping. Because the mongo and starrocks entrypoints are copied into their images, an edit to them takes effect only after the image is rebuilt (`docker compose --profile mongo --profile mongo_mcp build mongo-mcp`, and the same shape for starrocks); `python sdp.py db start` does not rebuild MCP images.

The three MongoDB patches, applied in order on the pinned commit:

| Patch | What it adds |
|-------|--------------|
| `config/mcp/mongo-mcp-multidb.patch` | A `database` argument on every tool and a `listDatabases` tool, since MongoDB holds one database per source and the connection URI names none |
| `config/mcp/mongo-mcp-maxtimems.patch` | `maxTimeMS` on `query`, `aggregate` and `listCollections`, so long queries can be bounded |
| `config/mcp/mongo-mcp-readonly.patch` | Hides the write tools (`insert`, `update`, `createIndex`) in read-only mode, and names `MCP_MONGODB_READONLY` in the refusal message |

## Which credentials a server uses

Each server uses the database's read-only user when its user variable is set: `POSTGRES_MCP_USER`, `MONGO_MCP_USER` (passed into the container as `MCP_MONGODB_USER`) or `STARROCKS_MCP_USER`. `python sdp.py db setup-mcp` writes these only when auth and a read-only user exist, and refuses to run when auth is on but a database has no read-only user or its `.ro_credentials` file is missing. The entrypoint then reads the password from the parent database's `.ro_credentials`, mounted read-only into the MCP container, and exits with an error when the file is empty or missing.

When the user variable is not set (auth off), each server falls back:

| Server | Connects as |
|--------|-------------|
| postgres | `DATABASE_URI` from compose: the `postgres` user |
| mongo | The unauthenticated `mongodb://mongo:27017` |
| starrocks | `root` with an empty password |

The database entrypoint wrappers create or update the read-only user on every start, from `.ro_credentials` in the data volume. `python sdp.py db setup-mcp` only configures the MCP server settings (ports, access mode); it needs an existing read-only user when auth is on. Enabling auth after `setup-mcp` needs a `setup-mcp` re-run. `python sdp.py db unsetup-mcp` removes the MCP configuration and stops the MCP containers.

## Where read-only is enforced

Read-only is the default, and it is enforced in a different layer for each server:

| Server | Application layer | Database layer (auth on) |
|--------|-------------------|--------------------------|
| postgres | `--access-mode=restricted`, from `POSTGRES_MCP_ACCESS_MODE`; `unrestricted` when you allow write access in `setup-mcp` | Read-only role with `pg_read_all_data` |
| mongo | `--read-only` when `MCP_MONGODB_READONLY=true` (from `MONGO_MCP_READ_ONLY`, default `true`), plus the readonly patch | Read-only user with `readAnyDatabase` and `clusterMonitor` |
| starrocks | None: the server has no read-only flag and keeps its write tool | The read-only user's `sdp_readonly` role, SELECT only |

So with auth on, allowing write access in `setup-mcp` changes nothing in practice, because the read-only user cannot write. With auth off, the StarRocks MCP runs as `root` with full write access.

## Network exposure

The servers have no authentication of their own. Compose publishes each one as `"${X_MCP_PORT}:<port>"` on all host interfaces, so anyone who can reach the host's port can use it; the only access control is the database credential the server uses. Each MCP service waits for its database with `depends_on: condition: service_healthy`, and under auth with a read-only user each database health check logs in as that user, so an MCP server does not start against a missing or drifted read-only user.
