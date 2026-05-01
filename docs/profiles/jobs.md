# Jobs Profile (Query Scheduler)

A queue-and-approve layer in front of the configured databases. Agents (or humans) submit a query over MCP; a human approves/rejects it in a web UI; the runner executes the approved job and writes results to disk.

The jobs profile bridges two needs in the agentic flow:

- **Read-only MCP servers** ([postgres-mcp / mongo-mcp / starrocks-mcp](database.md#mcp-servers)) are great for short ad-hoc questions but answer in-band — large result sets can blow up an agent's context, and read-only enforcement blocks any query that needs more than `SELECT`.
- **Direct DB clients** (psql, MySQL, mongosh) work for humans but aren't safe to hand to an autonomous agent.

`jobs` sits between them: agents submit through MCP tools, humans gate execution through a web UI, results land as files on disk (Parquet / CSV / NDJSON). Long-running aggregations don't block the agent's session and don't pollute its context.

## Setup

```bash
python sdp.py db setup                    # configure at least one database
python sdp.py db setup-mcp                # configure the per-DB MCP servers
python sdp.py db setup-jobs               # configure the scheduler (interactive)
python sdp.py db start                    # starts DBs + MCPs + jobs together
```

`setup-mcp` is a prerequisite — the jobs scheduler reuses the read-only DB credentials provisioned by the MCP setup flow (`.ro_credentials` in each data volume). Run `setup-mcp` before `setup-jobs`.

`db start` includes the jobs container automatically once `setup-jobs` has been run — no separate command needed. To start only the scheduler: `python sdp.py db start jobs`.

`setup-jobs` writes:

| Artifact | Purpose |
|----------|---------|
| `config/jobs/config.local.yaml` | Targets, port, timeouts, history retention, auth toggle |
| `.env` (`JOBS_PORT`, `JOBS_RESULT_ROOT`) | Compose interpolation |
| `docker-compose.override.yml` | Bind-mounts the result root into PG/SR containers as `/jobs_export` so the DB server can write results directly |
| `config/starrocks/fe.local.conf` | Sets `enable_outfile_to_local = true` (only when a SR target exists) |
| `data/jobs/`, `<JOBS_RESULT_ROOT>/` | Created on disk |

The committed `config/jobs/config.yaml` is the default template; `config.local.yaml` is gitignored and merged on top at runtime — same pattern as `postgresql.local.conf` and `fe.local.conf`.

## Targets

A **target** is a named connection profile that agents reference on every submission. One target = one backend + (optional) database. You can define several:

```yaml
# config/jobs/config.local.yaml (excerpt)
targets:
  reddit_pg:
    backend: postgres
    database: datasets
  sr_main:
    backend: starrocks
    database: ""              # SR targets have no default DB — agents fully-qualify table names
  mongo_main:
    backend: mongodb
    database: ""              # Mongo targets pick the database per query
```

- **PostgreSQL targets** require `database` (the PG database to connect to).
- **StarRocks targets** leave `database` empty by design. Queries must fully qualify (`reddit.comments`, not just `comments`) — keeps a single SR target reusable across sources.
- **MongoDB targets** leave `database` empty. Agents call `list_mongo_databases(target)` to discover what's available, then pass `database=` on each `submit_mongo_query`.

Tools for backends with no configured target are **not registered** at all — they don't appear in MCP discovery. So an agent that sees `submit_starrocks_query` knows there's at least one SR target it can reach.

## Lifecycle of a job

```
[agent submits via MCP]
        ↓
   pending/<job_id>.json     ── visible in WebUI "Pending" tab
        ↓ human clicks Approve
   approved/<job_id>.json    ── runner picks it up
        ↓
   running/<job_id>.json     ── backend is executing it
        ↓
   history/<job_id>.json     ── done | failed | rejected | cancelled
```

Each phase is a directory under `data/jobs/` containing one JSON file per job. `history_retention` (default 500) caps the history directory; older entries are pruned in submission order.

## MCP tools

The MCP endpoint is served at `http://<host>:<JOBS_PORT>/mcp` (Streamable HTTP transport). Add to your agent's MCP config the same way as the per-DB MCPs.

| Tool | Backend gate | Purpose |
|------|--------------|---------|
| `submit_postgres_query` | at least one `postgres` target | Queue a SELECT; runner wraps it in `COPY (...) TO '/jobs_export/<job_id>/<file>' WITH (FORMAT parquet\|csv)` |
| `submit_starrocks_query` | at least one `starrocks` target | Queue a SELECT; runner appends `INTO OUTFILE '...' FORMAT AS PARQUET\|CSV` (chunked output) |
| `submit_mongo_query` | at least one `mongodb` target | Queue an aggregation pipeline; runner streams the cursor into NDJSON (default) or CSV |
| `list_mongo_databases` | at least one `mongodb` target | Discover databases available on a Mongo target |
| `query_status` | always | Poll job phase + result location |
| `query_cancel` | always | Cancel a *pending* job (running jobs must be killed from the UI) |

Submission tools all require a short `description` — the human approver reads it before clicking Approve. The agent submits the SELECT or aggregation only; the runner builds the export wrapper, so agents can't smuggle in writes by appending `INSERT` / `INTO OUTFILE` clauses themselves.

### Output formats

| Backend | File extension | How it's produced |
|---------|---------------|-------------------|
| PostgreSQL | `.parquet`, `.csv` | Server-side `COPY ... TO '/jobs_export/<job_id>/<file>'` |
| StarRocks | `.parquet`, `.csv` | Server-side `INTO OUTFILE 'file:///jobs_export/<job_id>/<stem>_'` — SR chunks output, so the folder contains `<stem>_0.<ext>`, `<stem>_1.<ext>`, … (readable as a single dataset by pyarrow / polars / DuckDB) |
| MongoDB | `.ndjson` (default), `.csv` | Runner streams the cursor; CSV requires a terminal `$project` of flat scalars (runner fails fast on nested values) |

Result files land in `<JOBS_RESULT_ROOT>/<job_id>/<filename>` on the host. The folder is created per-job; agents get the path back via `query_status`.

## Web UI

`http://localhost:<JOBS_PORT>/`

| Tab | Contents |
|-----|----------|
| Pending | Queued submissions awaiting Approve / Reject. SQL is rendered with syntax highlighting; agents' `description` is shown alongside |
| Running | Currently executing jobs with elapsed time and a Kill button |
| History | Last `history_retention` jobs with status, duration, output size, and a link to result files |

Other UI features:

- **EXPLAIN** button on pending PG/SR jobs — runs `EXPLAIN` on the submitted SQL without executing the full query, so the approver can sanity-check cost before approving
- **Results preview** — for done jobs, a small sample of the output file is rendered inline
- **Disk usage panel** — shows bytes consumed by `<JOBS_RESULT_ROOT>` and free bytes on its filesystem
- **SQL formatting** — submitted SQL is pretty-printed in the pending view

## Per-backend timeouts

Set during `setup-jobs`, applied at execution time:

| Backend | Default | Notes |
|---------|---------|-------|
| PostgreSQL | 0 (no limit) | `statement_timeout` set per session; 0 disables |
| MongoDB | 0 (no limit) | `maxTimeMS` skipped when 0 |
| StarRocks | 259200 (72h) | StarRocks caps `query_timeout` at 72h and rejects 0; setup enforces 1–72h |

All three are tunable in `config/jobs/config.local.yaml` under `default_timeouts`.

## Auth (optional)

The web UI is open by default — anyone reaching `JOBS_PORT` on the host can approve / reject / kill jobs. Suitable for solo / single-user deployments.

When `auth: true` (toggleable in `setup-jobs`), the UI prompts for the database admin password — the same password used by `sdp db setup` for any auth-enabled DB. The jobs container reads the password from its process env (passed through by `sdp db start` when any DB has auth enabled) and HMAC-authenticates UI sessions. Startup fails if no password is in the env.

> [!IMPORTANT]
> UI auth is independent from MCP. The MCP endpoint at `/mcp` is not gated by the UI password — anything that can reach `JOBS_PORT` on the network can submit jobs. Treat the network boundary as the trust boundary, the same way you would for the per-DB MCP servers.

## Concurrency and resume

- `max_concurrent` (default 5) caps how many jobs run in parallel — each gets one thread from a `ThreadPoolExecutor`. Submissions beyond the cap stay in `approved/` until a slot frees up.
- The store on disk is the source of truth — if the container restarts, jobs in `pending/` and `approved/` resurface. Jobs caught mid-execution in `running/` are marked failed on next start (no automatic re-run; the agent can resubmit).

## Configuration reference

### `config/jobs/config.local.yaml`

| Key | Type | Default | Purpose |
|-----|------|---------|---------|
| `port` | int | 8050 | UI + MCP port (host-side via `JOBS_PORT` in `.env`) |
| `result_root` | path | `./data/jobs/results` | Host directory where job output files land. Bind-mounted into PG/SR as `/jobs_export` and into the jobs container as `/data/jobs/results` |
| `max_concurrent` | int | 5 | Worker pool size |
| `history_retention` | int | 500 | Number of completed jobs kept in `history/` |
| `default_timeouts.postgres` | seconds | 0 | 0 = no limit |
| `default_timeouts.mongodb` | seconds | 0 | 0 = no limit |
| `default_timeouts.starrocks` | seconds | 259200 | SR caps at 72h, rejects 0 |
| `auth` | bool | false | Require DB admin password for UI access |
| `targets.<name>.backend` | enum | required | `postgres` \| `starrocks` \| `mongodb` |
| `targets.<name>.database` | string | varies | Required for PG; empty for SR/Mongo |

### Env vars (in `.env`)

| Var | Default | Purpose |
|-----|---------|---------|
| `JOBS_PORT` | 8050 | Host port for UI + MCP |
| `JOBS_RESULT_ROOT` | `./data/jobs/results` | Host path mounted into containers as `/jobs_export` (PG/SR) and `/data/jobs/results` (jobs) |

## Removing the scheduler

```bash
python sdp.py db unsetup-jobs
```

Stops the jobs container, removes `config/jobs/config.local.yaml`, removes `JOBS_PORT` / `JOBS_RESULT_ROOT` from `.env`, and strips the `/jobs_export` mount from `docker-compose.override.yml`. The `data/jobs/` directory and result files are left alone — delete manually if no longer needed.
