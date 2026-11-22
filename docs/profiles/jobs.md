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
- **Auto-accept** — opt-in panel for skipping the manual approve step on trusted targets (see below)

## Auto-accept

A pill next to the **SDP Jobs** title in the navbar. Click it to open a floating popover with a row per configured target. Each row has a slider (0..`max_concurrent`) and an enable switch. When a target's switch is on, the runner approves its pending jobs automatically up to the slider's value (FIFO).

- **Per-target only** — there is no master toggle. A target with the switch off never auto-approves; a target with it on auto-approves up to the slider's value. Targets the user has never touched default to disabled.
- **Default limit = `max_concurrent`** — first time you flip a target's switch, the slider is at its top (the runner's global cap). Lower it if you want to leave headroom for other targets.
- **FIFO** — within a target, the oldest pending job is approved first (by submission time).
- **Global cap still applies** — the runner's `max_concurrent` (config) gates the total number of jobs *executing* at once. Auto-accept just removes the human approval step; it doesn't bypass the worker pool. Slider values are clamped to `max_concurrent`.
- **Slot accounting** — the cap counts `approved + running` for the target, so a backed-up approved queue doesn't sneak past the limit while the runner is busy on other targets.
- **Persistence** — settings are stored in `data/jobs/auto_accept.json` (atomic writes). State survives container restarts; deleting the file resets every target to disabled.

Settings update reactively — flipping a switch or releasing a slider posts the change immediately. There is no Save button.

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

## Control socket

When its data folder (`data/jobs` on the host) belongs to a user other than root, the scheduler opens a unix socket at `data/jobs/control/door.sock` for that user, the operator. The folder is made with mode 0700 and the socket with mode 0600, both given to the owner of `data/jobs`; a connection from any other user (root included) is closed without a reply. The web UI's Queue window uses it (see [The web UI](../guides/webui.md#the-queue-window)): `sdp webui start` mounts the control folder read-only into the web UI's container. Through it a program of the operator can list the jobs of each tab, read one job with a digest of what it runs, approve it (only when that digest still matches the job as it is at that moment), reject it with a reason, kill it, run EXPLAIN on it (on an admin connection, like the EXPLAIN button), preview a finished result and change the auto-accept settings. Who approved or rejected a job, and who asked to kill it, through the socket is kept in its record (`acted_by`). The kill's label names who asked: it is set on a job that may still finish by itself, and it is lost when the scheduler restarts before the job is retired. Every approval, rejection, kill, EXPLAIN and auto-accept change through the socket is logged twice, the attempt and its outcome. The page, its port and its password gate are unchanged.

`sdp db setup-jobs` makes `data/jobs` as the operator. If Docker made it instead (a `db start jobs` before setup), it belongs to root and the socket stays closed. Each reason the socket stays closed is one line in the scheduler's log (`docker logs <jobs container>`), beginning with "the control socket stays closed:":

| Reason in the log | What to do |
|---|---|
| the data folder cannot be opened | Check that `data/jobs` exists and is a folder |
| the data folder belongs to root | `sudo chown <you>:<your group> data/jobs`, then restart the scheduler |
| others may write into the data folder | `chmod o-w data/jobs`, then restart the scheduler; anyone could otherwise move the control folder away. Write for the folder's group is accepted (a private group of your own is the common default) |
| the results folder is the data folder or contains it | Set `JOBS_RESULT_ROOT` (`python sdp.py db setup-jobs`) to a folder that is not `data/jobs` and does not contain it: the database servers can write into the results folder |
| the results folder lies inside the control folder | Set `JOBS_RESULT_ROOT` to a folder outside `data/jobs/control` |
| the results folder cannot be looked at | Check that the results folder exists; it also stays closed when the folders up to three levels below the results folder (job folders aside) hold more than 10,000 entries |
| the control folder is not a real folder | Remove the link or file at `data/jobs/control`; the scheduler makes the folder at its next start |
| the control folder belongs to another user than the data folder | `sudo chown <owner of data/jobs> data/jobs/control` |
| the control folder is open to the group or to others | `chmod 700 data/jobs/control` |
| the control folder could not be made or given to the data folder's owner, or cannot be used | Check the permissions of `data/jobs` |
| something other than a socket is in the place of door.sock | Remove `data/jobs/control/door.sock` |
| the socket could not be made, or its thread could not be started | Restart the scheduler; a socket left by a scheduler that was killed is replaced by itself |

The scheduler never deletes or re-creates `data/jobs/control` itself. The socket is removed when the scheduler stops. The scheduler's code is in its image, so `python sdp.py db start jobs` (which rebuilds the image) is what brings the socket to a running deployment.

## Concurrency and resume

- `max_concurrent` (default 20) caps how many jobs run in parallel — each gets one thread from a `ThreadPoolExecutor`. Submissions beyond the cap stay in `approved/` until a slot frees up.
- The store on disk is the source of truth — if the container restarts, jobs in `pending/` and `approved/` resurface. Jobs caught mid-execution in `running/` are marked failed on next start (no automatic re-run; the agent can resubmit).

## Configuration reference

### `config/jobs/config.local.yaml`

| Key | Type | Default | Purpose |
|-----|------|---------|---------|
| `port` | int | 8050 | UI + MCP port (host-side via `JOBS_PORT` in `.env`) |
| `result_root` | path | `./data/jobs/results` | Host directory where job output files land. Bind-mounted into PG/SR as `/jobs_export` and into the jobs container as `/data/jobs/results` |
| `max_concurrent` | int | 20 | Worker pool size |
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
