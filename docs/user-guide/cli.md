---
title: "CLI reference"
parent: "User guide"
nav_order: 15
---
# CLI reference

Every operation goes through `sdp.py`, in seven command groups. This page lists every command and flag; the other pages explain when to use them.

```bash
python sdp.py <db|source|run|runs|broker|webui|chat> [options]
python sdp.py <group> <command> --help      # the options of one command
```

With a `pipx` install (see [Install](install.md)) the same commands are `sdp <group> <command>`.

## Global options

| Option | What it does |
|---|---|
| `--tag` | Prefixes interactive prompts with `[tag_id]` identifiers, for automation tools like pexpect. Give it before the group: `python sdp.py --tag db setup`. |

## Databases (`python sdp.py db`)

| Command | What it does |
|---|---|
| `python sdp.py db setup` | Configure databases (PostgreSQL, MongoDB, StarRocks, optional auth): global, one-time |
| `python sdp.py db setup --add <db>` | Add or reconfigure a single database (`postgres\|mongo\|starrocks`) without touching the others |
| `python sdp.py db setup-mcp` | Configure the MCP servers for AI tool access (ports, read-only mode) |
| `python sdp.py db setup-jobs` | Configure the query scheduler (`jobs` profile): page + MCP for agent query submission |
| `python sdp.py db setup-llm` | Bundle: runs `setup-mcp` followed by `setup-jobs` |
| `python sdp.py db restart-needed` | List the running services whose settings files changed after they started (restart them to apply) |
| `python sdp.py db llm-settings` | Show the current answers of `setup-mcp` and `setup-jobs` (no secrets) |
| `python sdp.py db start [service]` | Start services: `postgres\|mongo\|starrocks\|postgres-mcp\|mongo-mcp\|starrocks-mcp\|jobs` (all configured if unspecified) |
| `python sdp.py db stop [service]` | Stop services: `postgres\|mongo\|starrocks\|postgres-mcp\|mongo-mcp\|starrocks-mcp\|jobs` (all configured if unspecified) |
| `python sdp.py db status [--verbose]` | Show database config, health, MCP and query scheduler status; `--verbose` (`-v`) adds a per-source ingestion breakdown for each configured database |
| `python sdp.py db settings [--json]` | Show the answers `db setup` takes as its defaults when run again (never a password) |
| `python sdp.py db verify [--db <name>] [--json]` | Check config, env, credentials, mounts, MCP and scheduler coherence; exits 1 on drift. Use as a CI or pre-pipeline preflight. `--db` checks a single database (the cross-cutting MCP and scheduler checks still apply); `--json` emits machine-readable JSON |
| `python sdp.py db recover-password [--regenerate-ro]` | Reset the database admin password (requires auth enabled). With `--regenerate-ro`, also rewrite `.ro_credentials` with a fresh password (combinable with the admin recovery; use it when `.ro_credentials` is missing, lost, or known-leaked) |
| `python sdp.py db create-indexes [--source <name>]` | Interactively create database indexes (`-s` for short; auto-selects when only one source is configured) |
| `python sdp.py db index-targets --source <name> --db <db>` | List the tables, columns and existing indexes `create-indexes` offers for one database of a source |
| `python sdp.py db unsetup` | Remove the database configuration; data deletion is behind a double confirmation |
| `python sdp.py db unsetup --db <postgres\|mongo\|starrocks>` | Fully remove one database (config, containers, and data) without touching the others |
| `python sdp.py db reset --db <postgres\|mongo\|starrocks>` | Wipe one database's data while preserving all config, env, MCP, and per-source overrides. Use it to re-ingest from scratch |
| `python sdp.py db unsetup-mcp` | Remove the MCP configuration and stop the MCP containers |
| `python sdp.py db unsetup-jobs` | Remove the query scheduler's configuration and stop the jobs container |

`db setup` generates `.env`, `config/db/*.yaml`, `config/postgres/postgresql.local.conf`, and `config/starrocks/{fe,be}.conf` (plus `pg_hba.local.conf` and `.ro_credentials` when auth is enabled). Teardown comes in three depths:

| Command | Removes | Keeps |
|---|---|---|
| `db unsetup` | Everything (double confirmation) | Nothing |
| `db unsetup --db <db>` | One database end to end: containers, config, data, env and compose entries | The other databases |
| `db reset --db <db>` | That database's data and ingestion state | All config, env, MCP, and per-source overrides; `db start` brings it back up ready for re-ingestion |

## Sources (`python sdp.py source`)

| Command | What it does |
|---|---|
| `python sdp.py source add <name>` | Add a new data source (interactive setup) |
| `python sdp.py source add <name> --hf <dataset_id>` | Add from a Hugging Face dataset (fetches metadata as defaults) |
| `python sdp.py source download <name> [--token <token>] [--data-type <type>]` | Download Hugging Face dataset files (mirrors to `data/dumps/`, organizes to `data/extracted/`). `--token` for private datasets (falls back to the `HF_TOKEN` environment variable); `--data-type` downloads only that data type |
| `python sdp.py source hf-inspect <owner>/<name> [--token <token>]` | Show a Hugging Face dataset's schema groups, as `--hf` will offer them |
| `python sdp.py source configure <name>` | Reconfigure an existing source (platform-specific) |
| `python sdp.py source add-classifiers <name>` | Add ML classifiers for a source |
| `python sdp.py source settings <name> [--json]` | Show the current answers `source configure` takes as its defaults (never a secret) |
| `python sdp.py source remove <name>` | Remove a source's configuration |
| `python sdp.py source list` | List configured sources |
| `python sdp.py source status [name]` | Show source processing and ingestion status (all sources by default) |
| `python sdp.py source progress [name] [--json]` | Show how many files each processing stage (parse, lingua, classifiers) has finished, counted in the stage output folders |
| `python sdp.py source size [name] [--json]` | Show how much disk space each source's dumps, extracted, parsed and output folders take (regular files only, links inside a folder not followed), and their total |
| `python sdp.py source error-logs [name] [--profile <profile>]` | Show database ingestion error logs; `--profile` (`-p`) filters by ingestion profile: `postgres_ingest`, `postgres_ml` or `mongo_ingest` |

`source add` walks you through platform selection, file patterns, fields, indexes, and optional classifier configuration. The name decides the platform: `reddit` uses the Reddit platform, any other name a platform of your own. With `--hf`, it fetches the Hugging Face dataset's metadata (configs, fields, types) to pre-populate defaults; you still go through the full interactive setup. All per-source config is written to `config/sources/<name>/`.

`source status` reads pipeline state files to show ingestion progress (datasets processed, in progress, failed) without querying the database. `source error-logs` shows the full error details and the relevant mongoimport log output for failed datasets.

## Pipeline (`python sdp.py run`)

| Command | What it does |
|---|---|
| `python sdp.py run <profile>` | Run a pipeline profile |
| `python sdp.py run <profile> --source <name>` | Run for a specific source (`-s`; auto-selects if only one is configured) |
| `python sdp.py run <profile> --build` | Rebuild the Docker image before running |
| `python sdp.py run <profile> --filter <pattern>` | Only process files matching the pattern (`-f`; an fnmatch glob on the file ID, e.g. `"*2024*"`, `"RS_2024-*"`) |
| `python sdp.py run parse --skip-lingua-files` | Parse only: also skip files that already have a lingua output (`<OUTPUT_PATH>/lingua/<data_type>/<id>_lingua.{csv,parquet}`), so you can delete old extracted and parsed files without re-decompressing on the next run |
| `python sdp.py run <profile> --classifier <name>` | `ml`, `lingua` and `sr_ml` only: run a single classifier from the configured list (`-c`; e.g. `toxic_roberta`) instead of all of them |

Valid profiles: `parse`, `lingua`, `ml`, `postgres_ingest`, `postgres_ml`, `mongo_ingest`, `sr_ingest`, `sr_ml`. The `jobs` profile starts with `python sdp.py db start jobs`, not `run`. `--build` is needed after code or dependency changes.

## Runs (`python sdp.py runs`)

| Command | What it does |
|---|---|
| `python sdp.py runs list [--limit N] [--json]` | List the pipeline runs: the recorded runs (started from the web UI), newest first and at most N of them (1 to 1000, default 50), then the runs started in a terminal that are alive now; with id, kind, profile, source, state and start time |
| `python sdp.py runs logs <run_id> [--tail LINES] [--follow]` | Print the output of a run; `--tail` prints only the last LINES lines, `--follow` keeps printing until the run ends (Ctrl-C to stop) |
| `python sdp.py runs logs <run_id> [--offset BYTES] [--limit BYTES]` | For a recorded run, start at byte `--offset` of its log (default 0) and print at most `--limit` bytes (default: to its end); for the output Docker holds, `--limit` keeps its end (default 65536 bytes) |
| `python sdp.py runs cancel <run_id>` | Cancel a run: its container is sent the interrupt signal |

A run started with `python sdp.py run` in a terminal is found through Docker while its container is running, with an id of the form `live-<12 hex digits>`; a recorded run has an id of the form `r<date>T<time>-<6 hex digits>`. `runs logs` and `runs cancel` take both. A pipeline started with `docker compose up` instead of `python sdp.py run` is not listed, and neither is a run that is still in its checks or its image build (it has no container yet). The output of a run started in a terminal is at most its last 500 lines. `runs cancel` sends the run's container the interrupt signal, so the stage ends through its own interrupt handling instead of being killed. The `runs` commands refuse a `.sdp/` folder in the deployment folder that belongs to another user, with the message `the run state folder cannot be used`.

## Broker (`python sdp.py broker`)

| Command | What it does |
|---|---|
| `python sdp.py broker passwd` | Set the management password with which a session logs in (asked twice, not echoed) |
| `python sdp.py broker start [--foreground]` | Start the broker in the background (or in this terminal with `--foreground`, until Ctrl-C) |
| `python sdp.py broker stop [--force]` | Stop the broker; it lets a running action finish first, `--force` ends that action |
| `python sdp.py broker status` | Say whether the broker runs, its pid, since when, the socket's path, and whether a password is set |
| `python sdp.py broker audit [--limit N] [--json]` | Print the newest N lines (1 to 10000, default 50) of the broker's audit log |
| `python sdp.py broker audit --verify` | Check the chain of the whole audit log (exit code 1 on a break or a log that cannot be read) |

The broker is a host process on a unix socket through which a logged-in web UI session runs a fixed list of typed actions. See [Web UI](webui.md#commands).

## Web UI (`python sdp.py webui`)

| Command | What it does |
|---|---|
| `python sdp.py webui setup` | Ask the port, the addresses the page is opened at besides this machine (IP addresses and names, or `all` for every address) and the certificate (generated or your own); build the image `sdp-webui`; print the certificate's fingerprint |
| `python sdp.py webui start` | Start the web UI's container and say whether the broker answered from inside it |
| `python sdp.py webui stop` | Stop and remove the container (exit code 1 when it was not running) |
| `python sdp.py webui rebuild` | Rebuild the image `sdp-webui` from the saved settings, with no questions, then stop the web UI (when it runs) and start it; logged-in sessions end. A failed build leaves the running web UI as it was (exit code 1). Use it to pick up new code; run `webui setup` only to change the settings |
| `python sdp.py webui status` | Say whether it runs, where it answers and the certificate's fingerprint and end date (exit code 0 when it runs) |
| `python sdp.py webui unsetup` | After a confirmation, remove the container, the network, the image and `.sdp/webui/` |

See [Web UI](webui.md).

## Chat (`python sdp.py chat`)

| Command | What it does |
|---|---|
| `python sdp.py chat setup` | Ask the back end (Claude Code on your own Claude subscription login, or an OpenAI-compatible endpoint with its address and model), the secret (asked without echo, stored in `.sdp/chat/credential/model.token`, mode 0600) and the work folder's soft cap; build the images `sdp-gateway` and `sdp-sandbox` |
| `python sdp.py chat start` | Start the gateway and the sandbox, and attach the running MCP server of each database that is on a read-only user; refuses when no database can be read, saying why for each |
| `python sdp.py chat stop` | Stop and remove both containers and the chat's two networks, and detach the databases (exit code 1 when it does not run) |
| `python sdp.py chat rebuild` | Rebuild the images `sdp-gateway` and `sdp-sandbox` from the saved settings, with no questions, then stop the chat (when it runs) and start it. A failed build leaves the running chat as it was (exit code 1). Use it to pick up new code; run `chat setup` only to change the settings |
| `python sdp.py chat status` | Say whether each container runs, the back end, which databases the chat can read now (and which dropped out since the start) and the work folder's size against its soft cap (exit code 0 when it runs) |
| `python sdp.py chat settings` | Show the chat's settings for the whole deployment: the read-only query limit of its MongoDB and StarRocks MCP servers (`CHAT_QUERY_TIMEOUT_SECONDS`, 90 seconds by default); PostgreSQL ignores it |
| `python sdp.py chat query-limit [<seconds>]` | Show the read-only query limit, or set it (whole seconds, 1 to 3600): written to `.env`, and when the chat runs its MongoDB and StarRocks MCP servers are started again with it (the gateway and the sandbox go on; running chats keep their own waits until `chat stop` and `chat start`) |
| `python sdp.py chat auto-title [on\|off]` | Show whether a new chat gets a short title made from its first question by the chat's model (`CHAT_AUTO_TITLE`, on by default), or set it: written to `.env`, taken by the chat at its next start (`chat stop`, then `chat start`); nothing is restarted |
| `python sdp.py chat prompt [--instructions-file=<path>] [--queue-file=<path>] [--reset]` | Show the starting prompt of new chats (its instructions and queue instructions; the deployment's facts are always added), set a part from a UTF-8 file (the other part is kept), or go back to the built-in prompt; written to `.sdp/chat/prompt/prompt.json`, used at once by chats made afterwards, while each chat keeps its own |
| `python sdp.py chat unsetup` | After a confirmation, remove the two images and the chat's settings and credential; after a second one, also all chats and their files (`.sdp/chat/work`). Refused while the chat's containers exist |

You ask the chat in the web UI's Chat window (start the web UI after `chat start`). See [Chat](chat.md).
