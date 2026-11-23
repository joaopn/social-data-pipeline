---
title: "Chat"
parent: "User guide"
nav_order: 13
---
# Chat

The chat is an AI assistant that answers questions about your databases, writes files and plots, and can submit long queries to the query scheduler's queue for your approval. You ask it in the [web UI](webui.md)'s Chat window; this page covers setting it up and running it.

The assistant works in a sandbox container whose only network peer is a gateway container. The gateway holds the model's credential, so the sandbox never sees it, and reaches the model and the databases' MCP servers for it. What the sandbox runs and what it can reach is in [Broker, web UI and chat](../architecture/control-plane.md#the-chat).

## Back ends

| Back end | What it uses | What setup asks |
|---|---|---|
| `claude-code` | Claude Code on your own Claude subscription login | The token: run `claude setup-token` on a machine where you are logged in to Claude Code and paste what it prints. This uses YOUR subscription, under the vendor's terms for it. |
| `openai-compatible` | An endpoint that speaks the OpenAI API, such as OpenRouter | The endpoint's address (default `https://openrouter.ai/api/v1`), the model name, and the endpoint's API key |

The address must be `https://host[:port]/path`: https only, and no user name, query, fragment, `..` or `%`. The model name is 1 to 200 visible ASCII characters without spaces.

## Set up and start

**1. Have a database the chat can read.** The chat reads a database only through its MCP server on the database's read-only user. Set the database up with authentication and a read-only user, then its MCP server, then start it.

```bash
python sdp.py db setup                      # authentication and a read-only user
python sdp.py db setup-mcp                  # the MCP servers
python sdp.py db start                      # databases, MCP servers, scheduler
```

**2. Set up the chat.** It asks the back end, the secret and the work folder's cap, and builds the images `sdp-gateway` and `sdp-sandbox`.

```bash
python sdp.py chat setup
```

**3. Start it.**

```bash
python sdp.py chat start                    # gateway and sandbox; attaches the databases
```

**4. Start the web UI, or restart it.** The Chat window is in the web UI; start it once the chat runs (stop and start it when it already ran).

```bash
python sdp.py webui start
```

## What chat setup asks

1. **The back end** (1: `claude-code`, 2: `openai-compatible`).
2. **For `openai-compatible`:** the address and the model name.
3. **The secret** (the token or the API key), asked once and without echo, as `python sdp.py broker passwd` asks its password. White space around it is dropped; it is refused when it is empty, longer than 4096 characters, or holds anything but visible ASCII characters (a space inside included).
4. **The soft cap of the chats' work folder** in GiB (a whole number from 1 to 10000, default 10).

On a second setup the earlier answers are the defaults. When a credential is already stored, setup first asks whether to replace it; the default keeps it and asks no secret. When you choose another back end than the saved one, that question is not asked and a new secret is required, because the stored one belongs to the other back end; the same holds when the settings are missing, cannot be read or name no back end, because then the stored secret's back end is not known.

A refused address, model name or cap is asked again; a refused secret ends the setup with nothing saved. Setup refuses a deployment folder whose path holds a comma or a line break. At its start it removes a leftover temporary file (a regular file only) of the credential (`model.token.<8 hex>.tmp`, left by an interrupted write) from `credential/`.

Then it writes the settings and the credential and builds the two images (see [The chat's images](../architecture/control-plane.md#the-chats-images)). It ends with a summary (the back end, the address and model, the cap), names the command that starts the chat, and says that the Chat window is in the web UI (started, or restarted, with `python sdp.py webui start` once the chat runs). The secret and its length are never printed, logged, passed on a command line or put in an environment.

### What it stores

Everything is under `.sdp/chat/` in the deployment folder:

| Path | Mode | What it holds |
|---|---|---|
| `.sdp/chat/` | 0700 | The chat's state |
| `.sdp/chat/config.json` | 0600 | The back end; for `openai-compatible` the address (`upstream`) and the model (`model`); the work folder's cap (`work_cap_gib`); the Claude Code version of the sandbox image (`claude_code_version`) |
| `.sdp/chat/credential/` | 0700 | The credential's folder |
| `.sdp/chat/credential/model.token` | 0600 | The secret, and nothing else: a file only you can read |
| `.sdp/chat/work/` | 0700 | The chats: one folder per chat, made by the runner (see [A chat's files](#a-chats-files)); the sandbox's `/work` |
| `.sdp/chat/socket/` | 0700 | The sandbox's `/run/sdp-chat`, which holds the runner's socket `runner.sock` (0600) while the chat runs |
| `.sdp/chat/gateway/` | 0700 | The gateway's config (`config.json`, 0600), written by `chat start` and removed by `chat stop`; it holds no secret |

The modes are set explicitly, whatever your umask. Each folder must be a real folder that belongs to you: a symbolic link or a folder of another user in its place is refused and left as it is. The settings and the secret are written to a temporary file made with mode 0600 and then renamed, so a file is never half written.

## What chat start does

It refuses, with the command to run:

- when the chat is not set up (the settings, the token file and both images must be there);
- when Docker runs rootless or remaps user ids (the sandbox's files would land on this machine owned by another user, and the gateway could not read your token);
- when the chat already runs (stop it first);
- when a container `sdp-gateway` or `sdp-sandbox`, or a network `sdp-chat` or `sdp-chat-up`, belongs to another deployment;
- when Docker refuses or ignores the isolated gateway mode of the chat's network: it then says that a newer Docker is needed (it never falls back to a plain internal network).

A container or network left by an earlier start that does not run is removed.

**Which databases the chat reads.** For each of `postgres-mcp`, `mongo-mcp` and `starrocks-mcp`, the running container of this deployment is attached only when its MCP server is on the database's read-only user: its read-only user key (`POSTGRES_MCP_USER`, `MCP_MONGODB_USER`, `STARROCKS_MCP_USER`) has a value. Docker is asked only whether that key is set; nothing of the container's environment, which holds passwords, is read or printed. A database is left out when its MCP server does not run, or is not on a read-only user. When no database can be attached, start refuses, names each database with its reason, says how to fix it (set the database up with authentication and a read-only user with `python sdp.py db setup`, its MCP server with `python sdp.py db setup-mcp`, and start it with `python sdp.py db start`), and makes nothing.

**The query time limit.** The chat does not use the shared `mongo-mcp` and `starrocks-mcp` containers, which other MCP clients use unchanged: for each of these two databases that passes the check above, start runs the chat's own copy of its MCP server, the compose service `chat-mongo-mcp` or `chat-starrocks-mcp` (the same image, environment and read-only data mount, no published port), and attaches that copy. The copy stops every read-only query after `CHAT_QUERY_TIMEOUT_SECONDS` of the deployment's `.env` (whole seconds from 1 to 3600; 90 when the key is missing or empty), and the database enforces it: MongoDB through `maxTimeMS` on every tool that runs a query (the smaller of the one asked and the limit), StarRocks through its session variable `query_timeout`, set on every connection (a statement that names `query_timeout` is refused). PostgreSQL ignores it: the chat uses the shared `postgres-mcp`, which keeps its own limit (30 seconds in its restricted mode, the default). Start also refuses when the `mongo-mcp` or `starrocks-mcp` image is missing or was built before the limit (it would ignore it), naming the build command. A malformed or out-of-range value makes start refuse, naming the key, before it makes anything. To change the limit, see [The query time limit](#the-query-time-limit). Start prints the limit in force; the assistant is told it, and a query that may take longer goes to the query queue when it is attached. `chat stop` removes the copies. After updating the clone, rebuild the `mongo-mcp` and `starrocks-mcp` images (`docker compose --profile mongo --profile mongo_mcp build mongo-mcp`, and the same for starrocks) and the chat's images (`python sdp.py chat rebuild`) for the limit to take effect.

Then it makes the chat's two networks, the gateway and the sandbox (the steps are in [What chat start makes](../architecture/control-plane.md#what-chat-start-makes)). When a step fails, start removes what it made (containers, networks, attachments, the gateway's config) and says what failed. It ends with a summary: the back end, which databases the chat can read, for each one left out why, the query time limit in force (and that PostgreSQL ignores it), whether the query queue is attached (or one line saying why the assistant cannot use it), and that the Chat window is in the web UI (started, or restarted, with `python sdp.py webui start`). The token is never read, printed or passed by the command: the gateway reads it from its mounted file.

## What you can ask

Ask about the data in your databases. In the Chat window, Enter sends the question and Shift+Enter starts a new line. Each question is one **turn** of the assistant, run in the chat's own folder; the window's right column ("This chat") adds up the turns of the chat, their time and their cost as the backend reported them (see [The Chat window](webui.md#the-chat-window)):

- **The databases.** It reads each attached database through that database's MCP server, read-only.
- **A look first.** The first time it uses a table or collection in a chat, it checks its columns and their types, a few rows (at most 5) and how the table is laid out (on StarRocks `SHOW CREATE TABLE`: its indexes, sort key and partitions), so its queries use the real field names and the indexes. A column that names a row's source file or period, like `dataset` (the dump month) in Reddit tables, is the way it counts per period.
- **Where a query runs.** It judges a query by what it must read, not by the table's size: an aggregation over one or two small or indexed columns runs at once on the read-only connection, even on a very large StarRocks table. It sends a query to the queue only when it must read many columns of very large tables, join large tables, or the read-only query is refused or times out. It is told the query time limit of MongoDB and StarRocks (see [What chat start does](#what-chat-start-does)), and with the queue attached it sends a query on them that may take longer there.
- **The shell and files.** It can run shell commands and read and write files in the chat's folder (with `claude-code`: the tools `Bash`, `Read`, `Write`, `Edit`, `Glob` and `Grep`; with `openai-compatible`: `run_shell`).
- **Query results in scripts.** A script it writes in the sandbox gets query results with `sdp_data.read_query(server, query)` (see [Query results in a script](#query-results-in-a-script)), so the numbers in a chart come from the database, not copied by hand. The shell has no other way to a database: no database driver is installed and only the gateway can be reached.
- **Plots.** It makes every chart with Altair, unless you ask for another library (a request for an image or a PNG is still an Altair chart), and saves each one twice in its folder, as `<name>.vl.json` (the chart's Vega-Lite spec, with its data inside) and as `<name>.png`, and names both in its answer. The Chat window draws the spec as an interactive chart (hover tooltips, zoom and pan, a legend that picks a series, in the page's colours), with the PNG as the fallback and the download; see [Files and plots](webui.md#files-and-plots). When the values span several orders of magnitude, it is told to draw one chart with a log y axis rather than two panels.
- **No internet.** It cannot reach the internet, and it is told never to suggest sending your data or files to websites or services outside your deployment.
- **Memory.** The assistant remembers the chat: each later question continues the same conversation. A new chat starts from nothing.

### The model of a chat

With `claude-code`, each chat runs on one of `opus`, `sonnet` and `haiku`, or on Claude Code's own default ("backend default"). Choose it in the Chat window's header; the next question you send runs on it, and the chat keeps it for its later questions until you choose another. A new chat starts on the default model of your browser (set in the web UI's settings) when it is one of these, else on Claude Code's default. The chat's gateway forwards only these three models, by the ids the pinned Claude Code sends for them. With `openai-compatible`, every chat runs on the model saved at setup, which the header shows; there is nothing to choose. The list is fixed: `chat setup` asks nothing about it.

The sandbox image has Python 3.13 with pandas, numpy, matplotlib, pyarrow, Altair 6.3.0 and vl-convert 1.9.0 (which writes Altair's PNG inside the sandbox, without a browser or the network). How each back end runs a turn is in [What the sandbox runs](../architecture/control-plane.md#what-the-sandbox-runs).

### Query results in a script

The sandbox's Python has the module `sdp_data` (it imports under `python3 -I` too). `sdp_data.read_query(server, query)` runs a read-only query on an attached database server (`starrocks`, `postgres` or `mongo`) and returns its rows as a list of dicts, or as a pandas DataFrame with `as_frame=True`. The first argument is always the server, never a database or schema inside it: a StarRocks table is named in the SQL with its database (`reddit.comments`), a MongoDB database in the query's `"database"`. It goes through the same read-only MCP server and the same gateway port as the assistant's own database tools, and holds no credential: the gateway adds it.

| Database | `query` | Tool of its MCP server |
|---|---|---|
| `starrocks` | SQL text (name tables with their database, `db.table`); values come as the server gives them, a DECIMAL as text and a date as ISO text | `read_query` |
| `postgres` | SQL text; a numeric value comes as an int when it is whole, else as a float, and a UUID as text | `execute_sql` |
| `mongo` | A dict: `{"collection": ..., "database": ..., "pipeline": [...]}` for an aggregation, or `{"collection": ..., "database": ..., "filter": {...}, "projection": {...}, "sort": {...}, "limit": n}` for a find (only `collection` is required); an ObjectId comes as hex text and a date as ISO text | `aggregate`, `query` |

Each call is bounded: at most 1,000,000 rows (`max_rows=`), at most 128 MiB read from the gateway (`max_bytes=`) and 100 seconds in all (`timeout=`; MongoDB is also given it as `maxTimeMS`); on MongoDB and StarRocks the default is the query time limit plus 10 seconds instead, a larger `timeout=` is lowered to that, and MongoDB's `maxTimeMS` is at most the limit (PostgreSQL keeps the default of 100 seconds and your value). A query stopped at the limit, or a call that ran out of time, raises an error that names the limit and points to the query queue. Past a bound, a name that is not a database server, a server not attached to the chat, a query the server refuses or a server that cannot be reached, it raises `sdp_data.QueryError`, whose message names the server and the reason; it never returns an empty result in place of an error. A SQL query it gives up on may go on running on the server until the server's own limit.

### The limits

| Limit | Value |
|---|---|
| Time of one turn | 30 minutes; then the turn is ended (`SDP_CHAT_TURN_SECONDS` in the sandbox changes it) |
| Turns at once | 2 in the sandbox, 1 per chat |
| Time of one query on MongoDB or StarRocks | `CHAT_QUERY_TIMEOUT_SECONDS` in `.env` (90 seconds by default, 1 to 3600), enforced by the database; PostgreSQL ignores it (its MCP server keeps its own limit: 30 seconds in restricted mode, the default) |
| Soft cap | Before each turn the work folder is measured (without following a link); over the cap set at setup, the turn is refused until chats are deleted |
| Chats | 200 |
| A message | 1 to 32,000 characters |
| A chat's log | 16 MiB (and one line per 32 bytes of that); then the chat is full and takes no more messages |

A stopped turn, and one past its time limit, is ended with all its processes. With `openai-compatible`, a turn makes at most 40 model calls, and the end of a turn reports its time and number of model calls but no cost.

## Files and plots

What the assistant writes lands in the chat's folder, which you can download from the Chat window (see [Files and plots](webui.md#files-and-plots)) or find on this machine.

### A chat's files

A chat's files are in `.sdp/chat/work/<chat id>/` (a chat id is `c_` and 12 hexadecimal digits):

| Path | What it holds |
|---|---|
| `files/` | The assistant's working folder, where it writes its plots and other files |
| `home/` | The back end's home: Claude Code's session files and its `mcp.json`, or the agent's `openai-session.json` |
| `events.jsonl` | The chat's log: your messages, the assistant's text, each tool call and its result cut to 2,000 characters, each file that is new or changed in `files/` after a turn, and the end of each turn with its cost, for Claude Code |
| `meta.json` | When it was made, its title (the first message cut to 80 characters), its session id and its number of turns |
| `backend.log` | The last turn's error output of Claude Code or of the agent (at most 1 MiB) |

The chats persist on this machine across `chat stop` and `chat start`. A deleted chat leaves the list at once and its files are removed in the background. `chat unsetup` removes all chats only when you say so.

### Exporting a chat

The **Export** link in the Chat window's header downloads the open chat as one JSON file, `chat-<chat id>.json`, for analysis outside the web UI:

| Key | What it holds |
|---|---|
| `format` | `1`, the version of this layout |
| `exported_at` | When the file was made (UTC, ISO 8601) |
| `chat` | The chat's `id`, `title`, `model` (null for the back end's default), `backend` (`claude-code` or `openai-compatible`; null when the sandbox did not say) and `created` (seconds since 1970) |
| `events` | Every event of the chat's log (`events.jsonl`) in order, as the page draws them: your questions, the assistant's text, each tool call with its whole input, each tool result cut at 2,000 characters, each file a turn made, and each turn's end with its time and cost |
| `backend_transcript` | With `claude-code`: Claude Code's own record of the chat's session, uncut (every message, tool call and whole tool result), one item per line of its file, each the JSON object the line holds, or the line as text when it is not one. It is in Claude Code's own format, which may change between its versions. `null` with `openai-compatible`, before the chat's first turn, or when it was not read |
| `stopped` | `null` when the file is whole; otherwise one sentence saying where and why it stopped: past 50,000 events, past 64 MiB in all, past 10 minutes, or the sandbox could not be read |

The transcript is the file Claude Code keeps in the chat's `home/` (`projects/<the working folder's path>/<session id>.jsonl`); the sandbox hands it over read-only, only the chat's own file and never through a link. Everything in the file is data as the sandbox gave it, written as JSON: open it with a JSON tool, not a browser.

## The query queue

When the deployment's [query scheduler](query-scheduler.md) runs, the assistant can also submit long or heavy queries to its queue. Each one runs once it is approved, by the scheduler itself when its target's [auto-accept](query-scheduler.md#auto-accept) is on and has room under its limit, or else by you in the Queue window of the web UI; the assistant cannot approve it:

1. **The assistant submits** a query and tells you the job id and that the query may already be approved by auto-accept or else waits for your approval, then ends its turn instead of waiting.
2. **You approve it** (or reject it) in the web UI's [Queue window](webui.md#the-queue-window), where it is labelled "Submitted by the chat's assistant", unless auto-accept approved it already: its queries are auto-accepted like any other query when the target's auto-accept is on.
3. **In a later turn** the assistant checks where the query stands, and fetches the parts of a finished result into the chat's folder, where they show as the turn's files. It can also withdraw one of its own queries while it is pending.

Only the assistant's own queries are visible to it. The Chats page shows whether the queue is attached (the line "Query queue").

**When the queue is attached.** `chat start` attaches it when the scheduler runs (its `jobs` container) and `data/jobs/control` is a real folder of yours without access for the group or others (the scheduler makes it). Otherwise start says so in one line, `The assistant cannot use the query queue: <why>.`, and starts the chat without it: when the scheduler does not run, or the control folder is missing, open to others, a link or not yours, or `JOBS_PORT` is not a port number. A link or anything but a regular file of yours in the place of `data/jobs/control/assistant.secret` also starts the chat without the queue. How the queue is reached is in [The assistant's query queue](../architecture/control-plane.md#the-assistants-query-queue).

The queue uses the port of the deployment's settings (`JOBS_PORT` of `.env`, 8050 when it is not set): if the scheduler's own configuration was edited by hand to another `port`, the assistant's queue requests fail (the page's published port is then wrong as well).

`chat status` says whether the queue is attached now (and shows it dropped when the scheduler's container was replaced since the start); `chat stop` detaches the scheduler and removes the gateway's copy of the secret; `chat unsetup` removes that copy with `.sdp/chat/gateway/` and leaves `data/jobs/control/assistant.secret`, which belongs to the scheduler's folder.

## When a database drops out

A database is attached to the running container its MCP server had at start: for PostgreSQL the shared `postgres-mcp`, for MongoDB and StarRocks the chat's own copy. When that container is replaced (for `postgres-mcp` by `python sdp.py db stop` and `db start`, a new `db setup-mcp`, or anything else that makes Compose create it anew), the new container is not attached and the chat can no longer read that database; `chat status` shows it as dropped. `db stop` and `db start` leave the chat's copies alone (their profiles are not among those `db stop` names), so a copy stays attached while its database is stopped and started again; while the chat runs, `db stop` then also prints Compose's note that the compose project's network is still in use. To bring a dropped database back:

```bash
python sdp.py chat stop
python sdp.py chat start
```

A database started after the chat is likewise attached only by a new start.

## The query time limit

The limit is one value for the whole deployment, `CHAT_QUERY_TIMEOUT_SECONDS` in `.env`. Show it with `python sdp.py chat query-limit` (or `python sdp.py chat settings`): the limit in force, whether it is the default (90 seconds when the key is missing or empty), and that PostgreSQL ignores it. Set it with `python sdp.py chat query-limit <seconds>`, a whole number from 1 to 3600 (anything else is refused with exit code 1 and changes nothing), or in the web UI's settings (the gear in the top bar, **This deployment**, see [The web UI](webui.md#the-pages)), which asks for the management password.

Setting it writes the key to `.env` (every other line stays; a commented-out line of the key is replaced by it). When the chat runs, it then removes the chat's copies of the MongoDB and StarRocks MCP servers that run now, starts them again with the new limit and attaches them again under the same names, so the databases hold queries to it within seconds; a query running on one of them at that moment is cut off. The gateway and the sandbox are not stopped: a turn that runs goes on. The sandbox's own waits and what the assistant is told (`sdp_data`'s default time of a call, the time the OpenAI-compatible back end waits for a tool, the limit named in the role text) follow the new limit only after `python sdp.py chat stop`, then `python sdp.py chat start`; the command says so. When the chat does not run, only `.env` is written, and the next `chat start` uses the value.

When a copy cannot be started again (its image missing or built before the limit, which is checked for every copy before any is removed, or Compose refusing), the command names it, says how to recover (`python sdp.py chat stop`, then `python sdp.py chat start`) and exits with 1; the value stays written in `.env`.

## Stop, rebuild, status and unsetup

- **`python sdp.py chat stop`** stops and removes both containers and the chat's copies of the MongoDB and StarRocks MCP servers, detaches every container from the chat's two networks (whatever its state: a container left attached to a removed network could no longer start; one that is already gone is no error), and removes both networks, the gateway's config and its copy of the scheduler's secret. The exit code is 0 when the chat ran, 1 when nothing ran (what an earlier start left is removed then too) or something could not be removed. It refuses, and stops nothing, when a container or network of the chat's names belongs to another deployment.
- **`python sdp.py chat rebuild`** picks up new code of the chat or the gateway (after updating the clone): it builds `sdp-gateway` and `sdp-sandbox` again from the saved settings, asking nothing and printing Docker's output, then stops the chat when it runs and starts it, printing what `chat stop` and `chat start` print (a chat that did not run is simply started). It refuses, before building anything, when the chat is not set up (settings or credential missing). When an image cannot be built it stops there with exit code 1 and the running chat is left as it was; otherwise its exit code is the start's. The chats and their files are kept. Run `chat setup` again only to change the back end, the credential or the cap.
- **`python sdp.py chat status`** says whether the gateway and the sandbox run, the back end, which databases the chat can read now (read from the members of `sdp-chat-up` by their compose service label, so a database whose container was replaced since the start shows as dropped, with the commands that bring it back), whether the query queue is attached now, and the work folder's size: the sum of the sizes of its files, measured from this machine without following a link, against the soft cap set at setup. The walk stops after 1,000,000 entries and then says "more than". Status says when the folder is over the soft cap; the runner refuses to start a new turn then. The exit code is 0 when both containers run, 1 otherwise.
- **`python sdp.py chat unsetup`** says what it removes (the images `sdp-gateway` and `sdp-sandbox`, and `.sdp/chat/config.json`, `.sdp/chat/credential/`, `.sdp/chat/socket/` and `.sdp/chat/gateway/`) and asks to confirm (default no). It then refuses, removing nothing, while a container of the chat exists for this deployment (stop it first with `python sdp.py chat stop`), and also when Docker cannot be asked. Then it asks separately whether to also delete all chats and their files (`.sdp/chat/work/`, default no). Leftover temporary files of an interrupted setup beside `config.json` go too, and `.sdp/chat/` itself goes when nothing is left in it; when something else stayed in it, one line says how many entries stayed. Nothing is removed through a symbolic link (see [How chat unsetup removes](../architecture/control-plane.md#how-chat-unsetup-removes)); what could not be removed is counted, and the command then exits with 1. A piece that is already gone is not an error.

## Commands

| Command | What it does |
|---|---|
| `python sdp.py chat setup` | Asks the back end, the address and model (`openai-compatible`), the secret (without echo; on a second setup only when you replace it) and the work folder's soft cap; writes `.sdp/chat/`; builds the images `sdp-gateway` and `sdp-sandbox`. Exit code 0, or 1 with one line. |
| `python sdp.py chat start` | Starts the gateway and the sandbox and attaches each database whose running MCP server is on a read-only user (for MongoDB and StarRocks through the chat's own copy of the server, with the query time limit `CHAT_QUERY_TIMEOUT_SECONDS`); refuses, saying why for each database, when none can be attached. Exit code 0, or 1 with the reason (what it made is removed). |
| `python sdp.py chat stop` | Stops and removes both containers, the chat's copies of the MCP servers and both networks, and detaches the databases. Exit code 0, or 1 when nothing ran. |
| `python sdp.py chat rebuild` | Builds both images again from the saved settings, with no questions, then stops the chat (when it runs) and starts it. Refused when the chat is not set up. Exit code: the start's, or 1 when an image could not be built (the running chat is then left as it was). |
| `python sdp.py chat status` | Whether each container runs, the back end, the databases the chat can read now and those dropped since the start, the work folder's size against its soft cap. Exit code 0 when both containers run. |
| `python sdp.py chat settings` | Shows the chat's settings for the whole deployment: the read-only query limit in force, whether it is the default, and that PostgreSQL ignores it. Exit code 1 when `.env` holds a malformed value. |
| `python sdp.py chat query-limit [<seconds>]` | Without a value, shows the limit as `chat settings` does. With one (1 to 3600), writes `CHAT_QUERY_TIMEOUT_SECONDS` to `.env` and, when the chat runs, starts its MongoDB and StarRocks MCP servers again with it, leaving the gateway and the sandbox running. Exit code 0, or 1 with the reason and the recovery. |
| `python sdp.py chat unsetup` | After a confirmation, removes the two images, the settings and the credential; after a second one, also all chats and their files. Refused while the chat's containers exist. |
