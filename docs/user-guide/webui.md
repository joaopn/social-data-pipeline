---
title: "Web UI reference"
parent: "User guide"
nav_order: 12
---
# Web UI reference

The web UI is a page in your browser for the pipeline of one deployment: you start and follow pipeline runs, start and stop databases, approve the queries in the query scheduler's queue and ask the chat's assistant about your data. Every action of the Management window shows its `sdp` command and runs only once you confirm it; the buttons of a query in the Queue window act at once.

New to it? Start with [Getting started in the browser](getting-started.md), which walks through it step by step with screenshots.

## What it is

The web UI has three windows:

| Window | What it is for |
|---|---|
| **Management** | The **Overview** and the pages **Sources**, **Databases**, **Runs** and **System**: the pipeline of this deployment. |
| **Queue** | The [query scheduler](query-scheduler.md)'s queue: approve, reject, kill and EXPLAIN queries, preview results, set auto-accept. |
| **Chat** | The [chat](chat.md)'s assistant: ask questions about the data and get its answers, tool calls, files and plots. |

It runs in a container of its own and does everything through the **broker**, a process on the host that runs a fixed list of CLI actions for a logged-in session, and, for the Queue window, through the scheduler's control socket. The container holds no database password, no Docker access and no file of the deployment: only the broker's socket folder, its own certificate and, when they are there, the scheduler's control folder and results folder and the chat's socket folder. Reading a page changes nothing.

How the web UI, the broker and the chat are built and what they defend against is in [Broker, web UI and chat](../architecture/control-plane.md).

## Requirements

- **Linux.** The broker runs on Linux only.
- **A Docker without user-id mapping.** Rootless Docker and `userns-remap` are refused, because the broker accepts connections only from the operator's own user id, and under such a mapping the container's connections arrive as another one.
- **A management password and a running broker.** The login of the page checks the management password at the broker.

## Set up and start

**1. Set the management password.** You log in to the page with it, and the page asks it again for some actions.

```bash
python sdp.py broker passwd                 # asked twice, not echoed
```

**2. Start the broker.**

```bash
python sdp.py broker start                  # in the background; prints its pid
```

**3. Set up the web UI.** Setup asks the port (default 8443), the addresses the page is opened at besides this machine, and the certificate. It builds the image `sdp-webui` and prints the certificate's SHA-256 fingerprint.

```bash
python sdp.py webui setup
```

**4. Start it, and open the page.**

```bash
python sdp.py webui start                   # says whether the broker answered from inside it
```

The page answers at `https://localhost:<port>/` and `https://127.0.0.1:<port>/` on this machine, and at `https://<address or name>:<port>/` for each address and name you gave, or under any address or name with `all`. The first visit shows the browser's certificate warning: see [The certificate](#the-certificate).

To use the Queue and Chat windows, set up and start the scheduler and the chat first, then start (or stop and start) the web UI: see [What the Queue window needs](#what-the-queue-window-needs) and [What the Chat window needs](#what-the-chat-window-needs).

## Commands

### The broker

| Command | What it does |
|---|---|
| `python sdp.py broker passwd` | Sets the management password, asked twice and not echoed. Any password that is not empty is accepted (up to 1024 characters, no line break or control character); a short one is easier to guess within the login limits. Works whether or not the broker runs; sessions that logged in with an older password end at their next request. |
| `python sdp.py broker start` | Starts the broker in the background and waits up to 10 seconds until it answers. Prints its pid, or that it runs already, or why it did not start (exit code 1); a broker that has not answered by then is ended. |
| `python sdp.py broker start --foreground` | Runs the broker in this terminal until Ctrl-C. |
| `python sdp.py broker stop` | Stops the broker. From then on it takes no new login and no new call (they are answered `stopping`); an action that changes something and is still running is waited for, without a time limit; the command says once that it waits. |
| `python sdp.py broker stop --force` | Stops the broker after ending such an action (TERM, then KILL after 10 seconds). |
| `python sdp.py broker status` | Says whether the broker runs, its pid, since when, the socket's path and whether a management password is set. Exit code 0 when it runs, 1 when not. |
| `python sdp.py broker audit [--limit N] [--json]` | Prints the newest N lines (1 to 10000, default 50) of the audit log: time, session, event and its details. |
| `python sdp.py broker audit --verify` | Checks the chain of the whole audit log; exit code 1 when a line was removed, edited or put in between, or the log cannot be read. |

Stopping the broker never stops a pipeline run: each run started through it has a supervising process of its own, which goes on. Such runs show in `python sdp.py runs list` as kind `web` and are cancelled with `python sdp.py runs cancel` like any other.

### The web UI

| Command | What it does |
|---|---|
| `python sdp.py webui setup` | Asks the port (default 8443), the addresses the page is opened at besides this machine (IP addresses and names, comma-separated; `all` for every address; a second setup offers the previous answer), and the certificate (generate one, or give the paths of your own certificate and key, which are copied). Refuses when no management password is set (`python sdp.py broker passwd` first) and while the web UI's container exists. Builds the image `sdp-webui`, makes or copies the certificate, and prints its SHA-256 fingerprint and the addresses the page will answer on. |
| `python sdp.py webui start` | Starts the container `sdp-webui` on its own network `sdp-webui` and says whether the broker answered from inside it (start the broker with `python sdp.py broker start`; the page needs it to log in). Mounts the scheduler's control folder and results folder for the Queue window when they pass their checks, and prints one line for each that does not. Mounts the chat's socket folder for the Chat window when it passes its check, and prints one line when it does not. Says so with exit code 0 when it already runs. Refused when the web UI is not set up and under rootless Docker or user-id remapping. When Docker refuses (an address the machine does not have, a port in use), or the server ends right after its start (a certificate or key it cannot read), the container is removed, Docker's reason or the server's last lines are printed, and the exit code is 1. |
| `python sdp.py webui stop` | Stops and removes the container. Exit code 1 when it was not running. |
| `python sdp.py webui rebuild` | Builds the image `sdp-webui` again from the saved settings, with no questions, then stops the web UI (when it runs) and starts it: logged-in sessions end. Refused when the web UI is not set up or its certificate is missing. Exit code: the start's, or 1 when the image could not be built (the running web UI is then left as it was). |
| `python sdp.py webui status` | Says whether it is set up and runs, whether the Queue window's two folders and the Chat window's socket folder are mounted, its port, listen addresses and names, the certificate's fingerprint and end date, and the addresses of the page (with `all`: that it answers on every address, and the warning). Exit code 0 when it runs, 1 when not. |
| `python sdp.py webui unsetup` | Asks first, then removes the container, the network, the image and `.sdp/webui/`. |

To change the port, addresses, names or certificate: `python sdp.py webui stop`, `python sdp.py webui setup`, `python sdp.py webui start`. To pick up new code of the web UI (after updating the clone): `python sdp.py webui rebuild`; the settings and the certificate are kept.

### When the broker does not start

- **A path that is a link, another user's, or writable by others.** The broker refuses to start when the deployment folder, `sdp.py`, or any folder or `.py`/`.pyc` file under `social_data_pipeline/` is a symbolic link, belongs to another user, or can be written by others. The group may write them only when it is the operator's private group (the user's primary group, with nobody else in it). `.sdp`, `.sdp/broker` and `.sdp/socket` may not be writable by the group at all. The message names the path and the `chmod` command that fixes it.
- **A deployment folder with a long path.** The socket is `.sdp/socket/broker.sock`; its full path must fit in the 108 bytes of a unix socket address, and a deployment folder with a longer path is refused at start with a message saying so.
- **A broker that already runs.** A second broker is refused.

The broker's files under `.sdp/` are listed in [The broker's files](../architecture/control-plane.md#the-brokers-files).

## What a session can do

- **A login** takes the management password and starts a session that lasts at most eight hours. It ends at the logout, when the broker ends it (the password was changed, the broker was restarted) and when the broker cannot be reached; the login page then says why. Restarting the web UI logs everyone out.
- **Failed logins** are limited: five failed logins, one given back every 30 seconds, counted apart for logins at this machine and from elsewhere, so that failed guesses through the network never lock out a login at the machine itself. The web UI also limits each client address to five failed logins in five minutes.
- **Every action** is a typed action of the CLI; the page cannot run anything else.
- **The management password again** is asked for a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, setting up a database, setting up the MCP servers or setting up the scheduler, removing a source, adding a source, reconfiguring a source, adding classifiers to a source, creating indexes, changing the read-only query limit for chats, and keeping the database admin password for the session. Five wrong ones end the session.
- **The database admin password** can be kept for the session, after giving the management password again. A run start and a database start then use it when their form leaves the field empty.
- **Refused while a run is alive:** a database reset, removing a database, removing the MCP servers' configuration, stopping services and removing a source are refused while a pipeline run is alive, and when Docker cannot be asked whether one is. Removing the MCP servers' configuration is among them because the CLI stops each database that has an MCP server configured together with that server.
- **A PostgreSQL start** (or a start of every service) is refused with a database password that holds a quote, `$`, a backtick or a backslash.

## The pages

After the login you get the Management window: the **Overview** and the pages **Sources**, **Databases**, **Runs** and **System**.

The top bar names the three windows (Chat, Management, Queue); Chat reopens the chat you last opened in this login (the chat list before you have opened one, or after you deleted that chat; a chat deleted elsewhere opens once as not found, and Chat then leads to the list). The number on Queue is the queries waiting for your approval. On its right, a dot per service says whether it answers: broker, scheduler and sandbox, green when it answered, red when it did not, grey when the web UI was not given it (no query queue or no chat set up). They are read after the page has loaded and every 30 seconds, so a slow service never slows a page; the title of each says what was found. A window with more than one page shows them as tabs under the top bar.

The gear beside them opens the **settings** (Escape or a click outside closes them). Both are kept in this browser only, never sent to the web UI:

| Setting | Choices |
|---|---|
| **Theme** | Dark (the default), Light, or Follow the browser (the browser's light or dark preference, also when it changes while the page is open). Every page, the login included, is drawn in it. |
| **Default model for new chats** | The backend's default, `opus`, `sonnet` or `haiku`. It applies to Claude Code; an OpenAI-compatible backend always uses its one configured model. |

In a private window, or with the site's storage blocked, the settings apply to the open page only and every page opens dark.

Under them, apart, **This deployment** holds a setting of the server, one value for the whole deployment, read from the broker when the box is first opened on a page:

| Setting | What it does |
|---|---|
| **Read-only query limit for chats (seconds)** | The time after which MongoDB and StarRocks stop a chat's query (`CHAT_QUERY_TIMEOUT_SECONDS`, 1 to 3600; the box shows the value in force and whether it is the default of 90). **Save…** opens the confirm page, which asks for the management password, as `python sdp.py chat query-limit <seconds>`. When the chat runs, its MongoDB and StarRocks MCP servers are started again with the new limit; the chat itself is not restarted, and running chats keep their own waits until it is (`chat stop`, then `chat start`). PostgreSQL ignores the limit (its MCP server keeps its own). See [The query time limit](chat.md#the-query-time-limit). |

| Page | What it shows | What you can do there |
|---|---|---|
| **Overview** (`/`) | **Needs attention**: a database without authentication, ports open beyond this machine, the running services whose settings files changed after they started ("Restart to apply", `db restart-needed`), a read that failed, Docker out of reach, and the queries that wait for your approval. **Right now**: the pipeline runs alive now and the state of the databases, the MCP servers and the scheduler. **Sources and pipeline profiles**: one row per source, one column per `sdp run` profile. Beside each source's name is its size on disk, the bytes of its dumps, extracted, parsed and output folders as `python sdp.py source size <name>` counts them (decimal units: "1.4 GB" is 1.4 billion bytes); "≥ 1.4 GB" means a folder of the source could not be read whole, so the size is at least that. A cell says what is known of that profile on that source: "✓ 250 files" for `parse`, `lingua` and `ml`, the finished files of that stage (`source progress`, one count per classifier folder for `ml`; it knows no total, so no fraction is shown); "✓ 48 loaded" and "✕ 2 failed" for a database profile, the files loaded and failed from its state files, the figures the source's Pipeline tab shows (`source status`); "running" while a run of it is alive; "configured · nothing yet" when the source has the profile and it has produced nothing; "–" when the source does not have it. A stage's unfinished files are "being written" only while a run of that profile on the source, or a task of the source, is alive; otherwise the cell says they were left by a run that ended. The legend under the grid says what the counts are. The queries waiting, the figures and the sizes are read after the page has loaded, for at most 50 sources and about 20 seconds, so they never slow the page; until then a cell says "configured" and no size is shown, and a source whose figures or size could not be read says so in its row | Open a run's page, the Queue, the Databases page, or a source (a cell opens the source at its profile's run form); restart the services that Needs attention names (below) |
| **Sources** (`/sources`, a source at `/sources/<name>`) | On the left the configured sources (`source list`); on the right the first source, or the one opened, with three tabs. **Pipeline**: its stages as in the pipeline (dumps, parse, lingua, ml, then per database its loads), each marked running, set up, not set up or with failed files, with the finished files per stage (`source progress <name>`; a stage's unfinished files are "being written" only while a run of that profile on the source, or a task of the source, is alive, otherwise "left by a run that ended") and the files loaded and failed per database from its state files (`source status <name>`), and the forms to run its profiles. **Configuration**: platform, data types, profiles and paths (`source status <name>`), with **Reconfigure…** (`source configure <name>`), **Add classifiers…** (`source add-classifiers <name>`, for a source with `lingua` or `ml`) and **Create indexes…**. **Errors**: the error logs of its profiles among `postgres_ingest`, `postgres_ml` and `mongo_ingest` (`source error-logs`; one too large for the page names the terminal command that prints it) | Run each of its profiles, one small form per profile with only that profile's options: rebuild the image (all), a file filter (all but `sr_ingest` and `sr_ml`), skip files with a lingua output (`parse`), one classifier (`ml`, `lingua`, `sr_ml`), the database admin password (the profiles that use a database; the form says whether one is kept for the session). Remove the source. Reconfigure it or its classifiers (see "Reconfiguring a source and its classifiers" below). Add a source (**+ Add source** on the left, **Add source** in the tab bar; see "Adding a source" below) |
| **Databases** (`/databases`, one database at `/databases/<database>`) | Four tabs. **Servers**: a card per database with its state, ports, data folder, admin user and whether its checks found drift (`db status`, `db verify`), and a card for a database that is not set up. **MCP servers**: each server's state, port and whether it logs in as a read-only user, and **Change…**, which sets them up (below). **Queue setup**: the scheduler's state and port, and **Change…**, which sets it up (below). **Verify**: every check of `db verify`, one line each, with what to run for a drift | Start or stop one database, an MCP server or the scheduler with its own buttons, or any service (or all) with the forms under the cards (with the database admin password when one is needed); reset or remove a database, set up or remove the MCP servers and the scheduler, keep the database admin password for this session or forget it. **Set up a database…** (and **Set up…** on the card of one that is not set up), **Change the setup…** (and **Change…** on the card of one that is set up): see [Setting up a database](#setting-up-a-database). **Start it now** on the card of a stopped database: the start with its admin password field. **Create indexes…** (below) |
| **Runs** (`/runs`) | The runs, recorded ones (started from the web UI) and those started in a terminal that are alive now: state, profile, source, start, end, exit code, where it was started from (`runs list`) | Open a run, cancel one that is alive |
| **A run** (`/runs/<run id>`) | Its facts (the non-secret fields it was started with, who started it, its state) and its output, which follows the run as it goes (see [The output of a run](#the-output-of-a-run)) | Cancel it while it is alive |
| **System** (`/system`) | Two tabs. **Broker and verbs**: what the web UI may ask the broker, by class (read, write, step-up), the broker's greeting (its protocol, whether a management password is set), and the table of every entry with its class and terminal command. **Audit log**: the newest 200 lines of the broker's audit log (time, session, event, verb, outcome; a refused call with its code) | Nothing |

Each block of a page is read on its own: a read that fails (a time-out, a state file that cannot be read, Docker out of reach) is shown as a sentence in its block, and the rest of the page stays. A name becomes a link only when it is a plain name (a letter or digit, then letters, digits, `_`, `.`, `-`, at most 64 characters); everything the broker hands over is shown as text.

## Setting up the MCP servers and the scheduler

**Databases → MCP servers → Change…** (`/setup/mcp`) asks the questions of `python sdp.py db setup-mcp` as one form, and **Databases → Queue setup → Change…** (`/setup/queue`) those of `python sdp.py db setup-jobs`. Each is filled with the answers the terminal offers as its defaults (`python sdp.py db llm-settings` prints them; no password or user name is read). Both write the same files as the terminal for the same answers; nothing is written before you confirm on the confirm page, with the management password.

- **MCP servers:** per database set up, whether its server is on, its port and, for PostgreSQL and MongoDB, write access; the confirm page says for each server whether it is read-only or has write access. With write access on, the confirm page warns that any agent that reaches the port can change data (unless the database has authentication with a read-only user, as which the server then logs in). When the terminal command would refuse (a database with authentication and no read-only user or no read-only credentials file), the page shows its refusal instead of the form. With every server off nothing is written, as in the terminal; Remove… removes the configuration.
- **The scheduler:** the port, the results folder, the queries at once (1 to 1000), the jobs kept in the history (1 to 100000), and one target per database set up with its name, PostgreSQL's database and the default timeout in hours (StarRocks 1 to 72, the others 0 for no limit, at most 8760). The confirm page lists the files written and the mounts that change: the `/jobs_export` mount of PostgreSQL and StarRocks in `docker-compose.override.yml`, the scheduler's and StarRocks's mounts of `JOBS_RESULT_ROOT`, and the web UI's results folder at its next start. The scheduler page's login can be switched on here; while it is on the form keeps it on, and switching it off is done in a terminal with `python sdp.py db setup-jobs`.
- **Refused from the browser only** (the terminal is unchanged): a port that another service of this deployment uses (a database, StarRocks's 8030 and 8040, an MCP server, the scheduler, the web UI itself, and a port of one of these named in `.env` while it is not set up; a service keeps its own port), two targets of one name, a results folder that starts with `-`, has white space at an end or holds `:`, `$`, `#`, a quote or a backslash, and a results folder that is `data/jobs`, holds it or lies inside its `control` folder (the scheduler would keep its control socket closed).
- **Nothing is restarted.** The outcome page and Needs attention on the Overview say which running services must restart to apply what was written: a service is named when one of the files it reads (`.env`, which counts for every service, the compose override for PostgreSQL and StarRocks, their local configuration files, `config/db/mcp.yaml` for the MCP servers, `config/jobs/config.local.yaml` for the scheduler) was modified after its container started (`python sdp.py db restart-needed`). Its **Restart…** shows the steps and the database admin password field of `db start`; after the confirm (with the management password, checked before anything stops) each service is stopped and started again, `db stop` then `db start`, one after the other, each step only after the one before ended with exit code 0. The outcome page shows each step with its exit code and output, and names the steps that were not started. A database's MCP server is not restarted on its own when its database is: stopping and starting the database takes it along.

## Adding a source

**Add source** asks the questions of `python sdp.py source add` for Reddit dumps, a Hugging Face dataset (`--hf`) or your own files, in six steps, with Back, Next and Cancel on each:

1. **Origin**: Reddit dumps (always the source `reddit`), a Hugging Face dataset, or your own files.
2. **Files**: the name, the data types (comma-separated, at most eight here; for a Hugging Face dataset its id `<owner>/<name>` and, for a private or gated one, its access token instead), the four folders (empty: the terminal's default, `<DATA_PATH>/<stage>/<name>` with `.env`'s `DATA_PATH`, `./data` when it sets none, computed on the host; any path, absolute or relative to the deployment, a relative one written with its leading `./`), the intermediate file format and Parquet row-group size, the raw input format and CSV delimiter (your own files), the profiles (those of the databases that are set up) and the parse workers.
3. **Fields**: for your own files, per data type the dump file pattern, its compression when the pattern's extension does not name it, and the fields to extract; for Reddit, the template's fields to leave out (the question of `source configure reddit`); for a Hugging Face dataset, the grouping `source add --hf` asks (below).
4. **Classifiers** (with `lingua` or `ml`): text columns, strings and patterns to remove and extra output fields (your own files), the Lingua settings, the GPUs, workers, classifiers and the data types of each, and a Hugging Face token for gated models (saved to `.env` as `HF_TOKEN`, as the terminal does).
5. **Databases** (with a database profile, or for Reddit): per database what `source add` asks (Lingua files, index workers, tablespaces, schema, MongoDB collections and database name, StarRocks buckets, indexes, the primary key), and for Reddit its schema and index columns.
6. **Review**: every answer, the answers left to the terminal's default, the files written in `config/sources/<name>/`, the folders and the commands it stands for; for a Hugging Face dataset also **Download now** (ticked).

A field left empty is the terminal's Enter: the broker computes that default on the host as `source add` does (the CPU cores, the GPUs, the base classifier configuration). Nothing is written until the confirm page that follows Review, which needs the management password. The broker refuses, before anything is written, a name that is already a source (change a source with Reconfigure), a folder that is a folder of another source or lies inside or around one (compared after following symlinks), and an answer the terminal would not ask for the profiles chosen. None of the four folders is created: the first run of a profile creates each one that is missing. The answers so far are kept in your session with the pending actions: they count against their limit of sixteen and are forgotten five minutes after the last step.

**A Hugging Face dataset.** Next on Files reads the dataset's schema from the Hub through the broker, on the host (`python sdp.py source hf-inspect <owner>/<name>` prints the same), and groups its configs by their fields as the terminal does. Fields then shows each group: its configs (empty: all of them), the name of its data type (empty: the terminal's suggestion, the configs' common prefix), the fields with a SQL type (boxes, or a comma-separated list when a dataset has too many for one page) and, for each field whose Hub type has no SQL mapping, skip it or one of the types `text`, `integer`, `bigint`, `float`, `boolean` (the terminal also takes other text there, which the loaders read as text). The fields left out are kept out of MongoDB's documents (`mongo_exclude_fields`). A dataset whose config or field names are not plain names, or with more than 8 groups or 256 configs or fields in a group, is refused with a line: add it in a terminal. The add itself asks the Hub nothing: it writes the grouping read here.

With **Download now** ticked, the confirm page shows both commands, `python sdp.py source add <name> --hf <dataset>` and `python sdp.py source download <name>`: when the add has ended with exit code 0, the download starts as a run (its log and Cancel in Runs and on the Overview), and the outcome links to it. The download asks the Hub for the list of Parquet files and refuses the whole list, before anything is written, when a config, split or file name in it is not a plain name or an address is not an https address on `huggingface.co`; then it writes the files as `source download` does (into `<dumps>/<config>/<split>/`, each through a `.partial` file, then copied into the extracted folder by data type). The access token is used to read the schema and for that download, and is stored nowhere: not in a file, the add, the run's record or its log (the broker overwrites it in what it sends back), and the session lets it go once the download was asked. A source whose dataset is not downloaded yet shows **Download** on its Pipeline tab (with a field for the token). The broker refuses a download while a pipeline run of that source is alive (and when Docker cannot be asked whether one is), and a pipeline run of a source while its download runs.

## Reconfiguring a source and its classifiers

**Reconfigure…** on a source's Configuration tab asks the questions of `python sdp.py source configure <name>` in the Add source wizard's steps, each field filled in with the source's current value (read by the broker as `python sdp.py source settings <name>` prints it; never a secret: the page only says whether `.env` has a Hugging Face token). It starts at **Files**: the origin and the name stay. A field left empty keeps the current value, as Enter does in the terminal; where the terminal offers no current value, it takes the terminal's default (the GPUs: every GPU found; the compression of a dump pattern whose extension names none: zst). Confirmed unchanged, it writes the same files again.

- **The files**: the profiles chosen get their files written again in `config/sources/<name>/`, each existing file first copied to its `.bak`; a profile left out keeps its file, nothing is removed. For a source of your own files, `platform.yaml`'s field types are written again as the generic ones, as the terminal does (types refined there by hand are lost; the `.bak` keeps them). StarRocks indexes written in the typed form of `db create-indexes` (bitmap and bloom filter) are kept as they are when their field is left empty.
- **A Hugging Face source** keeps its dataset, its grouping of configs into data types, its fields and their types: there is no Fields step and its data types are not asked.
- **Reddit**: `platform.yaml` is written again from the Reddit template, then the Reddit schema, field and index answers are applied to it, as `source configure reddit` applies them (the fields shown filled in are the current ones; one left empty is the template's). In a terminal, `source configure reddit` asks the Reddit questions first and, when it adds profiles, writes `platform.yaml` from the template afterwards, which drops them; the web UI applies them last.
- **Folders**: any path; a folder of another source (or inside or around one) is refused, the source's own are not. No folder is created, moved or emptied.

**Add classifiers…** (shown for a source with `lingua` or `ml`) is the Classifiers step alone, filled in with the current values, then Review: it writes `lingua.yaml` and `ml.yaml` as `python sdp.py source add-classifiers <name>` does, and a Hugging Face token typed there is saved to `.env` as `HF_TOKEN`, as the terminal does (the field never shows it again). The next `lingua` or `ml` run reads the new settings; a file a classifier has already finished is skipped, not classified again.

Both are confirmed with the management password, on a confirm page that shows the command, the files written and, with a token, the `HF_TOKEN` line. The broker refuses both, before anything is written, while a run of that source is alive: a pipeline run, a download or an index build of it, or a run of it started in a terminal that Docker reports (and when Docker cannot be asked whether one is). The refusal names the run and links to its page. Nothing is restarted or rerun: each profile reads the new settings at its next run, and when PostgreSQL or StarRocks is running, the output says whether it must be restarted to see changed folders.

## How an action is confirmed

**1. You choose.** A form (a button on one of the pages) sends what you chose. The web UI checks it against its own table of actions without asking the broker: only the fields that action takes, the closed choices, names of the plain form, a file filter of letters, digits and `_ . * ? [ ] ! -` (not only dots), and the rules of `run`: no file filter with `sr_ingest` and `sr_ml`, skipping lingua files only with `parse`, a classifier only with `ml`, `lingua` and `sr_ml`. A choice that does not pass is refused and nothing is kept or sent.

**2. The page shows the pending action.** It is kept as a pending action of your session, and the confirm page (a dialog drawn in the page, never with a scroll box of its own) shows the `sdp` command it stands for (what you would type in a terminal; it never holds a password) and every input (a password as dots). For an action that destroys something, one sentence says what it destroys and names it. For a start or a stop, the page says what else the service chosen acts on (a database's MCP server, the scheduler, the database an MCP server needs; "every configured service" when none is chosen). A terminal would also ask for the secrets the command does not show (the database admin password, a confirmation).

**3. You confirm.** For the actions that need it, the page has a field for the management password: a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, setting up a database, setting up the MCP servers or setting up the scheduler, removing a source, adding a source, reconfiguring a source, adding classifiers to a source, creating indexes, changing the read-only query limit for chats, and keeping the database admin password for the session. Starting and cancelling a run, stopping services and forgetting the database password need none. Confirm sends only that password, or nothing. The pending action is then taken out of the session, so a second confirm (a double click, another tab) finds nothing; it is passed to the broker once.

**4. The outcome page follows it.** The action runs in the background, not tied to your request, with no time limit (a state-changing command can last many minutes). The confirmation waits up to two seconds for an answer: a refusal that comes at once is shown on the pending page. A wrong management password keeps the action, so you can type it again; five wrong ones end the session at the broker, and so here. Otherwise the outcome page says "under way" and looks again every two seconds, until it shows the exit code with the end of the command's output and errors, the link to the run that was started, or the refusal's sentence. A broker that cannot be reached is an outcome; it does not log you out.

At most two actions run at once from the web UI; a third is answered "busy" and stays pending. Cancelling a run has two places of its own, so that a cancel gets through while a database start or a reset runs for minutes. A pending action is kept for five minutes, or until confirmed.

## Setting up a database

**Set up a database…** on the Databases page asks the questions of `python sdp.py db setup` as a form in steps, for a database that is not set up yet. When none is set up it is the first-time questionnaire: the databases (StarRocks, MongoDB, PostgreSQL, one or more), the data base path and the export path. When one is set up already, one database is added at a time, as `python sdp.py db setup --add <db>` adds it, under the data base path set up before. Then one step per database: its data path and ports; for PostgreSQL the database name, tablespaces (a name of lower-case letters, digits and `_`, and a host folder each), the filesystem (standard or ZFS), the PGTune output pasted into a box (blank lines are left out; there is no way to read it from a file here) and the container's memory limit; for MongoDB the WiredTiger cache, the memory limit and the validation before import; for StarRocks the FE HTTP port, the FE heap, the container's and the BE's memory, the alter workers, the compression and extra storage disks. Then authentication and users: on or off, the admin password typed twice (it is stored nowhere: you give it when the database starts, as in the terminal), the read-only user, and its password generated or typed twice. When a database is added to a setup whose authentication is on, that step is left out and the new database gets the setup's authentication and read-only user, as the terminal gives them.

A field left empty takes what pressing Enter takes in the terminal; the memory sizes and the alter workers are then computed on the host from its memory and cores, as the terminal computes them, and the field says how. Each step keeps its answers in your session (Back and Next keep them; a password field left empty keeps the password typed before) for as long as a pending action is kept, five minutes from the last step; nothing is written before the confirm. Review shows every answer and what the setup will write; its confirm page lists every file and `.env` key written, the folders made and what happens to the passwords, and needs the management password. The files are the ones the terminal writes for the same answers.

The page refuses, before anything is written: a port another service of the deployment uses (the same reading as for the MCP servers and the scheduler above: the ports of the services set up, from `.env` or their defaults, StarRocks's 8030 and 8040, any other database, MCP or scheduler port named in `.env`, and the web UI's) or two services of the setup on one port (the terminal does not check this); a path holding `$ : # " ' \` or a backtick, or white space at its ends (they would break `.env`, a mount line or YAML); a tablespace named `pgdata` or twice; a read-only user name or tablespace name that is not lower-case letters, digits and `_` (both are written into SQL unquoted); a typed read-only password with a quote, a backslash, a backtick or white space; and the two copies of a password that differ. A database that is set up already is changed with **Change…** (below). After the setup the outcome page offers **Start it now**, which opens the start of the new database (or of every configured service after a first-time setup) with its database admin password field.

### Changing a database

**Change…** on the card of a database that is set up (or **Change the setup…** on the Databases page) runs the same questions over the setup as `python sdp.py db setup` does when run again in a terminal: every database set up is part of it (one is removed with **Remove…**), a database not set up yet can be added in the same run, and every field starts with its current value, read with `python sdp.py db settings` (the read never returns a password: whether a read-only password can be kept is a yes or no). Every answer can be changed, the data paths included. The terminal's "keep" questions are boxes, ticked when there is something to keep: the current tablespaces, the extra StarRocks disks, the PGTune output (a new one is pasted, never read from a file), and the read-only password (kept only for the same user name). A path left as it is, is written exactly as it is. The filesystem starts at the one `config/postgres/postgresql.local.conf` was written for (in the terminal its default is always standard).

Authentication can be switched on here, with the admin password typed twice and the read-only user as for a first setup; with it on, the admin password is asked again, twice, as the terminal asks it. **Switching authentication off is refused**: the step says so, and so does the broker if the request comes another way; it is done in a terminal with `python sdp.py db setup`.

Review and the confirm page list **what changes**, each setting with its value now and after, and every file written, as for a first setup (`.env` is rewritten, so `HF_HOME` and `HF_TOKEN` values are dropped, as the terminal drops them; `docker-compose.override.yml` is rewritten whole when tablespaces or extra disks remain, and left as it is when the last ones are given up, so their mounts stay in it). **A data path changed moves no data**: the confirm page says that the database will start empty at the new path and that its old data stays in the old folder, which it names; nothing is deleted. Nothing is restarted, as after setting up the MCP servers or the scheduler: the outcome page says so, and Needs attention on the Overview names the running databases whose settings files changed (`config/db/<database>.yaml`, `.env`, the compose override, their local configuration files), with **Restart…**.

## Creating indexes

Databases → **Create indexes…** (also on a source's Configuration tab) is `python sdp.py db create-indexes --source <name>` in three steps, for PostgreSQL, MongoDB and StarRocks.

1. **Source and database.** The source (with several, it is chosen first), then one of the databases it loads into (by its profiles, as the command offers them; the first is chosen). The database must be running.
2. **Columns.** The broker reads the source's tables from the database (`db.index_targets`, the same as `python sdp.py db index-targets --source <name> --db <database>`): for PostgreSQL the tables of the source's schema, for StarRocks those of its database, for MongoDB its data types (an index is created on every collection of a data type). Each table shows its existing indexes and one box per column; you can only tick a real column. For MongoDB, whose documents have no fixed columns, the fields offered are the source's configured fields of the data type and the top-level fields of one stored document. For StarRocks the page gives the terminal's explanation of the two index types: every column is indexed with a bloom filter unless you tick its BITMAP box; a column whose type takes neither (FLOAT, DOUBLE) has no box, and one that takes only bitmap (TINYINT, BOOLEAN, DECIMAL) is marked so. The table also shows the bloom filter columns set now, which the build keeps. A box at the end answers the terminal's last question, whether to save the new indexes to the source's `platform.yaml`. At most 60 boxes can be ticked in one build; tables or columns whose names are not plain names are not offered, and the step names them (the first 20; use the terminal for those). With authentication on, the read uses the database admin password kept for the session (keep it first under Servers); without one the step says that the password is required.
3. **Confirm.** The page reads the tables again, refuses a box naming a column the database did not list, and shows the confirm page: the command, the tables and columns, the statements it will run, and the management password field.

Confirmed, the build runs as a run: it is listed in Runs and on the Overview, with its output and Cancel, and one build per database can be alive at a time. Before it creates anything it reads the tables and columns from the database again and refuses a table or column the database does not have now; then it runs the same statements, in the same order, as the terminal command for the same answers, and prints the same progress lines for each index. A cancel ends the command, but does not undo what the database already did: an index the database is building may go on inside it.

## The output of a run

The run page follows the run's output by itself.

| Run | What is shown | How often |
|---|---|---|
| Started from the web UI | Its log file, appended in pieces of 64 KiB, whole lines only | Every 3 seconds while the run is alive, at once for the rest of a run that has ended |
| Started in a terminal, or a recorded run whose supervisor is gone while its container still runs | What Docker holds, its last 500 lines, replacing what is shown | Every 10 seconds |

One view shows at most 4 MiB; then a link continues from that point in a fresh view (`/runs/<run id>?offset=<byte>`). When the places for reading output are full, the view says it is busy and tries again in 10 seconds. Escape sequences, control characters and characters that change the direction of text are removed before the text is shown. The details are in [The output of a run](../architecture/control-plane.md#the-output-of-a-run).

## The Queue window

The Queue window (`/queue`) is the query scheduler's own page, one to one, reached through the scheduler's control socket instead of the scheduler's port and password: the queries that agents submitted over the scheduler's MCP endpoint, waiting for a human approval.

> **Approving a query makes the scheduler run it on its database with the database's admin login**, not with the read-only login of the MCP servers. Treat every approval as running that statement yourself as the database's administrator.

At the right end of the window's tab bar, read when a tab is shown, are the results folder's size and the space free on its disk ("at least" when the scheduler stopped counting at its bound, or could not read a folder below), and a pill with the scheduler's slots (the queries that may run at a time) and how many are busy, its dot amber when every slot is taken; when the scheduler cannot give these figures, the bar says "figures unavailable" and the page is shown as usual.

| Page | What it shows | What you can do there |
|---|---|---|
| **Pending**, **Running**, **History** (`/queue?tab=...`, the tabs under the top bar) | The number of jobs waiting for approval and of those approved or running, on the tabs; 50 jobs per list and page (the oldest first, the history newest first; "More" while there are more), one row per job: when (a "waiting for you", "waiting to start" or "running" pill, or the outcome in the history, with its time), the job's id (a link to its page), "assistant" for a query of the chat's assistant, target and backend, output file, description and its actions; in the history the rows and size, the error or the rejection reason. A click on a row (not on its link, a button or a field, and not one that ends a selection of text) or its "SQL · EXPLAIN" button (for MongoDB "Pipeline · EXPLAIN"; "SQL" or "Pipeline" outside the Pending tab) opens the job in a floating window over the list, with what its page shows (below); Escape, Close or a click outside closes it. The Running tab has two lists, each with its own pages: "Approved, waiting to start" above "Running"; the running list refreshes itself every 5 seconds. The old address `/queue?tab=approved` shows the Running tab. Under the Pending list, the **auto-accept panel**: each target with its switch (on or off), its backend, its limit of the scheduler's largest, and "Change..." to the Auto-accept page | Pending: Approve and Reject, which act at once: the row's action cell then says what came of it ("approved by you", "rejected by you"), or why it was refused, with the buttons kept. In the floating window: the job's facts, its whole statement with the hints, EXPLAIN (its plan in the window), Approve, and Reject with a reason; what is done there is also shown in the row underneath. Running: Kill, at once ("killed by you"), also in the window. History: Preview (the job's page with the preview loaded) and Download (the job's page, where each part is offered). Open a job |
| **A job** (`/queue/<job id>`) | Everything the scheduler's page shows of it: its phase and status, target, backend (for MongoDB the collection and database), output file (and whether it overwrites an existing result), times, rows and size, description, error or rejection reason, who approved or rejected it or asked to kill it through the web UI (`web-<n>`, the broker's number of the session), and its statement as the scheduler holds it, with its hints | Pending: approve, reject with a reason, EXPLAIN, each at once (the answer is shown beside the buttons, the plan under EXPLAIN). Running: kill, at once. Finished: preview the result and download each of its parts |
| **Auto-accept** (`/queue/auto-accept`, from the panel's "Change...") | Each target of the scheduler with its backend and database, whether auto-accept is on and its limit | Switch it on or off and set the limit, one form per target |

A job whose record is too large for the web UI (more than 1 MiB) is listed, but its page says to use the scheduler's own page, and it offers no approval.

### Approving, rejecting, killing, EXPLAIN

Approve, Reject, Kill and EXPLAIN act when clicked, without a confirm page: the answer is shown where the buttons were (a pill such as "approved by you", or the refusal's sentence with the buttons kept), and an answer that has not come within 2 seconds says the request was sent and links its outcome page. With a session they need no password. A change of auto-accept still goes through its confirm page (below). Beside the buttons, the floating window and the job's page say what the confirm pages said: that an approved query runs with the database's admin login, what a rejection does, what a kill does, and that EXPLAIN runs before anyone approved the query.

- **Approve.** The floating window and the job's page show the target and backend, for MongoDB the collection and database, the output file and whether it overwrites an existing result, the description, and the whole statement exactly as the scheduler holds it, with every invisible character written as an escape and a list of **hints, not a check** (words of a short list per backend that the statement holds). Nothing checks the statement. Their Approve approves only the job as they showed it: the scheduler refuses it when the job changed since ("changed") or is no longer pending. A row's Approve approves the job as the scheduler holds it at the click (the web UI reads it then, and refuses when it is no longer pending), without its statement having been on screen. How the approval is built is in [How an approval is confirmed, and what the page does not check](../architecture/control-plane.md#how-an-approval-is-confirmed-and-what-the-page-does-not-check).
- **Reject.** A rejection's reason (in the floating window and on the job's page; a row's Reject sends none) is at most 500 characters, without a line break or a control character, and is checked before anything is sent.
- **Kill.** A kill says whether the job was running; a job may still finish by itself before the database stops it.
- **EXPLAIN.** It is sent to the database with the database's admin login, before anyone approved the query, as the scheduler's own EXPLAIN button does: a statement written for it can execute, and the window and the page say so. The plan (its first 16 KiB) or the database's error text is shown under the button.

A change whose answer never came may have been done: its outcome says so and asks you to open the job (or the auto-accept page) again to see. An EXPLAIN that the database did not answer in time may still be running there, and a change the scheduler did not finish in time may still happen: the outcome says so. A step before the change that fails (the session's check, the password's, the second look at auto-accept's settings) says that nothing was sent. A session the broker has ended goes to the login page and the scheduler is not asked.

### Queries the chat's assistant submitted

A query that the [chat](chat.md)'s assistant submitted is labelled wherever it is shown: "assistant" (and "from a chat") in its row of each tab, "Submitted by the chat's assistant" in its floating window and on its page.

- The floating window and the page of such a query also say, beside Approve and Reject: "The chat's assistant wrote this query, and text in the data it read can steer what it writes: read the statement as carefully as one from a stranger."
- They say, beside EXPLAIN and above its own sentence: "The chat's assistant wrote this query, and text in the data it read can steer what it writes: an EXPLAIN of it runs before anyone approved it."

### Auto-accept and the management password

With auto-accept on, the scheduler approves every query submitted to that target itself, the chat's assistant's included, up to the limit of the target's queries approved or running at a time, and each runs with the database's admin login without anyone looking at it (a query the assistant wrote included, and text in the data it read can steer the assistant); the pending page of switching it on says so.

Switching auto-accept on for a target, or raising its limit, needs the management password again: the confirmation checks it at the broker before the scheduler is asked, and a wrong one keeps the pending action (five wrong ones end the session, as for any action). Switching it off or lowering the limit needs none. Only what you changed is sent. The web UI reads the target's settings again at the confirmation and refuses the change when they are no longer those its pending page showed as "Now" (another session changed them meanwhile), so that what is set is what the page's "After" line said. A target the scheduler does not have, and a limit above the scheduler's largest (the page names it), are refused before anything is kept.

### Previews and downloads

For a finished job the page has a **Preview** button: the scheduler reads at most 1 MiB of the first part of the result and sends at most 50 rows, which are shown as text, with the names and sizes of the parts and "truncated" when the first part is longer than what was read. The page also offers one download link per part (`/queue/<job id>/download/<n>`, counted from 0 in the order of the parts). The web UI reads the parts itself, from its read-only mount of the results folder, and only for a job the scheduler has in its history as done. At most two downloads run at a time; a third is answered "busy". Which files are offered and how they are sent is in [Previews and downloads of the Queue](../architecture/control-plane.md#previews-and-downloads-of-the-queue).

### What the Queue window needs

- **The scheduler set up** (`python sdp.py db setup-jobs`, which makes `data/jobs` as you) and started with an image that has the control socket (`python sdp.py db start jobs`, which rebuilds it).
- **The web UI started** (`python sdp.py webui start`) after `data/jobs` exists. The start makes `data/jobs/control` (mode 0700) when it is missing and mounts it read-only into the container; it mounts the results folder (`JOBS_RESULT_ROOT` of `.env`, by default `data/jobs/results`) read-only for the downloads.

`python sdp.py webui status` says which of the two folders are mounted. After fixing a reason below, stop and start the web UI.

| What the start finds | What happens |
|---|---|
| `data/jobs` is not there, is a link or belongs to another user; or `data/jobs/control` is a link or belongs to another user | The start prints one line saying so and starts the web UI without the Queue's mounts |
| The results folder is missing, has a comma in its path, is `data/jobs` or a folder above it, or lies inside the control folder | The start prints one line and starts the web UI without downloads |
| `JOBS_RESULT_ROOT` written in quotes in `.env`, or overridden in the shell's environment when the scheduler was started | The start reads that key from the deployment's `.env` the way `sdp` reads it, so such a value is not followed: the downloads are then not available (or show nothing for a job) |

Each page of the window says why it cannot reach the scheduler:

| The page says | What to do |
|---|---|
| The folders were not there when the web UI started | Set up the scheduler, then stop and start the web UI |
| The control folder is a link or not a folder, belongs to another user or is open to the group or others (nothing is sent to it) | `chmod 700 data/jobs/control`, or fix the owner |
| Something other than a socket of your user that only you may use is in the socket's place (a file, a link, a socket of another user or one open to others: nothing is sent to it) | Remove it; the scheduler makes the socket at its start |
| There is no socket | The scheduler is not running, or its image was built before it had one: `python sdp.py db start jobs` |
| Nobody listens on it | The scheduler is not running |
| The web UI may not open it, or the scheduler closed the connection without an answer | The web UI does not run as the user `data/jobs` belongs to |

The scheduler's own page on its port is unchanged and keeps working. Why the scheduler itself may keep the socket closed is in [The control socket](../architecture/query-scheduler.md#the-control-socket).

## The Chat window

The Chat window (`/chat`, the first entry of the window bar, which leads back to the chat last opened in this login) is where you ask questions about the data and the [chat](chat.md)'s assistant answers. The assistant runs in the chat's sandbox container; the web UI talks to the chat's runner there through its socket (`.sdp/chat/socket/runner.sock`) and holds nothing of the chat itself: no credential, no file of the work folder.

The window has three columns at full height, the same for `/chat` and for one chat (`/chat/<id>`):

| Column | What it shows | What you can do there |
|---|---|---|
| **Left** | "New chat"; the chats, newest first, each by its title (the start of its first question), the open one marked and a pulsing dot on a chat whose turn runs (as of the page's load); "Data in reach", docked at the bottom of the column (the chat list above it scrolls on its own) in a box that its heading opens and closes (open unless you closed it last in this browser; a long list scrolls inside it): per database the sources the assistant can query, one per line with its size on disk in parentheses (as the Overview shows it, for example "reddit (1.4 GB)"; "≥" when a folder of the source could not be read whole, and no size when it could not be read, with a note saying so), and whether the query queue is attached ("Queue · long queries, after approval", each query then waiting until a person approves it in the Queue window, or "Queue · not attached"). A database is listed with its sources only when the chat's gateway attaches it, it runs, and its MCP server runs and is read-only; a source is listed under it when that database holds data loaded from it (more than no file loaded, as the source's Pipeline tab counts them). Otherwise one muted line says why: "stopped", "no MCP server set up", "MCP server down", "MCP server not read-only", "no data loaded", "its loads could not be read", or, for a database that is set up but that the chat does not attach, "not attached to the chat". It is read once per login, for at most 50 sources and about 20 seconds, and later pages of the Chat window draw that read at once; when it is more than a minute old, the page still draws it at once and reads it again in the background, and the block changes only if something changed. Only the first read shows "checking…" for each database the chat attaches until it is done; a read that fails says so in the block alone and is not kept, so the next page reads it again | Start a new chat (on this browser's default model), open one |
| **Middle** | With no chat open, an invitation to start one. With a chat open, its title, its model and back end (with `claude-code` a choice of `opus`, `sonnet`, `haiku` or "backend default", the chat's own model chosen; the question you send next runs on the model chosen there, and the chat keeps it; with `openai-compatible` the one model, not choosable), an **Export** link that downloads the whole chat as one JSON file (see [Exporting a chat](chat.md#exporting-a-chat)) and its number of files, then the conversation: your questions on the right, the assistant's answers, each tool call as a slim row (a tool of an MCP server named `mcp__<server>__<tool>` shown as `<server> · <tool>`) whose input opens from it, and each tool result as a row under it whose text opens from it (a result that was an error is marked red), each file a turn made or changed in a card with its size and its buttons (a PNG or JPEG image is shown in the card, with "PNG" or "JPEG" to download it and, for each file of the chat's folder in the same subfolder and with the same name before its ending, "Data (CSV)" (or TSV, Parquet, JSON), "Spec (JSON)" for a `.vl.json`, "Code" for a `.py` and "Table view" for a `.csv`; when the folder holds a `.vl.json` of the image's name, the card draws it as an interactive chart in place of the image; a `.vl.json` without an image of its name is drawn in its own card, with "Spec (JSON)"; any other file has "Download", and "Table view" when it is a `.csv`), and at the end of each turn a line with how long it took and the cost the back end reported (Claude Code reports one, the `openai-compatible` back end none). The conversation opens at its newest message; while a turn runs, new events appear by themselves (the page asks every 2 seconds) and the view follows them only while it is at the bottom, so scrolling up to read is not interrupted. One view shows at most 2,000 events and then a link "Continue from here", which opens the chat from that event on (`/chat/<id>?from=<n>`). The answer box is pinned at the bottom, with "Stop the turn" beside it while a turn runs: Enter sends the question, Shift+Enter starts a new line, Enter while an input method is composing (Chinese, Japanese, Korean) does nothing, and a box holding only spaces or line breaks sends nothing | Send a question, stop the turn that runs, download a file, open a CSV file as a table, export the chat |
| **Right** | **This chat** (with no chat open, **Sandbox**): the chat's backend ("Claude Code" or "OpenAI-compatible") and its model, as text (it is chosen in the header); its turn totals: the turns that ended (and how many ended with an error), their time and their cost as the backend reported them (Claude Code reports a cost, the `openai-compatible` back end none: "not reported"), added up from the ends of turns in the chat's log from its start (at most its first 2,000 events, which the column then says), read after the page has loaded and again whenever a turn ends while the page is open; the sandbox as one line ("Claude Code · sandbox up · read-only access to 2 databases", or that it did not answer and why), which opens on what it can reach (the model, the databases' read-only MCP servers and, with the queue, the queue's endpoint for the assistant, only through the chat's gateway), cannot (approve its own queries; reach Docker, the broker, this machine, the pipeline's files or the internet), reads and writes (the chats' folders and its socket folder under `.sdp/chat`); **Files**: the open chat's files with their download links ("Table view" beside a `.csv`); **Queries in the queue**: the queries the assistant submitted in this chat, newest first, each with its state ("waiting for approval", "approved, waiting to start", "running", "finished", "failed", "rejected", "cancelled") and a link to its page in the Queue window, read after the page has loaded and every 60 seconds | Hide the column (**Hide**; the conversation then takes its width, and the button "This chat" at the top right of the conversation shows it again), open or close the sandbox's facts (both are kept in this browser), delete the open chat, open a query in the Queue |

When the sandbox does not answer its `hello`, the side columns say so and the conversation is still shown. The sandbox is restarted or stopped at the terminal only (`python sdp.py chat stop`, `python sdp.py chat start`). Below 1150 pixels of width, the side columns are stacked under the conversation.

The chat's queries are found in the chat itself: every job id written in its tool calls and their results (at most the first 2,000 events are searched, the 20 newest ids kept), each looked up in the scheduler and shown only when the scheduler says the assistant submitted it; an id the scheduler does not know is left out, and one it could not read is a row that says so. With no queue attached to the chat, or the scheduler not given to the web UI, the section says so in one line.

- **Send.** A question is 1 to 32,000 characters. Send makes the sandbox start a turn and leads back to the chat, where the answer comes in as it is written; one turn runs per chat, and at most two in the whole sandbox.
- **A refused question** stays in the box with the reason: a turn of this chat still runs, two turns run already, the work folder is over its soft cap, or the chat's log is full (start a new chat).
- **Stop** ends the turn that runs; the conversation then says the turn was stopped.
- **Delete** asks first on a page of its own (which changes nothing) and then removes the chat and every file in its folder; a chat whose turn runs cannot be deleted until the turn has ended.

Sending, stopping and deleting need no management password: they change nothing outside the sandbox. Everything the sandbox sends is shown as text: an answer's links, images and HTML are shown as their literal text, never followed or loaded (see [Everything from the sandbox is shown as text](../architecture/control-plane.md#everything-from-the-sandbox-is-shown-as-text)).

### Files and plots

The files are those in the chat's folder in the sandbox (what the assistant writes, such as a plot made with a shell command), including those in its subfolders, which are listed by their path (`scripts/vol.py`).

| What | How |
|---|---|
| A download | Sent as an attachment (`application/octet-stream`), under its name (the last part of its path) with every character but letters, digits, `.`, `_` and `-` replaced by `_`; at most 1 GiB, two at a time, within 30 minutes. If the file changes size while it is sent, or a piece before the end comes short, the download ends without the rest. |
| An image in the page | Shown only when the file's first bytes are those of a PNG or a JPEG (whatever its name says), at most 8 MiB, read within 30 seconds; any other file named `.png` is not shown. |
| An interactive chart | A chart spec of Vega-Lite (`<name>.vl.json`, which the assistant writes with Altair) is drawn in the card: hover a point for its values, scroll to zoom, drag to pan, click a legend entry to pick its series (where the chart defines them), and "Save as SVG" or "Save as PNG" for the view as it is. A chart wider than the conversation column is scaled down to the column's width (its text gets smaller with it); hovering, zooming and panning still land on the points. When the chart draws its y axis on a log scale (`log`, or `symlog`), in a single view or in any of its layers, panels or facets, a "Linear / Log" switch above it redraws every such axis linear, or as the spec has it again; the card keeps its size, and keeps the choice while the page is open (a reload shows the spec's own scale). The web UI reads the spec through the runner's socket (at most 2 MiB) and checks it before the page gets it: strict JSON, nested at most 64 deep, a Vega-Lite chart, its data only rows held in the spec (at most 50,000), at most 100 layers or views, and nothing that loads data from an address, links a mark to an address, draws an image or places a control outside the chart. A refused spec, or one the page cannot draw, is one sentence in the card ("This chart could not be drawn: …"), and the PNG of its name is shown instead. |
| The export | `/chat/<id>/export`: the whole chat as one JSON file, `chat-<id>.json`, sent as an attachment while it is read (what it holds is in [Exporting a chat](chat.md#exporting-a-chat)); at most 50,000 events and 64 MiB in all, within 10 minutes, and two downloads or exports at a time. A bound or a failure ends it early with the file still valid JSON, its `stopped` saying where and why. |
| The table view | `/chat/<id>/table/<name>`, for a `.csv` only: the first 256 KiB of the file, read through the runner's socket, shown as at most 500 rows under the header row and 50 columns, saying what was left out; a file that is not CSV text (a NUL byte, a quote that never closes) is one sentence instead. |
| Which files are listed | Every file directly in the chat's folder, and the `.py`, `.png`, `.jpg`, `.jpeg`, `.svg`, `.json`, `.csv`, `.txt` and `.md` files of its subfolders up to three folders down; folders whose names start with `.` are not looked into, and links are never followed. Only names of letters, digits, spaces, dots, dashes and underscores are listed or linked (the runner's own rule, for each folder of a path too); the list says how many names it left out by that rule and how many files of subfolders by their type. A file that is not listed is in the work folder on this machine (`.sdp/chat/work`). |

### What the Chat window needs

- **The chat set up and started** (`python sdp.py chat setup`, `python sdp.py chat start`), which makes `.sdp/chat/socket` (mode 0700) and starts the sandbox that listens there.
- **The web UI started after that folder exists.** The start mounts it read-only at `/run/sdp-chat`. When it is not there, is a link (or a folder above it is), belongs to another user or is open to the group or others, the start prints one line saying the Chat window will not be available and why, and starts without it (`python sdp.py webui status` says whether it is mounted). Stop and start the web UI once the chat is set up.

The page says why it cannot reach the chat:

| The page says | What to do |
|---|---|
| The folder was not there when the web UI started | Stop and start the web UI |
| The folder or the socket fails its check (a link, another user's, open to others: nothing is sent) | Fix the folder; `python sdp.py chat start` makes it |
| There is no socket, or nobody listens on it | The chat is not running: `python sdp.py chat start` |
| The sandbox closed the connection without an answer it can read | Stop and start the chat |

## Reaching it from another machine or a tailnet

By default the page is published on 127.0.0.1 only. To reach it from another machine, answer the address question of `python sdp.py webui setup` in one of two ways:

- **A list**: the addresses of this machine on the networks you want (a LAN address, or the address this machine has on a tailnet such as Tailscale) and the names you will type in the browser (a tailnet name, for example), comma-separated and in any order. Each address is published on its own. The page answers only to the names and addresses you gave: a request with any other `Host` is refused, which also stops a page of another site from reaching it by pointing a name of its own at your machine. An entry that is neither an IP address nor a host name is refused with a line naming it, and so are loopback addresses, multicast, a link-local address with a zone, `localhost` and an entry given twice.
- **`all`** (or `0.0.0.0`, `::`, `any`, `*`), alone or beside addresses and names: the page's port is published once on every address of this machine (every IPv4 and IPv6 address Docker can bind; a Docker daemon configured with a default bind address of its own publishes there instead), and the page answers under any address or name, with nothing else to list. Every address includes networks you did not choose (a VPN, an interface added later, Docker's own networks), which setup, `webui start` and `webui status` warn about. A login there still needs the management password, and every other check of the page stays (a post must come from the page's own origin and carry its form token). In this mode every login arrives on the container's ordinary port and counts as one from elsewhere: there is no separate budget of wrong passwords for logins at the machine itself.

Loopback addresses (`::1`, `127.0.0.2`, ...) are not taken as listen addresses: the machine itself is served on 127.0.0.1. How a login at the machine is told from one from elsewhere is in [Local and remote logins](../architecture/control-plane.md#local-and-remote-logins); "local" means anyone on this machine who can reach 127.0.0.1, other accounts of the machine included.

The session cookie is sent by the browser to every service on the same host name, whatever its port. Reach the page under a name or an address that no other service of yours is reached by.

## The certificate

The page is served over HTTPS only. Setup offers two choices:

- **generate** makes a self-signed certificate (EC P-256, valid 825 days, for serving only) whose names are `localhost`, `127.0.0.1` and the addresses and names you gave (with `all`, those listed beside it). A browser does not know it: the first visit shows a warning. Compare the SHA-256 fingerprint the browser shows for the certificate with the one `webui setup` and `webui status` print; only when they are equal is it your web UI. With `all`, a page opened under any other address shows the browser's certificate warning there too: clear it by the fingerprint the same way. Alternatively import `.sdp/webui/tls/cert.pem` into the browser's or the system's trusted certificates.
- **Your own certificate** (from your tailnet or your own certificate authority): give `webui setup` the paths of the certificate and the key, which are copied. Its names must include the names and addresses you browse to.

The page sends no `Strict-Transport-Security` header, so a browser does not pin the name to a certificate that may change at the next setup.
