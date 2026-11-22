---
title: "Web UI"
parent: "User guide"
nav_order: 11
---
# Web UI

The web UI is a page in your browser for the pipeline of one deployment: you start and follow pipeline runs, start and stop databases, approve the queries in the query scheduler's queue and ask the chat's assistant about your data. Every action shows its `sdp` command and runs only once you confirm it.

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
| `python sdp.py webui status` | Says whether it is set up and runs, whether the Queue window's two folders and the Chat window's socket folder are mounted, its port, listen addresses and names, the certificate's fingerprint and end date, and the addresses of the page (with `all`: that it answers on every address, and the warning). Exit code 0 when it runs, 1 when not. |
| `python sdp.py webui unsetup` | Asks first, then removes the container, the network, the image and `.sdp/webui/`. |

To change the port, addresses, names or certificate: `python sdp.py webui stop`, `python sdp.py webui setup`, `python sdp.py webui start`.

### When the broker does not start

- **A path that is a link, another user's, or writable by others.** The broker refuses to start when the deployment folder, `sdp.py`, or any folder or `.py`/`.pyc` file under `social_data_pipeline/` is a symbolic link, belongs to another user, or can be written by others. The group may write them only when it is the operator's private group (the user's primary group, with nobody else in it). `.sdp`, `.sdp/broker` and `.sdp/socket` may not be writable by the group at all. The message names the path and the `chmod` command that fixes it.
- **A deployment folder with a long path.** The socket is `.sdp/socket/broker.sock`; its full path must fit in the 108 bytes of a unix socket address, and a deployment folder with a longer path is refused at start with a message saying so.
- **A broker that already runs.** A second broker is refused.

The broker's files under `.sdp/` are listed in [The broker's files](../architecture/control-plane.md#the-brokers-files).

## What a session can do

- **A login** takes the management password and starts a session that lasts at most eight hours. It ends at the logout, when the broker ends it (the password was changed, the broker was restarted) and when the broker cannot be reached; the login page then says why. Restarting the web UI logs everyone out.
- **Failed logins** are limited: five failed logins, one given back every 30 seconds, counted apart for logins at this machine and from elsewhere, so that failed guesses through the network never lock out a login at the machine itself. The web UI also limits each client address to five failed logins in five minutes.
- **Every action** is a typed action of the CLI; the page cannot run anything else.
- **The management password again** is asked for a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, removing a source, and keeping the database admin password for the session. Five wrong ones end the session.
- **The database admin password** can be kept for the session, after giving the management password again. A run start and a database start then use it when their form leaves the field empty.
- **Refused while a run is alive:** a database reset, removing a database, removing the MCP servers' configuration, stopping services and removing a source are refused while a pipeline run is alive, and when Docker cannot be asked whether one is. Removing the MCP servers' configuration is among them because the CLI stops each database that has an MCP server configured together with that server.
- **A PostgreSQL start** (or a start of every service) is refused with a database password that holds a quote, `$`, a backtick or a backslash.

## The pages

After the login you get the Management window: the **Overview** and the pages **Sources**, **Databases**, **Runs** and **System**.

The top bar names the three windows (Chat, Management, Queue); the number on Queue is the queries waiting for your approval. On its right, a dot per service says whether it answers: broker, scheduler and sandbox, green when it answered, red when it did not, grey when the web UI was not given it (no query queue or no chat set up). They are read after the page has loaded and every 30 seconds, so a slow service never slows a page; the title of each says what was found. A window with more than one page shows them as tabs under the top bar.

| Page | What it shows | What you can do there |
|---|---|---|
| **Overview** (`/`) | **Needs attention**: a database without authentication, ports open beyond this machine, a read that failed, Docker out of reach, and the queries that wait for your approval. **Right now**: the pipeline runs alive now and the state of the databases, the MCP servers and the scheduler. **Sources and pipeline profiles**: one row per source, one column per `sdp run` profile; a cell says "running" while a run of that profile on that source is alive, "set up" when the source has the profile, "not set up" otherwise, and, for `parse`, `lingua` and `ml`, the number of finished files of that stage (`source progress`; it knows no total, so no fraction is shown). The queries waiting and the counts are read after the page has loaded, for at most 50 sources and about 20 seconds, so they never slow the page; a source whose count could not be read says so in its row | Open a run's page, the Queue, the Databases page, or a source (a cell opens the source at its profile's run form) |
| **Sources** (`/sources`, a source at `/sources/<name>`) | On the left the configured sources (`source list`); on the right the first source, or the one opened, with three tabs. **Pipeline**: its stages as in the pipeline (dumps, parse, lingua, ml, then per database its loads), each marked running, set up, not set up or with failed files, with the finished files per stage (`source progress <name>`) and the files loaded and failed per database from its state files (`source status <name>`), and the forms to run its profiles. **Configuration**: platform, data types, profiles and paths (`source status <name>`). **Errors**: the error logs of its profiles among `postgres_ingest`, `postgres_ml` and `mongo_ingest` (`source error-logs`; one too large for the page names the terminal command that prints it) | Run each of its profiles, one small form per profile with only that profile's options: rebuild the image (all), a file filter (all but `sr_ingest` and `sr_ml`), skip files with a lingua output (`parse`), one classifier (`ml`, `lingua`, `sr_ml`), the database admin password (the profiles that use a database; the form says whether one is kept for the session). Remove the source |
| **Databases** (`/databases`, one database at `/databases/<database>`) | Four tabs. **Servers**: a card per database with its state, ports, data folder, admin user and whether its checks found drift (`db status`, `db verify`), and a card for a database that is not set up. **MCP servers**: each server's state, port and whether it logs in as a read-only user. **Queue setup**: the scheduler's state and port. **Verify**: every check of `db verify`, one line each, with what to run for a drift | Start or stop one database, an MCP server or the scheduler with its own buttons, or any service (or all) with the forms under the cards (with the database admin password when one is needed); reset or remove a database, remove the MCP servers' or the scheduler's configuration, keep the database admin password for this session or forget it |
| **Runs** (`/runs`) | The runs, recorded ones (started from the web UI) and those started in a terminal that are alive now: state, profile, source, start, end, exit code, where it was started from (`runs list`) | Open a run, cancel one that is alive |
| **A run** (`/runs/<run id>`) | Its facts (the non-secret fields it was started with, who started it, its state) and its output, which follows the run as it goes (see [The output of a run](#the-output-of-a-run)) | Cancel it while it is alive |
| **System** (`/system`) | Two tabs. **Broker and verbs**: what the web UI may ask the broker, by class (read, write, step-up), the broker's greeting (its protocol, whether a management password is set), and the table of every entry with its class and terminal command. **Audit log**: the newest 200 lines of the broker's audit log (time, session, event, verb, outcome; a refused call with its code) | Nothing |

Each block of a page is read on its own: a read that fails (a time-out, a state file that cannot be read, Docker out of reach) is shown as a sentence in its block, and the rest of the page stays. A name becomes a link only when it is a plain name (a letter or digit, then letters, digits, `_`, `.`, `-`, at most 64 characters); everything the broker hands over is shown as text.

## How an action is confirmed

**1. You choose.** A form (a button on one of the pages) sends what you chose. The web UI checks it against its own table of actions without asking the broker: only the fields that action takes, the closed choices, names of the plain form, a file filter of letters, digits and `_ . * ? [ ] ! -` (not only dots), and the rules of `run`: no file filter with `sr_ingest` and `sr_ml`, skipping lingua files only with `parse`, a classifier only with `ml`, `lingua` and `sr_ml`. A choice that does not pass is refused and nothing is kept or sent.

**2. The page shows the pending action.** It is kept as a pending action of your session, and the confirm page (a dialog drawn in the page, never with a scroll box of its own) shows the `sdp` command it stands for (what you would type in a terminal; it never holds a password) and every input (a password as dots). For an action that destroys something, one sentence says what it destroys and names it. For a start or a stop, the page says what else the service chosen acts on (a database's MCP server, the scheduler, the database an MCP server needs; "every configured service" when none is chosen). A terminal would also ask for the secrets the command does not show (the database admin password, a confirmation).

**3. You confirm.** For the actions that need it, the page has a field for the management password: a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, removing a source, and keeping the database admin password for the session. Starting and cancelling a run, stopping services and forgetting the database password need none. Confirm sends only that password, or nothing. The pending action is then taken out of the session, so a second confirm (a double click, another tab) finds nothing; it is passed to the broker once.

**4. The outcome page follows it.** The action runs in the background, not tied to your request, with no time limit (a state-changing command can last many minutes). The confirmation waits up to two seconds for an answer: a refusal that comes at once is shown on the pending page. A wrong management password keeps the action, so you can type it again; five wrong ones end the session at the broker, and so here. Otherwise the outcome page says "under way" and looks again every two seconds, until it shows the exit code with the end of the command's output and errors, the link to the run that was started, or the refusal's sentence. A broker that cannot be reached is an outcome; it does not log you out.

At most two actions run at once from the web UI; a third is answered "busy" and stays pending. Cancelling a run has two places of its own, so that a cancel gets through while a database start or a reset runs for minutes. A pending action is kept for five minutes, or until confirmed.

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

| Page | What it shows | What you can do there |
|---|---|---|
| **Pending**, **Running**, **History** (`/queue?tab=...`, the tabs under the top bar) | The number of jobs waiting for approval and of those approved or running, on the tabs; 50 jobs per list and page (the oldest first, the history newest first; "More" while there are more), one row per job: when (a "waiting for you", "waiting to start" or "running" pill, or the outcome in the history, with its time), the job's id (a link to its page), "assistant" for a query of the chat's assistant, target and backend, output file, description and its actions; in the history the rows and size, the error or the rejection reason. "SQL" (for MongoDB "Pipeline") opens the start of the statement in a row under the job. The Running tab has two lists, each with its own pages: "Approved, waiting to start" above "Running"; the running list refreshes itself every 5 seconds. The old address `/queue?tab=approved` shows the Running tab. Under the Pending list, the **auto-accept panel**: each target with its switch (on or off), its backend, its limit of the scheduler's largest, and "Change..." to the Auto-accept page | Pending: Approve, Reject, EXPLAIN (each leads to its confirm page, as on the job's page; a rejection from the row has no reason). Running: Kill. History: Preview (the job's page with the preview loaded) and Download (the job's page, where each part is offered). Open a job |
| **A job** (`/queue/<job id>`) | Everything the scheduler's page shows of it: its phase and status, target, backend (for MongoDB the collection and database), output file (and whether it overwrites an existing result), times, rows and size, description, error or rejection reason, who approved or rejected it or asked to kill it through the web UI (`web-<n>`, the broker's number of the session), and its statement as the scheduler holds it, with its hints | Pending: approve, reject with a reason, EXPLAIN. Running: kill. Finished: preview the result and download each of its parts |
| **Auto-accept** (`/queue/auto-accept`, from the panel's "Change...") | Each target of the scheduler with its backend and database, whether auto-accept is on and its limit | Switch it on or off and set the limit, one form per target |

A job whose record is too large for the web UI (more than 1 MiB) is listed, but its page says to use the scheduler's own page, and it offers no approval.

### Approving, rejecting, killing, EXPLAIN

Every change of the Queue (approve, reject, kill, EXPLAIN, auto-accept) takes the Management window's way: a pending action, its page, the confirmation, an outcome page. With a session, an approval, a rejection, a kill and an EXPLAIN need no password.

- **Approve.** The approval's page shows the target and backend, for MongoDB the collection and database, the output file and whether it overwrites an existing result, the description, and the whole statement exactly as the scheduler holds it, with every invisible character written as an escape and a list of **hints, not a check** (words of a short list per backend that the statement holds). The page does not check the statement. The scheduler refuses the approval when the job changed since its page was made ("changed") or is no longer pending. How the page and the approval are built is in [How an approval is confirmed, and what the page does not check](../architecture/control-plane.md#how-an-approval-is-confirmed-and-what-the-page-does-not-check).
- **Reject.** A rejection's reason is at most 500 characters, without a line break or a control character, and is checked before anything is kept.
- **Kill.** A kill says whether the job was running; a job may still finish by itself before the database stops it.
- **EXPLAIN.** It is sent to the database with the database's admin login, before anyone approved the query, as the scheduler's own EXPLAIN button does: a statement written for it can execute, and the page says so. Its outcome shows the plan (its first 16 KiB) or the database's error text.

A change whose answer never came may have been done: its outcome says so and asks you to open the job (or the auto-accept page) again to see. An EXPLAIN that the database did not answer in time may still be running there, and a change the scheduler did not finish in time may still happen: the outcome says so. A step before the change that fails (the session's check, the password's, the second look at auto-accept's settings) says that nothing was sent. A session the broker has ended goes to the login page and the scheduler is not asked.

### Queries the chat's assistant submitted

A query that the [chat](chat.md)'s assistant submitted is labelled wherever it is shown: "assistant" (and "from a chat") in its row of each tab, "Submitted by the chat's assistant" on its page and on the confirm pages of its approval, rejection, kill and EXPLAIN.

- The approval's confirm page of such a query also says, above the statement: "The chat's assistant wrote this query, and text in the data it read can steer what it writes: read the statement as carefully as one from a stranger."
- The EXPLAIN confirm page of such a query says, above its own sentence: "The chat's assistant wrote this query, and text in the data it read can steer what it writes: an EXPLAIN of it runs before anyone approved it."
- When the job could not be read for a rejection, a kill or an EXPLAIN, its page says "Whether the chat's assistant submitted this query could not be read." and the action can still be confirmed; the EXPLAIN page then says that the query may have been written by the chat's assistant.
- The auto-accept page and panel say that the assistant's queries are never auto-accepted: the scheduler leaves each one waiting for a person, whatever the target's setting.

### Auto-accept and the management password

With auto-accept on, the scheduler approves every query submitted to that target itself, up to the limit of the target's queries approved or running at a time, and each runs with the database's admin login without anyone looking at it; the pending page of switching it on says so.

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

The Chat window (`/chat`, the first entry of the window bar) is where you ask questions about the data and the [chat](chat.md)'s assistant answers. The assistant runs in the chat's sandbox container; the web UI talks to the chat's runner there through its socket (`.sdp/chat/socket/runner.sock`) and holds nothing of the chat itself: no credential, no file of the work folder.

The window has three columns at full height, the same for `/chat` and for one chat (`/chat/<id>`):

| Column | What it shows | What you can do there |
|---|---|---|
| **Left** | "New chat"; the chats, newest first, each by its title (the start of its first question), the open one marked and a pulsing dot on a chat whose turn runs (as of the page's load); "Data in reach": each database the assistant reads (read-only) and whether the query queue is attached ("Queue · long queries, after approval", each query then waiting until a person approves it in the Queue window, or "Queue · not attached") | Start a new chat, open one |
| **Middle** | With no chat open, an invitation to start one. With a chat open, its title and its number of files, then the conversation: your questions on the right, the assistant's answers, each tool call as a slim row (a tool of an MCP server named `mcp__<server>__<tool>` shown as `<server> · <tool>`) whose input opens from it, and each tool result as a row under it whose text opens from it (a result that was an error is marked red), each file a turn made or changed in a card with its size and a download button (and a PNG or JPEG image shown in the card), and at the end of each turn a line with how long it took and the cost the back end reported (Claude Code reports one, the `openai-compatible` back end none). The conversation opens at its newest message; while a turn runs, new events appear by themselves (the page asks every 2 seconds) and the view follows them only while it is at the bottom, so scrolling up to read is not interrupted. One view shows at most 2,000 events and then a link "Continue from here", which opens the chat from that event on (`/chat/<id>?from=<n>`). The answer box is pinned at the bottom, with "Stop the turn" beside it while a turn runs | Send a question, stop the turn that runs, download a file |
| **Right** | The sandbox: its back end and whether it answered, and what it can reach (the model, the databases' read-only MCP servers and, with the queue, the queue's endpoint for the assistant, only through the chat's gateway), cannot (approve its own queries; reach Docker, the broker, this machine, the pipeline's files or the internet), reads and writes (the chats' folders and its socket folder under `.sdp/chat`); the open chat's files with their download links | Delete the open chat |

When the sandbox does not answer its `hello`, the side columns say so and the conversation is still shown. The sandbox is restarted or stopped at the terminal only (`python sdp.py chat stop`, `python sdp.py chat start`). Below 1150 pixels of width, the side columns are stacked under the conversation.

- **Send.** A question is 1 to 32,000 characters. Send makes the sandbox start a turn and leads back to the chat, where the answer comes in as it is written; one turn runs per chat, and at most two in the whole sandbox.
- **A refused question** stays in the box with the reason: a turn of this chat still runs, two turns run already, the work folder is over its soft cap, or the chat's log is full (start a new chat).
- **Stop** ends the turn that runs; the conversation then says the turn was stopped.
- **Delete** asks first on a page of its own (which changes nothing) and then removes the chat and every file in its folder; a chat whose turn runs cannot be deleted until the turn has ended.

Sending, stopping and deleting need no management password: they change nothing outside the sandbox. Everything the sandbox sends is shown as text: an answer's links, images and HTML are shown as their literal text, never followed or loaded (see [Everything from the sandbox is shown as text](../architecture/control-plane.md#everything-from-the-sandbox-is-shown-as-text)).

### Files and plots

The files are those in the chat's folder in the sandbox (what the assistant writes, such as a plot made with a shell command).

| What | How |
|---|---|
| A download | Sent as an attachment (`application/octet-stream`), under its name with every character but letters, digits, `.`, `_` and `-` replaced by `_`; at most 1 GiB, two at a time, within 30 minutes. If the file changes size while it is sent, or a piece before the end comes short, the download ends without the rest. |
| An image in the page | Shown only when the file's first bytes are those of a PNG or a JPEG (whatever its name says), at most 8 MiB, read within 30 seconds; any other file named `.png` is not shown. |
| Which files are listed | Only names of letters, digits, spaces, dots, dashes and underscores are listed or linked (the runner's own rule); a file with another name is in the work folder on this machine (`.sdp/chat/work`). |

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
