# The broker

The broker is a process that runs on the host, next to the CLI, and listens on a unix socket in the deployment folder. Through that socket a logged-in session can run a fixed list of actions of the CLI: the status commands, starting, listing, reading and cancelling pipeline runs, starting and stopping databases, and a few removals. It is the piece the [web UI](webui.md) talks to: the web UI runs in a container as the operator's user, holding nothing but the socket's folder, while the broker does the work on the host. Its client is `social_data_pipeline/control/client.py`. It runs on Linux only.

## Commands

| Command | What it does |
|---------|--------------|
| `sdp.py broker passwd` | Sets the management password, asked twice and not echoed (at least 12 characters). Works whether or not the broker runs; sessions that logged in with an older password end at their next request. |
| `sdp.py broker start` | Starts the broker in the background and waits up to 10 seconds until it answers. Prints its pid, or that it runs already, or why it did not start (exit code 1); a broker that has not answered by then is ended. |
| `sdp.py broker start --foreground` | Runs the broker in this terminal until Ctrl-C. |
| `sdp.py broker stop` | Stops the broker. From then on it takes no new login and no new call (they are answered `stopping`); an action that changes something and is still running is waited for, without a time limit; the command says once that it waits. |
| `sdp.py broker stop --force` | Stops the broker after ending such an action (TERM, then KILL after 10 seconds). |
| `sdp.py broker status` | Says whether the broker runs, its pid, since when, the socket's path and whether a management password is set. Exit code 0 when it runs, 1 when not. |
| `sdp.py broker audit [--limit N] [--json]` | Prints the newest N lines (default 50) of the audit log: time, session, event and its details. |
| `sdp.py broker audit --verify` | Checks the chain of the whole audit log; exit code 1 when a line was removed, edited or put in between, or the log cannot be read. |

Stopping the broker never stops a pipeline run: each run started through it has a supervising process of its own, which goes on. Such runs show in `sdp.py runs list` as kind `web` and are cancelled with `sdp.py runs cancel` like any other.

## Files

| Path (in the deployment folder) | What it is |
|---------------------------------|------------|
| `.sdp/socket/broker.sock` | The socket, in a folder of mode 0700. The full path must fit in the 108 bytes of a unix socket address; a deployment folder with a longer path is refused at start with a message saying so. |
| `.sdp/broker/password.json` | The management password as a salted scrypt hash, never the password. |
| `.sdp/broker/audit.jsonl` | The audit log: one line per login, logout, session end, refused call, accepted call and ended call, each naming the hash of the line before it. Failed logins, requests without a valid session and read calls are only counted, in a `counts` line at most once a minute. Secrets are written as `<secret>`. |
| `.sdp/broker/broker.log` | The broker's own lines: start, stop, and the kind of any error it met (never its text). |
| `.sdp/broker/lock` | Held by the running broker; holds its pid and start time. A second broker is refused. |
| `.sdp/broker/write.lock` | Held while an action that changes something runs, by the broker and by the action's process, so that only one such action runs at a time, also after a broker crash. Starting a pipeline run holds it shared, so several runs can be started at once but none while such an action runs. |
| `.sdp/broker/calls/` | The output of an action that changes something, in two files while it runs (read and deleted when it ends). Because the action writes to files and not to the broker, it runs to its end even when the broker crashed; files that no running action owns are deleted at the next start and every 30 seconds while the broker runs, whenever no such action is alive. |

`.sdp/broker` has mode 0700 and its files 0600, whatever the umask.

The broker refuses to start when the deployment folder, `sdp.py`, or any folder or `.py`/`.pyc` file under `social_data_pipeline/` is a symbolic link, belongs to another user, or can be written by others; the group may write them only when it is the operator's private group (the user's primary group, with nobody else in it). `.sdp`, `.sdp/broker` and `.sdp/socket` may not be writable by the group at all. The message names the path and the `chmod` command that fixes it.

## What a session can do

A session starts with a login with the management password and lasts at most eight hours. A login names its channel, `local` (the operator's own machine, as the web UI tells it) or `remote` (anything else, and the default), and each channel has its own budget of five failed logins, one given back every 30 seconds, so that failed guesses through the network never lock out a login on the operator's own machine; the audit log counts the failed logins of both as one number. Every action is a verb of the registry (`social_data_pipeline/control/verbs.py`) with typed fields; a field that is not listed for the verb is refused. Verbs that remove or start database services (`db.start`, `db.reset`, `db.unsetup`, `db.unsetup_mcp`, `db.unsetup_jobs`, `source.remove`) need the management password again with the call; five wrong ones end the session. A session can keep a database admin password, after giving the management password again; it is then added to `run.start` and `db.start` when the call carries none. `db.reset`, `db.unsetup`, `db.stop` and `source.remove` are refused while a pipeline run is alive, and when Docker cannot be asked whether one is; they look for live runs only once they hold the write lock, so no run can be started between that look and the action. `db.start` of PostgreSQL (or of every service) is refused with a database password that holds a quote, `$`, a backtick or a backslash.

Each action runs in a process of its own, started in the deployment folder with the request on its standard input and an environment cut down to a short list of names. A read action has at most 60 seconds and 8 run at a time; an action that changes something runs to its end even when the client has gone. What leaves the broker has every database password a session holds or held, every one of at least 8 characters a request carried in its fields (a shorter one is overwritten in that request's own reply only), and the `HF_TOKEN` of its environment, overwritten with `*`, as is the rest of every `$ docker compose` line the CLI prints; an answer too large to go through that is refused rather than sent. The log of a pipeline run that has not ended is served without its last few bytes (as many as the longest of those secrets, less one), so that a secret it is still writing is never shown half written; once the run has ended, its log is served to the end. The audit view (`audit.list`) is overwritten with the sessions' passwords and the environment's token only, so that words sent as passwords cannot blank it.

Not bounded or not done, on purpose: the output files of an action have no size limit (the CLI's actions print little); a process that an action leaves behind after it has ended is not ended by the broker; and whether a group is the operator's private one is best effort where accounts come from a directory service that is not enumerated.

## What it defends against, and what not

The broker defends against:

- **Another user of the machine.** The socket's folder is 0700, and a connection from any other user id, read from the kernel, is closed without a reply. The broker refuses to start when the CLI, the package, the `control` package folder or its own folders belong to another user or can be written by the group or by others.
- **Whatever can send bytes to the socket** (the web UI's container). One request per connection, at most 64 KiB and ten seconds to arrive; nothing but `hello` and `login` is answered without a valid session; only the registry's verbs, with typed fields, deny by default.
- **A stolen session token.** It can run the typed verbs only, and needs the management password again for anything that removes or starts services.

It does NOT defend against a program that runs on the host as the operator: such a program already has Docker and the deployment's files.
