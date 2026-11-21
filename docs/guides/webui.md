# The web UI

The web UI is a page in your browser for the pipeline of one deployment. It runs in a container of its own and does everything through the [broker](broker.md): it holds no database password, no Docker access and no file of the deployment, only the broker's socket folder and its own certificate. After the login you get the Management window: the **Overview** (the databases and whether they run, the configured sources with their profiles, the pipeline runs that are alive now, and standing notices for a database without authentication and for ports open beyond this machine) and the pages **Sources**, **Databases**, **Runs** and **System**, described below. Chat and Queue are shown as not available yet. Reading a page changes nothing; every action is shown with its `sdp` command and runs only once you confirm it.

It needs Linux and a Docker without user-id mapping: rootless Docker and `userns-remap` are refused, because the broker accepts connections only from the operator's own user id, and under such a mapping the container's connections arrive as another one.

## The pages

| Page | What it shows | What you can do there |
|------|---------------|-----------------------|
| **Sources** (`/sources`) | The configured sources and their profiles (`source list`) | Open a source |
| **A source** (`/sources/<name>`) | Its ingestion state (`source status <name>`: platform, data types, profiles, paths, the state files per database), the finished files per stage (`source progress <name>`), and the error logs of its profiles among `postgres_ingest`, `postgres_ml` and `mongo_ingest` (`source error-logs`; one too large for the page names the terminal command that prints it) | Run each of its profiles, one small form per profile with only that profile's options: rebuild the image (all), a file filter (all but `sr_ingest` and `sr_ml`), skip files with a lingua output (`parse`), one classifier (`ml`, `lingua`, `sr_ml`), the database admin password (the profiles that use a database; the form says whether one is kept for the session). Remove the source |
| **Databases** (`/databases`, one database at `/databases/<database>`) | Per database its state, ports, data folder and admin user (`db status`) and its checks (`db verify`); the MCP servers and the scheduler; the checks that are no database's | Start services (one of the services `db start` takes, or all; with the database admin password when one is needed), stop them (one or all), reset or remove a database, remove the MCP servers' or the scheduler's configuration, keep the database admin password for this session or forget it |
| **Runs** (`/runs`) | The runs, recorded ones (started from the web UI) and those started in a terminal that are alive now: kind, profile, source, state, start, end, exit code (`runs list`) | Open a run |
| **A run** (`/runs/<run id>`) | Its facts (the non-secret fields it was started with, who started it, its state) and its output, which follows the run as it goes (below) | Cancel it while it is alive |
| **System** (`/system`) | The broker's greeting (its protocol, whether a management password is set), the newest 200 lines of the broker's audit log (time, session, event, verb, outcome; a refused call with its code), and the table of what the web UI may ask the broker, with each entry's class and terminal command | Nothing |

Each block of a page is read on its own: a read that fails (a time-out, a state file that cannot be read, Docker out of reach) is shown as a sentence in its block, and the rest of the page stays. A name becomes a link only when it is a plain name (a letter or digit, then letters, digits, `_`, `.`, `-`, at most 64 characters); everything the broker hands over is shown as text.

## How an action is confirmed

1. A form (a button on one of the pages) sends what you chose. The web UI checks it against its own table of actions without asking the broker: only the fields that action takes, the closed choices, names of the plain form, a file filter of letters, digits and `_ . * ? [ ] ! -` (not only dots), and the rules of `run`: no file filter with `sr_ingest` and `sr_ml`, skipping lingua files only with `parse`, a classifier only with `ml`, `lingua` and `sr_ml`. A choice that does not pass is refused and nothing is kept or sent.
2. It is kept as a pending action of your session, and the page shows it: the `sdp` command it stands for (what you would type in a terminal; it never holds a password), every input (a password as dots), for an action that destroys something one sentence saying what it destroys and the name of what is destroyed, for a start or a stop what else the service chosen acts on (a database's MCP server, the scheduler, the database an MCP server needs; "every configured service" when none is chosen), and a field for the management password for the actions that need it again: a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, removing a source, and keeping the database admin password for the session. Starting and cancelling a run, stopping services and forgetting the database password need none. A terminal would also ask for the secrets the command does not show (the database admin password, a confirmation).
3. Confirm sends only that password, or nothing. The pending action is then taken out of the session, so a second confirm (a double click, another tab) finds nothing; it is passed to the broker once.
4. The action runs in the background, not tied to your request, with no time limit (a state-changing command can last many minutes). The confirmation waits up to two seconds for an answer: a refusal that comes at once is shown on the pending page (a wrong management password keeps the action, so you can type it again; five wrong ones end the session at the broker, and so here). Otherwise the outcome page says "under way" and looks again every two seconds, until it shows the exit code with the end of the command's output and errors, the link to the run that was started, or the refusal's sentence. A broker that cannot be reached is an outcome; it does not log you out.

At most two actions run at once from the web UI; a third is answered "busy" and stays pending. Cancelling a run has two places of its own, so that a cancel gets through while a database start or a reset runs for minutes. A run start and a database start use the database admin password kept for the session when their form leaves the field empty.

## The output of a run

The run page follows the run's output by itself. For a run started from the web UI the output is read from its log file in pieces of 64 KiB and appended, every 3 seconds while the run is alive and at once for the rest of a run that has ended; whole lines only are appended, and the unfinished last line is shown apart until its end arrives, so that colour codes and a progress bar that rewrites its line are cleaned as one line (a line longer than 64 KiB without an end is shown as it is). For a run started in a terminal, and for a recorded run whose supervisor is gone while its container still runs, the output is what Docker holds, its last 500 lines, read every 10 seconds and replacing what is shown. The pieces of all open views are read through four places of their own and Docker's tails through two, apart from the pages, so that open run pages cannot make the other pages busy; when those places are full, the view says it is busy and tries again in 10 seconds. A request for output the view has already shown (a stale tab) adds nothing and stops that view. The view stops when the run has ended and nothing more came, after one empty answer for a run that is lost or whose record cannot be read, and after 4 MiB in one view, where a link continues from that point in a fresh view (`/runs/<run id>?offset=<byte>`). Escape sequences, control characters and characters that change the direction of text are removed before the text is shown.

## What the web UI keeps in its memory

Nothing of this is written to a file, and restarting the web UI drops all of it.

| What | How long | How much |
|------|----------|----------|
| Your session: the broker's token, the form token | Until the logout, the broker's end of the session (at most eight hours) or a restart | 64 sessions; a new login beyond that drops the oldest |
| Pending actions, with a database admin password typed into a form | Five minutes, or until confirmed | 16 per session; the oldest is dropped |
| Outcomes of confirmed actions: the exit code and the end of the output and the errors | As long as the session, among its newest | 32 per session (finished ones are dropped before running ones), 16 KiB of each text |
| Output views: where each open run page reads next, and its unfinished last line | As long as the session | 8 per session (the oldest is dropped; its page then says so), 64 KiB held per view |
| Whether a database admin password was kept at the broker for the session (the password itself is kept by the broker only) | As long as the session | One flag |

So a session holds at most about 1.6 MiB of text: 1 MiB of outcomes, 512 KiB of unfinished lines and a few KiB of pending actions; 64 sessions about 100 MiB of the container's 512 MiB. The management password typed again for an action is passed to the broker and kept nowhere.

## Commands

| Command | What it does |
|---------|--------------|
| `sdp.py webui setup` | Asks the port (default 8443), the listen addresses besides 127.0.0.1, the names the page is reached by besides `localhost`, and the certificate (generate one, or give the paths of your own certificate and key, which are copied). Refuses when no management password is set (`sdp.py broker passwd` first) and while the web UI's container exists. Builds the image `sdp-webui`, makes or copies the certificate, and prints its SHA-256 fingerprint and the addresses the page will answer on. |
| `sdp.py webui start` | Starts the container `sdp-webui` on its own network `sdp-webui` and says whether the broker answered from inside it (start the broker with `sdp.py broker start`; the page needs it to log in). Says so with exit code 0 when it already runs. Refused when the web UI is not set up and under rootless Docker or user-id remapping. When Docker refuses (an address the machine does not have, a port in use), or the server ends right after its start (a certificate or key it cannot read), the container is removed, Docker's reason or the server's last lines are printed, and the exit code is 1. |
| `sdp.py webui stop` | Stops and removes the container. Exit code 1 when it was not running. |
| `sdp.py webui status` | Says whether it is set up and runs, its port, listen addresses and names, the certificate's fingerprint and end date, and the addresses of the page. Exit code 0 when it runs, 1 when not. |
| `sdp.py webui unsetup` | Asks first, then removes the container, the network, the image and `.sdp/webui/`. |

The page answers at `https://localhost:<port>/` and `https://127.0.0.1:<port>/` on this machine, and at `https://<address or name>:<port>/` for each listen address and name you gave.

## Reaching it from another machine or a tailnet

By default the page is published on 127.0.0.1 only. To reach it from another machine, give `webui setup` the address of this machine on the network you want (a LAN address, or the address this machine has on a tailnet such as Tailscale) as a listen address, and the name you will type in the browser (a tailnet name, for example) as a name. Each address is published on its own; "every address" (`0.0.0.0`, `::`, `*`) is refused, because it would also answer on networks you did not choose. The page answers only to the names and addresses you gave: a request with any other `Host` is refused, which also stops a page of another site from reaching it by pointing a name of its own at your machine.

The container listens on two ports: 127.0.0.1 of this machine is published to its port 8444, every other listen address to its port 8443. A login is sent to the broker as `local` only with two signs: it arrived on 8444, and it comes from the address of the container network's gateway (as which Docker's proxy shows a client on 127.0.0.1). Any other login is `remote`. Each channel draws on the broker's budget of that name (five failed logins, one given back every 30 seconds), so that failed guesses from elsewhere never lock out a login at the machine itself. One sign is not enough: Docker's proxy can connect from the gateway for a client of any address on 8443, and a machine that routes to the container's network through this one may reach 8444 directly with its own address. "Local" therefore means anyone on this machine who can reach 127.0.0.1, other accounts of the machine included. Where Docker's proxy is switched off for loopback traffic, a login at the machine itself counts as coming from elsewhere, which is the safe direction. Loopback addresses (`::1`, `127.0.0.2`, ...) are not taken as listen addresses: the machine itself is served on 127.0.0.1.

Besides, the web UI limits each client address (an IPv6 address by its /64; every local login counts as one address) to five failed logins in five minutes. That limit is best effort: the address can be the proxy's, and one IPv6 /48 holds 65,536 /64 networks. The broker's two budgets are the real bound on guessing.

The session cookie is sent by the browser to every service on the same host name, whatever its port. Reach the page under a name or an address that no other service of yours is reached by.

## The certificate

The page is served over HTTPS only. "generate" makes a self-signed certificate (EC P-256, valid 825 days, for serving only) whose names are `localhost`, `127.0.0.1`, the listen addresses and the names you gave. A browser does not know it: the first visit shows a warning. Compare the SHA-256 fingerprint the browser shows for the certificate with the one `webui setup` and `webui status` print; only when they are equal is it your web UI. Alternatively import `.sdp/webui/tls/cert.pem` into the browser's or the system's trusted certificates, or give `webui setup` a certificate of your own (from your tailnet or your own certificate authority), whose names must include the names and addresses you browse to. The page sends no `Strict-Transport-Security` header, so a browser does not pin the name to a certificate that may change at the next setup.

To change the port, addresses, names or certificate: `sdp.py webui stop`, `sdp.py webui setup`, `sdp.py webui start`.

## What the container holds and what it does not

| It has | It does not have |
|---------|------------------|
| The broker's socket folder (`.sdp/socket`), mounted read-only at `/run/sdp`: it can connect to the socket, not remove or create anything there | The Docker socket, the deployment folder, `.env`, any database password, the broker's own folder |
| The certificate folder (`.sdp/webui/tls`), mounted read-only at `/tls` | Any other mount; a writable root file system (only a 16 MiB `/tmp` without exec) |
| The broker's client and the web package, and FastAPI, Jinja2 and uvicorn | The CLI, the verb registry, the setup code, PyYAML |
| The operator's user and group id, no capabilities, no new privileges, at most 256 processes and 512 MiB; two listening ports, 8443 and 8444 | A way out to the internet: its network does no masquerading, so it reaches the machine and nothing beyond |

The image pins htmx 2.0.4 and Pico CSS 2.1.1 and checks them against SHA-256 sums written in `Dockerfile.webui` when it is built. Its Python packages (FastAPI, uvicorn, Jinja2) are installed from PyPI with lower bounds in `requirements-webui.txt`, not pinned to versions or checked against hashes, as elsewhere in this project; only the two static files are checked against recorded sums.

Files on the host, in the deployment folder: `.sdp/webui/` (mode 0700) with `config.json` (0600: the port, addresses, names, the certificate's fingerprint and end date) and `tls/` (0700) with `cert.pem` and `key.pem` (0600).

## Sessions

A login sends the management password to the broker and keeps nothing of it. The broker's session token stays in the web UI's memory; the browser gets only a random session id in a cookie (`__Host-sdp_session`: HTTPS only, not readable by scripts, not sent by other sites, gone when the browser closes). Every form that changes something carries the session's form token, one per session; a choice that is taken is answered with a redirect, so a reload never sends it again. A session lasts as long as the broker's (at most eight hours) and ends at the logout, when the broker ends it (the password was changed, the broker was restarted) and when the broker cannot be reached; the login page then says why. The logout is passed on to the broker under a bound of its own; when that stays full for a few seconds, the page says that the broker could not be told, and the broker's session then ends by itself within eight hours (its token was only in the web UI's memory, which drops it). Restarting the web UI logs everyone out.

A request body has ten seconds to arrive in full (408 beyond). Slow request headers are bounded only by the server's limit of 256 connections at a time. Every answer of the application carries the security headers described here; two answers come from the server itself and carry only its fixed text: its 503 when that limit is reached, and its 400 for a request it cannot parse.

## What it defends against, and what not

The web UI is written for these threats:

- **Anyone who can reach an address it answers on, without the password.** Nothing but the login page, the health check and the static files is served without a session; failed logins are limited by the broker's two budgets and, best effort, per address; requests over 64 KiB and anything but form data are refused before they are read.
- **A page of another site open in your browser.** Every request that is not a GET must come from the page itself (its `Origin`), and a logged-in request must also carry the session's form token; the page cannot be framed; a foreign `Host` is refused.
- **Hostile text shown on the page** (a source name, a file name, Docker's output). Everything is escaped, the page runs no inline script and allows no script but its own, and a name becomes a link only when it is a plain name.
- **A stolen cookie.** It works only from a browser that can reach the page, only until the logout or the session's end, and only for what the broker lets a session do (typed verbs; the management password again for a database start, a database reset, removing a database, the MCP servers' configuration or the scheduler's configuration, removing a source, and keeping the database admin password for the session).
- **A taken-over web UI process** (someone who gets to run code in it). Such a process holds the broker token of every live session, and reads every management password typed into the page after the takeover, which opens new logins and the verbs that need the password again. It can probably reach services of this machine that listen on all of its addresses through its network's gateway: the databases, the MCP servers and the scheduler's page are published that way. What stays out of its reach: the Docker socket, the deployment's files, the internet, and anything the broker's typed verbs do not allow.

It does not defend against someone who can run programs on the host as you (that already has Docker and the files), nor against someone who has your management password. "Local" logins are those from anyone on this machine, other accounts included.
