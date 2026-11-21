# Testing

This is the backend / developer reference for the test suite. The pipeline is mostly LLM-written orchestration glue, so tests are the load-bearing way to catch regressions — every commit class with non-trivial behavior has at least one pinning test.

Two layers run in CI / locally:

- **Unit tests** — pure logic, no Docker, no databases. Fast (full suite < 30s).
- **E2E tests** — real `sdp.py` commands inside a sysbox container, real Docker images, real databases. Local-only (sysbox required).

## CI

Two GitHub Actions workflows run on every push and PR (path-filtered):

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| ShellCheck | `.github/workflows/shellcheck.yml` | `**/*.sh` changes | Lint entrypoint scripts in `config/` |
| Tests | `.github/workflows/tests.yml` | `**/*.py`, `requirements*.txt`, `tests/**`, `config/**/*.yaml` | Run unit test suite (Python 3.13) |

A third workflow runs `ruff check` against an explicit rule baseline (F, E9, W6) — see `.github/workflows/ruff.yml`. All actions are pinned to full commit SHAs for supply-chain hardening.

CI installs only `requirements-test.txt`, which is self-contained (project deps `pyyaml`, `polars`, `pyarrow` plus test deps `pytest`, `pyzstd` — no `lingua`, `psycopg`, `torch`). Pytest output goes to the Actions step log directly; no artifact upload. Two directories are excluded: `--ignore=tests/e2e` (requires sysbox) and `--ignore=tests/benchmark` (requires a populated database, runs for minutes to hours).

## Running unit tests locally

```bash
pip install -r requirements-test.txt
pytest --override-ini="pythonpath=." --ignore=tests/e2e --ignore=tests/benchmark --ignore=data -v
```

`--override-ini="pythonpath=."` lets pytest import the project package without installing it. `--ignore=data` skips the runtime DB volumes (owned by container UIDs when containers are running, unreadable by pytest's auto-collection). CI doesn't have `data/` so the flag is a no-op there. Skip slow decompression tests with `-m "not slow"`.

Benchmarks under `tests/benchmark/` are measurement tools with no assertions — they are always excluded and run by hand. See `tests/benchmark/README.md`.

## Test layout

```
tests/
    conftest.py                        # Root fixtures (paths, NDJSON helpers, marker registration)
    fixtures/                          # NDJSON / CSV / Parquet / config / state fixtures
    test_cli_help.py                   # Smoke: every subcommand --help exits 0
    test_runstore.py                   # Run store: records, logs, locks, terminal runs found through Docker
    control/                           # Verb registry and its typed inputs (what a request may carry); the processes that run one verb, supervise a recorded run and start one; the building blocks of the host broker (frames, password file, sessions, audit log, redaction, environment) and the broker itself
    core/                              # Pure-logic core
    db/                                # Pure-logic DB ingestion
    jobs/                              # Jobs scheduler (store, runner, backends, auth, config)
    orchestrators/                     # File detection
    platforms/                         # Reddit + custom parser
    setup/                             # CLI input helpers, env/compose generators, HF organize
    e2e/                               # Sysbox-backed end-to-end tests
        Dockerfile.e2e                 # Sysbox image (Python 3.13, Docker-in-Docker, deps)
        run.sh                         # Host-side runner: build → start sysbox → pytest
        questions.yaml                 # Tagged-prompt dictionary
        conftest.py                    # Per-test workspace lifecycle
        helpers/                       # pexpect wrapper, fixtures, DB assertion helpers
        tests/                         # Test scenarios
        manual/                        # Hand-run checks against a real Docker (plain scripts, not collected by pytest)
    benchmark/                         # Performance measurement tools (no assertions, CI-excluded)
        README.md                      # What lives here and why it is excluded
        sr_compression.sh              # StarRocks LZ4 vs ZSTD: size, query time, CPU
```

## Unit tests

Unit tests target **silent-failure code** — config merge, dispatch, dedup SQL, auto-derivation. Code that fails loudly (column-mismatch errors, missing-file exceptions) is left to E2E and manual catch on first run.

### `core/` — config / parsing / state / decompression

| Module | What's covered |
|--------|----------------|
| `test_config.py` | YAML config loading, deep merge (overlay rules, list replacement), `get_required` strictness, `validate_processing_config` / `validate_classifier_config` |
| `test_parser.py` | Type enforcement, CSV escaping, null-byte stripping, nested dot-notation field access |
| `test_state.py` | Pipeline state JSON resume/recovery, atomic temp-rename, state file shape |
| `test_decompress.py` | `.zst`, `.gz`, `.xz`, `.tar.gz` round-trips. Marked `@pytest.mark.slow` |
| `test_classifier_scope.py` | `normalize_classifier_entries` — per-classifier `data_types` scope. Pins the breaking change in commit `b1bbeb2` |
| `test_resolve_classifier_runs.py` | `resolve_classifier_runs` — shared by `postgres_ml` and `sr_ml`. Composes ordered classifier-ingestion runs from `ml.yaml` + `lingua.yaml` + per-source overrides |
| `test_ml_index_plan.py` | `build_ml_index_plan` — `postgres_ml`'s flat `ml_indexes` lookup: profile-over-platform, the no-fallback-to-`indexes` rule, and the guard rejecting the StarRocks per-type shape |
| `test_sr_index_spec.py` | `normalize_index_spec` / `build_sr_index_plan` — plain list always means bitmap, per-type `{bitmap, bloomfilter}` shape, sr_ingest's `sr_indexes` → `indexes` fallback vs sr_ml's no-fallback |

### `db/` — database ingestion logic

Focused on the SQL shape of dedup / upsert paths and contract checks that span parser ↔ DB schema.

| Module | Bug class pinned |
|--------|-------------------|
| `test_ingest_dedup_sql.py` | Non-NULL-safe upsert `WHERE` / dedup `ORDER BY`. The fast-load and ON CONFLICT paths must both treat `NULL retrieved_utc` as smaller than any real timestamp; same-NULL re-ingests must be no-ops |
| `test_mongo_validate.py` | Pre-import file validation for `mongoimport` |
| `test_reddit_column_contract.py` | Parser CSV columns ↔ `COPY` column list match. Pins a bug where `retrieved_utc` was listed both as a real column and a mandatory column, producing a duplicate-column COPY |
| `test_starrocks_ingest.py` | StarRocks ingestion pure-logic helpers (table DDL, schema inference, `INSERT ... SELECT FROM FILES()` SQL shape, BITMAP index DDL) |
| `test_bloom_filter_columns.py` | Parsing existing `bloom_filter_columns` out of `SHOW CREATE TABLE`, and the merge-not-replace / abort-on-unreadable rules that keep an `ALTER … SET` from dropping existing bloom columns |

### `jobs/` — query scheduler

| Module | What's covered |
|--------|----------------|
| `test_config.py` | `JobsConfig` loading (default + `config.local.yaml` merge), per-backend timeout normalization, target validation |
| `test_store.py` | Filesystem state machine: `pending` → `approved` → `running` → `history`. Atomicity, history retention pruning, idempotent transitions |
| `test_runner.py` | Backend wiring, target → backend dispatch, cancel propagation, orphan recovery (running jobs on container restart) |
| `test_backends_postgres.py` | PG backend: SQL wrapping (`COPY ... TO`), output filename validation, error mapping |
| `test_backends_starrocks.py` | SR backend: `INTO OUTFILE` wrapping, chunked-output handling, 72h timeout cap |
| `test_backends_mongo.py` | Mongo backend: aggregation execution, NDJSON streaming, CSV flat-projection enforcement |
| `test_backends_base.py` | Shared backend protocol (cancel signaling, file path conventions) |
| `test_auth.py` | Optional admin-password gate on the web UI: HMAC session, env-var lookup, startup failure when `auth: true` but no password set |

### `orchestrators/`

| Module | What's covered |
|--------|----------------|
| `test_parse_orchestrator.py` | File detection: regex pattern matching across compressed / extracted / parsed file lists, data-type routing |

### `platforms/`

| Module | What's covered |
|--------|----------------|
| `test_reddit_parser.py` | Base-36 ID conversion, deletion waterfall (8 priority levels), field extraction, mandatory-field emission, format-drift handling (`retrieved_on` → `retrieved_utc` → `_meta.retrieved_2nd_on`) |
| `test_custom_parser.py` | NDJSON / CSV / Parquet input, dot-notation + array-index field access, type enforcement, NDJSON → Parquet/CSV output |

### `control/`

The registry itself is covered by `test_control_verbs.py`, `test_control_types.py`, `test_control_names.py` and `test_control_actions.py`, whose module docstrings list their contracts. The three modules that act on a request:

| Module | What's covered |
|--------|----------------|
| `test_control_child.py` | `control.child`, the process that runs the action of one verb from a JSON request on standard input, with every action of the CLI replaced by a stand-in of the same signature: the keyword arguments each kind of action gets (a secret as plain text, `_confirm` only where the action has it, the run id only for `run.start`), the exit codes (the action's integer when it is 0 to 255, 0 after a getter's data was printed as one JSON document, an `ActionError`'s code, 2 for a refused request, 1 otherwise, also for a result no process can exit with), every refusal with its one line and no call of any action, a planted secret in nothing that is printed; and the real `python -m` entry point once against the real CLI |
| `test_control_supervise.py` | `control.supervise`, the process that keeps one recorded run alive, with small Python programs as the child and Docker replaced by the fake: the child's output in the log in order, the record from `starting` over `running` to `finished` with the exit code, the lock held to the end of the process and not passed to the child, the request in no file, argument list or environment (read from `/proc`), a supervisor without the lock refusing, cancel without a container (the child's process group, TERM then KILL after the grace time, and only when Docker answered that there is none) and with one (stopped through the run store, the child left to end), `cancelled` recorded only on a listing made after the processes ended, a child that dies alone not ending the run while what it started still runs, a process group that is looked at the moment the child is collected and, once seen empty by two looks in a row, never looked at or signalled again, a container that exists and does not run (one that never started is removed before the end is written, one that starts meanwhile is stopped or waited for, one that is still listed delays the end), TERM and INT to the supervisor as a cancel and HUP ignored, a container that outlives the compose process (the run stays `running`, the container's output and exit code come from Docker), no end recorded when Docker does not answer or does not stop the container, and the supervisor's own problems as plain lines in the log. Linux only |
| `test_control_runs.py` | `control.runs.start_run` with the supervisor replaced by a stand-in program: a refused request, starter label or folder creates nothing, a second run of the same profile and source is refused for every state in which the first is alive (also a run started in a terminal, also with two calls at the same moment) and not for one that is over, the run is refused when Docker cannot be reached, the run's lock is held from the first write of its record and handed to the supervisor while the starter keeps no descriptor, the record's flags hold no secret, the supervisor gets the request on standard input only and the run's log as its own output (one that fails at once leaves its error text there), a supervisor that cannot be started leaves the run recorded as finished with exit code 1 and no pipe open, and every other failure is a `RunNotStarted` with a constant message. Linux only |

The building blocks of the host broker each have their own test file, with a made-up clock, token source and password derivation, and the real file system under a temp folder:

| Module | What's covered |
|--------|----------------|
| `test_control_framing.py` | `control.framing`, one frame of the broker's socket (a 4-byte big-endian length and that many bytes of UTF-8 JSON whose top level is an object): a document read back however the bytes arrive, the largest request and reply accepted and one byte more refused on both sides, every refusal with its own code and a constant message that holds nothing of the payload (also no chained exception that does), a length of 0 or above the limit refused before any byte of the payload is asked for, the deadline as one total for the whole frame, a repeated key, `NaN`, `Infinity` and a number too large for a float refused, nesting beyond 32 levels refused with its own code (brackets inside strings and after an escaped quote not counted, brackets that do not match still "not JSON"), the depth check's work counted on hostile text (unclosed strings of escaped quotes, quotes or backslashes only, closers) and growing in proportion to the text, errors of the connection as typed refusals, and what the encoder refuses to write |
| `test_control_passwords.py` | `control.passwords`, the management password file (a salted scrypt hash with its parameters) and `control.files`, how the broker folder and its files are opened: each form of a new password that is refused, with its code and before anything is derived or written, the exact content of the file and the explicit memory limit, folder 0700 and file 0600 under several umasks, the replacement in one step through a temporary file in the same folder and the old file left by a failed write, a link, a named pipe (not waited on) or a folder in the file's place and a linked folder refused on reading and writing, the owner checked on folder and file each by itself, a mount that refuses a change of mode or a failing look at a descriptor turned into the module's own refusal, a file with any parameters other than the ones `set_password` writes refused without any derivation, the digest of exactly the bytes read with one open and one read, a candidate that cannot be a password answered without any derivation, the constant-time comparison, and the real scrypt once (three derivations in the whole file) |
| `test_control_sessions.py` | `control.sessions`: the login budget (five failures, one unit back every 30 seconds counted from the first failure, nothing verified while it is empty, a success neither taking nor giving, the reset), logins from twenty threads getting no more verifications than the budget has, the token kept nowhere in the object (a search of everything reachable from it), a session number that is a counter, the end of a session at exactly eight hours, every session of the old password ended at the first request after the password file changed or went away (the login that raced the change included), at most 64 live sessions (a login beyond that ends the expired ones first, then the oldest, and says so), the boot-time clock as the default and the monotonic one where there is none, the second check of the password (not drawn from the login budget, the fifth failure in the session's life ending it and saying so whatever successes came between, waiting for a login that is running: the threads' order is recorded, no timing decides), a session's database password in no repr and checked like any request, the passwords of ended, cleared and replaced sessions still known for redaction (the newest 64, each once), the passwords requests carried remembered in a list of their own (the newest 64, none shorter than 8 characters, left out of the list for the audit view), so that many requests never push a session's old password out, and no refusal holding a token, a password or any input |
| `test_control_audit.py` | `control.audit`, the audit log: two lines written out byte for byte, the fields of every event in their fixed order, values an event cannot hold not written and not raised, a client's verb or field text never written, secrets replaced by the type of their field and never by its name (a wrapped secret anywhere inside a value included, searched for in the file), the chain across restarts of the writer, a cut-off last line closed and marked `repaired`, two or three short writes in a row closed by one `repaired` line that names the last real line before them (so that a change of that line is still found), a cut `tail_changed` line repaired before it is written again, a fragment left in a file that was then replaced not taken for the new file's, an end that was appended to, cut, edited, replaced or removed marked `tail_changed` before the next line, a write that fails returning False, raising nothing and being counted, `writable()` before and after, the lost lines named by the next `counts` line, one `write` call and one sync per line, counts at most once a minute and as numbers only, naming at most 64 sessions and totalling the rest so that the line always fits and `writable()` comes back, modes, the single writer, links and special files refused without waiting, `verify` finding a removed, edited, moved, inserted and hand-appended line and a cut-off end and never raising on garbage, a line of megabytes costing no more memory than a line may have, and `read_tail` returning a marker for every line it cannot read and holding at most 8 MiB, with a marker when it cut |
| `test_control_redact.py` | `control.redact`: secrets overwritten byte for byte in their plain, `quote` and `quote_plus` forms, overlapping occurrences all covered (compared with marking every occurrence one by one on random texts), dense matches of a megabyte costing memory in proportion to the text only, a secret's repeat unit worked out once per call and only when it is found (calls counted over hundreds of strings), the bounds of one call (4 MiB of text, a piece refused before it is read, 256 secrets; for data, work counted as bytes plus one unit per form for every string, charged before the string is searched, the walk stopping early: millions of empty strings and a million short ones with many secrets refused with their searches, masks and period computations counted, the largest lists of small strings that fit with few secrets passing with the right result, short strings and forms longer than the string not searched) and the worst case at that bound counted at no more than one step per byte, secrets shorter than four characters ignored, the `$ docker compose ` line overwritten after its prefix up to a bounded reach that never cuts a character, every string and key of nested data to a bounded depth, and a log read in pieces: every piece of every offset and size of a sample log with bytes that are not UTF-8 and runs of continuation bytes, for several reaches, compared with the same bytes cut out of the whole log after overwriting it, and sequences of pieces of the smallest real size over a log of a few hundred kilobytes |
| `test_control_environ.py` | `control.environ`: only the listed names (the compose, proxy and Hugging Face variables and the time zone among them) and the `LC_` names kept, nothing added, the token among them listed for redaction and nothing else, the rest removed from the process itself (a child of a reduced process is asked what it sees) with only names returned; and the import rules of the modules above, each in a fresh interpreter: none of them loads the CLI or the run store or changes the environment when it is loaded, the CLI loads none of them, and `framing`, `redact`, `environ` and `files` load neither PyYAML nor the setup helpers nor the registry |
| `test_control_broker.py` | `control.broker`, the broker itself, against real unix sockets in a temp deployment, with a stand-in child program, a fast stand-in for scrypt, stubs for the run starter and the run listing, and real broker processes where one is killed or signalled: another uid closed without a reply, the modes of every folder and file under a umask of 002, a stale socket replaced only after the lock is taken and a second broker refused, an over-long socket path, a symbolic link and code or folders others may write, or the group may write unless it is the operator's private group (the deployment folder, `sdp.py`, every folder and module file of the package, also a link under it), the state folders the group may write at all, another owner, and the lock tried again while someone looks whether a broker runs, all refusing or allowing the start as they should, when a group counts as private, and the group asked about once per group in one check; nothing but `hello` and `login` answered without a valid token (every op and every verb, with no token, a wrong one, an expired one, one ended by a password change and one ended by a logout); the registry's refusals with their codes and no value echoed, the second check of the management password decided by the verb's class alone, its fifth failure ending the session; the session's database password stored only after that check, added to `run.start` and `db.start` only when the fields have none (the audit line says where it came from), refused with shell characters for the PostgreSQL starts and not for MongoDB; the destructive verbs refused for each alive state of a run and with Docker out of reach; the request on the child's standard input only (read from `/proc`), an environment of the allowed names only, a getter's data and failure, a state-changing verb's exit code and both streams, tails that begin with a whole line, a child that prints too much or takes too long ended with its whole process group, read children bounded; one state-changing call at a time, `run.start` busy meanwhile and `run.cancel` going through, several `run.start` at once, a destructive verb busy while a run is being started and looking for runs only while it holds the lock, an orphaned child that writes its output to files running to its end after the broker is killed while it holds the lock (its files kept until then and cleared at the next start), the housekeeping sweep of leftover output files only while the lock is free, never of a running `run.cancel`'s and without taking the lock when there is nothing to sweep, a call that meets a brief holder of the lock going through after a retry, a client that goes away not ending its call, an audit log that cannot be written refusing a call before anything runs; `run.logs` read in pieces of the smallest size with a secret at and across every boundary, byte for byte what overwriting the whole log gives (also with `$ docker compose ` inside a line at a piece's start), the end of a log that is still written held back in every state that is not final so that a secret written in two parts is never served half covered, also with the password of an ended session or one a request carried (one shorter than 8 characters only in its own reply), and an audit view no password sent by a request can blank; secrets and the `$ docker compose` line overwritten in data, keys and output, data too large to overwrite never sent; the transport's bounds (an oversized request, a reply over the limit, a connection that sends nothing, a client that never reads, too many connections, two clients at once, a repeated key); what gets an audit line and what is only counted, no secret or token in the logs, replies or argument lists, `audit.list`, the chain across a restart; a serving loop that fails (also in its setup: a selector that cannot be made, a housekeeper that cannot start) still stopping the broker with its error's kind only in the log, a connection thread that cannot be started dropped and its connection closed, a write lock refused (never taken elsewhere) without the broker folder, TERM waiting for a running action, USR1 ending it with its group (also while connections are accepted, and a signalled stop that takes no lock), requests refused with `stopping` once a stop was asked while running ones finish, a run's supervisor outliving the broker; a thread or a descriptor that cannot be made ending the child, writing `call_ended` and leaving no descriptor, lock or read slot behind; a password file that cannot be used answered as such everywhere; a rest after an accept that finds no descriptor; and the import rules of the broker and its client. Linux only |

### `setup/`

| Module | What's covered |
|--------|----------------|
| `test_utils.py` | CLI input helpers (`ask`, `ask_bool`, `ask_choice`, `ask_multi_select`, `ask_int`) with mocked stdin, glob-to-regex conversion, file-pattern derivation |
| `test_source.py` | Per-source config generation: platform routing, field/index derivation, deep merge with templates |
| `test_hf.py` | `organize_hf_downloads` — copies HF parquet files from `dumps/` into `extracted/<data_type>/` according to `hf_config_map` |
| `test_actions.py` | The state-changing commands as action functions (`do_run_start`, `do_db_start`, `do_db_reset`, ...): no prompt and no process exit without a terminal, confirmation fails closed, a missing password fails before any side effect, and the terminal wrappers still ask as before; the private run id of `do_run_start` (without it the compose call is the literal one of `sdp run`, with it the container gets the run's name and label before the service name, and a malformed one is refused before anything is read or changed); the same syntax-tree walk checks that `do_run_cancel` and the eight getters (`get_db_status`, `get_db_verify`, `get_source_list`, `get_source_status`, `get_source_progress`, `get_source_error_logs`, `get_run_list`, `get_run_logs`) reach no prompt and no exit |
| `test_status_golden.py` | Golden files (`golden/`) for the whole terminal text and the exit code of `db status`, `db verify`, `source list`, `source status` and `source error-logs` on fixture deployments built in a temp folder; regenerate with `SDP_REGENERATE_GOLDEN=1` after an intended change of the text |
| `test_status_data.py` | The functions that return the data of those five commands (`get_db_status`, `get_db_verify`, ...): they print nothing, return JSON-serialisable data that prints the golden text, carry no secret, and refuse a malformed source name; `get_source_progress` is held to the no-print, no-secret and source-name checks here as well, `get_run_list` and `get_run_logs` to the no-print and no-secret checks |
| `test_source_progress.py` | `source progress`: finished (`.csv`, `.parquet`) and being-written (`.temp`) files counted per stage and data type with the newest file by modification time, the stage list (parse, lingua, classifier folders without `lingua_ingest`), relative paths resolved under the project root, a damaged config or tree reported as "not readable" instead of a traceback, names with control or format characters or non-UTF-8 bytes made safe to print and send, a data type that is not text given a constant label, the exit codes, the `--json` output (a refusal goes to standard error) and the text for a fixed tree in a pinned time zone |
| `test_runs_commands.py` | The `runs` commands (`runs list`, `runs logs`, `runs cancel`) and their functions `get_run_list`, `get_run_logs`, `do_run_cancel`: a malformed or unknown run id refused with a constant message before a path is built or Docker is asked, strings from Docker and from records made safe to print and send while a log's text is returned as decoded, nothing of a container's environment but the source in the data, the whole text of `runs list` for a fixed set of runs, `--json`, `--tail`, `--follow`, the exit codes, and the three verbs in the registry (the `RunId` type and the store refuse the same ids, the socket's number ranges are the getters') |
| `test_broker_commands.py` | The `broker` commands: `passwd` (a mismatch and a short password refused with one line and exit code 1, the file written with the library's modes under a umask of 002, checked with the real scrypt), `start`, `status` and `stop` against a real broker started detached in a temp deployment with a copy of the CLI and the package (started, already running, the start error's own line with exit code 1, a broker that never answers ended (by KILL when it ignores TERM, the stand-in ready before the wait ends) and reported after the wait, both states of `status` with their exit codes and a password file that cannot be used, stopped with its socket removed and its last audit line written), `stop` signalling only the process that holds the lock (a stale lock file naming a live process, a lock held by another process than the one named: nothing signalled), a broker that ends before the signal counted as stopped, and saying once that it waits, `audit` as text, JSON and with `--limit`, `--verify` on a whole, a damaged and a missing log with their exit codes, and the bare `sdp broker` printing its help. Linux only |
| `test_env_and_compose.py` | `.env` updates (preserves siblings on partial writes), `docker-compose.override.yml` generation (tablespaces, SR multi-disk, jobs `/jobs_export` mount) |
| `test_profile_gating.py` | Profile-gating tables in `sdp.py` — every pipeline profile is mapped to its required DB / source / classifier prerequisites; `cmd_run` blocks on missing config |
| `test_index_persistence.py` | `sdp db create-indexes` persistence routing — base tables to `indexes` / `sr_indexes`, classifier tables to `ml_indexes` / `sr_ml_indexes`; StarRocks entries merge per index type and a stored plain list is promoted, not overwritten |

### Top-level

| Module | What's covered |
|--------|----------------|
| `test_cli_help.py` | Smoke: every `sdp.py` subcommand's `--help` exits 0. Catches argparse regressions and import errors |
| `test_runstore.py` | The run store (`social_data_pipeline/runstore.py`) with Docker replaced by a fake: the two forms of a run id checked before any path or Docker use, the state folder private (0700 / 0600), owned by the user who runs the command and never reached through a symbolic link, a named pipe in a file's place refused without waiting on it, records replaced in one step (one too large to read back is never written) and a damaged one reported as `unreadable`, the computed states (`detached`, `lost`, `unknown`), which containers count as a run started in a terminal, the exact arguments of every Docker call (a container's environment is never read apart from `SOURCE`), log pieces by byte offset that never split a character, output read from Docker cut to its last bytes, and cancel (interrupt signal, full container id, refusals, a container Docker could not stop). What the supervisor of a recorded run uses is covered too: a run created with its lock already held, the check that a descriptor holds that lock, the end of a run written into its record and never over an end that is already there, the run's containers counted, stopped, waited for and followed by its label, the last look before an end (the run's containers in every state, only one that was created and never started removed, by its full id and without force), and the two Docker commands that last as long as a container (run for real with a stand-in program in the place of the Docker binary: ended by their caller, never left behind). The `run_docker_guard` fixture of `tests/conftest.py` fails any test of this file, of `test_runs_commands.py` or of the three `control/` files above that reaches Docker without the fake in its place |

## E2E tests

E2E tests exercise the real pipeline inside a sysbox Docker-in-Docker container. Nothing is mocked — each test runs real `sdp.py` commands, builds real Docker images, starts real databases, and verifies results by querying the database and reading output files.

### How it works

1. `run.sh` builds a sysbox container image with Python 3.13, docker compose, and all test deps (pexpect, psycopg, pymongo, polars, mysql-connector-python).
2. The host repo is bind-mounted **read-only** at `/repo` inside the sysbox container.
3. Each test gets a fresh `/workspace` — a `git ls-files`-driven copy of the repo, so untracked files and gitignored state can't leak in.
4. Interactive commands (`sdp db setup`, `sdp source add`) are driven by pexpect, matching prompts by their `[tag_id]` prefix (enabled via `sdp.py --tag`). Answers are a `{tag: answer}` dict per test.
5. Non-interactive commands use subprocess directly.
6. Verification: psycopg (PG), pymongo (Mongo), `mysql.connector` (StarRocks), polars (CSV/Parquet file reads).
7. Teardown: `docker compose down --volumes` plus workspace removal.

### Tagged prompt automation

Every interactive prompt in `sdp.py` setup flows has a stable `tag=` identifier (e.g. `db_data_path`, `src_profiles`). When `--tag` is passed, prompts are prefixed with `[tag_id]`, allowing pexpect to match by tag rather than prompt text. Tests are robust to prompt reordering or rewording. Tags are documented in `tests/e2e/questions.yaml`.

### Running E2E tests

Requires [sysbox](https://github.com/nestybox/sysbox) **>= 0.7.0** on the host — `run.sh` checks the version and exits before building if it is older. The E2E image installs current `docker-ce`, which ships runc >= 1.3.3; its procfs safety check (CVE-2025-52881) only works inside sysbox 0.7.0+, which traps `openat2` for sysbox-fs mounts under `/proc` and `/sys`. Check with `sysbox-runc --version`.

```bash
./tests/e2e/run.sh                  # All E2E tests
./tests/e2e/run.sh -k parse         # Only tests matching "parse"
./tests/e2e/run.sh -x               # Stop on first failure
```

First run builds Docker images inside the sysbox container (~3-5 min). Subsequent runs use the Docker layer cache (~5-10 min total).

### Hand-run checks

`tests/e2e/manual/` holds plain scripts that need a real Docker but no sysbox and no database. They are not collected by pytest and not started by `run.sh`.

| Script | What it checks |
|--------|----------------|
| `check_runs.py` | The `runs` commands against a real Docker: in a throwaway compose project, a container started with `docker compose run` is listed as a terminal run with its profile and source, its output is read, a made-up password planted in its environment appears nowhere in the data, and cancel stops it within seconds with the program seeing the interrupt (exit code 3); the same through `sdp.py runs list / logs / cancel`; a labelled container next to a hand-made record is a `detached` recorded run and is cancelled by its label; a one-off container of another service is not a run. Needs the image `python:3.13-alpine`; run with `python tests/e2e/manual/check_runs.py` |
| `check_run_start.py` | Starting a recorded run through the real chain (`start_run`, the supervisor, the child, `do_run_start`, `docker compose run`) in a throwaway deployment: the run is listed by `runs list` as kind `web` and `running`, `runs logs` reads its log file in pieces, the container carries the run's name and label, a second run of the same profile and source is refused while it runs and allowed afterwards, `runs cancel` ends it within seconds as `cancelled` with exit code 3 (the program saw the interrupt), a run whose `docker compose run` process is killed stays `running` and ends `finished` with the container's exit code once the container stops, a `mongo_ingest` run with authentication on ends at once without a password and hands the password to the container with one, and a made-up password is in no file under `.sdp/`, in no argument list and not in `runs list --json`. Needs the image `python:3.13-alpine` and Linux; run with `python tests/e2e/manual/check_run_start.py` |
| `check_broker.py` | The broker against a real Docker in a throwaway deployment: `broker passwd` and `broker start` through the CLI, `hello`, a wrong and a right login, `db.status` and `source.list` through the socket, a made-up database password kept in the session after the second check, `run.start` with it, the run in `run.list`, its log read in pieces through `run.logs` (byte for byte the overwritten file, the `$ docker compose` line overwritten), the made-up password in neither the log on disk nor any reply, `db.stop` refused while the run is alive, `run.cancel`, `db.stop` then run with its output in files that are deleted afterwards, `broker stop` while a second run lives (the run goes on and is cancelled with `runs cancel`), `broker audit --verify`, neither password nor the token in the audit log or the broker's log, and nothing left behind. Needs the image `python:3.13-alpine` and Linux; run with `python tests/e2e/manual/check_broker.py` |

### Test scenarios

| Test | What it exercises |
|------|-------------------|
| `test_parse_reddit` | `db setup` → `source add reddit` → compress fixtures to .zst → `run parse` → verify CSV/Parquet output (row counts, mandatory columns, field presence) |
| `test_parse_custom` | Custom platform: `db setup` → `source add` → place NDJSON in `extracted/` → `run parse` → verify Parquet output (row counts, dot-notation field resolution) |
| `test_postgres_flow` | Full PG lifecycle: setup → add → parse → `db start` → `run postgres_ingest` → verify schema, row counts, no duplicate IDs, indexes, column types |
| `test_postgres_dedup_null_safe` | Pins null-safe + deterministic dedup behavior across the full case table (NULL vs. real `retrieved_utc`, same-NULL re-ingests, cross-dataset tiebreakers) |
| `test_recovery_postgres` | `postgres_ingest` recovers from an interrupted prior fast-load (orphaned no-PK table is auto-detected and re-routed) |
| `test_mongo_flow` | Full Mongo lifecycle: setup → add → `db start` → `run mongo_ingest` → verify databases, collections, document counts, `_sdp_metadata` state |
| `test_sr_flow` | Full StarRocks lifecycle: parse → `sr_ingest` → verify Primary Key tables, BITMAP indexes (plain-list config), row counts |
| `test_sr_ml_flow` | StarRocks ML flow: parse → lingua → `sr_ingest` → `sr_ml` → verify classifier tables, merge_condition upsert, and the BITMAP index built from `sr_ml_indexes` |
| `test_auth_postgres` | PG auth: fresh install + migration path (existing trust-auth DB → scram-sha-256), RO user + `.ro_credentials` propagation |
| `test_auth_mongo` | Mongo auth: fresh DB delegation via `docker-entrypoint.sh`, existing-DB migration via localhost exception |
| `test_auth_starrocks` | SR auth: root password rotation, RO user role sync (`sdp_readonly`), recovery via `enable_auth_check = false` |
| `test_idempotency` | Re-running PG / Mongo / SR ingestion is a no-op (state tracking diverges silently otherwise) |
| `test_filter` | `--filter` flag wiring on the parse profile — fnmatch glob narrows the file set without breaking state-skip logic |
| `test_mcp_lifecycle` | PG and Mongo MCP entrypoint scripts: read `.ro_credentials` from the data volume, build authenticated connection, healthcheck passes |
| `test_unsetup_gating` | `db unsetup` cleans per-source DB overrides; `run <profile>` blocks when the corresponding DB is no longer configured |

## Adding a test

The bar for adding a unit test is high. Before writing one, ask what bug class it catches — pure-logic tests for silent-failure code (auto-derivation, dispatch, dedup conditions, config merge that preserves siblings) are gold, because nothing else catches those bugs. Skip:

- Tests of trivial one-liners (`fnmatch`, list comprehensions) — they end up testing the standard library.
- Mock-heavy tests of polling loops or driver dispatch — they end up testing the mock. Use E2E.
- Tests of code that fails loudly — first manual run catches it.
- Tests of one-shot tools (recover-password, migration scripts).

For E2E: prefer scenarios that exercise a real bug class (auth migration, dedup correctness, MCP credential flow). New scenarios reuse `helpers/sdp.py` (pexpect wrapper) and `helpers/db.py` (DB assertions); see `test_postgres_flow.py` as a template.

## Swapping test data

To test against a different upstream schema (e.g. a new Reddit dump format):

1. Drop the new NDJSON into `tests/fixtures/reddit/`.
2. Run tests — the structural checks catch column / type drift and row-count mismatches.
3. No Python changes needed.
