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

CI installs only `requirements-test.txt`, which is self-contained (project deps `pyyaml`, `polars`, `pyarrow` plus test deps `pytest`, `pyzstd` — no `lingua`, `psycopg`, `torch`). Pytest output goes to the Actions step log directly; no artifact upload. E2E tests are excluded (`--ignore=tests/e2e`) because they require sysbox.

## Running unit tests locally

```bash
pip install -r requirements-test.txt
pytest --override-ini="pythonpath=." --ignore=tests/e2e --ignore=data -v
```

`--override-ini="pythonpath=."` lets pytest import the project package without installing it. `--ignore=data` skips the runtime DB volumes (owned by container UIDs when containers are running, unreadable by pytest's auto-collection). CI doesn't have `data/` so the flag is a no-op there. Skip slow decompression tests with `-m "not slow"`.

## Test layout

```
tests/
    conftest.py                        # Root fixtures (paths, NDJSON helpers, marker registration)
    fixtures/                          # NDJSON / CSV / Parquet / config / state fixtures
    test_cli_help.py                   # Smoke: every subcommand --help exits 0
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

### `db/` — database ingestion logic

Focused on the SQL shape of dedup / upsert paths and contract checks that span parser ↔ DB schema.

| Module | Bug class pinned |
|--------|-------------------|
| `test_ingest_dedup_sql.py` | Non-NULL-safe upsert `WHERE` / dedup `ORDER BY`. The fast-load and ON CONFLICT paths must both treat `NULL retrieved_utc` as smaller than any real timestamp; same-NULL re-ingests must be no-ops |
| `test_mongo_validate.py` | Pre-import file validation for `mongoimport` |
| `test_reddit_column_contract.py` | Parser CSV columns ↔ `COPY` column list match. Pins a bug where `retrieved_utc` was listed both as a real column and a mandatory column, producing a duplicate-column COPY |
| `test_starrocks_ingest.py` | StarRocks ingestion pure-logic helpers (table DDL, schema inference, `INSERT ... SELECT FROM FILES()` SQL shape, BITMAP index DDL) |

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

### `setup/`

| Module | What's covered |
|--------|----------------|
| `test_utils.py` | CLI input helpers (`ask`, `ask_bool`, `ask_choice`, `ask_multi_select`, `ask_int`) with mocked stdin, glob-to-regex conversion, file-pattern derivation |
| `test_source.py` | Per-source config generation: platform routing, field/index derivation, deep merge with templates |
| `test_hf.py` | `organize_hf_downloads` — copies HF parquet files from `dumps/` into `extracted/<data_type>/` according to `hf_config_map` |
| `test_env_and_compose.py` | `.env` updates (preserves siblings on partial writes), `docker-compose.override.yml` generation (tablespaces, SR multi-disk, jobs `/jobs_export` mount) |
| `test_profile_gating.py` | Profile-gating tables in `sdp.py` — every pipeline profile is mapped to its required DB / source / classifier prerequisites; `cmd_run` blocks on missing config |

### Top-level

| Module | What's covered |
|--------|----------------|
| `test_cli_help.py` | Smoke: every `sdp.py` subcommand's `--help` exits 0. Catches argparse regressions and import errors |

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

Requires [sysbox](https://github.com/nestybox/sysbox) installed on the host.

```bash
./tests/e2e/run.sh                  # All E2E tests
./tests/e2e/run.sh -k parse         # Only tests matching "parse"
./tests/e2e/run.sh -x               # Stop on first failure
```

First run builds Docker images inside the sysbox container (~3-5 min). Subsequent runs use the Docker layer cache (~5-10 min total).

### Test scenarios

| Test | What it exercises |
|------|-------------------|
| `test_parse_reddit` | `db setup` → `source add reddit` → compress fixtures to .zst → `run parse` → verify CSV/Parquet output (row counts, mandatory columns, field presence) |
| `test_parse_custom` | Custom platform: `db setup` → `source add` → place NDJSON in `extracted/` → `run parse` → verify Parquet output (row counts, dot-notation field resolution) |
| `test_postgres_flow` | Full PG lifecycle: setup → add → parse → `db start` → `run postgres_ingest` → verify schema, row counts, no duplicate IDs, indexes, column types |
| `test_postgres_dedup_null_safe` | Pins null-safe + deterministic dedup behavior across the full case table (NULL vs. real `retrieved_utc`, same-NULL re-ingests, cross-dataset tiebreakers) |
| `test_recovery_postgres` | `postgres_ingest` recovers from an interrupted prior fast-load (orphaned no-PK table is auto-detected and re-routed) |
| `test_mongo_flow` | Full Mongo lifecycle: setup → add → `db start` → `run mongo_ingest` → verify databases, collections, document counts, `_sdp_metadata` state |
| `test_sr_flow` | Full StarRocks lifecycle: parse → `sr_ingest` → verify Primary Key tables, BITMAP indexes, row counts |
| `test_sr_ml_flow` | StarRocks ML flow: parse → lingua → `sr_ingest` → `sr_ml` → verify classifier tables and merge_condition upsert |
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
