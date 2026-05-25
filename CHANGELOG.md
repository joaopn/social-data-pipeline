# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0]

Major release. Adds StarRocks as a first-class OLAP backend, a jobs scheduler with a web UI and MCP server for human-in-the-loop agentic query workflows, GitHub Codespaces support for a click-to-try demo, and a comprehensive authentication hardening sweep across PostgreSQL, MongoDB, and StarRocks.

### Added

**StarRocks support**

- StarRocks `allin1-ubuntu` (FE+BE) database backend with auto-tuned `fe.conf` / `be.conf` written by `sdp db setup`.
- `sr_ingest` profile: ingests parsed Parquet/CSV into StarRocks Primary Key tables via `INSERT INTO ... SELECT FROM FILES()`; native upsert/dedup with conditional `merge_condition`.
- `sr_ml` profile: ingests ML classifier outputs (lingua, toxic_roberta, go_emotions, etc.) into per-classifier tables with auto-inferred schemas.
- StarRocks MCP server (`starrocks_mcp` profile): read-only `mcp-server-starrocks` over streamable HTTP.
- StarRocks RBAC: per-DB `root` admin + `sdp_readonly` role enforced at the database layer; entrypoint syncs the RO user from `.ro_credentials` on every start.
- BITMAP index creation with poll-based parallel async schema-change handling.
- Tablet / bucket management; multi-disk `storage_paths` support with per-disk bind mounts.

**Jobs scheduler (`jobs` profile)**

- FastAPI single-container app bundling a review/approval web UI, an MCP server for query submission, a runner thread, and a result-file writer.
- Backends supported: PostgreSQL, StarRocks, MongoDB; per-backend timeouts; per-target FIFO concurrency cap.
- MCP tools: `submit_postgres_query`, `submit_starrocks_query`, `submit_mongo_query`; pre-submit validation runs `EXPLAIN` against the target before queueing.
- Web UI features: SQL syntax highlighting + formatting, results preview, `EXPLAIN` button, history page with download + copy-to-clipboard, reject-reason popup, disk-usage stats, dark theme toggle, optional auto-accept toggle with per-target FIFO concurrency cap.
- Optional admin authentication on the web UI, configured independently from DB auth.

**MCP improvements**

- MongoDB MCP server bundles three patches applied at build time: `listDatabases` tool + per-tool `database` parameter (multidb), `maxTimeMS` on read tools, and read-only tool-list filter (`insert` / `update` / `createIndex` removed from `tools/list` when `--read-only`).
- Scheduler MCP pre-submit validation: agents see a clear error from `EXPLAIN` instead of silent failures after approval.

**Authentication chain hardening**

- `.ro_credentials` is now a single-line password-only file, host-owned with mode 0600; existing `username:password` format files are auto-converted on first read.
- DB entrypoints fail loud (exit non-zero) when auth is enabled but RO credentials are missing or unreadable.
- Host-side credential reads raise `ConfigurationError` instead of silently returning `None`.
- Container healthchecks probe the RO user when auth is enabled; `depends_on: service_healthy` no longer green-lights MCP boot on a broken-auth DB.
- `sdp db verify` subcommand: exits non-zero on drift across `config/db/*.yaml`, `.env`, `.ro_credentials`, container state, and mount set; supports `--db <name>` and `--json` for CI use.
- `sdp db status` now surfaces per-DB drift information without leaking foreign-source state.
- `sdp db recover-password --regenerate-ro`: orthogonal flag, combinable with admin recovery; rewrites `.ro_credentials` per DB on each branch's final restart.

**New CLI surfaces**

- `sdp db setup --add <db>`: incrementally add a database to an existing install without re-running full setup.
- `sdp db setup-llm`: bundles `setup-mcp` + `setup-jobs` for the agentic-AI path.
- `sdp db reset --db <name>`: drops a database's data while keeping config; recreates clean tables on next ingest.
- `sdp run <profile> --filter <pattern>`: restrict file processing by glob pattern across profiles.
- `sdp run parse --skip-lingua-files`: skip files whose lingua output already exists.
- `DB_EXPORT_PATH` environment variable: bind-mounts a host directory to `/export` inside DB containers for query data extraction.

**Codespaces demo**

- Devcontainer with docker-outside-of-docker, pre-baked Reddit + three-DB workspace, auto-opening `WELCOME.md`, README "Open in Codespace" badge.
- Pre-tuned PostgreSQL for the 8 GB Codespace tier; four MCP servers (PostgreSQL, MongoDB, StarRocks, jobs scheduler) wired into GitHub Copilot.

**Custom platform improvements**

- Optional `primary_key` for custom platforms; StarRocks falls back to Duplicate Key tables when absent.
- Custom platforms now pin `dataset` as a mandatory field at generation time, fixing column-count mismatch on PostgreSQL COPY.
- `check_duplicates: false` is gated to custom platforms only; Reddit always upserts.

**Packaging**

- `pyproject.toml` for clone-only install via `pipx install --editable .`.

### Changed

- **BREAKING:** ML profiles (`postgres_ml`, `sr_ml`) now derive classifier list and per-classifier suffix from the source's `config/sources/<name>/ml.yaml` and `lingua.yaml`. The `suffix` and `enabled` declarations in `config/sr_ml/services.yaml` and `config/postgres_ml/services.yaml` are ignored; those files are reduced to an optional override layer carrying only `enabled`, `source_dir`, and `column_overrides`.
- `gpu_classifiers` / `cpu_classifiers` list entries now accept mixed form: a bare string means "all data types" (existing v2.x behavior), or a `{name, data_types}` dict scopes a classifier to specific data types.
- `gpu_classifiers` ships with `multilingual_sentiment` and `xlmr_toxicity` registered by default.
- `db unsetup` stops all DB + MCP containers upfront before deletion to avoid stale container mounts after `unsetup` + `setup` cycles.
- README rewritten to lead with the agentic pipeline / jobs scheduler / MCP story; quick-start path updated.

### Fixed

- Mongo password recovery handled correctly on existing `directoryPerDB` data layouts.
- `recover-password` no longer races against PostgreSQL socket readiness during the trust-auth swap.
- PostgreSQL ingest repairs orphaned no-PK tables on no-work runs.
- Reddit `retrieved_utc` stays nullable (HF parquet dumps don't always provide it).
- `postgres_ingest` skips index creation and `ANALYZE` for data types with no files in the current run.
- `prefer_lingua` is strict either/or (Reddit-detected or lingua-detected language), not a soft fallback.
- HF reconfigure crash and source-download robustness fixes.
- Layered server bind mounts: parent base mount + per-source override only when needed; out-of-parent sources get pre-created placeholder directories.
- Source / server mount sync warnings on `source add` / `source remove` when a running DB needs restart; `sdp run` validates mounts before launching ingest.
- `db status` no longer leaks foreign-source state; per-DB info surfaces correctly.
- Postgres `.ro_credentials` previously chowned to the in-container `postgres` UID is now host-owned and readable on the host side for verification.
- Default `db status` output is quiet; verbose info gated behind explicit flags.

### Removed

- `pip install -e .` workflow superseded by `pipx install --editable .` (via the new `pyproject.toml`).
- Several vestigial fallback messages and unused imports.

### Compatibility

- **MongoDB on Linux kernel 6.19+ (Ubuntu 26.04 and later):** the `mongo` service in `docker-compose.yml` sets `GLIBC_TUNABLES=glibc.pthread.rseq=1` to work around an mongo 8.x vendored-tcmalloc RSEQ ABI incompatibility. Removing this variable causes mongo to deterministically `SIGSEGV` (exit 139) approximately 30 seconds after start with no crash trace in any log. See the in-file comment in `docker-compose.yml` for details. Do not remove this variable until MongoDB upstream ships a patched tcmalloc.

### Upgrade notes from v2.x

- **ML services.yaml migration (only if you use the `postgres_ml` or `sr_ml` profiles):** move any `enabled` and `suffix` declarations from `config/sr_ml/services.yaml` and `config/postgres_ml/services.yaml` into your source's `config/sources/<name>/ml.yaml` and `lingua.yaml`. If your `services.yaml` only carried `column_overrides` or `source_dir`, no migration is needed.
- **`.ro_credentials` migration:** existing `username:password` format files are auto-converted on first read; no manual action required. If you encounter a permission error mid-upgrade, run `sdp db recover-password --regenerate-ro` to regenerate.
- **Install path:** uninstall the v2.x development install (`pip uninstall social-data-pipeline`) before running `pipx install --editable .` from the v3.0.0 checkout.
