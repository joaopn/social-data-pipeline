---
title: "Configuration reference"
parent: "User guide"
nav_order: 13
---
# Configuration reference

This page is the full configuration reference: every environment variable, configuration file and tunable setting across all profiles. You rarely edit these by hand: the setup commands write them, and this page says what each one does.

Run `python sdp.py db setup` to configure databases, then `python sdp.py source add <name>` to add sources. The scripts detect your hardware, walk you through every setting with sensible defaults, and generate `.env`, `config/db/*.yaml`, the per-source config in `config/sources/<name>/`, `config/postgres/postgresql.local.conf` and `config/starrocks/{fe,be}.local.conf`. To add a database later without reconfiguring existing ones, use `python sdp.py db setup --add <db>`.

For profile-specific usage and workflows, see [Parsing](parsing.md), [Language detection and classifiers](classifiers.md), [Databases](databases.md) and [Query scheduler](query-scheduler.md).

## Environment variables

All environment variables are set in the `.env` file at the project root. Docker Compose reads this file automatically.

| Variable | Description | Default |
|----------|-------------|---------|
| `PLATFORM` | Platform to use for parsing (`reddit` or `custom/<name>`) | `reddit` |
| `SOURCE` | Source name — selects config from `config/sources/<name>/` | (auto or set by `python sdp.py run --source`) |
| `DUMPS_PATH` | Directory containing compressed dump files | `./data/dumps` |
| `EXTRACTED_PATH` | Storage for decompressed JSON files | `./data/extracted` |
| `PARSED_PATH` | Storage for parsed files | `./data/parsed` |
| `OUTPUT_PATH` | Storage for classifier output files | `./data/output` |
| `PGDATA_PATH` | PostgreSQL data directory | `./data/database/postgres` |
| `DB_NAME` | PostgreSQL database name | `datasets` |
| `POSTGRES_PORT` | PostgreSQL port exposed to host | `5432` |
| `MONGO_DATA_PATH` | MongoDB data directory | `./data/database/mongo` |
| `MONGO_PORT` | MongoDB port exposed to host | `27017` |
| `MONGO_CACHE_SIZE_GB` | MongoDB WiredTiger cache size in GB | `2` |
| `DB_EXPORT_PATH` | Host directory bind-mounted at `/export` in database containers for SQL COPY, mongoexport, etc. | `./data/export` |
| `STARROCKS_DATA_PATH` | StarRocks data directory | `./data/database/starrocks` |
| `STARROCKS_PORT` | StarRocks MySQL protocol port | `9030` |
| `STARROCKS_FE_HTTP_PORT` | StarRocks FE HTTP port (admin, Stream Load) | `8030` |
| `STARROCKS_MEM_LIMIT` | StarRocks container memory limit | `0` (unlimited) |
| `HF_HOME` | HuggingFace model cache directory | (system default) |
| `HF_TOKEN` | HuggingFace authentication token | (none) |
| `CLASSIFIER` | Run a single GPU classifier by name | (all enabled) |
| `FILE_FILTER` | Only process files matching this fnmatch pattern (set by `--filter`) | (all files) |
| `SKIP_LINGUA_FILES` | Parse profile only: skip files that already have a lingua output (set by `--skip-lingua-files`) | (unset) |
| `PROFILE` | Internal: set automatically by docker-compose | (auto) |
| `POSTGRES_AUTH_ENABLED` | Enable PostgreSQL authentication | (unset) |
| `MONGO_AUTH_ENABLED` | Enable MongoDB authentication | (unset) |
| `STARROCKS_AUTH_ENABLED` | Enable StarRocks authentication | (unset) |
| `POSTGRES_PASSWORD` | PostgreSQL admin password (set at runtime, never stored) | (unset) |
| `MONGO_ADMIN_PASSWORD` | MongoDB admin password (set at runtime, never stored) | (unset) |
| `STARROCKS_ROOT_PASSWORD` | StarRocks root password (set at runtime, never stored) | (unset) |
| `POSTGRES_RO_USER` | PostgreSQL read-only user name (used by MCP) | (unset) |
| `MONGO_RO_USER` | MongoDB read-only user name (used by MCP) | (unset) |
| `STARROCKS_RO_USER` | StarRocks read-only user name (used by MCP) | (unset) |
| `POSTGRES_MCP_PORT` | PostgreSQL MCP server port | `8000` |
| `POSTGRES_MCP_ACCESS_MODE` | PostgreSQL MCP access mode (`restricted` or `unrestricted`) | `restricted` |
| `POSTGRES_MCP_USER` | PostgreSQL MCP connection user | (unset) |
| `MONGO_MCP_PORT` | MongoDB MCP server port | `3000` |
| `MONGO_MCP_READ_ONLY` | MongoDB MCP read-only flag | `true` |
| `MONGO_MCP_USER` | MongoDB MCP connection user (passed into the MCP container as `MCP_MONGODB_USER`) | (unset) |
| `STARROCKS_MCP_PORT` | StarRocks MCP server port | `9000` |
| `STARROCKS_MCP_USER` | StarRocks MCP connection user | (unset) |
| `JOBS_PORT` | Jobs scheduler UI + MCP port | `8050` |
| `JOBS_RESULT_ROOT` | Host directory for job result files (bind-mounted into PG/SR as `/jobs_export`) | `./data/jobs/results` |

Notes on these variables:

- `SOURCE` controls which source config directory is loaded. When running via `python sdp.py run`, it is set automatically (auto-selects if only one source configured, or via `--source`).
- `CLASSIFIER` is used with the `ml` profile to run only one GPU classifier instead of all enabled classifiers.
- `FILE_FILTER` is set automatically by `python sdp.py run --filter`. It applies fnmatch glob matching on file IDs (e.g. `*2024*`, `RS_2024-*`) to restrict which files are processed. Works with all profiles.
- `SKIP_LINGUA_FILES` is set automatically by `python sdp.py run parse --skip-lingua-files`. When set, parse skips files for which `<OUTPUT_PATH>/lingua/<data_type>/<id>_lingua.{csv,parquet}` already exists, so users running a `parse → lingua` workflow can delete old extracted/parsed files without parse re-decompressing them on the next run. Parse profile only.
- `PROFILE` is set internally by docker-compose service definitions (`lingua` or `ml`). Do not set this manually.
- `HF_TOKEN` is optional but recommended to avoid rate limits and to access private models. Obtain one at https://huggingface.co/settings/tokens.
- `PARSED_PATH` and `OUTPUT_PATH` are used by `python sdp.py run` for per-source container mounts and as fallback mounts for the PostgreSQL server in `docker-compose.yml`. `python sdp.py db start` auto-generates per-source volume mounts in `docker-compose.override.yml` so the PG server can access files for all sources regardless of filesystem layout.
- Auth env vars (`POSTGRES_PASSWORD`, `MONGO_ADMIN_PASSWORD`, `STARROCKS_ROOT_PASSWORD`) are set at runtime via `getpass` prompting — they are never stored in `.env` or on disk.
- MCP env vars are written to `.env` by `python sdp.py db setup-mcp`. RO user credentials are generated by `python sdp.py db setup` and stored in the database data volume as `.ro_credentials` (chmod 600).

## Config directory structure

```
config/
├── db/                            # Global database config (written by python sdp.py db setup)
│   ├── postgres.yaml             # Port, name, tablespaces, auth flag
│   ├── mongo.yaml                # Port, cache size, auth flag
│   ├── starrocks.yaml            # Port, FE/BE memory, table compression, storage paths, auth flag
│   └── mcp.yaml                  # MCP server config (written by python sdp.py db setup-mcp)
├── sources/                       # Per-source config (written by python sdp.py source add)
│   └── <name>/                   # One directory per source
│       ├── platform.yaml         # Platform config (fields, types, indexes, schema, file patterns)
│       ├── parse.yaml            # Parse profile overrides
│       ├── postgres.yaml         # Postgres ingestion overrides (optional)
│       ├── lingua.yaml           # Lingua overrides (optional)
│       ├── ml.yaml               # ML GPU overrides (optional)
│       ├── postgres_ml.yaml      # Postgres ML overrides (optional)
│       ├── mongo.yaml            # Mongo ingestion overrides (optional)
│       ├── starrocks.yaml        # StarRocks ingestion overrides (optional)
│       └── sr_ml.yaml            # StarRocks ML overrides (optional)
├── templates/                     # Platform templates (copied on source add)
│   └── reddit.yaml               # Base Reddit platform config
├── parse/
│   └── pipeline.yaml             # Parse profile base settings
├── lingua/
│   ├── pipeline.yaml             # CPU classifier pipeline settings
│   └── cpu_classifiers.yaml      # Lingua configuration
├── ml/
│   ├── pipeline.yaml             # GPU classifier pipeline settings
│   └── gpu_classifiers.yaml      # Transformer model configurations
├── postgres/
│   ├── pipeline.yaml             # Database ingestion settings
│   ├── postgresql.conf           # PostgreSQL server tuning
│   ├── pg_hba.conf               # PostgreSQL authentication (default, no-auth)
│   ├── postgresql.local.conf     # Generated PGTune config (written by python sdp.py db setup)
│   ├── pg_hba.local.conf         # Generated auth config (written by python sdp.py db setup when auth enabled)
│   ├── entrypoint-wrapper.sh     # Container entrypoint (pg_parquet, auth migration, RO user sync)
│   ├── healthcheck.sh            # Container health check (logs in as the RO user under auth)
│   └── initdb.d/                 # First-init scripts
│       ├── 01-pg-parquet.sql     # Creates the pg_parquet extension
│       └── 02-ro-user.sh         # Creates the read-only user on a fresh database (when one is configured)
├── mongo/
│   ├── pipeline.yaml             # MongoDB ingestion settings
│   ├── mongod.conf               # MongoDB server configuration
│   ├── entrypoint-wrapper.sh     # Container entrypoint (auth migration)
│   └── healthcheck.sh            # Container health check (logs in as the RO user under auth)
├── starrocks/
│   ├── fe.conf                   # StarRocks FE config template (copied into fe.local.conf by python sdp.py db setup)
│   ├── be.conf                   # StarRocks BE config template (copied into be.local.conf by python sdp.py db setup)
│   ├── fe.local.conf             # Generated FE config, the one mounted (written by python sdp.py db setup)
│   ├── be.local.conf             # Generated BE config, the one mounted (written by python sdp.py db setup)
│   ├── entrypoint-wrapper.sh     # Container entrypoint (auth: root password + RO user sync)
│   └── healthcheck.sh            # Container health check (logs in as the RO user under auth)
├── sr/
│   └── pipeline.yaml             # StarRocks ingestion settings
├── sr_ml/
│   ├── pipeline.yaml             # StarRocks ML classifier ingestion settings
│   └── services.yaml             # Classifier table definitions (same format as postgres_ml)
├── mcp/
│   ├── entrypoint-postgres.sh    # PostgreSQL MCP entrypoint
│   ├── entrypoint-mongo.sh       # MongoDB MCP entrypoint
│   ├── entrypoint-starrocks.sh   # StarRocks MCP entrypoint
│   └── mongo-mcp-*.patch         # Patches applied to the MongoDB MCP server at image build
├── postgres_ml/
│   ├── pipeline.yaml             # ML classifier ingestion settings
│   └── services.yaml             # Classifier table definitions
└── jobs/
    ├── config.yaml               # Jobs scheduler default template (committed)
    ├── config.local.yaml         # Local override (written by python sdp.py db setup-jobs, gitignored)
    └── entrypoint.sh             # Jobs container entrypoint
```

## Source configuration overrides

Each source has per-profile override files in `config/sources/<name>/`, laid over the base profile configs. `python sdp.py source add <name>` generates them, but you can also write them by hand.

An override file holds only the values you want to change. Its top-level keys name the base file they override, without `.yaml` (`pipeline:` overrides `pipeline.yaml`, `gpu_classifiers:` overrides `gpu_classifiers.yaml`), and the values you set win over the base. A list replaces the base list entirely; it is not merged. How the layers are merged is in [Setup and configuration](../architecture/configuration.md#how-the-layers-merge).

```yaml
# config/sources/reddit/parse.yaml
pipeline:
  processing:
    data_types:
      - submissions
      - comments
    parallel_mode: true
    parse_workers: 8
```

In this example, the `pipeline:` key overrides settings from `config/parse/pipeline.yaml` for the `reddit` source.

### Per-source override files

| Source file | Overrides |
|------------|-----------|
| `config/sources/<name>/platform.yaml` | Platform config (fields, types, indexes, schema, file patterns) |
| `config/sources/<name>/parse.yaml` | `config/parse/pipeline.yaml` |
| `config/sources/<name>/lingua.yaml` | `config/lingua/pipeline.yaml`, `cpu_classifiers.yaml` |
| `config/sources/<name>/ml.yaml` | `config/ml/pipeline.yaml`, `gpu_classifiers.yaml` |
| `config/sources/<name>/postgres.yaml` | `config/postgres/pipeline.yaml` |
| `config/sources/<name>/postgres_ml.yaml` | `config/postgres_ml/pipeline.yaml`, `services.yaml` |
| `config/sources/<name>/mongo.yaml` | `config/mongo/pipeline.yaml` |
| `config/sources/<name>/starrocks.yaml` | `config/sr/pipeline.yaml` |
| `config/sources/<name>/sr_ml.yaml` | `config/sr_ml/pipeline.yaml`, `services.yaml` |

## Per-profile configuration reference

The sections below cover each profile's base config files, their settings and their defaults. A source changes them through its override files (above).

## Parse profile

**File:** `config/parse/pipeline.yaml`

```yaml
processing:
  data_types: []            # Data types to process (set via source config)
  parallel_mode: true       # Parse multiple files in parallel
  parse_workers: 8          # Number of parallel workers
  cleanup_temp: false       # Delete intermediate files after processing
  watch_interval: 0         # Check for new files every N minutes (0 = run once)
```

| Setting | Description | Default |
|---------|-------------|---------|
| `data_types` | Data types to process. Typically set via source config. | `[]` |
| `parallel_mode` | Parse multiple files in parallel. | `true` |
| `parse_workers` | Number of parallel CSV parsing workers. | `8` |
| `cleanup_temp` | Delete intermediate files (extracted JSON) after processing. | `false` |
| `watch_interval` | Poll for new files every N minutes. `0` means run once and exit. | `0` |

See also: [Parsing](parsing.md).

## Lingua profile

### Pipeline: `config/lingua/pipeline.yaml`

```yaml
processing:
  data_types:               # Which data types to process
    - submissions
    - comments
  watch_interval: 0

cpu_classifiers:            # CPU classifiers to run
  - lingua
```

| Setting | Description | Default |
|---------|-------------|---------|
| `data_types` | Data types to process. | `[submissions, comments]` |
| `watch_interval` | Poll interval in minutes (`0` = run once). | `0` |
| `cpu_classifiers` | List of CPU classifiers to run (references keys in `cpu_classifiers.yaml`). | `[lingua]` |

### Classifiers: `config/lingua/cpu_classifiers.yaml`

Global settings (apply to all CPU classifiers):

| Setting | Description | Default |
|---------|-------------|---------|
| `text_columns` | Columns to classify per data type. | `submissions: [title, selftext]`, `comments: [body]` |
| `remove_strings` | Exact strings removed before classification. | `[deleted]`, `[removed]`, `[unavailable]` |
| `remove_patterns` | Regex patterns removed before classification. | URLs, `r/subreddit`, `u/user` |
| `fields` | Additional columns to keep in `lingua_ingest` CSV (beyond mandatory `id`, `dataset`, `retrieved_utc`). Empty means only mandatory fields. | `[]` |

Lingua classifier settings (`lingua:` key):

| Setting | Description | Default |
|---------|-------------|---------|
| `suffix` | Output filename suffix (e.g., `RS_2024-01_lingua.csv`). | `_lingua` |
| `min_chars` | Minimum character count after text cleaning. Texts shorter than this are skipped. | `5` |
| `low_accuracy` | Faster but less accurate detection mode. | `false` |
| `workers` | Total parallel workers, divided among `file_workers`. | `16` |
| `file_workers` | Number of files processed in parallel. | `2` |
| `batch_size` | Rows per batch (larger = less overhead, more memory). | `2000000` |
| `languages` | List of Lingua enum names to detect. | 54 languages across 5 tiers (see below) |

The language tiers are listed in [Language tiers](classifiers.md#language-tiers).

See also: [Language detection and classifiers](classifiers.md).

## ML GPU profile

### Pipeline: `config/ml/pipeline.yaml`

```yaml
processing:
  data_types:
    - submissions
    - comments
  watch_interval: 0

gpu_classifiers:
  - toxic_roberta                    # runs on every data_type in processing.data_types
  - name: xlmr_toxicity
    data_types: [comments]           # runs only on comments
```

| Setting | Description | Default |
|---------|-------------|---------|
| `data_types` | Data types to process. | `[submissions, comments]` |
| `watch_interval` | Poll interval in minutes (`0` = run once). | `0` |
| `gpu_classifiers` | List of GPU classifiers to run (references keys in `gpu_classifiers.yaml`). Each entry is either a bare classifier name (runs on every `processing.data_types` entry) or a `{name, data_types}` dict that scopes the classifier to a subset. Unknown `data_types` raise a config error. | `[toxic_roberta, go_emotions]` |

### Classifiers: `config/ml/gpu_classifiers.yaml`

Global settings (apply to all GPU classifiers unless overridden per classifier):

| Setting | Description | Default |
|---------|-------------|---------|
| `text_columns` | Columns to classify per data type. | `submissions: [title, selftext]`, `comments: [body]` |
| `remove_strings` | Exact strings removed before classification. | `[deleted]`, `[removed]`, `[unavailable]` |
| `remove_patterns` | Regex patterns removed before classification. | URLs, `r/subreddit`, `u/user` |
| `gpu_ids` | GPU device IDs to parallelize across. | `[0]` |
| `file_workers` | Files processed in parallel (each loads its own models). | `1` |
| `tokenize_workers` | Parallel tokenization workers (`0` = single-threaded). | `4` |
| `batch_size` | Rows per disk I/O batch (prevents memory blow-up on large files). | `2000000` |
| `classifier_batch_size` | Batch size per GPU for inference. | `32` |
| `use_lingua` | Use `lang` column from Lingua output for language filtering. | `true` |
| `lang2_fallback` | Also check `lang2` column when filtering by language. | `false` |
| `min_tokens` | Global minimum token count after text cleaning (`0` = no filter). | `0` |
| `fields` | Additional columns to keep in output (beyond mandatory `id`, `dataset`, `retrieved_utc` + classifier columns). Empty or unset keeps all input columns. | `[author, subreddit]` |

Per-classifier options (defined as top-level keys like `toxic_roberta:`):

| Option | Description | Default |
|--------|-------------|---------|
| `suffix` | Output filename and table suffix (e.g., `_toxicity_en`). | *(required)* |
| `type` | Model backend: `onnx_fp16`, `onnx`, or `pytorch`. | `onnx_fp16` |
| `model` | HuggingFace model ID (e.g., `joaopn/unbiased-toxic-roberta-onnx-fp16`). | *(required)* |
| `file_name` | ONNX model filename within the model repo. | `model.onnx` |
| `activation` | `sigmoid` for multi-label or `softmax` for single-label classification. | `softmax` |
| `supported_languages` | Filter texts by `lang` column (requires `use_lingua: true`). | *(all languages)* |
| `lang2_fallback` | Override global `lang2_fallback` for this classifier. | global value |
| `classifier_batch_size` | Override global batch size per GPU for inference. | `32` |
| `max_length` | Maximum token length for the model. | `512` |
| `chunking_strategy` | `truncate` (default) or `chunk` with sliding window overlap. | `truncate` |
| `stride` | Token overlap between chunks (only used with `chunk` strategy). | `64` |
| `top_k` | Top-k chunks to average for prediction. `1` = max-pooling. | `2` |
| `min_tokens` | Minimum token count after cleaning (overrides global value). | global value |
| `gpu_ids` | Override global GPU ID list for this classifier. | global value |
| `batch_size` | Override global rows-per-batch for disk I/O. | global value |
| `fields` | Override global additional output columns. | global value |

Bundled classifiers:

| Classifier | Model | Type | Activation | Languages |
|-----------|-------|------|------------|-----------|
| `toxic_roberta` | `joaopn/unbiased-toxic-roberta-onnx-fp16` | `onnx_fp16` | `sigmoid` (multi-label) | English |
| `go_emotions` | `joaopn/roberta-base-go_emotions-onnx-fp16` | `onnx_fp16` | `sigmoid` (multi-label) | English |
| `multilingual_sentiment` | `joaopn/multilingual-sentiment-analysis-onnx-fp16` | `onnx_fp16` | `softmax` (single-label) | 21 languages (en, zh, es, hi, ar, bn, pt, ru, ja, de, ms, te, vi, ko, fr, tr, it, pl, uk, nl, sw) |
| `xlmr_toxicity` | `joaopn/xlmr-large-toxicity-classifier-v2-onnx-fp16` | `onnx_fp16` | `softmax` (binary) | 14 languages (en, ru, uk, de, es, ar, am, hi, zh, it, fr, he, ja, tt) |

Output columns are auto-derived from the model's `config.id2label` mapping.

See also: [Language detection and classifiers](classifiers.md).

## PostgreSQL profiles

### Base table ingestion: `config/postgres/pipeline.yaml`

```yaml
database:
  host: postgres             # Docker service name
  port: 5432                 # Override with POSTGRES_PORT env var
  name: datasets             # Override with DB_NAME env var
  schema: null               # Set via source platform.yaml
  user: postgres

processing:
  data_types: []             # Set via source config
  parallel_mode: false       # Extract all -> parse -> ingest sequentially
  parallel_ingestion: true   # Ingest data types concurrently
  parse_workers: 12          # CSV parsing workers
  check_duplicates: true     # Handle duplicate IDs (ON CONFLICT)
  create_indexes: true       # Create indexes after ingestion
  parallel_index_workers: 8  # Workers per index build
  cleanup_temp: false        # Delete intermediate files
  watch_interval: 0          # Run once (0) or poll every N minutes
  prefer_lingua: true        # Ingest only lingua-enriched files (strict either/or, no fallback)

indexes: {}                  # Per-data-type index fields (set via source platform.yaml)
```


| Setting | Description | Default |
|---------|-------------|---------|
| **database.host** | PostgreSQL hostname (Docker service name). | `postgres` |
| **database.port** | PostgreSQL port. Overridden by `POSTGRES_PORT` env var. | `5432` |
| **database.name** | Database name. Overridden by `DB_NAME` env var. | `datasets` |
| **database.schema** | Schema name. Set via source `platform.yaml`. | `null` |
| **database.user** | PostgreSQL user. | `postgres` |
| **processing.data_types** | Data types to ingest. Set via source config. | `[]` |
| **processing.parallel_mode** | If `true`: extract all, parse in parallel, then ingest all. | `false` |
| **processing.parallel_ingestion** | Ingest multiple data types concurrently (requires `parallel_mode: true`). | `true` |
| **processing.parse_workers** | Number of parallel CSV parsing workers. | `12` |
| **processing.check_duplicates** | Handle duplicate IDs during ingestion (`ON CONFLICT`). | `true` |
| **processing.create_indexes** | Create indexes after ingestion completes. | `true` |
| **processing.parallel_index_workers** | `max_parallel_maintenance_workers` per index build. | `8` |
| **processing.cleanup_temp** | Delete intermediate files after ingestion. | `false` |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |
| **processing.prefer_lingua** | If `true`, ingest only the lingua-enriched files (from the `lingua` output folder); if `false`, ingest the original parsed files. Strict either/or with no fallback: with `true` and no lingua output, ingest finds zero files. | `true` |
| **indexes** | Index fields per data type (e.g., `{submissions: [dataset, author, subreddit]}`). | `{}` |
| **tablespaces** | Tablespace definitions: map of name to host path (e.g., `{nvme1: /mnt/nvme1/pg-tablespace}`). | `{}` |
| **table_tablespaces** | Table-to-tablespace assignments: map of data type to tablespace name (e.g., `{submissions: nvme1}`). Use `pgdata` for the default PostgreSQL data directory. | `{}` |

See [Tablespaces](databases.md#tablespaces) for details on multi-disk setups.

### ML table ingestion: `config/postgres_ml/pipeline.yaml`

```yaml
database:
  host: postgres
  port: 5432
  name: datasets
  schema: null
  user: postgres

processing:
  data_types: []
  check_duplicates: true
  parallel_ingestion: true
  type_inference_rows: 1000  # Rows sampled for column type inference
  use_foreign_key: true      # FK constraint to main table
  create_indexes: true       # Index classifier tables after ingestion
  parallel_index_workers: 8  # Maintenance workers per index build
  watch_interval: 0

ml_indexes: {}               # Per classifier table, e.g. {comments_lingua: [lang]}
```

| Setting | Description | Default |
|---------|-------------|---------|
| **processing.data_types** | Data types to ingest. Set via source config. | `[]` |
| **processing.check_duplicates** | Handle duplicate IDs during ingestion. | `true` |
| **processing.parallel_ingestion** | Ingest data types concurrently. | `true` |
| **processing.type_inference_rows** | Number of rows sampled to infer column types from CSV data. | `1000` |
| **processing.use_foreign_key** | Add foreign key constraint linking to the main table. Requires the main table to exist; set `false` for independent ingestion. | `true` |
| **processing.create_indexes** | Create indexes on classifier tables after ingestion completes. | `true` |
| **processing.parallel_index_workers** | `max_parallel_maintenance_workers` per index build. | `8` |
| **ml_indexes** | Index fields per classifier table, keyed by table name (data type + classifier suffix). **No fallback** to `indexes` — base index fields name columns classifier tables do not have. | `{}` (from platform config) |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |

Only tables that ingested at least one file in the current run are indexed; each index lands in the same tablespace as its table. Use `python sdp.py db create-indexes` to add indexes to tables that are already fully ingested.

`prefer_lingua` is read from the postgres profile (`config/postgres/pipeline.yaml`). When `true`, the lingua classifier is skipped during `postgres_ml` ingestion because lingua data is already in the main table. When `false`, lingua data is ingested from the `lingua_ingest` directory.

### Classifier resolution

What ingestion runs comes from the source's ml/lingua profile config, not from a separate declaration. For each classifier listed in `config/sources/<name>/ml.yaml`'s `pipeline.gpu_classifiers` (and lingua via `cpu_classifiers`), `postgres_ml` reads the merged profile config to get its `suffix` and `data_types` scope. Source overrides win over the global `gpu_classifiers.yaml` defaults.

### Ingestion overrides: `config/postgres_ml/services.yaml`

This file is for ingestion-only tweaks that don't belong in the ml profile. Empty by default; everything is derived.

```yaml
# All optional. Per-source overrides live in config/sources/<name>/postgres_ml.yaml.
classifiers:
  toxic_roberta:
    enabled: false              # ran in ml profile, but skip PG ingestion
  go_emotions:
    source_dir: custom_outputs  # non-default output directory
    column_overrides:
      confidence: REAL          # force a specific column type
  lingua:
    source_dir_ingest: lingua_ingest   # used when prefer_lingua: false
```

| Option | Description |
|--------|-------------|
| `enabled` | Skip ingestion of a classifier without removing it from the ml profile. Defaults to `true`. |
| `source_dir` | Directory under `/data/output/` (default: classifier name). |
| `source_dir_ingest` | Lingua-only: alternative dir used when `prefer_lingua: false`. Defaults to `lingua_ingest`. |
| `column_overrides` | Force specific column types instead of inferring from data. |

`suffix` is **not** an ingestion override — it lives in the ml/lingua profile (so the ML run and the ingestion always agree on file names). Column types are auto-inferred from data unless `column_overrides` is set.

See also: [Databases](databases.md).

## PostgreSQL server tuning

The PostgreSQL container loads its configuration from `config/postgres/`:

| File | Purpose |
|------|---------|
| `postgresql.conf` | Server tuning parameters (shared_buffers, work_mem, etc.) |
| `pg_hba.conf` | Client authentication rules (default: trust/no-auth) |
| `postgresql.local.conf` | **Local override** — if present, replaces `postgresql.conf` |
| `pg_hba.local.conf` | **Local override** — if present, replaces `pg_hba.conf` (generated by `python sdp.py db setup` when auth enabled) |

Run `python sdp.py db setup`, which handles PGTune integration and ZFS optimization as part of the interactive setup.

To tune by hand instead:

1. Go to [PGTune](https://pgtune.leopard.in.ua/) and generate settings with:
   - **DB Type:** Data Warehouse
   - **Data Storage:** SSD
   - Configure RAM, CPUs, and connections for your system
2. Append the generated settings to `config/postgres/postgresql.conf`, or create a `postgresql.local.conf` with the full configuration.

The local override files (`postgresql.local.conf`, `pg_hba.local.conf`) are not tracked by git, making them safe for machine-specific tuning.

## MongoDB profile

### Ingestion: `config/mongo/pipeline.yaml`

```yaml
database:
  host: mongo              # Docker service name
  port: 27017              # Override with MONGO_PORT env var

processing:
  data_types: []           # Set via source config
  num_insertion_workers: 4  # mongoimport --numInsertionWorkers
  create_indexes: true      # Create indexes after ingestion
  cleanup_temp: false       # Delete extracted JSON after ingestion
  watch_interval: 0         # Run once (0) or poll every N minutes

mongo_indexes: {}           # Per-data-type index fields (set via platform config)
```

| Setting | Description | Default |
|---------|-------------|---------|
| **database.host** | MongoDB hostname (Docker service name). | `mongo` |
| **database.port** | MongoDB port. Overridden by `MONGO_PORT` env var. | `27017` |
| **processing.data_types** | Data types to process. Set via source config. | `[]` |
| **processing.num_insertion_workers** | Workers for `mongoimport --numInsertionWorkers`. | `4` |
| **processing.create_indexes** | Create indexes after ingestion completes. | `true` |
| **processing.cleanup_temp** | Delete extracted files (JSON/Parquet) after ingestion. | `false` |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |
| **mongo_indexes** | Index fields per data type (e.g., `{submissions: [id, author]}`). Set via source `platform.yaml`. | `{}` |

Platform-specific settings (`mongo_collection_strategy`, `mongo_db_name`/`mongo_db_name_template`, `mongo_collections`, `mongo_indexes`) are configured in the source's `platform.yaml`, not in the pipeline config. See [Platform configuration](#platform-configuration).

### Server: `config/mongo/mongod.conf`

Mounted into the mongo container. Key settings:
- `directoryPerDB: true` — each database in its own directory
- zstd compression for both journal and collection blocks
- `diagnosticDataCollectionEnabled: false`

Cache size is controlled via `MONGO_CACHE_SIZE_GB` env var (default: 2 GB), not in the conf file.

## StarRocks ingestion profile

### Ingestion: `config/sr/pipeline.yaml`

```yaml
database:
  host: starrocks          # Docker service name
  port: 9030               # MySQL protocol port, override with STARROCKS_PORT
  user: root               # Default StarRocks user

processing:
  data_types: []           # Set via source config
  check_duplicates: true   # Use merge_condition for conditional upsert
  create_indexes: true     # Create indexes after ingestion
  index_poll_interval: 10  # Seconds between alter-job status polls during index build
  cleanup_temp: false      # Delete intermediate files after ingestion
  watch_interval: 0        # Run once (0) or poll every N minutes
  prefer_lingua: true      # Ingest only lingua-enriched files (strict either/or, no fallback)

sr_indexes: {}             # Per-data-type index fields (set via platform config).
                           # Plain list = bitmap; {bitmap: [...], bloomfilter: [...]}
                           # for per-type control.
```

| Setting | Description | Default |
|---------|-------------|---------|
| **database.host** | StarRocks hostname (Docker service name). | `starrocks` |
| **database.port** | MySQL protocol port. Overridden by `STARROCKS_PORT` env var. | `9030` |
| **database.user** | StarRocks user. Overridden by `STARROCKS_ROOT_PASSWORD` for auth. | `root` |
| **processing.data_types** | Data types to process. Set via source config. | `[]` |
| **processing.check_duplicates** | Enable `merge_condition` for conditional upsert (keeps row with higher `upsert_order_field`). | `true` |
| **processing.create_indexes** | Create indexes after ingestion completes. | `true` |
| **processing.index_poll_interval** | Seconds between `SHOW ALTER TABLE COLUMN` polls while waiting for an index build to finish. | `10` |
| **processing.prefer_lingua** | If `true`, ingest only the lingua-enriched files; if `false`, the original parsed files. Strict either/or with no fallback: with `true` and no lingua output, ingest finds zero files. | `true` |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |
| **sr_indexes** | Index fields per data type. Plain list = bitmap, or `{bitmap: [...], bloomfilter: [...]}`. Falls back to `indexes` from platform config, which is read list-only since postgres_ingest shares it. See [Choosing a StarRocks Index Type](databases.md#choosing-a-starrocks-index-type). | `{}` |

StarRocks uses database-per-source (database name = source name). Tables are Primary Key tables with `DISTRIBUTED BY HASH(pk)` and `enable_persistent_index = true` when the platform defines a `primary_key`. Custom platforms (e.g. HF datasets) can leave `primary_key` unset; tables then use the Duplicate Key model with `DISTRIBUTED BY RANDOM`, re-runs append rows, and `check_duplicates` is forced off so no `merge_condition` is emitted.

**Table compression.** New tables are created with the global `compression` codec from `config/db/starrocks.yaml` (default `ZSTD`; see [Table compression](databases.md#table-compression)). Override per source under `pipeline.database.compression` in the source's `starrocks.yaml` / `sr_ml.yaml`.

**Index builds.** `sr_ingest`, `sr_ml` and `python sdp.py db create-indexes` build bitmap indexes and bloom filters; different tables are indexed in parallel and the indexes of one table one at a time, and `sr_ml` caps this at 8 tables at once. BE-side parallelism is capped by `alter_tablet_worker_count` in `config/starrocks/be.local.conf` (set at `python sdp.py db setup` from the hardware, from the `be.conf` template); raising it speeds up builds at the cost of BE memory, and high-cardinality columns (for example `author`) can push the BE past its memory limit if the pool is too large. How builds are submitted, polled and confirmed is in [Databases and ingestion](../architecture/databases.md#index-builds).

### ML classifier ingestion: `config/sr_ml/pipeline.yaml` + `services.yaml`

```yaml
database:
  host: starrocks          # Docker service name
  port: 9030               # MySQL protocol port, override with STARROCKS_PORT
  user: root               # Default StarRocks user

processing:
  data_types: []           # Set via platform config
  check_duplicates: true   # Use merge_condition for conditional upsert
  parallel_ingestion: true # Ingest submissions and comments concurrently
  type_inference_rows: 1000  # Rows to sample for CSV column type inference
  create_indexes: true     # Index classifier tables after ingestion
  index_poll_interval: 10  # Seconds between alter-job polls
  watch_interval: 0        # Run once (0) or poll every N minutes

sr_ml_indexes: {}          # Per classifier table, e.g. {comments_lingua: [lang]}
```

| Setting | Description | Default |
|---------|-------------|---------|
| **database.host** | StarRocks hostname (Docker service name). | `starrocks` |
| **database.port** | MySQL protocol port. Overridden by `STARROCKS_PORT` env var. | `9030` |
| **database.user** | StarRocks user. Overridden by `STARROCKS_ROOT_PASSWORD` for auth. | `root` |
| **processing.data_types** | Data types to process. Set via source config. | `[]` |
| **processing.check_duplicates** | Enable `merge_condition` for conditional upsert (keeps row with higher `upsert_order_field`). | `true` |
| **processing.parallel_ingestion** | Process submissions and comments concurrently. | `true` |
| **processing.type_inference_rows** | Number of CSV rows to sample for column type inference. | `1000` |
| **processing.create_indexes** | Create indexes on classifier tables after ingestion completes. | `true` |
| **processing.index_poll_interval** | Seconds between `SHOW ALTER TABLE COLUMN` polls while an index builds. | `10` |
| **sr_ml_indexes** | Index fields per classifier table, keyed by table name (data type + classifier suffix). Plain list = bitmap, or `{bitmap: [...], bloomfilter: [...]}`. **No fallback** — unlike `sr_indexes`, it never falls back to `indexes`. | `{}` (from platform config) |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |

Only tables that ingested at least one file in the current run are indexed; tables build in parallel (capped at 8), fields within a table serially. Use `python sdp.py db create-indexes` to add indexes to tables that are already fully ingested.

Classifier tables (e.g., `submissions_lingua`, `comments_toxicity_en`) are created in the same database as base tables. Schema is auto-inferred from Parquet metadata or CSV sampling. `prefer_lingua` is read from the `sr_ingest` profile config.

`sr_ml` resolves classifier runs (name, suffix, `data_types` scope) from the source's ml/lingua profile config, the same way `postgres_ml` does. `config/sr_ml/services.yaml` (and the per-source `config/sources/<name>/sr_ml.yaml`) holds optional ingestion-only overrides — see *Ingestion Overrides* under PostgreSQL ML above for the schema.

## Jobs scheduler

Configured via `python sdp.py db setup-jobs`. Reference is in [query-scheduler.md](query-scheduler.md) — covers `config/jobs/config.yaml` (committed defaults) merged with `config/jobs/config.local.yaml` (gitignored override): targets, port, per-backend timeouts, history retention, and the optional UI auth toggle.

## Platform configuration

Each source has a `platform.yaml` in `config/sources/<name>/` that defines the platform-specific settings. For built-in platforms (like Reddit), this is copied from a template in `config/templates/` during `python sdp.py source add`. For custom platforms, it is generated interactively.

| Key | Description |
|-----|-------------|
| `db_schema` | Database schema name for this source. |
| `data_types` | List of data types this source supports. |
| `file_format` | Output format: `parquet` (default) or `csv` (alternate, for external tool compatibility). |
| `input_format` | Input file format: `ndjson` (default), `csv`, or `parquet`. CSV input uses Polars for robust parsing. Parquet is auto-set when using `--hf`. Custom platforms only. |
| `input_csv_delimiter` | Delimiter for CSV input files (default: `,`). Supports tab (`\t`), pipe (`|`), etc. Only used when `input_format: csv`. |
| `hf_dataset` | Hugging Face dataset ID (e.g., `user/dataset-name`). Set by `--hf` during `source add`. Used by `source download`. |
| `hf_config_map` | Mapping of data_type → list of HF config names. Set by `--hf` during `source add`. Used by `source download` to organize files into data type directories. |
| `file_patterns` | File detection patterns per data type (keys: `dump`, `json`, `csv`, `parquet`, `prefix`, and optionally `dump_glob`, `compression`). |
| `indexes` | Default index fields per data type (used by the postgres profile). |
| `ml_indexes` | Index fields per classifier table (used by postgres_ml). Keyed by data type + classifier suffix, e.g. `comments_lingua`. No fallback to `indexes`. |
| `mongo_collection_strategy` | `per_file` or `per_data_type` (used by mongo_ingest). |
| `mongo_db_name` | Explicit MongoDB database name (custom sources). |
| `mongo_db_name_template` | Database name template with `{platform}` and `{data_type}` placeholders (built-in platforms). |
| `mongo_collections` | Per-data-type MongoDB collection names (custom sources). |
| `mongo_indexes` | Index fields per data type (used by mongo_ingest). |
| `sr_indexes` | Index fields per data type (used by sr_ingest). Plain list = bitmap, or `{bitmap: [...], bloomfilter: [...]}`. Falls back to `indexes` (read list-only). |
| `sr_ml_indexes` | Index fields per classifier table (used by sr_ml). Keyed by data type + classifier suffix. Same shapes as `sr_indexes`. No fallback. |
| `field_types` | Type definitions for each field (integer, text, boolean, etc.). |
| `fields` | Fields to extract per data type. |
| `paths` | Optional per-source data paths (overrides global `DUMPS_PATH`, etc.). |

### Reddit platform: `config/sources/reddit/platform.yaml`

For Reddit, the template `config/templates/reddit.yaml` is copied into `config/sources/reddit/platform.yaml` during `python sdp.py source add reddit`, then customized interactively.

```yaml
db_schema: reddit
file_format: parquet                       # 'parquet' (default) or 'csv'

data_types:
  - submissions
  - comments

file_patterns:
  submissions:
    dump: '^RS_(\d{4}-\d{2})\..+$'       # Any compressed dump file
    zst: '^RS_(\d{4}-\d{2})\.zst$'       # Legacy: .zst only
    json: '^RS_(\d{4}-\d{2})$'
    csv: '^RS_(\d{4}-\d{2})\.csv$'
    parquet: '^RS_(\d{4}-\d{2})\.parquet$'
    prefix: 'RS_'
  comments:
    dump: '^RC_(\d{4}-\d{2})\..+$'
    zst: '^RC_(\d{4}-\d{2})\.zst$'
    json: '^RC_(\d{4}-\d{2})$'
    csv: '^RC_(\d{4}-\d{2})\.csv$'
    parquet: '^RC_(\d{4}-\d{2})\.parquet$'
    prefix: 'RC_'

indexes:
  submissions: [dataset, author, subreddit, domain]
  comments: [dataset, author, subreddit, link_id]

ml_indexes: {}        # Classifier tables, e.g. {comments_lingua: [lang]}
sr_ml_indexes: {}     # Same for StarRocks; a plain list is bitmap, or
                      # {bitmap: [...], bloomfilter: [...]} per table

field_types:
  created_utc: integer
  score: integer
  author: text
  # ... all type definitions ...

fields:
  submissions:
    - created_utc
    - id10
    - score
    # ... 25 fields total ...
  comments:
    - created_utc
    - link_id
    # ... 17 fields total ...
```

### Custom platforms: `config/sources/<name>/platform.yaml`

Custom platform configs are generated interactively during `python sdp.py source add <name>`. Users choose an input format (NDJSON, CSV, or Parquet) and enter glob patterns for file matching (e.g., `tweets_*.json.gz`, `data_*.csv.zst`) which are converted to regex patterns with auto-detected compression. For Hugging Face datasets (`--hf` flag), input format is set to `parquet` automatically and file patterns are auto-generated.

```yaml
db_schema: my_data
file_format: parquet                       # Output: 'parquet' (default) or 'csv'
# input_format: csv                        # Input: 'ndjson' (default), 'csv', or 'parquet'
# input_csv_delimiter: ","                 # CSV delimiter (default: comma)
# hf_dataset: user/dataset-name           # HF dataset ID (set by --hf, used by source download)
# hf_config_map:                           # HF config → data_type mapping (set by --hf)
#   posts: [config1, config2]

data_types:
  - posts

paths:
  dumps: ./data/dumps/my_data
  extracted: ./data/extracted/my_data
  parsed: ./data/parsed/my_data
  output: ./data/output/my_data

file_patterns:
  posts:
    dump: '^posts_.*\.json\.gz$'
    dump_glob: '*.json.gz'
    json: '^posts_.*$'
    csv: '^posts_.*\.csv$'
    parquet: '^posts_.*\.parquet$'
    prefix: 'posts_'
    compression: gz

mongo_collection_strategy: per_data_type
mongo_db_name: my_data
mongo_collections:
  posts: posts

indexes:
  posts: [author]

ml_indexes:
  posts_lingua: [lang]     # Classifier tables: data type + classifier suffix

field_types:
  id: text
  created_at: integer
  author: text
  content: text

fields:
  posts:
    - id
    - created_at
    - author
    - content
```

Run `python sdp.py source add <name>` to generate the config interactively.

See [Your own data and Hugging Face](custom-data.md) for a complete setup guide.
