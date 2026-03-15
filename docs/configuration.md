# Configuration Reference

This document is the master configuration reference for Social Data Bridge. It covers every environment variable, configuration file, and tunable setting across all profiles.

> [!TIP]
> Run `python sdb.py db setup` to configure databases, then `python sdb.py source add <name>` to add sources. The scripts auto-detect your hardware, walk you through every setting with sensible defaults, and generate `.env`, `config/db/*.yaml`, per-source config in `config/sources/<name>/`, and `postgresql.local.conf`. The reference below documents what each setting does.

For profile-specific usage and workflows, see:
- [Parse Profile](profiles/parse.md)
- [Classification Profiles](profiles/classification.md)
- [Database Profiles](profiles/database.md)

---

## 1. Environment Variables

All environment variables are set in the `.env` file at the project root. Docker Compose reads this file automatically.

| Variable | Description | Default |
|----------|-------------|---------|
| `PLATFORM` | Platform to use for parsing (`reddit` or `custom/<name>`) | `reddit` |
| `SOURCE` | Source name — selects config from `config/sources/<name>/` | (auto or set by `sdb.py run --source`) |
| `DUMPS_PATH` | Directory containing compressed dump files | `./data/dumps` |
| `EXTRACTED_PATH` | Storage for decompressed JSON files | `./data/extracted` |
| `CSV_PATH` | Storage for parsed CSV files | `./data/csv` |
| `OUTPUT_PATH` | Storage for classifier output files | `./data/output` |
| `PGDATA_PATH` | PostgreSQL data directory | `./data/database/postgres` |
| `DB_NAME` | PostgreSQL database name | `datasets` |
| `POSTGRES_PORT` | PostgreSQL port exposed to host | `5432` |
| `MONGO_DATA_PATH` | MongoDB data directory | `./data/database/mongo` |
| `MONGO_PORT` | MongoDB port exposed to host | `27017` |
| `MONGO_CACHE_SIZE_GB` | MongoDB WiredTiger cache size in GB | `2` |
| `HF_HOME` | HuggingFace model cache directory | (system default) |
| `HF_TOKEN` | HuggingFace authentication token | (none) |
| `CLASSIFIER` | Run a single GPU classifier by name | (all enabled) |
| `PROFILE` | Internal: set automatically by docker-compose | (auto) |
| `POSTGRES_AUTH_ENABLED` | Enable PostgreSQL authentication | (unset) |
| `MONGO_AUTH_ENABLED` | Enable MongoDB authentication | (unset) |
| `POSTGRES_PASSWORD` | PostgreSQL admin password (set at runtime, never stored) | (unset) |
| `MONGO_ADMIN_PASSWORD` | MongoDB admin password (set at runtime, never stored) | (unset) |
| `POSTGRES_RO_USER` | PostgreSQL read-only convenience user name | (unset) |
| `MONGO_RO_USER` | MongoDB read-only convenience user name | (unset) |
| `POSTGRES_MCP_PORT` | PostgreSQL MCP server port | `8000` |
| `POSTGRES_MCP_ACCESS_MODE` | PostgreSQL MCP access mode (`restricted` or `unrestricted`) | `restricted` |
| `POSTGRES_MCP_USER` | PostgreSQL MCP connection user | (unset) |
| `MONGO_MCP_PORT` | MongoDB MCP server port | `3000` |
| `MONGO_MCP_READ_ONLY` | MongoDB MCP read-only flag | `true` |
| `MCP_MONGODB_USER` | MongoDB MCP connection user | (unset) |

> [!NOTE]
> - `SOURCE` controls which source config directory is loaded. When running via `sdb.py run`, it is set automatically (auto-selects if only one source configured, or via `--source`).
> - `CLASSIFIER` is used with the `ml` profile to run only one GPU classifier instead of all enabled classifiers.
> - `PROFILE` is set internally by docker-compose service definitions (`ml_cpu` or `ml`). Do not set this manually.
> - `HF_TOKEN` is optional but recommended to avoid rate limits and to access private models. Obtain one at https://huggingface.co/settings/tokens.
> - Auth env vars (`POSTGRES_PASSWORD`, `MONGO_ADMIN_PASSWORD`) are set at runtime via `getpass` prompting — they are never stored in `.env` or on disk.
> - MCP env vars are written to `.env` by `sdb db mcp`. MCP credentials are stored in the database data volume as `.mcp_credentials` (chmod 600).

---

## 2. Config Directory Structure

```
config/
├── db/                            # Global database config (written by sdb db setup)
│   ├── postgres.yaml             # Port, name, tablespaces, auth flag
│   ├── mongo.yaml                # Port, cache size, auth flag
│   └── mcp.yaml                  # MCP server config (written by sdb db mcp)
├── sources/                       # Per-source config (written by sdb source add)
│   └── <name>/                   # One directory per source
│       ├── platform.yaml         # Platform config (fields, types, indexes, schema, file patterns)
│       ├── parse.yaml            # Parse profile overrides
│       ├── postgres.yaml         # Postgres ingestion overrides (optional)
│       ├── ml_cpu.yaml           # ML CPU overrides (optional)
│       ├── ml.yaml               # ML GPU overrides (optional)
│       ├── postgres_ml.yaml      # Postgres ML overrides (optional)
│       └── mongo.yaml            # Mongo ingestion overrides (optional)
├── templates/                     # Platform templates (copied on source add)
│   └── reddit.yaml               # Base Reddit platform config
├── parse/
│   └── pipeline.yaml             # Parse profile base settings
├── ml_cpu/
│   ├── pipeline.yaml             # CPU classifier pipeline settings
│   └── cpu_classifiers.yaml      # Lingua configuration
├── ml/
│   ├── pipeline.yaml             # GPU classifier pipeline settings
│   └── gpu_classifiers.yaml      # Transformer model configurations
├── postgres/
│   ├── pipeline.yaml             # Database ingestion settings
│   ├── postgresql.conf           # PostgreSQL server tuning
│   ├── pg_hba.conf               # PostgreSQL authentication (default, no-auth)
│   ├── postgresql.local.conf     # Generated PGTune config (written by sdb db setup)
│   ├── pg_hba.local.conf         # Generated auth config (written by sdb db setup when auth enabled)
│   ├── entrypoint-wrapper.sh     # Container entrypoint (pg_parquet, auth migration)
│   └── initdb.d/                 # First-init scripts (pg_parquet extension)
├── mongo/
│   ├── pipeline.yaml             # MongoDB ingestion settings
│   ├── mongod.conf               # MongoDB server configuration
│   └── entrypoint-wrapper.sh     # Container entrypoint (auth migration)
├── mcp/
│   ├── entrypoint-postgres.sh    # PostgreSQL MCP entrypoint
│   └── entrypoint-mongo.sh       # MongoDB MCP entrypoint
└── postgres_ml/
    ├── pipeline.yaml             # ML classifier ingestion settings
    └── services.yaml             # Classifier table definitions
```

---

## 3. Source Configuration Overrides

Each source has per-profile override files in `config/sources/<name>/` that are deep-merged over the base profile configs. These are generated by `python sdb.py source add <name>` but can also be created manually.

### How it works

1. Base config is loaded from `config/{profile}/pipeline.yaml` (and other profile-level files like `cpu_classifiers.yaml`).
2. Source overrides from `config/sources/<name>/<profile>.yaml` are deep-merged on top.
3. Overrides are scoped by config filename — each top-level key corresponds to a config file (without the `.yaml` extension).
4. **List values fully replace the base config** (they are not merged).

### Example

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
| `config/sources/<name>/ml_cpu.yaml` | `config/ml_cpu/pipeline.yaml`, `cpu_classifiers.yaml` |
| `config/sources/<name>/ml.yaml` | `config/ml/pipeline.yaml`, `gpu_classifiers.yaml` |
| `config/sources/<name>/postgres.yaml` | `config/postgres/pipeline.yaml` |
| `config/sources/<name>/postgres_ml.yaml` | `config/postgres_ml/pipeline.yaml`, `services.yaml` |
| `config/sources/<name>/mongo.yaml` | `config/mongo/pipeline.yaml` |

---

## 4. Per-Profile Configuration Reference

### Parse Profile

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

See also: [Parse Profile guide](profiles/parse.md)

---

### ML CPU Profile

#### Pipeline: `config/ml_cpu/pipeline.yaml`

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

#### Classifiers: `config/ml_cpu/cpu_classifiers.yaml`

<details>
<summary><strong>Global settings</strong> (apply to all CPU classifiers)</summary>

| Setting | Description | Default |
|---------|-------------|---------|
| `text_columns` | Columns to classify per data type. | `submissions: [title, selftext]`, `comments: [body]` |
| `remove_strings` | Exact strings removed before classification. | `[deleted]`, `[removed]`, `[unavailable]` |
| `remove_patterns` | Regex patterns removed before classification. | URLs, `r/subreddit`, `u/user` |
| `fields` | Additional columns to keep in `lingua_ingest` CSV (beyond mandatory `id`, `dataset`, `retrieved_utc`). Empty means only mandatory fields. | `[]` |

</details>

<details>
<summary><strong>Lingua classifier settings</strong> (<code>lingua:</code> key)</summary>

| Setting | Description | Default |
|---------|-------------|---------|
| `suffix` | Output filename suffix (e.g., `RS_2024-01_lingua.csv`). | `_lingua` |
| `min_chars` | Minimum character count after text cleaning. Texts shorter than this are skipped. | `5` |
| `low_accuracy` | Faster but less accurate detection mode. | `false` |
| `workers` | Total parallel workers, divided among `file_workers`. | `16` |
| `file_workers` | Number of files processed in parallel. | `2` |
| `batch_size` | Rows per batch (larger = less overhead, more memory). | `2000000` |
| `languages` | List of Lingua enum names to detect. | 54 languages across 5 tiers (see below) |

</details>

**Language tiers:**

- **Tier 1 (Top 10 countries):** English, German, Spanish, Portuguese, French, Italian, Russian, Hindi, Tagalog, Turkish
- **Tier 2 (High volume):** Dutch, Swedish, Polish, Malay, Arabic, Vietnamese, Thai, Chinese, Japanese, Korean, Romanian, Greek, Czech, Danish, Finnish, Bokmal, Hebrew
- **Tier 3 (Moderate/Regional):** Ukrainian, Hungarian, Slovak, Croatian, Serbian, Bulgarian, Slovene, Lithuanian, Latvian, Estonian, Bosnian, Indonesian, Persian
- **Tier 4 (Indian Regional):** Bengali, Tamil, Telugu, Marathi, Gujarati, Punjabi, Urdu
- **Tier 5 (Low volume):** Commented out by default — uncomment in `cpu_classifiers.yaml` to enable (Afrikaans, Albanian, Armenian, Azerbaijani, Basque, Belarusian, Catalan, Esperanto, and others)

See also: [Classification Profile guide](profiles/classification.md)

---

### ML GPU Profile

#### Pipeline: `config/ml/pipeline.yaml`

```yaml
processing:
  data_types:
    - submissions
    - comments
  watch_interval: 0

gpu_classifiers:
  - toxic_roberta
  - go_emotions
```

| Setting | Description | Default |
|---------|-------------|---------|
| `data_types` | Data types to process. | `[submissions, comments]` |
| `watch_interval` | Poll interval in minutes (`0` = run once). | `0` |
| `gpu_classifiers` | List of GPU classifiers to run (references keys in `gpu_classifiers.yaml`). | `[toxic_roberta, go_emotions]` |

#### Classifiers: `config/ml/gpu_classifiers.yaml`

<details>
<summary><strong>Global settings</strong> (apply to all GPU classifiers unless overridden per-classifier)</summary>

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

</details>

<details>
<summary><strong>Per-classifier options</strong> (defined as top-level keys like <code>toxic_roberta:</code>)</summary>

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

</details>

**Bundled classifiers:**

| Classifier | Model | Type | Activation | Languages |
|-----------|-------|------|------------|-----------|
| `toxic_roberta` | `joaopn/unbiased-toxic-roberta-onnx-fp16` | `onnx_fp16` | `sigmoid` (multi-label) | English |
| `go_emotions` | `joaopn/roberta-base-go_emotions-onnx-fp16` | `onnx_fp16` | `sigmoid` (multi-label) | English |

Output columns are auto-derived from the model's `config.id2label` mapping.

See also: [Classification Profile guide](profiles/classification.md)

---

### PostgreSQL Profiles

#### Base Table Ingestion: `config/postgres/pipeline.yaml`

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
  prefer_lingua: true        # Ingest lingua-enriched files instead of originals

indexes: {}                  # Per-data-type index fields (set via source platform.yaml)
```

<details>
<summary><strong>Full options table</strong></summary>

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
| **processing.prefer_lingua** | Ingest lingua-enriched files (from `ml_cpu` output) instead of originals. Falls back to original if not found. | `true` |
| **indexes** | Index fields per data type (e.g., `{submissions: [dataset, author, subreddit]}`). | `{}` |
| **tablespaces** | Tablespace definitions: map of name to host path (e.g., `{nvme1: /mnt/nvme1/pg-tablespace}`). | `{}` |
| **table_tablespaces** | Table-to-tablespace assignments: map of data type to tablespace name (e.g., `{submissions: nvme1}`). Use `pgdata` for the default PostgreSQL data directory. | `{}` |

</details>

See [Tablespaces](profiles/database.md#tablespaces) for details on multi-disk setups.

#### ML Table Ingestion: `config/postgres_ml/pipeline.yaml`

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
  watch_interval: 0
```

| Setting | Description | Default |
|---------|-------------|---------|
| **processing.data_types** | Data types to ingest. Set via source config. | `[]` |
| **processing.check_duplicates** | Handle duplicate IDs during ingestion. | `true` |
| **processing.parallel_ingestion** | Ingest data types concurrently. | `true` |
| **processing.type_inference_rows** | Number of rows sampled to infer column types from CSV data. | `1000` |
| **processing.use_foreign_key** | Add foreign key constraint linking to the main table. Requires the main table to exist; set `false` for independent ingestion. | `true` |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |

> [!NOTE]
> `prefer_lingua` is read from the postgres profile (`config/postgres/pipeline.yaml`). When `true`, the lingua classifier is skipped during `postgres_ml` ingestion because lingua data is already in the main table. When `false`, lingua data is ingested from the `lingua_ingest` directory.

#### Classifier Table Definitions: `config/postgres_ml/services.yaml`

```yaml
classifiers:
  lingua:
    enabled: true
    source_dir: lingua
    source_dir_ingest: lingua_ingest  # Used when prefer_lingua: false
    suffix: "_lingua"

  toxic_roberta:
    enabled: true
    source_dir: toxic_roberta
    suffix: "_toxicity_en"

  go_emotions:
    enabled: true
    source_dir: go_emotions
    suffix: "_emotions_en"
```

| Option | Description |
|--------|-------------|
| `enabled` | Whether to process this classifier (`true`/`false`). |
| `source_dir` | Directory under `/data/output/` containing classifier output files. |
| `source_dir_ingest` | Alternative source directory (lingua only, used when `prefer_lingua: false`). |
| `suffix` | File suffix pattern and table name suffix (e.g., `_lingua` produces table `submissions_lingua`). |

Column types are auto-inferred from CSV data — no manual column definitions are needed.

See also: [Database Profile guide](profiles/database.md)

---

### PostgreSQL Server Tuning

The PostgreSQL container loads its configuration from `config/postgres/`:

| File | Purpose |
|------|---------|
| `postgresql.conf` | Server tuning parameters (shared_buffers, work_mem, etc.) |
| `pg_hba.conf` | Client authentication rules (default: trust/no-auth) |
| `postgresql.local.conf` | **Local override** — if present, replaces `postgresql.conf` |
| `pg_hba.local.conf` | **Local override** — if present, replaces `pg_hba.conf` (generated by `sdb db setup` when auth enabled) |

> [!TIP]
> Run `python sdb.py db setup`, which handles PGTune integration and ZFS optimization as part of the interactive setup.

**Manual approach:**

1. Go to [PGTune](https://pgtune.leopard.in.ua/) and generate settings with:
   - **DB Type:** Data Warehouse
   - **Data Storage:** SSD
   - Configure RAM, CPUs, and connections for your system
2. Append the generated settings to `config/postgres/postgresql.conf`, or create a `postgresql.local.conf` with the full configuration.

The local override files (`postgresql.local.conf`, `pg_hba.local.conf`) are not tracked by git, making them safe for machine-specific tuning.

---

### MongoDB Profile

#### Ingestion: `config/mongo/pipeline.yaml`

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
| **processing.cleanup_temp** | Delete extracted JSON files after ingestion. | `false` |
| **processing.watch_interval** | Poll for new files every N minutes (`0` = run once). | `0` |
| **mongo_indexes** | Index fields per data type (e.g., `{submissions: [id, author]}`). Set via source `platform.yaml`. | `{}` |

Platform-specific settings (`mongo_collection_strategy`, `mongo_db_name`/`mongo_db_name_template`, `mongo_collections`, `mongo_indexes`) are configured in the source's `platform.yaml`, not in the pipeline config. See [Platform Configuration](#5-platform-configuration).

#### Server: `config/mongo/mongod.conf`

Mounted into the mongo container. Key settings:
- `directoryPerDB: true` — each database in its own directory
- zstd compression for both journal and collection blocks
- `diagnosticDataCollectionEnabled: false`

Cache size is controlled via `MONGO_CACHE_SIZE_GB` env var (default: 2 GB), not in the conf file.

---

## 5. Platform Configuration

### Overview

Each source has a `platform.yaml` in `config/sources/<name>/` that defines the platform-specific settings. For built-in platforms (like Reddit), this is copied from a template in `config/templates/` during `sdb source add`. For custom platforms, it is generated interactively.

| Key | Description |
|-----|-------------|
| `db_schema` | Database schema name for this source. |
| `data_types` | List of data types this source supports. |
| `file_format` | Output format: `parquet` (default) or `csv` (alternate, for external tool compatibility). |
| `file_patterns` | File detection patterns per data type (keys: `dump`, `json`, `csv`, `parquet`, `prefix`, and optionally `dump_glob`, `compression`). |
| `indexes` | Default index fields per data type (used by the postgres profile). |
| `mongo_collection_strategy` | `per_file` or `per_data_type` (used by mongo_ingest). |
| `mongo_db_name` | Explicit MongoDB database name (custom sources). |
| `mongo_db_name_template` | Database name template with `{platform}` and `{data_type}` placeholders (built-in platforms). |
| `mongo_collections` | Per-data-type MongoDB collection names (custom sources). |
| `mongo_indexes` | Index fields per data type (used by mongo_ingest). |
| `field_types` | Type definitions for each field (integer, text, boolean, etc.). |
| `fields` | Fields to extract per data type. |
| `paths` | Optional per-source data paths (overrides global `DUMPS_PATH`, etc.). |

### Reddit Platform: `config/sources/reddit/platform.yaml`

For Reddit, the template `config/templates/reddit.yaml` is copied into `config/sources/reddit/platform.yaml` during `sdb source add reddit`, then customized interactively.

<details>
<summary><strong>Full config</strong></summary>

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

</details>

### Custom Platforms: `config/sources/<name>/platform.yaml`

Custom platform configs are generated interactively during `sdb source add <name>`. Users enter glob patterns for file matching (e.g., `tweets_*.json.gz`) which are converted to regex patterns with auto-detected compression.

<details>
<summary><strong>Example config</strong></summary>

```yaml
db_schema: my_data
file_format: parquet                       # 'parquet' (default) or 'csv'

data_types:
  - posts

paths:
  dumps: ./data/dumps/my_data
  extracted: ./data/extracted/my_data
  csv: ./data/csv/my_data
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

</details>

Run `python sdb.py source add <name>` to generate the config interactively.

See [Custom Platforms](platforms/custom.md) for a complete setup guide.
