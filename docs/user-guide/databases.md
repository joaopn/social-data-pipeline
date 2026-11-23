---
title: "Databases"
parent: "User guide"
nav_order: 8
---
# Databases

This page covers getting your data into PostgreSQL, StarRocks or MongoDB and querying it: choosing a database, starting it, the ingest profiles, indexes, server settings, authentication and reprocessing. You end up with one table (or collection) per data type, plus one table per classifier.

## Choosing a database

PostgreSQL and StarRocks ingest the same parsed files and support the same pipeline features (classifiers, lingua, MCP servers, authentication). MongoDB ingests the raw extracted records instead, with every original field.

| | PostgreSQL | StarRocks | MongoDB |
|---|---|---|---|
| **Best for** | General-purpose queries, joins with external data, extensions (PostGIS, pg_parquet, pgvector) | Large-scale analytical queries (aggregations, scans, GROUP BY over billions of rows) | Raw data inspection: the extracted JSON/NDJSON, CSV or Parquet records with all their fields, straight after decompression or download, without parsing |
| **Query speed** | Good with proper indexing; row-oriented storage | Much faster (~10X) for analytics; columnar storage with vectorized execution | Not compared |
| **Ecosystem** | Mature, widely supported, connects to nearly everything | MySQL wire protocol: works with any MySQL client, but a smaller ecosystem | `mongosh`, `mongoexport` and any MongoDB client |
| **Extensibility** | Rich extension system (custom types, FDWs, procedural languages) | Limited, focused on analytics | Not compared |
| **Storage model** | Row-oriented, B-tree indexes, tablespaces for multi-disk | Columnar, bitmap and bloom filter indexes, built-in multi-disk through `storage_root_path` | Documents, zstd compression, one collection per file or per data type |
| **Ingestion** | Fast initial load (deferred PK), ON CONFLICT upsert | Single code path: Primary Key tables handle dedup natively | `mongoimport` bulk insert (blind insert, no upsert) |

**Use PostgreSQL** if you need a general-purpose database that integrates with other tools and workflows. **Use StarRocks** if your main goal is fast analytical queries over large datasets. **Use MongoDB** to keep every original field of the raw records. They can be used in parallel.

Each database is a set of Docker Compose profiles:

| Database | Server profile | Ingest profiles |
|----------|----------------|-----------------|
| PostgreSQL | `postgres` (PostgreSQL 18, custom image `Dockerfile.postgres` with the `pg_parquet` extension for native Parquet ingestion) | `postgres_ingest` (main tables), `postgres_ml` (classifier outputs) |
| StarRocks | `starrocks` (official `starrocks/allin1-ubuntu` image, FE + BE in one container) | `sr_ingest` (main tables), `sr_ml` (classifier outputs) |
| MongoDB | `mongo` (MongoDB 8, official image) | `mongo_ingest` (bulk import) |

All databases support optional authentication and MCP servers for AI tool access (see [AI access](ai-access.md)).

## Starting and stopping

```bash
python sdp.py db start                  # Start configured database(s) + MCP servers
python sdp.py db start postgres         # Start one: postgres, mongo, starrocks, postgres-mcp, mongo-mcp, starrocks-mcp or jobs
python sdp.py db stop                   # Stop configured database(s) + MCP servers
python sdp.py db status                 # Show database config, health, MCP and jobs scheduler status
```

Database servers must be running before the ingest profiles can connect. `python sdp.py db start` starts every database configured by `python sdp.py db setup`. With auth enabled, it prompts for the admin password each time.

PostgreSQL and StarRocks read the parsed and output files themselves, through bind mounts. A source whose `paths.parsed` and `paths.output` lie under `PARSED_PATH` and `OUTPUT_PATH` (the default-disk case) is visible to a running database as soon as its folders appear, so `python sdp.py source add` needs no restart. A source whose paths lie elsewhere (multi-disk, for example `paths.parsed: /mnt/fast_ssd/parsed/<src>`) needs a restart: `python sdp.py source add` and `python sdp.py source remove` warn when it does, and `python sdp.py db stop <service> && python sdp.py db start <service>` applies it. `python sdp.py run *_ingest` and `*_ml` refuse at the CLI when a mount is missing. The mechanics are in [Databases and ingestion](../architecture/databases.md#how-the-servers-see-the-files).

## Ingest profiles

```bash
python sdp.py run postgres_ingest       # Ingest parsed files into PostgreSQL
python sdp.py run postgres_ml           # Ingest ML classifier outputs into PostgreSQL
python sdp.py run mongo_ingest          # Ingest raw data into MongoDB
python sdp.py run sr_ingest             # Ingest parsed files into StarRocks
python sdp.py run sr_ml                 # Ingest ML classifier outputs into StarRocks
python sdp.py source error-logs         # Show error details for failed datasets
```

| Profile | What it loads | From | Into |
|---------|---------------|------|------|
| `postgres_ingest` | Main tables (for example `submissions`, `comments`) | Parsed files (Parquet or CSV), or the lingua-enriched files when `prefer_lingua: true` | PostgreSQL `{schema}.{data_type}` |
| `postgres_ml` | One table per classifier and data type | Classifier output files | PostgreSQL `{schema}.{data_type}{suffix}` |
| `sr_ingest` | Main tables | Parsed files, or the lingua-enriched files when `prefer_lingua: true` | StarRocks `{database}.{data_type}` |
| `sr_ml` | One table per classifier and data type | Classifier output files | StarRocks `{database}.{data_type}{suffix}` |
| `mongo_ingest` | One collection per file or per data type | Raw extracted JSON/NDJSON, CSV or Parquet | MongoDB |

Run them in this order:

1. **`lingua` before `postgres_ingest` / `sr_ingest`**, when `prefer_lingua: true` (the default). These profiles then read only the lingua output folder; if lingua has not run, they find no files to load. With `prefer_lingua: false` they read the original parsed files, and the lingua columns go to a separate table through `postgres_ml` / `sr_ml`.
2. **`postgres_ml` after `postgres_ingest`**, and **`sr_ml` after `sr_ingest`**. `postgres_ml` adds a foreign key to the main table by default (`use_foreign_key: true`), which needs the main table to exist.

All ingest profiles accept `--filter` (`-f`) to restrict processing to files whose file id matches an fnmatch glob. The state-based skip still applies; `--filter` narrows the file set before the state check:

```bash
python sdp.py run postgres_ingest --filter "*2024*"   # Only ingest 2024 months
```

Every ingest profile tracks which files it has loaded, so a rerun continues where the last one stopped (see [Reprocessing](#reprocessing) to load files again).

## Checking for drift: `db verify`

`python sdp.py db verify` is the operator preflight. It checks every drift surface in one shot and exits non-zero on any finding. It covers auth coherence (`config/db/<db>.yaml` ↔ `.env` ↔ `.ro_credentials`), credential file mode and ownership, container env and health (when running), per-source mounts in `docker-compose.override.yml`, and MCP and jobs cross-references.

| Option | Output |
|--------|--------|
| (none) | A human-readable list of findings; each line names the recovery command |
| `--json` | Machine-readable for CI and scripts: the structure is stable and includes per-database `ok` flags and a typed `findings[]` array per check |
| `--db <name>` | Only one database; cross-cutting MCP and jobs findings still show, because they may name the filtered database |

As a pipeline preflight:

```bash
python sdp.py db verify && python sdp.py run postgres_ingest --source <name>
```

`python sdp.py db status` also prints a drift section with the same checks (without the container probes), but always exits 0, since it is an informational summary.

## Connecting and querying

| Database | Connect with |
|----------|--------------|
| PostgreSQL | `psql -h localhost -U postgres -d datasets` (port `POSTGRES_PORT`, default 5432) |
| StarRocks | Any MySQL client, for example `mysql -h 127.0.0.1 -P 9030 -u root --skip-password` (StarRocks uses the MySQL wire protocol) |
| MongoDB | `mongosh` on port `MONGO_PORT` (default 27017) |

The host folder `DB_EXPORT_PATH` (default `./data/export`) is bind-mounted at `/export` inside the PostgreSQL and MongoDB containers. Use this path in SQL queries, or with `mongoexport` or `mongosh`, to write results to the host:

```sql
COPY (SELECT * FROM reddit.submissions LIMIT 100) TO '/export/sample.csv' WITH CSV HEADER;
```

To give an AI agent access to the databases, see [AI access](ai-access.md).

## Indexes

Each ingest profile builds the indexes configured for its tables once loading is done. Index fields are set per source in `platform.yaml`:

| Key | Used by | Keyed by | Index type |
|-----|---------|----------|------------|
| `indexes` | `postgres_ingest` | Data type (Reddit: `[dataset, author, subreddit, domain]` for submissions) | B-tree |
| `ml_indexes` | `postgres_ml` | Classifier table name (data type + classifier suffix) | B-tree |
| `sr_indexes` | `sr_ingest` | Data type; falls back to `indexes`, read list-only (bitmap), since postgres_ingest shares it | Bitmap or bloom filter |
| `sr_ml_indexes` | `sr_ml` | Classifier table name | Bitmap or bloom filter |
| `mongo_indexes` | `mongo_ingest` | Data type | A single ascending index per field per collection |

```yaml
# config/sources/reddit/platform.yaml
ml_indexes:
  comments_lingua: [lang]
  comments_toxic_roberta: [toxic]
sr_ml_indexes:
  comments_lingua: [lang]                                  # plain list = bitmap
  comments_emotions_en: {bloomfilter: [author], bitmap: [subreddit]}
mongo_indexes:
  submissions: [id, author, subreddit, domain]
  comments: [id, author, subreddit, link_id]
```

- **Classifier tables have no fallback.** The base index lists name columns (`author`, `subreddit`, `domain`) that classifier tables do not have, so `ml_indexes` and `sr_ml_indexes` never fall back to `indexes` or `sr_indexes`. A classifier table with no entry gets no indexes.
- **Classifier tables are indexed only when they ingested at least one file in the current run** (`postgres_ml`, `sr_ml`). A plain re-run skips files already processed and so indexes nothing.
- **A failed classifier-table index build logs a warning and the run continues** (`postgres_ml`, `sr_ml`). For `postgres_ml`, an unknown tablespace does not abort ingestion.
- **PostgreSQL**: `parallel_index_workers` sets `max_parallel_maintenance_workers` per index build; each classifier-table index is placed in the same tablespace as its table (the data type's `table_tablespaces` entry).
- **StarRocks**: tables are indexed in parallel with each other and fields within a table one at a time; `sr_ml` caps this at 8 tables at once. See [Databases and ingestion](../architecture/databases.md#index-builds) for how builds are polled.

To add indexes after ingestion, run `python sdp.py db create-indexes`. This interactive command connects to the running databases, shows existing tables (or, for MongoDB, collections found through metadata), columns and indexes, and lets you choose which fields to index; for MongoDB it creates the index across all collections of a data type. It can save the new indexes to `platform.yaml`: base tables to `indexes` / `sr_indexes`, classifier tables to `ml_indexes` / `sr_ml_indexes`, MongoDB to `mongo_indexes` (MongoDB has no ML ingest profile, so there is no separate classifier set). The owning profile then rebuilds them on the next run.

### Choosing a StarRocks index type

`sr_indexes` and `sr_ml_indexes` accept either a plain list, which always means bitmap, or a per-type mapping:

```yaml
sr_indexes:
  comments: {bitmap: [subreddit], bloomfilter: [author]}
  submissions: [dataset, author]      # plain list = bitmap
```

| | Bitmap | Bloom filter |
|---|---|---|
| Predicates | `=`, `IN`, `>`, `>=`, `<`, `<=`, `IS NULL` | `=`, `IN` only |
| Accuracy | Exact | May false-positive (never false-negative) |
| Mechanism | `CREATE INDEX … USING BITMAP` | `bloom_filter_columns` table property |
| Column types | Not `FLOAT` / `DOUBLE` | Not `FLOAT` / `DOUBLE` / `TINYINT` / `BOOLEAN` / `DECIMAL` / `HLL` |
| Cost | Larger; built per column | Smaller |

Neither speeds up `LIKE`; that needs an N-gram bloom filter, which this pipeline does not manage.

Bitmap indexes are adaptive: StarRocks skips one at query time when it cannot filter out roughly 999/1000 of rows, so a column with very few distinct values gets the build cost without the benefit. Reference points from a real 3.1B-row classifier table: `dataset` has 255 distinct values (1 in 255, below the useful threshold), `subreddit` 23.8M, `author` 164M. High-cardinality columns queried only with `=` / `IN` are the bloom filter's case; columns that need range or null predicates are bitmap's.

`python sdp.py db create-indexes` defaults every StarRocks index it creates to a bloom filter and asks once, at the end, which should be bitmap instead. Column types are checked first: `FLOAT` and `DOUBLE` support neither mechanism and are dropped from the plan with a message (this covers every classifier score column, including `lang_prob`), while `TINYINT` / `BOOLEAN` / `DECIMAL` support only bitmap and are switched automatically.

> Whether adding a bloom filter column rebuilds existing data or applies only to newly written data is not documented upstream and has not been verified here. Treat bloom filters as reliable for data written after the change; if you need coverage of existing rows and cannot confirm the behaviour, use a bitmap index.

## StarRocks settings

These are set by `python sdp.py db setup`:

| Setting | Config key | Default |
|---------|------------|---------|
| MySQL protocol port | `port` | `9030` |
| FE HTTP port | `fe_http_port` | `8030` |
| FE JVM heap (GB) | `fe_jvm_heap` | `max(2, RAM/8)` |
| BE memory limit (GB) | `be_mem_limit` | `~50% RAM` |
| Table compression | `compression` | `ZSTD` |
| Extra storage disks | `storage_paths` | `[]` (primary only) |

| File | What it holds | Written by |
|------|---------------|------------|
| `config/db/starrocks.yaml` | Port, memory settings, compression, storage paths | `python sdp.py db setup` |
| `config/starrocks/fe.conf` | FE network, metadata, JVM heap: the template for `fe.local.conf` | Shipped; setup does not change it |
| `config/starrocks/be.conf` | BE network, storage paths, memory limit: the template for `be.local.conf` | Shipped; setup does not change it |
| `config/starrocks/fe.local.conf` | Copy of `fe.conf` with `jvm_heap_size` set | `python sdp.py db setup` |
| `config/starrocks/be.local.conf` | Copy of `be.conf` with `mem_limit`, `alter_tablet_worker_count` and, with extra disks, `storage_root_path` set | `python sdp.py db setup` |

Only the `.local.conf` files are mounted into the container. An edit to `fe.conf` or `be.conf` therefore takes effect only after you re-run `python sdp.py db setup` (and restart StarRocks).

### Memory

The all-in-one container runs both the FE (query planner, metadata) and the BE (storage, query execution). Setup splits the memory explicitly:

| Part | What it is |
|------|------------|
| FE JVM heap | Java heap for query planning and metadata; 2-8 GB is typical |
| BE memory limit | Native memory for the storage engine, block cache and query execution; the main tuning knob |
| Container memory limit | Docker `mem_limit` (`STARROCKS_MEM_LIMIT`); should be `fe_heap + be_mem + 2GB` headroom |

### Multi-disk storage

The primary data path (`STARROCKS_DATA_PATH`, set during setup) is always used as BE tablet storage. Extra disks can be added on top of it, and the BE spreads tablets across all configured paths:

```
Host                             Container                               Role
$STARROCKS_DATA_PATH/be/storage  →  /data/deploy/starrocks/be/storage    primary
/mnt/nvme0/starrocks             →  /data/deploy/starrocks/be/storage_0  extra 1
/mnt/nvme1/starrocks             →  /data/deploy/starrocks/be/storage_1  extra 2
```

The primary mount is declared in `docker-compose.yml`; extra disks are added to `docker-compose.override.yml` and listed in `storage_root_path` in `config/starrocks/be.local.conf`, with the primary path always first.

### Bucketing (tablet count per table)

StarRocks splits each table into tablets (one bucket is one tablet with `replication_num=1`). Tablets are the physical storage unit: they govern compaction granularity, disk distribution and scan-task scheduling. Query parallelism is set separately by the BE's `pipeline_dop` and is not capped by the bucket count: when there are fewer tablets than `pipeline_dop`, StarRocks splits tablets into finer work units (morsel-driven execution) to keep all compute threads busy.

The bucket count is set per source during `python sdp.py source add` and stored in `config/sources/<name>/platform.yaml`:

```yaml
# Uniform — all tables in this source get 256 buckets (the common case)
sr_buckets: 256

# Per-data-type override (hand-edit only; not prompted)
sr_buckets:
  submissions: 128
  comments: 512
```

The setup default is `logical_cpu_count × 8`, which targets roughly 1–10 GB per tablet for datasets of 100 GB – 10 TB.

| Source size | Buckets |
|-------------|---------|
| Under 10 GB | Drop to `logical_cpus` or `logical_cpus × 2`, to avoid wastefully small tablets (under 128 MB) |
| 100 GB – 10 TB | The default is fine |
| Over 10 TB | Raise proportionally. Target 1–10 GB per tablet; up to ~100 GB is tolerable on append-only workloads (base compaction is slow on large tablets) |

Tables created before an `sr_buckets` change keep their original bucket count: StarRocks does not reshard unpartitioned tables in place.

### Table compression

A single global compression codec is chosen during `python sdp.py db setup` and stored as `compression` in `config/db/starrocks.yaml`. It applies to every StarRocks table (base and classifier) as a `PROPERTIES("compression" = "<codec>")` clause at `CREATE TABLE` time. The default is `ZSTD`, which makes text-heavy columns roughly 33% smaller than StarRocks' own `LZ4` default while keeping scans fast. The prompt offers `ZSTD` and `LZ4`; `ZLIB` and `SNAPPY` are also accepted if set by hand.

The setting only affects newly created tables. Existing tables keep their codec until dropped and recreated, or altered explicitly:

```sql
ALTER TABLE reddit.comments SET ("compression" = "ZSTD");
```

To override it for one source, add it to `config/sources/<name>/starrocks.yaml` (base tables) or `sr_ml.yaml` (classifier tables) under the `pipeline` key; the per-source value wins over the global default:

```yaml
pipeline:
  database:
    compression: LZ4
```

When no codec is configured at all (for example installs that predate this setting), the property is omitted and StarRocks falls back to `LZ4`.

### Startup time

The container health check verifies MySQL connectivity (and, under auth with a read-only user, that this user can log in). StarRocks takes 30-60 s to start fully (FE boot plus BE registration), so the health check waits `start_period: 90s` before counting failures.

## PostgreSQL settings

| File | Purpose |
|------|---------|
| `config/postgres/postgresql.conf` | Server tuning (memory, parallelism, WAL) |
| `config/postgres/pg_hba.conf` | Authentication rules (default: trust, no auth) |
| `config/postgres/postgresql.local.conf` | Local tuning override, generated by `python sdp.py db setup`; if present, replaces `postgresql.conf` |
| `config/postgres/pg_hba.local.conf` | Local auth override, generated by `python sdp.py db setup` when auth is enabled; if present, replaces `pg_hba.conf` |
| `config/postgres/initdb.d/` | First-init scripts (create the `pg_parquet` extension and the read-only user) |

The Docker entrypoint loads the local override files when they exist. They are not tracked by git, so they are safe for machine-specific tuning.

### Server tuning

The default configuration is tuned for 60 GB RAM, 24 CPUs and SSD storage. Run `python sdp.py db setup` to generate a `postgresql.local.conf` with PGTune integration and optional ZFS optimization.

To tune by hand instead:

1. Go to [PGTune](https://pgtune.leopard.in.ua/) and generate settings with **DB Type:** Data Warehouse and **Data Storage:** SSD, and your system's RAM, CPUs and connections.
2. Append the generated settings to `config/postgres/postgresql.conf`, or create `config/postgres/postgresql.local.conf` with the full configuration.

### Tablespaces

PostgreSQL tablespaces spread tables across several physical disks. You need them when the database exceeds the capacity of one drive (full Reddit data approaches 8 TB and more).

**1. Run the setup.** Run `python sdp.py db setup` and answer yes to the tablespace question in the PostgreSQL section. The script asks you to define tablespaces (a name and a host path for each disk) and to assign each data type to a tablespace. It writes the tablespace config to `config/db/postgres.yaml` and generates `docker-compose.override.yml` with the volume mounts. `pgdata` is always available as a tablespace and means the default PostgreSQL data directory (no extra mount needed).

**2. Check the result.** The tablespace settings live in `config/postgres/pipeline.yaml` or the source overrides:

```yaml
# config/sources/reddit/postgres.yaml
pipeline:
  tablespaces:
    nvme1: /mnt/nvme1/pg-tablespace    # name: host_path
    nvme2: /mnt/nvme2/pg-tablespace
  table_tablespaces:
    submissions: nvme1                   # data_type: tablespace_name
    comments: nvme2                      # "pgdata" = default PG data directory
```

The matching `docker-compose.override.yml`, generated by `python sdp.py db setup`:

```yaml
services:
  postgres:
    volumes:
      - /mnt/nvme1/pg-tablespace:/data/tablespace/nvme1
      - /mnt/nvme2/pg-tablespace:/data/tablespace/nvme2
```

Classifier tables from `postgres_ml` go to the tablespace of their parent data type. How the pipeline creates and uses the tablespaces is in [Databases and ingestion](../architecture/databases.md#tablespaces).

PostgreSQL tables are not partitioned. Reddit-wide queries that are not bounded to a few months would hit 200+ partitions, splitting indexes and hurting performance. If your workload is dominated by analytical scans over the full dataset, StarRocks is the better fit.

## MongoDB settings

| File | Purpose |
|------|---------|
| `config/mongo/mongod.conf` | Server configuration (storage, compression, networking) |

`mongod.conf` sets:

- **`directoryPerDB: true`**: each database is stored in its own directory.
- **zstd compression**: both the journal and the collection block compressor use zstd.
- **Diagnostics disabled** (`diagnosticDataCollectionEnabled: false`): reduces disk noise.

The WiredTiger cache size is set by the `MONGO_CACHE_SIZE_GB` environment variable (default 2 GB), passed as a command-line flag in docker-compose, not in the conf file.

### Collections and database names

Set per source in `platform.yaml` with `mongo_collection_strategy`:

| Strategy | Description | Use case |
|----------|-------------|----------|
| `per_file` | Each input file becomes its own collection | Reddit: monthly dumps (`RS_2024-01` → collection `2024-01` in database `reddit_submissions`) |
| `per_data_type` | All files of a type merge into one collection | Twitter: many small files → a single `tweets` collection |

Database names are set in the source's `platform.yaml` in one of two ways:

| Source | Keys | Example |
|--------|------|---------|
| Built-in platforms | `mongo_db_name_template` with `{platform}` and `{data_type}` placeholders | `reddit_submissions`, `reddit_comments` |
| Custom sources | Explicit `mongo_db_name` plus a per-data-type `mongo_collections` mapping | `mongo_db_name: mydata` with `mongo_collections: {posts: posts, users: users}` |

### What mongo_ingest reads

`mongo_ingest` works on the raw data right after decompression or download, with no parse step. It reads JSON/NDJSON, CSV and Parquet, detecting the format from the file extension. For sources with `input_format: parquet` (for example Hugging Face datasets) it ingests the parquet files in `data/extracted/<data_type>/` directly, keeping all original columns. This is useful for inspecting the raw data in MongoDB while PostgreSQL gets a parsed field subset. If a Parquet conversion fails, the error names the location of its temp file, for manual cleanup.

## Ingest profile settings

### postgres_ingest

**Base config:** `config/postgres/pipeline.yaml`. **Source overrides:** `config/sources/<name>/postgres.yaml`.

| Option | Description | Default |
|--------|-------------|---------|
| `database.host` | PostgreSQL hostname | `postgres` |
| `database.port` | PostgreSQL port (override: `POSTGRES_PORT`) | `5432` |
| `database.name` | Database name (override: `DB_NAME`) | `datasets` |
| `database.schema` | Schema name (set via the source's `platform.yaml`) | `null` |
| `database.user` | Database user | `postgres` |
| `processing.data_types` | Data types to ingest | `[]` (from source config) |
| `processing.parallel_mode` | Extract, parse and ingest in parallel phases | `false` |
| `processing.parallel_ingestion` | Ingest data types concurrently | `true` |
| `processing.parse_workers` | CSV parsing workers | `12` |
| `processing.check_duplicates` | Handle duplicate IDs | `true` |
| `processing.create_indexes` | Create indexes after ingestion | `true` |
| `processing.parallel_index_workers` | Workers per index build | `8` |
| `processing.cleanup_temp` | Delete intermediate files | `false` |
| `processing.watch_interval` | Poll for new files (0 = once) | `0` |
| `processing.prefer_lingua` | Ingest only the lingua-enriched files (strict either/or, no fallback) | `true` |
| `indexes` | Index fields per data type | `{}` (from platform) |
| `tablespaces` | Tablespace definitions (name → host path) | `{}` |
| `table_tablespaces` | Data type → tablespace assignments | `{}` |

### postgres_ml

**Config file:** `config/postgres_ml/pipeline.yaml`. **Source overrides:** `config/sources/<name>/postgres_ml.yaml`.

| Option | Description | Default |
|--------|-------------|---------|
| `processing.data_types` | Data types to ingest | `[]` (from source config) |
| `processing.check_duplicates` | Handle duplicate IDs | `true` |
| `processing.parallel_ingestion` | Ingest data types concurrently | `true` |
| `processing.type_inference_rows` | Rows to sample for column type inference | `1000` |
| `processing.use_foreign_key` | Add an FK constraint to the main table | `true` |
| `processing.create_indexes` | Create indexes on classifier tables after ingestion | `true` |
| `processing.parallel_index_workers` | Maintenance workers per index build | `8` |
| `ml_indexes` | Index fields per classifier table; no fallback to `indexes` | `{}` (from platform config) |
| `processing.watch_interval` | Poll for new files (0 = once) | `0` |

Which classifiers are ingested, and each one's `suffix` and `data_types` scope, come from the source's ml and lingua profile config. `config/postgres_ml/services.yaml` and `config/sources/<name>/postgres_ml.yaml` hold only ingestion-only overrides:

| Option | Description |
|--------|-------------|
| `enabled` | Skip ingestion of a classifier without removing it from the ml profile. Default `true`. |
| `source_dir` | Directory under `OUTPUT_PATH/` (default: the classifier name) |
| `source_dir_ingest` | Lingua only: the directory used when `prefer_lingua: false`. Defaults to `lingua_ingest`. |
| `column_overrides` | Force specific column types instead of inferring them from the data |

`suffix` is not an ingestion override: it lives in the ml/lingua profile, so the file naming is the same for the run and the ingest.

### sr_ingest

**Base config:** `config/sr/pipeline.yaml`. **Source override:** `config/sources/<name>/starrocks.yaml`.

| Setting | Config key | Default | Notes |
|---------|------------|---------|-------|
| Host | `database.host` | `starrocks` | Docker service name |
| Port | `database.port` | `9030` | MySQL protocol port |
| User | `database.user` | `root` | |
| Data types | `processing.data_types` | `[]` | Falls back to platform config |
| Check duplicates | `processing.check_duplicates` | `true` | Use `merge_condition` for conditional upsert |
| Create indexes | `processing.create_indexes` | `true` | Create indexes after ingestion |
| Index poll interval | `processing.index_poll_interval` | `10` | Seconds between alter-job status polls during an index build |
| Prefer lingua | `processing.prefer_lingua` | `true` | Ingest only the lingua-enriched files (includes the lang columns); strict either/or, no fallback |
| Watch interval | `processing.watch_interval` | `0` | Minutes between checks (0 = run once) |
| Index fields | `sr_indexes` | `{}` | Per data type. Plain list = bitmap, or `{bitmap: [...], bloomfilter: [...]}`. Falls back to `indexes`, which is read list-only (bitmap) since postgres_ingest shares it |

### sr_ml

**Base config:** `config/sr_ml/pipeline.yaml` and `services.yaml`. **Source override:** `config/sources/<name>/sr_ml.yaml`.

| Setting | Config key | Default | Notes |
|---------|------------|---------|-------|
| Host | `database.host` | `starrocks` | Docker service name |
| Port | `database.port` | `9030` | MySQL protocol port |
| User | `database.user` | `root` | |
| Data types | `processing.data_types` | `[]` | Falls back to platform config |
| Check duplicates | `processing.check_duplicates` | `true` | Use `merge_condition` for conditional upsert |
| Parallel ingestion | `processing.parallel_ingestion` | `true` | Process data types concurrently |
| Type inference rows | `processing.type_inference_rows` | `1000` | CSV rows to sample for type inference |
| Create indexes | `processing.create_indexes` | `true` | Create indexes on classifier tables after ingestion |
| Index poll interval | `processing.index_poll_interval` | `10` | Seconds between alter-job status polls |
| Index fields | `sr_ml_indexes` | `{}` | Per classifier table. Plain list = bitmap, or `{bitmap: [...], bloomfilter: [...]}`. No fallback to `sr_indexes` / `indexes` |
| Watch interval | `processing.watch_interval` | `0` | Minutes between checks (0 = run once) |

`config/sr_ml/services.yaml` takes the same ingestion-only overrides as `postgres_ml` (above).

### mongo_ingest

**Base config:** `config/mongo/pipeline.yaml`. **Source overrides:** `config/sources/<name>/mongo.yaml`.

| Option | Description | Default |
|--------|-------------|---------|
| `database.host` | MongoDB hostname | `mongo` |
| `database.port` | MongoDB port (override: `MONGO_PORT`) | `27017` |
| `processing.data_types` | Data types to process | `[]` (from source config) |
| `processing.num_insertion_workers` | `mongoimport --numInsertionWorkers` | `4` |
| `processing.create_indexes` | Create indexes after ingestion | `true` |
| `processing.cleanup_temp` | Delete extracted JSON after ingestion | `false` |
| `processing.watch_interval` | Poll for new files (0 = once) | `0` |

## Authentication

Database authentication is optional and can be enabled during `python sdp.py db setup` (or from the web UI's Databases page, which asks the same questions for a database not set up yet: see [Setting up a database](webui.md#setting-up-a-database)). When it is enabled, it adds password-based access control to PostgreSQL, MongoDB and StarRocks. When you add a database with `python sdp.py db setup --add`, auth is extended to it automatically if it is already enabled on the existing databases.

### Users

When auth is enabled, two database users are created per database:

| Database | Admin user | RO user | Password storage |
|----------|------------|---------|------------------|
| PostgreSQL | `postgres` | `readonly` | Admin: runtime prompt. RO: `.ro_credentials` |
| MongoDB | `admin` | `readonly` | Admin: runtime prompt. RO: `.ro_credentials` |
| StarRocks | `root` | `readonly` | Admin: runtime prompt. RO: `.ro_credentials` |

The admin password is never stored on disk: it is prompted each time `python sdp.py db start` runs with auth enabled. If you forget it, `python sdp.py db recover-password` resets it. The read-only user's password is generated during `python sdp.py db setup` and stored in `.ro_credentials` (chmod 600) in each database's data volume. How each database applies these is in [Credentials and auth](../architecture/credentials.md).

### Recovering credentials

| Scenario | Command |
|----------|---------|
| Lost admin password | `python sdp.py db recover-password` |
| Lost or known-leaked RO password | `python sdp.py db recover-password --regenerate-ro` (also resets the admin password) |

`recover-password --regenerate-ro` writes a fresh `.ro_credentials` for every database with auth and an RO user, and the database applies the new password on the restart that follows.

Re-running `python sdp.py db setup` on an existing install keeps the existing RO password by default. An older default (always generate a new one) silently rotated it on every re-run and broke any client with cached credentials.

### Wiping data without losing config

`python sdp.py db reset --db <postgres|mongo|starrocks>` wipes one database's data and keeps every config surface: `config/db/<db>.yaml`, `config/sources/*/<db>.yaml`, `.env`, `docker-compose.override.yml`, `config/db/mcp.yaml` and `.ro_credentials`.

Reset stops the database and its MCP server and deletes:

- only the server's own subfolders under `<DB>_DATA_PATH` (`pgdata/`, `db/`, `fe/` + `be/`),
- the host-side `state_tracking/` and MongoDB's `logs/`,
- the contents of PostgreSQL tablespaces or StarRocks `storage_paths`.

Afterwards, `python sdp.py db start <db>` brings the database back up with the kept `.ro_credentials` (the read-only user is re-created from it), ready for `python sdp.py run <db>_ingest`. Reset refuses to run while an ingest container is running against the target database.

| To | Use |
|----|-----|
| Re-ingest from scratch | `python sdp.py db reset --db <db>` |
| Remove the database entirely, config included | `python sdp.py db unsetup --db <db>` |
| Re-run setup in place without losing data | `python sdp.py db setup --add <db>` |

## Reprocessing

Each ingest profile records the files it has loaded in a state file per data type, so a rerun skips them. To load data again, drop the table and remove the matching state entries. The state files are named after the source:

| Profile | State file | One entry per |
|---------|------------|---------------|
| `postgres_ingest` | `$PGDATA_PATH/state_tracking/{SOURCE}_postgres_ingest_{data_type}.json` | Ingested file (for example `RS_2024-01`) |
| `postgres_ml` | `$PGDATA_PATH/state_tracking/{SOURCE}_postgres_ml_{data_type}.json` | Classifier file, as `<classifier>/<file stem>`; one file shared by all classifiers of that data type |
| `sr_ingest` | `$STARROCKS_DATA_PATH/state_tracking/{SOURCE}_sr_ingest_{data_type}.json` | Ingested file |
| `sr_ml` | `$STARROCKS_DATA_PATH/state_tracking/{SOURCE}_sr_ml_{data_type}.json` | Classifier file, as `<classifier>/<file stem>` |
| `mongo_ingest` | `$MONGO_DATA_PATH/state_tracking/{SOURCE}_mongo_ingest_{data_type}.json` | Ingested file |

**PostgreSQL:**

```bash
# Reprocess a data type (postgres_ingest): drop the table, delete its state file
psql -h localhost -U postgres -d datasets -c "DROP TABLE reddit.comments;"
rm data/database/postgres/state_tracking/reddit_postgres_ingest_comments.json

# Reprocess one classifier table (postgres_ml): drop the table, then remove the
# classifier's "toxic_roberta/..." entries from the shared state file
psql -h localhost -U postgres -d datasets -c "DROP TABLE reddit.comments_toxicity_en;"
# edit data/database/postgres/state_tracking/reddit_postgres_ml_comments.json
```

**StarRocks:**

```bash
# Reprocess a data type (sr_ingest)
mysql -h 127.0.0.1 -P 9030 -u root -e "DROP TABLE reddit.comments;"
rm data/database/starrocks/state_tracking/reddit_sr_ingest_comments.json

# Reprocess one classifier table (sr_ml): drop the table, then remove the
# classifier's "toxic_roberta/..." entries from the shared state file
mysql -h 127.0.0.1 -P 9030 -u root -e "DROP TABLE reddit.comments_toxicity_en;"
# edit data/database/starrocks/state_tracking/reddit_sr_ml_comments.json
```

For a classifier table you can instead delete the whole `*_ml_{data_type}.json` file; then every classifier of that data type is ingested again on the next run.

Indexes are rebuilt with the table: the next run recreates them from `indexes` / `sr_indexes` for base tables and `ml_indexes` / `sr_ml_indexes` for classifier tables. This is also how to apply an index that was configured after everything had been ingested, since a plain re-run skips files already processed and therefore indexes nothing. (`python sdp.py db create-indexes` adds an index without reloading.)

## Database schema

| Database | Namespacing | Main tables | Classifier tables |
|----------|-------------|-------------|-------------------|
| PostgreSQL | One schema per source, set in `config/sources/<name>/platform.yaml` (`db_schema`): `reddit` for Reddit, user-defined for custom sources | `{schema}.{data_type}`, for example `reddit.submissions`, `reddit.comments` | `{schema}.{data_type}{suffix}`, for example `reddit.submissions_lingua`, `reddit.comments_toxicity_en` |
| StarRocks | One database per source, named after the source (StarRocks has no schemas) | `{database}.{data_type}`, for example `reddit.submissions` | `{database}.{data_type}{suffix}`, for example `reddit.comments_toxicity_en` |
| MongoDB | One database per data type or explicit names (see [Collections and database names](#collections-and-database-names)) | One collection per file or per data type | None (MongoDB has no ML ingest profile) |

### Primary keys

PostgreSQL and StarRocks use the platform's `primary_key` column as the table's primary key. The Reddit template sets `primary_key: id`; custom platforms set it interactively in `python sdp.py source add` (default: left blank).

| `primary_key` | PostgreSQL | StarRocks |
|---------------|------------|-----------|
| Set | `PRIMARY KEY (pk)`, added after the bulk load | Primary Key table model, `DISTRIBUTED BY HASH(pk)`, `enable_persistent_index = true` |
| Blank (common for HF datasets without a natural unique field) | No PK constraint | Duplicate Key model with `DISTRIBUTED BY RANDOM` |

Without a primary key, re-runs append rows: there is no source-level dedup and no `merge_condition` upsert.

## MCP servers

The read-only MCP servers that give AI agents access to these databases, their setup with `python sdp.py db setup-mcp` and their ports are on [AI access](ai-access.md).
