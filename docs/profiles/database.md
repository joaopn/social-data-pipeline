# Database Profiles

Social Data Bridge supports two database destinations:
- **PostgreSQL** — ingests parsed CSVs via three profiles: `postgres` (server), `postgres_ingest` (main tables), `postgres_ml` (classifier outputs)
- **MongoDB** — ingests raw JSON/NDJSON directly after extraction via two profiles: `mongo` (server), `mongo_ingest` (bulk import)

## Running

```bash
python sdb.py start                  # Start configured database(s)
python sdb.py run postgres_ingest    # Ingest parsed CSVs into PostgreSQL
python sdb.py run postgres_ml        # Ingest ML classifier outputs into PostgreSQL
python sdb.py run mongo_ingest       # Ingest raw JSON into MongoDB
python sdb.py stop                   # Stop configured database(s)
```

> [!IMPORTANT]
> Database servers must be running before ingestion profiles can connect. `sdb.py start` starts all databases selected during setup. Use `sdb.py start postgres` or `sdb.py start mongo` to start a specific one.

---

## postgres Profile (Server)

Runs a PostgreSQL 18 server using the official Docker image.

### Configuration Files

| File | Purpose |
|------|---------|
| `config/postgres/postgresql.conf` | Server tuning (memory, parallelism, WAL) |
| `config/postgres/pg_hba.conf` | Authentication rules |
| `postgresql.local.conf` | Local overrides (not tracked in git) |
| `pg_hba.local.conf` | Local auth overrides (not tracked in git) |

The Docker entrypoint automatically loads local override files if present.

### Tuning Recommendations

Use [PGTune](https://pgtune.leopard.in.ua/) to generate optimal settings:
- **DB Type**: Data Warehouse
- **Data Storage**: SSD

Append the output to `config/postgres/postgresql.conf`.

> [!TIP]
> The default configuration is tuned for 60GB RAM, 24 CPUs, SSD storage. Run `python sdb.py setup` to generate a `postgresql.local.conf` with PGTune integration and optional ZFS optimization.

---

## postgres_ingest Profile

Ingests parsed CSV files into PostgreSQL main tables (e.g., `submissions`, `comments`).

### How It Works

The postgres_ingest profile is a full pipeline that can also extract and parse if needed:

1. **Input Detection**: Detects .zst, JSON, and CSV files (same as parse profile)
2. **Extraction/Parsing**: Extracts and parses files that haven't been processed yet
3. **Ingestion**: Loads CSV data into PostgreSQL tables
4. **Indexing**: Creates indexes on configured fields
5. **Analyze**: Updates table statistics

### prefer_lingua Behavior

When `prefer_lingua: true` (default), postgres_ingest looks for Lingua-enriched CSVs in `OUTPUT_PATH/lingua/` instead of the original CSVs in `CSV_PATH/`. This means:
- The main table includes language columns (`lang`, `lang_prob`, `lang2`, `lang2_prob`) directly
- No separate lingua table is needed
- Falls back to original CSVs if Lingua output is not found

When `prefer_lingua: false`:
- Ingests original CSVs (without language columns) into the main table
- Lingua data is ingested separately via `postgres_ml` from the `lingua_ingest` directory

### Standard Ingestion (ON CONFLICT)

When the table already exists:
- Uses COPY with ON CONFLICT for duplicate handling
- Deduplicates by `(dataset, id)` composite key
- `check_duplicates: true` enables ON CONFLICT DO UPDATE

### Fast Initial Load (New Tables)

When a table does not exist yet, the pipeline automatically uses an optimized bulk ingestion strategy:

1. **CREATE TABLE (no PK)** — Table created without primary key to avoid per-row index maintenance
2. **Blind COPY** — All CSV files loaded without duplicate checking
3. **Deduplication** — In-place `ROW_NUMBER()` window function removes duplicates by `id`, keeping the row with the latest `retrieved_utc`
4. **PRIMARY KEY** — Adds primary key constraint in a single index build after all data is loaded

> [!NOTE]
> Faster than ON CONFLICT for initial loads with billions of rows. The speedup comes from deferring index maintenance until after all data is loaded. Once the table exists, subsequent ingestions always use ON CONFLICT.

### Indexing

After ingestion, indexes are created on configured fields:
- Index fields come from the platform's `platform.yaml` (e.g., Reddit: `[dataset, author, subreddit, domain]` for submissions)
- `parallel_index_workers` controls `max_parallel_maintenance_workers` per index build
- Indexes are B-tree by default

### Configuration

**Config file:** `config/postgres/pipeline.yaml`

<details>
<summary><strong>Full options table</strong></summary>

| Option | Description | Default |
|--------|-------------|---------|
| `database.host` | PostgreSQL hostname | `postgres` |
| `database.port` | PostgreSQL port (override: `POSTGRES_PORT`) | `5432` |
| `database.name` | Database name (override: `DB_NAME`) | `datasets` |
| `database.schema` | Schema name (set via platform config) | `null` |
| `database.user` | Database user | `postgres` |
| `processing.data_types` | Data types to ingest | `[]` (from platform) |
| `processing.parallel_mode` | Extract/parse/ingest in parallel phases | `false` |
| `processing.parallel_ingestion` | Ingest data types concurrently | `true` |
| `processing.parse_workers` | CSV parsing workers | `12` |
| `processing.check_duplicates` | Handle duplicate IDs | `true` |
| `processing.create_indexes` | Create indexes after ingestion | `true` |
| `processing.parallel_index_workers` | Workers per index build | `8` |
| `processing.cleanup_temp` | Delete intermediate files | `false` |
| `processing.watch_interval` | Poll for new files (0 = once) | `0` |
| `processing.prefer_lingua` | Use Lingua CSVs as input | `true` |
| `indexes` | Index fields per data type | `{}` (from platform) |
| `tablespaces` | Tablespace definitions (name → host path) | `{}` |
| `table_tablespaces` | Data type → tablespace assignments | `{}` |

</details>

---

## Tablespaces

PostgreSQL tablespaces allow spreading tables across multiple physical disks. This is necessary when the database exceeds the capacity of a single drive (e.g., full Reddit data approaches 8TB+).

### Setup

Run `python sdb.py setup` and answer "yes" to the tablespace question in the PostgreSQL section. The script will:

1. Ask you to define tablespaces (name + host path for each disk)
2. Ask you to assign each data type to a tablespace
3. Generate the tablespace config in `config/postgres/user.yaml`
4. Generate `docker-compose.override.yml` with the required volume mounts

`pgdata` is always available as a tablespace option and refers to the default PostgreSQL data directory (no extra volume mount needed).

### Configuration

Tablespace settings live in `config/postgres/pipeline.yaml` (or `user.yaml` override):

```yaml
# config/postgres/user.yaml
pipeline:
  tablespaces:
    nvme1: /mnt/nvme1/pg-tablespace    # name: host_path
    nvme2: /mnt/nvme2/pg-tablespace
  table_tablespaces:
    submissions: nvme1                   # data_type: tablespace_name
    comments: nvme2                      # "pgdata" = default PG data directory
```

The corresponding `docker-compose.override.yml` is auto-generated by `sdb.py setup`:

```yaml
services:
  postgres:
    volumes:
      - /mnt/nvme1/pg-tablespace:/data/tablespace/nvme1
      - /mnt/nvme2/pg-tablespace:/data/tablespace/nvme2
```

### How It Works

- On startup, the ingestion pipeline creates any missing tablespaces via `CREATE TABLESPACE`
- `CREATE TABLE` and `CREATE INDEX` statements include `TABLESPACE <name>` for non-pgdata assignments
- Data types assigned to `pgdata` use the default PostgreSQL data directory (no TABLESPACE clause)
- Classifier tables (from `postgres_ml`) inherit the tablespace of their parent data type
- Container paths follow the convention `/data/tablespace/<name>` (deterministic from tablespace name)

---

## postgres_ml Profile

Ingests ML classifier outputs into separate PostgreSQL tables.

### How It Works

1. **Service Discovery**: Reads `config/postgres_ml/services.yaml` for enabled classifiers
2. **File Detection**: Finds classifier output CSVs in `OUTPUT_PATH/{source_dir}/{data_type}/`
3. **Type Inference**: Samples `type_inference_rows` rows to auto-detect column types
4. **Table Creation**: Creates tables named `{data_type}{suffix}` (e.g., `submissions_toxicity_en`)
5. **Ingestion**: Loads CSV data with duplicate handling
6. **Foreign Keys**: Optionally adds FK constraint to main table via `(dataset, id)`

### prefer_lingua Interaction

- When `prefer_lingua: true` in the postgres profile: lingua data is already in the main table, so `postgres_ml` skips the lingua classifier
- When `prefer_lingua: false`: `postgres_ml` ingests lingua data from the `lingua_ingest` directory

### Fast Initial Load (New Tables)

Same algorithm as postgres_ingest, with an additional step:

1. CREATE TABLE (no PK, no FK)
2. Blind COPY of all classifier CSV files
3. Deduplication
4. Add PRIMARY KEY
5. **Add FOREIGN KEY** (validates all IDs exist in the main table)

Once the table exists, subsequent ingestions use ON CONFLICT.

### Classifier Table Definitions

**Config file:** `config/postgres_ml/services.yaml`

Each classifier entry has:

| Option | Description |
|--------|-------------|
| `enabled` | Whether to process this classifier |
| `source_dir` | Directory under `OUTPUT_PATH/` |
| `source_dir_ingest` | Alternative directory (for lingua when prefer_lingua: false) |
| `suffix` | File and table name suffix |

### Configuration

**Config file:** `config/postgres_ml/pipeline.yaml`

| Option | Description | Default |
|--------|-------------|---------|
| `processing.data_types` | Data types to ingest | `[]` (from platform) |
| `processing.check_duplicates` | Handle duplicate IDs | `true` |
| `processing.parallel_ingestion` | Ingest data types concurrently | `true` |
| `processing.type_inference_rows` | Rows to sample for column type inference | `1000` |
| `processing.use_foreign_key` | Add FK constraint to main table | `true` |
| `processing.watch_interval` | Poll for new files (0 = once) | `0` |

---

## Database Schema

The schema name is set per-platform in `platform.yaml`:
- Reddit: `reddit` schema
- Generic: user-defined in `user.yaml`

### Table Naming

| Table Type | Name Pattern | Example |
|------------|-------------|---------|
| Main tables | `{data_type}` | `reddit.submissions`, `reddit.comments` |
| Classifier tables | `{data_type}{suffix}` | `reddit.submissions_lingua`, `reddit.comments_toxicity_en` |

### Primary Keys

All tables use composite primary key: `(dataset, id)`

---

## Storage Requirements

Estimates for full Reddit data dumps:

| Component | Sequential Mode | Parallel Mode |
|-----------|-----------------|---------------|
| Intermediate files | ~4TB | ~51TB |
| With ZFS/BTRFS compression | ~4TB | ~9TB |
| PostgreSQL database | ~10TB (uncompressed) | ~6TB (LZ4) |

**Pipeline modes:**
- **Sequential** (`parallel_mode: false`): Process one file at a time, lower disk usage
- **Parallel** (`parallel_mode: true`): Extract all → Parse all → Ingest all, much faster but needs more intermediate storage

---

## Resume Behavior

### State Tracking

**PostgreSQL**: Each data type has a JSON state file in `$PGDATA_PATH/state_tracking/`:
- `{PLATFORM}_postgres_ingest_{data_type}.json` — tracks ingested datasets
- `{PLATFORM}_postgres_ml_{classifier}_{data_type}.json` — tracks ingested classifier files

**MongoDB**: State files in `$MONGO_DATA_PATH/state_tracking/`:
- `{PLATFORM}_mongo_ingest_{data_type}.json` — tracks ingested files

Additionally, MongoDB stores ingested file IDs in a `_sdb_metadata` collection per database, enabling state recovery even if state files are lost.

### Database Recovery

- **PostgreSQL**: On first run, if no state file exists, `postgres_ingest` queries the database for unique datasets already present.
- **MongoDB**: On first run, if no state file exists, `mongo_ingest` queries the `_sdb_metadata` collection to rebuild the list of already-ingested files.

### Reprocessing

```bash
# Reprocess a specific data type (postgres_ingest)
# 1. Drop the table
psql -h localhost -U postgres -d datasets -c "DROP TABLE reddit.comments;"
# 2. Delete the state file
rm data/database/state_tracking/reddit_postgres_ingest_comments.json

# Reprocess a classifier table (postgres_ml)
psql -h localhost -U postgres -d datasets -c "DROP TABLE reddit.comments_toxicity_en;"
rm data/database/state_tracking/reddit_postgres_ml_toxic_roberta_comments.json
```

---

## mongo Profile (Server)

Runs a MongoDB 8 server using the official Docker image.

### Configuration Files

| File | Purpose |
|------|---------|
| `config/mongo/mongod.conf` | Server configuration (storage, compression, networking) |

### Server Settings

The `mongod.conf` configures:
- **`directoryPerDB: true`** — each database stored in its own directory
- **zstd compression** — both journal and collection block compressor set to zstd
- **Diagnostics disabled** — reduces disk noise

WiredTiger cache size is controlled via the `MONGO_CACHE_SIZE_GB` environment variable (default: 2 GB), passed as a CLI flag in docker-compose.

---

## mongo_ingest Profile

Ingests raw extracted JSON/NDJSON files directly into MongoDB using `mongoimport`. Operates on raw data right after decompression — no CSV parsing needed.

### How It Works

1. **Extract**: Detects `.zst` dump files, decompresses via `zstd`
2. **Ingest**: For each extracted JSON file:
   - Creates a collection with zstd WiredTiger compression
   - Runs `mongoimport` for bulk insertion (blind insert, no upsert)
   - Records the file in the `_sdb_metadata` collection for state tracking
3. **Index**: Creates configured indexes on each collection

### Collection Strategies

Set per-platform in `platform.yaml` via `mongo_collection_strategy`:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `per_file` | Each input file becomes its own collection | Reddit: monthly dumps (`RS_2024-01` → collection `2024-01` in db `reddit_submissions`) |
| `per_data_type` | All files of a type merge into one collection | Twitter: many small files → single `tweets` collection |

### Database Naming

Controlled by `mongo_db_name_template` in `platform.yaml`:
- Default: `{platform}_{data_type}` (e.g., `reddit_submissions`, `reddit_comments`)
- `{platform}` and `{data_type}` are available placeholders

### Configuration

**Config file:** `config/mongo/pipeline.yaml`

| Option | Description | Default |
|--------|-------------|---------|
| `database.host` | MongoDB hostname | `mongo` |
| `database.port` | MongoDB port (override: `MONGO_PORT`) | `27017` |
| `processing.data_types` | Data types to process | `[]` (from platform) |
| `processing.num_insertion_workers` | `mongoimport --numInsertionWorkers` | `4` |
| `processing.create_indexes` | Create indexes after ingestion | `true` |
| `processing.cleanup_temp` | Delete extracted JSON after ingestion | `false` |
| `processing.watch_interval` | Poll for new files (0 = once) | `0` |

### Indexes

Index fields are configured per-platform in `platform.yaml` under `mongo_indexes`:

```yaml
# config/platforms/reddit/platform.yaml
mongo_indexes:
  submissions: [id, author, subreddit, domain]
  comments: [id, author, subreddit, link_id]
```

Each field gets a single ascending index per collection.
