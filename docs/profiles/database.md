# Database Profiles

Three profiles work together for PostgreSQL ingestion: `postgres` runs the database server, `postgres_ingest` loads parsed CSVs into main tables, and `postgres_ml` loads classifier outputs into separate tables.

## Running

```bash
# Start PostgreSQL server
docker compose --profile postgres up -d

# Ingest parsed CSVs into main tables
docker compose --profile postgres_ingest up

# Ingest ML classifier outputs
docker compose --profile postgres_ml up

# Stop PostgreSQL server
docker compose --profile postgres down
```

**Note:** The `postgres` profile must be running before `postgres_ingest` or `postgres_ml` can connect.

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

The default configuration is tuned for:
- 60GB RAM, 24 CPUs, SSD storage
- `shared_buffers`: 16GB
- `effective_cache_size`: 100GB
- `max_parallel_workers`: 24
- `synchronous_commit`: off (faster writes, safe with `fsync: on`)
- Autovacuum: disabled (manual VACUUM expected after bulk loads)

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

**Performance**: Faster than ON CONFLICT for initial loads with billions of rows. The speedup comes from deferring index maintenance until after all data is loaded.

Once the table exists, subsequent ingestions always use ON CONFLICT.

### Indexing

After ingestion, indexes are created on configured fields:
- Index fields come from the platform's `platform.yaml` (e.g., Reddit: `[dataset, author, subreddit, domain, created_utc]` for submissions)
- `parallel_index_workers` controls `max_parallel_maintenance_workers` per index build
- Indexes are B-tree by default

### Configuration

**Config file:** `config/postgres/pipeline.yaml`

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

---

## Tablespaces

PostgreSQL tablespaces allow spreading tables across multiple physical disks. This is necessary when the database exceeds the capacity of a single drive (e.g., full Reddit data approaches 8TB+).

### Setup

Run `python setup.py` and answer "yes" to the tablespace question in the PostgreSQL section. The script will:

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

The corresponding `docker-compose.override.yml` is auto-generated by `setup.py`:

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

Each data type has a JSON state file in `$PGDATA_PATH/state_tracking/`:
- `{PLATFORM}_postgres_ingest_{data_type}.json` — tracks ingested datasets
- `{PLATFORM}_postgres_ml_{classifier}_{data_type}.json` — tracks ingested classifier files

### Database Recovery

On first run, if no state file exists, `postgres_ingest` queries the database for unique datasets already present. This allows recovery after state file loss.

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
