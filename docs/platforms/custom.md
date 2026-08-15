# Custom Platforms

Custom platforms (`PLATFORM=custom/<name>`) provide JSON, CSV, and Parquet parsing to structured files (Parquet or CSV) for arbitrary data sources, without platform-specific logic. Each custom source gets a `platform.yaml` in `config/sources/<name>/`. Hugging Face datasets are supported natively via `--hf`.

---

## Setup Guide

### 1. Prepare Your Data

Place your compressed or decompressed data files in `data/dumps/<source>/{data_type}/` or `data/extracted/<source>/{data_type}/`:

```
data/dumps/mydata/
├── posts/
│   ├── data_2024-01.json.gz      # JSON/NDJSON input
│   └── data_2024-02.json.gz
└── users/
    └── users_export.csv.zst      # CSV input (with headers)
```

Supported input formats: JSON/NDJSON (one object per line), CSV (with header row), and Parquet (from HF downloads or other sources).
Supported compression: `.zst`, `.gz`, `.json.gz`, `.xz`, `.tar.gz`.

> [!TIP]
> For Hugging Face datasets, skip manual data preparation — use `--hf` during setup and `source download` to fetch parquet files automatically. See [Hugging Face Datasets](#hugging-face-datasets) below.

### 2. Configure Source

Run the interactive setup:

```bash
python sdp.py source add mydata
# Or with HF metadata as defaults:
python sdp.py source add mydata --hf user/dataset-name
```

Select `custom` as the platform type. The setup will walk you through:
- **Input format** — NDJSON (default), CSV (with configurable delimiter), or Parquet (auto-set when using `--hf`)
- **Output format** — Parquet (default, recommended) or CSV
- **Data types** — define your data categories (e.g., posts, users). With `--hf`, HF configs are grouped into data types interactively.
- **File patterns** — enter glob patterns (e.g., `data_*.json.gz`, `export_*.csv.zst`) for automatic file detection. Auto-generated for HF sources.
- **Fields** — configure which fields to extract (dot-notation for NDJSON, column headers for CSV/Parquet). With `--hf`, pre-populated from HF schema.
- **Field types** — set PostgreSQL column types for each field. With `--hf`, auto-mapped from HF feature types.
- **Indexes** — choose index fields for PostgreSQL and MongoDB ingestion

This generates `config/sources/mydata/platform.yaml` and per-profile override files.

To manually create the config instead, create `config/sources/<name>/platform.yaml`:

```yaml
db_schema: my_data
file_format: parquet                 # Output format: 'parquet' (default) or 'csv'
# input_format: csv                 # Input format: 'ndjson' (default), 'csv', or 'parquet'
# input_csv_delimiter: ","          # CSV delimiter (default: comma). Supports tab, pipe, etc.
# hf_dataset: user/dataset-name     # HF dataset ID (set by --hf, used by source download)
# hf_config_map:                    # HF config → data_type mapping (set by --hf)
#   posts: [config1, config2]
data_types:
  - posts
  - users
paths:
  dumps: ./data/dumps/mydata
  extracted: ./data/extracted/mydata
  parsed: ./data/parsed/mydata
  output: ./data/output/mydata
file_patterns:
  posts:
    dump: '^data_.*\.json\.gz$'
    dump_glob: '*.json.gz'
    json: '^data_.*$'
    csv: '^data_.*\.csv$'
    parquet: '^data_.*\.parquet$'
    prefix: 'data_'
    compression: gz
  users:
    dump: '^users_.*\.json\.gz$'
    dump_glob: '*.json.gz'
    json: '^users_.*$'
    csv: '^users_.*\.csv$'
    parquet: '^users_.*\.parquet$'
    prefix: 'users_'
    compression: gz
mongo_collection_strategy: per_data_type
mongo_db_name: mydata
mongo_collections:
  posts: posts
  users: users
indexes:
  posts:
    - dataset
    - author
ml_indexes:            # Classifier tables: data type + classifier suffix
  posts_lingua:
    - lang
field_types:
  id: text
  created_at: integer
  author: text
  content: text
  likes: integer
  username: text
  email: text
fields:
  posts:
    - id
    - created_at
    - author
    - content
    - likes
  users:
    - id
    - username
    - email
    - profile.bio        # Nested field access with dot notation
```

### 3. Run

```bash
python sdp.py run parse --source mydata

# Classification works the same way
python sdp.py run lingua --source mydata
python sdp.py run ml --source mydata
```

> [!TIP]
> When only one source is configured, `--source` is auto-selected and can be omitted.

---

## Features

- **JSON, CSV, and Parquet input**: Accepts NDJSON (one JSON object per line), CSV files with headers, or Parquet files (e.g., from HF downloads)
- **Hugging Face integration**: `--hf` flag fetches dataset metadata to pre-populate setup; `source download` fetches parquet files
- **Robust CSV handling**: Powered by Polars — handles ragged rows, encoding issues, mixed quoting, and configurable delimiters (comma, tab, pipe)
- **Dot-notation nested field access**: Access nested JSON with `user.profile.name` (NDJSON input)
- **Array indexing**: Access array elements with `items.0.id` (NDJSON input)
- **Type enforcement**: Field types defined in YAML are enforced during parsing
- **No platform-specific logic**: Pure data transformation, no assumptions about content
- **Self-contained config**: One file per platform, no merging

## Supported Field Types

| Type | Description | Example |
|------|-------------|---------|
| `integer` | Integer values | `42` |
| `bigint` | Large integer values | `1234567890123` |
| `float` | Floating point numbers | `3.14` |
| `boolean` | True/False values | `true` |
| `text` | Variable-length strings | `"hello world"` |
| `['char', N]` | Fixed-length string | `['char', 2]` |
| `['varchar', N]` | Variable-length string up to N chars | `['varchar', 10]` |

## Hugging Face Datasets

Datasets hosted on [Hugging Face](https://huggingface.co/datasets) can be added as custom sources with metadata auto-populated from the HF API:

```bash
python sdp.py source add idrama --hf iDRAMALab/iDRAMA-scored-2024
python sdp.py source download idrama
python sdp.py run parse --source idrama
```

### How `--hf` works

1. **Fetches metadata** from the HF API: dataset configs, field names, and feature types
2. **Groups HF configs by schema** — configs with identical field sets are grouped together
3. **Interactive data type assignment** — you name each group (e.g., `comments`, `submissions`) and select which fields to include
4. **Type auto-mapping** — HF feature types are mapped to SDP SQL types (`string`→`text`, `int64`→`bigint`, `float`→`float`, `bool`→`boolean`). Unmappable types (sequences, structs) can be skipped, cast to text, or given a custom type.
5. **Generates platform.yaml** with `input_format: parquet`, `hf_dataset`, `hf_config_map`, and auto-generated file patterns

The `source download` command uses the HF Hub parquet API in two phases:
1. **Download**: mirrors the HF repo 1-to-1 into `data/dumps/<source>/<config>/<split>/<index>.parquet`
2. **Organize**: copies files into `data/extracted/<source>/<data_type>/` using the config-to-data-type mapping from setup

It supports:
- **Resume**: skips files where local size matches the remote `Content-Length`
- **Atomic writes**: downloads to `.partial` suffix, renames on completion
- **Selective download**: `--data-type` to organize only one data type
- **Private datasets**: `--token` or `HF_TOKEN` env var

### Pipeline flow

```
HF Hub → source download → data/dumps/<source>/<config>/<split>/*.parquet (1-to-1 mirror)
                                    ↓ (organize)
                           data/extracted/<source>/<data_type>/*.parquet (grouped, all fields)
                                    ↓
                              run parse → data/parsed/<source>/<data_type>/*.parquet (field subset, typed)
                                    ↓
                        run postgres_ingest → PostgreSQL (field subset)
                        run sr_ingest      → StarRocks  (field subset)

data/extracted/ → run mongo_ingest → MongoDB (all fields, raw parquet → NDJSON → mongoimport)
```

The dumps folder preserves the original HF repo structure for inspection and re-organization. PostgreSQL and StarRocks get the configured field subset (via parse). MongoDB can ingest raw parquet directly from `data/extracted/`, preserving all original HF columns.

## Limitations

- No computed fields (unlike Reddit's `id10`, `is_deleted`, `removal_type`)
- No automatic file detection — file patterns must be configured (glob patterns are converted to regex during setup, auto-generated for HF sources)
- CSV input requires headers in the first row (headerless CSV is not supported)
- Dot-notation and array indexing are only available for NDJSON input (CSV and Parquet fields are flat column names)
