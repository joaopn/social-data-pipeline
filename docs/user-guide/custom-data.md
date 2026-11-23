---
title: "Your own data and Hugging Face"
parent: "User guide"
nav_order: 4
---
# Your own data and Hugging Face

This page shows how to add any record-based JSON, CSV or Parquet data, or a Hugging Face dataset, as a source. You end up with a `config/sources/<name>/platform.yaml` that tells the pipeline which files to read, which fields to keep and how to type them; from there, parse, the classifiers and the databases work as for Reddit.

Every source whose name is not `reddit` is a custom platform (`PLATFORM=custom/<name>`): the name you give `python sdp.py source add` decides it, and there is no platform question. Custom platforms parse to Parquet or CSV with no platform-specific logic.

## Your own files

**1. Put your files in place.** Place compressed or decompressed files in `data/dumps/<source>/<data_type>/` or `data/extracted/<source>/<data_type>/`, one subfolder per data type:

```
data/dumps/mydata/
├── posts/
│   ├── data_2024-01.json.gz      # JSON/NDJSON input
│   └── data_2024-02.json.gz
└── users/
    └── users_export.csv.zst      # CSV input (with headers)
```

| Input format | What it needs |
|--------------|---------------|
| JSON / NDJSON | One object per line |
| CSV | A header row |
| Parquet | Nothing extra (for example from Hugging Face downloads or other sources) |

Supported compression: `.zst`, `.gz`, `.json.gz`, `.xz`, `.tar.gz`. For Hugging Face datasets, skip this step and use [Hugging Face datasets](#hugging-face-datasets) below.

**2. Configure the source.** Run the interactive setup with any name other than `reddit`:

```bash
python sdp.py source add mydata
```

The same questions are asked in the browser by the web UI's **Add source** (Reddit dumps and your own files; see [Adding a source](webui.md#adding-a-source)); both write the same files. A Hugging Face dataset is added in a terminal.

The setup walks you through:

| Question | What it sets |
|----------|--------------|
| **Input format** | NDJSON (default), CSV (with a configurable delimiter), or Parquet (set automatically with `--hf`) |
| **Output format** | Parquet (default, recommended) or CSV |
| **Data types** | Your data categories (for example posts, users). With `--hf`, HF configs are grouped into data types interactively. |
| **File patterns** | Glob patterns (for example `data_*.json.gz`, `export_*.csv.zst`) for file detection, converted to regex with the compression detected from the pattern. Generated automatically for HF sources. |
| **Fields** | Which fields to extract: dot-notation for NDJSON, column headers for CSV and Parquet. With `--hf`, pre-filled from the HF schema. |
| **Field types** | The PostgreSQL column type of each field. With `--hf`, mapped from the HF feature types. |
| **Indexes** | Index fields for PostgreSQL and MongoDB ingestion |

This writes `config/sources/mydata/platform.yaml` and the per-profile override files.

**3. Run.** Parse, then classify or ingest as for any source:

```bash
python sdp.py run parse --source mydata
python sdp.py run lingua --source mydata      # classification works the same way
python sdp.py run ml --source mydata
```

When only one source is configured, `--source` is selected automatically and can be omitted.

## Writing platform.yaml by hand

To create the config without the interactive setup, write `config/sources/<name>/platform.yaml` yourself:

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

Every key is described in [Configuration reference](configuration.md#platform-configuration).

## What custom platforms do

| Feature | Detail |
|---------|--------|
| JSON, CSV and Parquet input | NDJSON (one JSON object per line), CSV with headers, or Parquet (for example from HF downloads) |
| Hugging Face integration | `--hf` fetches dataset metadata to pre-fill setup; `source download` fetches the parquet files |
| Robust CSV handling | Powered by Polars: handles ragged rows, encoding issues, mixed quoting, and configurable delimiters (comma, tab, pipe) |
| Nested fields | Dot-notation access to nested JSON, for example `user.profile.name` (NDJSON input) |
| Array indexing | Access array elements, for example `items.0.id` (NDJSON input) |
| Type enforcement | Field types defined in the YAML are enforced during parsing |
| No platform-specific logic | Pure data transformation, no assumptions about content |
| Self-contained config | One file per source, no merging |

## Field types

| Type | Description | Example |
|------|-------------|---------|
| `integer` | Integer values | `42` |
| `bigint` | Large integer values | `1234567890123` |
| `float` | Floating point numbers | `3.14` |
| `boolean` | True/False values | `true` |
| `text` | Variable-length strings | `"hello world"` |
| `['char', N]` | Fixed-length string | `['char', 2]` |
| `['varchar', N]` | Variable-length string up to N chars | `['varchar', 10]` |

## Hugging Face datasets

Datasets hosted on [Hugging Face](https://huggingface.co/datasets) can be added as custom sources, with the setup pre-filled from the HF API.

**1. Add the source.** Pass the dataset id with `--hf`. The setup fetches the dataset's configs, fields and types, groups configs with identical fields, and asks you to name each group as a data type and pick its fields.

```bash
python sdp.py source add idrama --hf iDRAMALab/iDRAMA-scored-2024
```

**2. Download the files.** This mirrors the dataset into `data/dumps/<source>/` and organizes the files by data type into `data/extracted/<source>/<data_type>/`. A re-run skips files already downloaded.

```bash
python sdp.py source download idrama
python sdp.py source download idrama --data-type comments    # organize only one data type
python sdp.py source download idrama --token <token>         # private dataset (or set HF_TOKEN)
```

**3. Parse and ingest.**

```bash
python sdp.py run parse --source idrama
```

PostgreSQL and StarRocks get the configured field subset through parse. MongoDB can ingest the raw parquet straight from `data/extracted/` with `python sdp.py run mongo_ingest`, keeping every original column. How the metadata fetch, the type mapping and the two download phases work is in [Platforms](../architecture/platforms.md#hugging-face-datasets).

## Limitations

| Limitation | Detail |
|------------|--------|
| No computed fields | Unlike Reddit's `id10`, `is_deleted` and `removal_type` |
| No automatic file detection | File patterns must be configured: glob patterns are converted to regex during setup, and generated automatically for HF sources |
| CSV needs headers | The header must be the first row; headerless CSV is not supported |
| Nested access is NDJSON only | Dot-notation and array indexing work only for NDJSON input; CSV and Parquet fields are flat column names |
