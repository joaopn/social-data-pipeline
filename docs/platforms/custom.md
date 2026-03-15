# Custom Platforms

Custom platforms (`PLATFORM=custom/<name>`) provide simple JSON parsing to structured files (Parquet or CSV) for arbitrary data sources, without platform-specific logic. Each custom source gets a `platform.yaml` in `config/sources/<name>/`.

---

## Setup Guide

### 1. Prepare Your Data

Place your compressed or decompressed data files in `data/dumps/<source>/{data_type}/` or `data/extracted/<source>/{data_type}/`:

```
data/dumps/mydata/
├── posts/
│   ├── data_2024-01.json.gz
│   └── data_2024-02.json.gz
└── users/
    └── users_export.json.gz
```

Supported compressed formats: `.zst`, `.gz`, `.json.gz`, `.xz`, `.tar.gz`.

### 2. Configure Source

Run the interactive setup:

```bash
python sdb.py source add mydata
```

Select `custom` as the platform type. The setup will walk you through:
- **File format** — Parquet (default, recommended) or CSV
- **Data types** — define your data categories (e.g., posts, users)
- **File patterns** — enter glob patterns (e.g., `data_*.json.gz`) for automatic file detection and compression auto-detection
- **Fields** — configure which JSON fields to extract, with support for dot-notation nested access
- **Field types** — set PostgreSQL column types for each field
- **Indexes** — choose index fields for PostgreSQL and MongoDB ingestion

This generates `config/sources/mydata/platform.yaml` and per-profile override files.

To manually create the config instead, create `config/sources/<name>/platform.yaml`:

```yaml
db_schema: my_data
file_format: parquet                 # 'parquet' (default) or 'csv'
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
python sdb.py run parse --source mydata

# Classification works the same way
python sdb.py run lingua --source mydata
python sdb.py run ml --source mydata
```

> [!TIP]
> When only one source is configured, `--source` is auto-selected and can be omitted.

---

## Features

- **Dot-notation nested field access**: Access nested JSON with `user.profile.name`
- **Array indexing**: Access array elements with `items.0.id`
- **Type enforcement**: Field types defined in YAML are enforced during parsing
- **No platform-specific logic**: Pure JSON-to-CSV conversion
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

## Limitations

- No computed fields (unlike Reddit's `id10`, `is_deleted`, `removal_type`)
- No automatic file detection — file patterns must be configured (glob patterns are converted to regex during setup)
