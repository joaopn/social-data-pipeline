# Generic Platform

The generic platform (`PLATFORM=generic`) provides simple JSON-to-CSV conversion for arbitrary data sources, without platform-specific logic.

## Setup Guide

### 1. Prepare Your Data

Place your decompressed NDJSON files in `data/extracted/{data_type}/`:

```
data/extracted/
├── posts/
│   ├── data_2024-01
│   └── data_2024-02
└── users/
    └── users_export
```

> **Note:** The generic platform currently requires pre-extracted files placed directly in `data/extracted/`. Automatic `.zst` decompression uses platform-specific filename patterns.

### 2. Configure Field List

Create `config/platforms/generic/field_list.yaml` (see `field_list.yaml.example`):

```yaml
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

### 3. Configure Field Types

Update `config/platforms/generic/field_types.yaml`:

```yaml
id: text
created_at: integer
author: text
content: text
likes: integer
username: text
email: text
```

### 4. Configure Platform

Create or edit `config/platforms/generic/user.yaml`:

```yaml
platform:
  db_schema: my_data
  data_types:
    - posts
    - users
  file_patterns:
    posts:
      zst: '^posts_(\d{4}-\d{2})\.zst$'
      json: '^posts_(\d{4}-\d{2})$'
      csv: '^posts_(\d{4}-\d{2})\.csv$'
      prefix: 'posts_'
    users:
      zst: '^users_.*\.zst$'
      json: '^users_.*$'
      csv: '^users_.*\.csv$'
      prefix: 'users_'
```

### 5. Update Pipeline Config

Edit `config/parse/user.yaml`:

```yaml
pipeline:
  processing:
    data_types:
      - posts
      - users
```

### 6. Run

```bash
python sdb.py run parse

# Classification works the same way
python sdb.py run ml_cpu
python sdb.py run ml
```

> **Note:** The platform is configured during `python sdb.py setup`. If you need to use the generic platform, select it during setup.

## Features

- **Dot-notation nested field access**: Access nested JSON with `user.profile.name`
- **Array indexing**: Access array elements with `items.0.id`
- **Type enforcement**: Field types defined in YAML are enforced during parsing
- **No platform-specific logic**: Pure JSON-to-CSV conversion

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

- Requires pre-extracted files in `data/extracted/` (no automatic `.zst` decompression from `data/dumps/`)
- No automatic file detection — file patterns must be configured in `platform.yaml`
- No computed fields (unlike Reddit's `id10`, `is_deleted`, `removal_type`)
