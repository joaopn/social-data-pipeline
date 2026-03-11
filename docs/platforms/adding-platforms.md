# Adding New Platforms

Social Data Bridge supports adding new platforms through configuration files and an optional custom parser.

## Steps

### 1. Create Platform Configuration

Create `config/platforms/{platform}/` with these files:

#### platform.yaml

```yaml
db_schema: my_platform
data_types:
  - posts
  - comments
file_patterns:
  posts:
    zst: '^posts_(\d{4}-\d{2})\.zst$'
    json: '^posts_(\d{4}-\d{2})$'
    csv: '^posts_(\d{4}-\d{2})\.csv$'
    prefix: 'posts_'
  comments:
    zst: '^comments_(\d{4}-\d{2})\.zst$'
    json: '^comments_(\d{4}-\d{2})$'
    csv: '^comments_(\d{4}-\d{2})\.csv$'
    prefix: 'comments_'
indexes:
  posts: [author, created_at]
  comments: [author, post_id, created_at]
```

#### field_list.yaml

```yaml
posts:
  - created_at
  - author
  - title
  - content
  - score

comments:
  - created_at
  - author
  - post_id
  - parent_id
  - content
  - score
```

#### field_types.yaml

```yaml
created_at: integer
author: text
title: text
content: text
score: integer
post_id: text
parent_id: text
```

### 2. Create Parser (Optional)

If your platform needs custom parsing logic (computed fields, format handling, etc.), create a parser module:

`social_data_bridge/platforms/{platform}/parser.py`

Required functions:

```python
def transform_json(data, dataset, data_type_config, fields_to_extract):
    """Transform a single JSON record into a list of CSV values."""
    ...

def process_single_file(input_file, output_file, data_type, data_type_config, fields_to_extract):
    """Process a single JSON file to CSV. Returns (input_size, output_file)."""
    ...

def parse_to_csv(input_file, output_dir, data_type, config_dir, use_type_subdir=True):
    """Main entry point. Parse a JSON file to CSV. Returns output CSV path."""
    ...

def parse_files_parallel(files, output_dir, config_dir, workers):
    """Parse multiple files in parallel. Returns list of (csv_path, data_type)."""
    ...
```

If no custom logic is needed, you can reuse the generic parser.

### 3. Register the Platform

Update `social_data_bridge/orchestrators/parse.py` to handle your platform in the `get_platform_parser()` function:

```python
def get_platform_parser(platform):
    if platform == 'reddit':
        from ..platforms.reddit import parser
        return parser
    elif platform == 'my_platform':
        from ..platforms.my_platform import parser
        return parser
    else:
        from ..platforms.generic import parser
        return parser
```

### 4. Run

```bash
python sdb.py run parse
```

> **Note:** The platform is configured during `python sdb.py setup`. Select your custom platform during setup.

## Using the Generic Parser

If your platform doesn't need custom logic, you can skip step 2 and let it fall through to the generic parser. Just create the config files and set `PLATFORM=generic` or register your platform to use the generic parser module.
