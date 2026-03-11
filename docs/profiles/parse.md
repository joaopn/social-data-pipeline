# Parse Profile

## Overview
The parse profile decompresses `.zst` compressed data dumps and parses JSON to CSV files. It's the entry point of the pipeline - all other profiles depend on its output.

## Running

```bash
python sdb.py run parse
```

> **Note:** The platform (Reddit or generic) is configured during `python sdb.py setup`.

## How It Works

Three-phase pipeline:

### Phase 1: Input Detection
- Scans `DUMPS_PATH` for `.zst` files matching platform file patterns
- Scans `EXTRACTED_PATH` for already-decompressed JSON files
- Scans `CSV_PATH` for already-parsed CSV files
- Supports root directory and per-data-type subdirectories
- Reddit: detects `RS_YYYY-MM.zst` (submissions) and `RC_YYYY-MM.zst` (comments), including torrent directory structure (`submissions/RS_*.zst`, `comments/RC_*.zst`)

### Phase 2: Extraction
- Decompresses `.zst` files using `zstd --long=31` (supports 2GB window)
- Writes to temp file (`.temp`), renames on success for atomicity
- Output: decompressed NDJSON files in `EXTRACTED_PATH/{data_type}/`

### Phase 3: Parsing
- Platform-specific JSON-to-CSV conversion
- Reddit: applies waterfall deletion detection, base-36 ID conversion, format compatibility (see [Reddit Platform](../platforms/reddit.md))
- Generic: simple field extraction with dot-notation and type enforcement (see [Generic Platform](../platforms/generic.md))
- Writes CSV with headers
- Uses temp files for atomic writes

## Configuration

**Config file:** `config/parse/pipeline.yaml`

| Option | Description | Default |
|--------|-------------|---------|
| `data_types` | Data types to process (must match keys in platform's field_list.yaml) | `[]` (set via platform) |
| `parallel_mode` | Process multiple files in parallel | `true` |
| `parse_workers` | Number of parallel workers | `8` |
| `cleanup_temp` | Delete intermediate JSON files after parsing | `false` |
| `watch_interval` | Check for new files every N minutes (0 = run once) | `0` |

See [Configuration Reference](../configuration.md) for full details and the user.yaml override system.

## Parallel vs Sequential Mode

- **Parallel** (`parallel_mode: true`): All detected files parsed concurrently using `parse_workers` processes. Faster but uses more disk space (all JSON files extracted before parsing).
- **Sequential** (`parallel_mode: false`): Process files one at a time. Lower disk usage since intermediate JSON can be cleaned up after each file.

## Platform-Specific Behavior

The parse profile delegates to a platform-specific parser based on the `PLATFORM` environment variable:

- **Reddit** (default): Specialized parsing with deletion detection, base-36 ID conversion, and format compatibility. See [Reddit Platform](../platforms/reddit.md).
- **Generic**: Simple JSON-to-CSV with configurable field extraction. See [Generic Platform](../platforms/generic.md).

## Output Structure

```
CSV_PATH/
├── submissions/
│   ├── RS_2024-01.csv
│   └── RS_2024-02.csv
└── comments/
    ├── RC_2024-01.csv
    └── RC_2024-02.csv
```

CSV files include headers. Column order: `dataset, id, retrieved_utc, ...fields from field_list.yaml...`

## Resume Behavior

- Skips `.zst` files that already have a corresponding JSON file in `EXTRACTED_PATH`
- Skips JSON files that already have a corresponding CSV file in `CSV_PATH`
- To reprocess: delete the output CSV (or JSON) file

## Watch Mode

Set `watch_interval` to a value > 0 to continuously check for new files every N minutes. Useful for processing data as new dumps become available.
