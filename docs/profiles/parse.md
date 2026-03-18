# Parse Profile

The parse profile decompresses compressed data dumps (`.zst`, `.gz`, `.xz`, `.tar.gz`) and parses JSON or CSV input to structured files (Parquet or CSV). It's the entry point of the pipeline — all other profiles depend on its output.

## Running

```bash
python sdp.py run parse [--source <name>]
```

> [!NOTE]
> The platform and source are configured during `python sdp.py source add <name>`. When only one source is configured, `--source` is auto-selected.

---

## How It Works

Three-phase pipeline:

### Phase 1: Input Detection
- Scans `DUMPS_PATH/<source>` for compressed files matching platform file patterns
- Scans `EXTRACTED_PATH/<source>` for already-decompressed JSON files
- Scans `PARSED_PATH/<source>` for already-parsed files (Parquet or CSV, based on `file_format` in platform config)
- File detection is scoped to per-data-type subdirectories
- Reddit: detects `RS_YYYY-MM.*` (submissions) and `RC_YYYY-MM.*` (comments), including torrent directory structure (`submissions/RS_*.zst`, `comments/RC_*.zst`)

### Phase 2: Extraction
- Decompresses files using format-appropriate tools:
  - `.zst`: `zstd --long=31` (supports 2GB window)
  - `.gz` / `.json.gz`: `gzip -d`
  - `.xz`: `xz -d`
  - `.tar.gz` / `.tgz`: `tar xzf` (extracts all files)
- Compression format is auto-detected from file extension, or specified per-data-type in `platform.yaml` via the `compression` key
- Writes to temp file (`.temp`), renames on success for atomicity
- Output: decompressed files in `EXTRACTED_PATH/<source>/{data_type}/` (NDJSON files have no extension, CSV files keep `.csv`)

### Phase 3: Parsing
- Platform-specific parsing to Parquet or CSV (controlled by `file_format` in `platform.yaml`)
- Reddit: applies waterfall deletion detection, base-36 ID conversion, format compatibility (see [Reddit Platform](../platforms/reddit.md))
- Custom NDJSON: field extraction with dot-notation and type enforcement (see [Custom Platforms](../platforms/custom.md))
- Custom CSV: column selection, type casting, and robust handling of messy files via Polars (see [Custom Platforms](../platforms/custom.md))
- **Parquet** (default for new sources): typed columns via Polars/PyArrow, null-byte stripping only (no CSV escaping)
- **CSV** (alternate, for external tool compatibility): writes CSV with headers, full escape handling
- Uses temp files for atomic writes

---

## Configuration

**Base config:** `config/parse/pipeline.yaml`
**Source overrides:** `config/sources/<name>/parse.yaml`

| Option | Description | Default |
|--------|-------------|---------|
| `data_types` | Data types to process (must match keys in platform's `fields` config) | `[]` (set via source config) |
| `parallel_mode` | Process multiple files in parallel | `true` |
| `parse_workers` | Number of parallel workers | `8` |
| `cleanup_temp` | Delete intermediate JSON files after parsing | `false` |
| `watch_interval` | Check for new files every N minutes (0 = run once) | `0` |

See [Configuration Reference](../configuration.md) for full details and the source override system.

---

## Parallel vs Sequential Mode

- **Parallel** (`parallel_mode: true`): All detected files parsed concurrently using `parse_workers` processes. Faster but uses more disk space (all JSON files extracted before parsing).
- **Sequential** (`parallel_mode: false`): Process files one at a time. Lower disk usage since intermediate JSON can be cleaned up after each file.

---

## Platform-Specific Behavior

The parse profile delegates to a platform-specific parser based on the `PLATFORM` environment variable:

- **Reddit** (default): Specialized parsing with deletion detection, base-36 ID conversion, and format compatibility. See [Reddit Platform](../platforms/reddit.md).
- **Custom** (`custom/<name>`): JSON and CSV parsing with configurable field extraction and type enforcement. See [Custom Platforms](../platforms/custom.md).

---

## Output Structure

```
PARSED_PATH/<source>/
├── submissions/
│   ├── RS_2024-01.parquet      # or .csv if file_format: csv
│   └── RS_2024-02.parquet
└── comments/
    ├── RC_2024-01.parquet
    └── RC_2024-02.parquet
```

Output format is controlled by `file_format` in `platform.yaml` (default: `parquet`, alternate: `csv`). Column order: `dataset, id, retrieved_utc, ...fields from platform config...`

---

## Resume Behavior

- Skips compressed files that already have a corresponding JSON file in `EXTRACTED_PATH/<source>/`
- Skips JSON files that already have a corresponding output file (`.parquet` or `.csv`) in `PARSED_PATH/<source>/`
- To reprocess: delete the output file (or JSON) file

## Watch Mode

Set `watch_interval` to a value > 0 to continuously check for new files every N minutes. Useful for processing data as new dumps become available.
