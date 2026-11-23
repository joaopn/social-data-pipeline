---
title: "Parsing"
parent: "User guide"
nav_order: 6
---
# Parsing

The parse profile decompresses data dumps (`.zst`, `.gz`, `.xz`, `.tar.gz`) and parses JSON, CSV or Parquet input into typed Parquet or CSV files. It is the entry point of the pipeline: the classifiers and the PostgreSQL and StarRocks ingestion read its output.

## Running parse

```bash
python sdp.py run parse [--source <name>] [--filter <pattern>] [--skip-lingua-files]
```

The platform and the source are configured by `python sdp.py source add <name>`. When only one source is configured, `--source` is selected automatically.

| Option | What it does |
|--------|--------------|
| `--source`, `-s` | The source to parse |
| `--filter`, `-f` | Process only files whose file id matches this fnmatch glob |
| `--skip-lingua-files` | Also skip files that already have a lingua output (see [Deleting intermediate files](#deleting-intermediate-files---skip-lingua-files)) |

`--filter` is useful when you add new data to an existing source, to parse only the new files without reprocessing everything:

```bash
python sdp.py run parse --source reddit --filter "*2024*"       # All 2024 months
python sdp.py run parse --source reddit --filter "RS_2024-*"    # 2024 submissions only
python sdp.py run parse --source reddit --filter "RC_2024-06"   # Single file
```

A run finds the dumps in `DUMPS_PATH/<source>/<data_type>/`, decompresses them into `EXTRACTED_PATH/<source>/<data_type>/`, and parses them into `PARSED_PATH/<source>/<data_type>/`. Reddit parsing adds deletion detection, base-36 id conversion and format compatibility (see [Reddit dumps](reddit.md)); custom sources get field extraction and type enforcement (see [Your own data and Hugging Face](custom-data.md)). The phases in detail are in [Pipeline and data folders](../architecture/pipeline.md#the-parse-phases).

## Output structure

```
PARSED_PATH/<source>/
├── submissions/
│   ├── RS_2024-01.parquet      # or .csv if file_format: csv
│   └── RS_2024-02.parquet
└── comments/
    ├── RC_2024-01.parquet
    └── RC_2024-02.parquet
```

The output format is set by `file_format` in `platform.yaml` (default `parquet`, alternate `csv`). Column order: `dataset, id, retrieved_utc, ...fields from platform config...`.

| Format | What you get |
|--------|--------------|
| Parquet (default for new sources) | Typed columns, with null bytes stripped |
| CSV (alternate, for external tool compatibility) | CSV with headers and full escape handling |

## Configuration

**Base config:** `config/parse/pipeline.yaml`. **Source overrides:** `config/sources/<name>/parse.yaml`.

| Option | Description | Default |
|--------|-------------|---------|
| `data_types` | Data types to process (must match keys in the platform's `fields` config) | `[]` (set via source config) |
| `parallel_mode` | Process multiple files in parallel | `true` |
| `parse_workers` | Number of parallel workers | `8` |
| `cleanup_temp` | Delete intermediate JSON files after parsing | `false` |
| `watch_interval` | Check for new files every N minutes (0 = run once) | `0` |

See [Configuration reference](configuration.md) for full details and the source override system.

## Parallel or sequential

| Mode | Behaviour |
|------|-----------|
| Parallel (`parallel_mode: true`) | All detected files are parsed concurrently, using `parse_workers` processes. Faster, but uses more disk space, since all JSON files are extracted before parsing. |
| Sequential (`parallel_mode: false`) | Files are processed one at a time. Lower disk usage, since intermediate JSON can be cleaned up after each file. |

## Resuming and reprocessing

A run skips work that is already done, so you can rerun parse safely:

| You have | Parse skips |
|----------|-------------|
| A decompressed file in `EXTRACTED_PATH/<source>/` | Decompressing its dump |
| An output file (`.parquet` or `.csv`) in `PARSED_PATH/<source>/` | Parsing its JSON |

To reprocess a file, delete its output file (or its JSON file). The mechanics are in [Pipeline and data folders](../architecture/pipeline.md#how-parse-resumes).

## Deleting intermediate files (`--skip-lingua-files`)

In a `parse → lingua` pipeline the parsed file is only an intermediate. Once lingua has consumed it, you can delete `EXTRACTED_PATH/<source>/<data_type>/<id>` and `PARSED_PATH/<source>/<data_type>/<id>.{csv,parquet}` to reclaim disk. Without the flag, the next parse run would re-extract and re-parse those files, because both skip signals are gone.

Pass `--skip-lingua-files` to add a third skip signal: parse skips any compressed, extracted or parquet input file whose id already has a lingua output at `OUTPUT_PATH/<source>/lingua/<data_type>/<id>_lingua.{csv,parquet}`.

```bash
python sdp.py run parse --source reddit --skip-lingua-files
```

The lingua suffix is hardcoded as `_lingua` (the project default). The flag composes with `--filter` and is rejected outside the parse profile.

## Watch mode

Set `watch_interval` to a value above 0 to check for new files every N minutes. This is useful for processing data as new dumps become available.
