---
title: "Pipeline and data folders"
parent: "Architecture"
nav_order: 1
---
# Pipeline and data folders

The pipeline moves data through four stage folders, from the raw dump to classifier output, and each profile reads one stage and writes the next. This page describes the folders, the parse profile's three phases, and how parse resumes without state files.

## Stage directories

```
data/
├── dumps/<source>/        # the origin, mirrored 1-to-1 (dumps as downloaded, HF repo layout)
├── extracted/<source>/    # decompressed or organized input, one folder per data type
├── parsed/<source>/       # field-selected and typed Parquet or CSV, one folder per data type
└── output/<source>/       # classifier results, one folder per classifier
```

Each folder is written only by the step that produces its stage:

| Folder | Written by | Read by |
|--------|------------|---------|
| `dumps/` | You, or `source download` (phase 1, the mirror) | Extraction |
| `extracted/` | Extraction (in `parse`, `mongo_ingest`, and `postgres_ingest` when it extracts), or `source download` (phase 2, organize) | Parsing, `mongo_ingest` |
| `parsed/` | Parsing (in `parse`, and `postgres_ingest` when it parses) | `lingua`, `ml`, `postgres_ingest`, `sr_ingest` |
| `output/` | `lingua` and `ml` | `ml` (lingua output), `postgres_ingest` / `sr_ingest` (with `prefer_lingua`), `postgres_ml`, `sr_ml` |

The host paths are set by `DUMPS_PATH`, `EXTRACTED_PATH`, `PARSED_PATH` and `OUTPUT_PATH`, or per source by `paths` in `platform.yaml` (see [Configuration reference](../user-guide/configuration.md)).

## The parse phases

Parse runs three phases.

### Phase 1: input detection

| Scans | For |
|-------|-----|
| `DUMPS_PATH/<source>/<data_type>/` | Compressed files matching the platform's `dump` pattern |
| `EXTRACTED_PATH/<source>/<data_type>/` | Already-decompressed JSON files |
| `EXTRACTED_PATH/<source>/<data_type>/` | Parquet input files (when `input_format: parquet`, for example HF datasets) |
| `PARSED_PATH/<source>/<data_type>/` | Already-parsed files (Parquet or CSV, from `file_format` in the platform config) |

File detection is scoped to the per-data-type subfolders; files in the root of a stage folder are not detected. For Reddit, that means `RS_YYYY-MM.*` in `submissions/` and `RC_YYYY-MM.*` in `comments/` (the torrent layout).

### Phase 2: extraction

Files are decompressed with the tool for their format:

| Extension | Tool |
|-----------|------|
| `.zst` | `zstd --long=31` (supports a 2 GB window) |
| `.gz` / `.json.gz` | `gzip -d` |
| `.xz` | `xz -d` |
| `.tar.gz` / `.tgz` | `tar xzf` (extracts all files) |

The compression format is detected from the file extension, or specified per data type in `platform.yaml` with the `compression` key. Output is written to a temp file (`.temp`) and renamed on success, for atomicity. Decompressed files land in `EXTRACTED_PATH/<source>/<data_type>/`: NDJSON files have no extension, CSV files keep `.csv`.

### Phase 3: parsing

Parsing is platform-specific (see [Platforms](platforms.md)) and writes Parquet or CSV, as set by `file_format` in `platform.yaml`:

| Input | Parsing |
|-------|---------|
| Reddit | Waterfall deletion detection, base-36 id conversion, format compatibility |
| Custom NDJSON | Field extraction with dot-notation and type enforcement |
| Custom CSV | Column selection, type casting, and robust handling of messy files through Polars |
| Custom Parquet (`input_format: parquet`) | Column selection, null-byte stripping and type enforcement through Polars; used for HF parquet files downloaded to `data/extracted/` |

| Output | How it is written |
|--------|-------------------|
| Parquet (default for new sources) | Typed columns through Polars / PyArrow, null-byte stripping only (no CSV escaping) |
| CSV (alternate, for external tool compatibility) | CSV with headers and full escape handling |

Parsing also writes to temp files and renames them, so a half-written output never looks finished.

## How parse resumes

Parse keeps no state file. It decides from the stage folders what is left to do:

- A compressed file that already has its decompressed file in `EXTRACTED_PATH/<source>/` is skipped.
- A JSON file that already has its output file (`.parquet` or `.csv`) in `PARSED_PATH/<source>/` is skipped.
- With `--skip-lingua-files`, an input whose id already has `OUTPUT_PATH/<source>/lingua/<data_type>/<id>_lingua.{csv,parquet}` is skipped too.

Because extraction and parsing write to temp names and rename on success, an interrupted run leaves no file that a later run would mistake for finished work. Deleting an output file (or its JSON) is therefore all it takes to reprocess it.
