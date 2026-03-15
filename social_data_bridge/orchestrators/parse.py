"""
Parse profile orchestrator for social-data-bridge.
Handles file detection, decompression, and JSON to CSV parsing.
Does NOT run classifiers or database ingestion.

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import re
import sys
import time
from pathlib import Path
from typing import List, Dict, Tuple

from ..core.decompress import decompress_file, is_compressed, strip_compression_extension
from ..core.config import (
    load_profile_config,
    load_platform_config,
    get_required,
    get_optional,
    validate_processing_config,
    ConfigurationError,
)


# Platform and source selection via environment variables
PLATFORM = os.environ.get('PLATFORM', 'reddit')
SOURCE = os.environ.get('SOURCE') or PLATFORM


def get_platform_parser():
    """
    Get the appropriate parser module for the current platform.

    Built-in platforms have dedicated parsers. Custom platforms (custom/*)
    use the shared custom parser.

    Returns:
        Parser module with parse_to_csv and parse_files_parallel functions
    """
    if PLATFORM == 'reddit':
        from ..platforms.reddit import parser
        return parser
    elif PLATFORM.startswith('custom/'):
        from ..platforms.custom import parser
        return parser
    else:
        raise ConfigurationError(f"Unknown platform: {PLATFORM}. Supported: reddit, custom/<name>")


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """
    Load parse profile configuration.

    Loads base config files and merges user.yaml overrides if present.

    Args:
        config_dir: Base configuration directory
        quiet: If True, suppress informational output

    Returns:
        Merged configuration dictionary

    Raises:
        ConfigurationError: If required config is missing
    """
    config = load_profile_config('parse', config_dir, source=SOURCE, quiet=quiet)
    validate_processing_config(config, 'parse')
    return config


def detect_dump_files(dumps_dir: str, data_types: List[str], file_patterns: Dict) -> List[Tuple[str, str]]:
    """
    Detect compressed dump files in the dumps directory and subfolders.

    Supports .zst, .gz, .json.gz, .xz, .tar.gz, .tgz formats.
    Uses 'dump' pattern (new format) or 'zst' pattern (legacy) from file_patterns.

    Searches in:
        - dumps_dir/ (root)
        - dumps_dir/<data_type>/

    Returns:
        List of tuples (filepath, data_type) sorted alphabetically
    """
    dumps_path = Path(dumps_dir)
    files = []

    # Build patterns from platform config
    # Support both new 'dump' key and legacy 'zst' key
    patterns = {}
    for data_type in data_types:
        if data_type not in file_patterns:
            continue
        dt_patterns = file_patterns[data_type]
        if 'dump' in dt_patterns:
            patterns[data_type] = re.compile(dt_patterns['dump'])
        elif 'zst' in dt_patterns:
            patterns[data_type] = re.compile(dt_patterns['zst'])

    for data_type in data_types:
        type_dir = dumps_path / data_type
        if not type_dir.is_dir() or data_type not in patterns:
            continue
        for filepath in type_dir.iterdir():
            if not filepath.is_file() or not is_compressed(filepath.name):
                continue
            match = patterns[data_type].match(filepath.name)
            if match:
                files.append((str(filepath), data_type, filepath.name))

    # Sort by data type index then filename
    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[1], 99), x[2]))

    return [(f[0], f[1]) for f in files]


def detect_json_files(extracted_dir: str, data_types: List[str], file_patterns: Dict) -> List[Tuple[str, str, str]]:
    """Detect decompressed JSON files in the extracted directory."""
    extracted_path = Path(extracted_dir)
    files = []

    # Build patterns from platform config
    patterns = {}
    for data_type in data_types:
        if data_type in file_patterns and 'json' in file_patterns[data_type]:
            patterns[data_type] = re.compile(file_patterns[data_type]['json'])

    for data_type in data_types:
        if data_type not in patterns:
            continue

        type_dir = extracted_path / data_type
        if not type_dir.is_dir():
            continue

        for filepath in type_dir.iterdir():
            if filepath.is_file():
                filename = filepath.name
                match = patterns[data_type].match(filename)
                if match:
                    file_id = filename
                    files.append((str(filepath), file_id, data_type, filename))

    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def detect_parsed_csv_files(csv_dir: str, data_types: List[str], file_patterns: Dict, file_format: str = 'csv') -> List[Tuple[str, str, str]]:
    """Detect parsed CSV/Parquet files in the CSV directory."""
    csv_base = Path(csv_dir)
    files = []

    ext = 'parquet' if file_format == 'parquet' else 'csv'

    # Build patterns from platform config
    patterns = {}
    for data_type in data_types:
        if data_type in file_patterns and file_format in file_patterns[data_type]:
            patterns[data_type] = re.compile(file_patterns[data_type][file_format])

    for data_type in data_types:
        if data_type not in patterns:
            continue

        type_dir = csv_base / data_type
        if not type_dir.is_dir():
            continue

        for filepath in type_dir.glob(f"*.{ext}"):
            filename = filepath.name
            match = patterns[data_type].match(filename)
            if match:
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, filename))

    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def get_file_identifier(filepath: str) -> str:
    """Extract identifier from filepath, stripping compression extensions.

    Matches the naming used by decompress_file: compression extension removed,
    then .json suffix removed (extracted files have no extension).

    E.g., RC_2023-01.zst -> RC_2023-01, data.json.gz -> data
    """
    name = Path(filepath).name
    if is_compressed(name):
        stem = strip_compression_extension(name)
        # decompress_gz also strips .json (extracted files have no extension)
        if stem.endswith('.json'):
            stem = stem[:-5]
        return stem
    return Path(filepath).stem


def run_pipeline(config_dir: str = "/app/config"):
    """
    Run the parse pipeline.

    Pipeline phases:
    1. Detect input sources (.zst files, existing JSON)
    2. Decompress .zst files
    3. Parse JSON to CSV

    Args:
        config_dir: Base configuration directory
    """
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir, PLATFORM, source=SOURCE)

    # Get platform-specific parser
    parser = get_platform_parser()

    proc_config = config['processing']

    # Get data types from profile config, fall back to platform config
    data_types = get_optional(config, 'processing', 'data_types', default=[])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    if not data_types:
        raise ConfigurationError("No data_types configured. Set in user.yaml or platform config.")

    # Get file patterns from platform config
    file_patterns = platform_config.get('file_patterns', {})

    print(f"[sdb] Profile: parse")
    print(f"[sdb] Platform: {PLATFORM}")
    print(f"[sdb] Data types: {data_types}")

    # Paths
    dumps_dir = "/data/dumps"
    extracted_dir = "/data/extracted"
    csv_dir = "/data/csv"

    # Phase 1: Detect input sources
    print("\n" + "="*60)
    print("PHASE 1: INPUT DETECTION")
    print("="*60)

    dump_files = detect_dump_files(dumps_dir, data_types, file_patterns)
    print(f"[sdb] Found {len(dump_files)} compressed files in {dumps_dir}")

    file_format = platform_config.get('file_format', 'csv')
    ext = 'parquet' if file_format == 'parquet' else 'csv'

    json_files = detect_json_files(extracted_dir, data_types, file_patterns)
    parsed_csv_files = detect_parsed_csv_files(csv_dir, data_types, file_patterns, file_format=file_format)

    print(f"[sdb] Found {len(json_files)} JSON files in extracted directory")
    print(f"[sdb] Found {len(parsed_csv_files)} {ext.upper()} files in csv directory")

    # Filter out compressed files that already have extracted JSON or parsed CSV
    # Use direct file existence check rather than pattern matching, so extracted
    # files are recognized even if file_patterns change across reconfigurations
    existing_csv_ids = {fid for _, fid, _ in parsed_csv_files}
    pending_dumps = []
    for p, dt in dump_files:
        file_id = get_file_identifier(p)
        extracted_path = Path(extracted_dir) / dt / file_id
        if extracted_path.exists() or file_id in existing_csv_ids:
            continue
        pending_dumps.append((p, dt))

    # Filter out JSON files that already have parsed CSV
    pending_json = [(p, fid, dt) for p, fid, dt in json_files if fid not in existing_csv_ids]

    print(f"[sdb] Pending: {len(pending_dumps)} compressed, {len(pending_json)} JSON")

    if not (pending_dumps or pending_json):
        print("\n[sdb] No files to process. Exiting.")
        return

    total_timings = {'extraction': 0.0, 'parsing': 0.0}
    success_count = 0
    fail_count = 0

    # Phase 2: Extract compressed files
    if pending_dumps:
        print("\n" + "="*60)
        print("PHASE 2: EXTRACTION")
        print("="*60)

        t_start = time.time()

        for filepath, data_type in pending_dumps:
            file_id = get_file_identifier(filepath)
            extract_dir = f"{extracted_dir}/{data_type}"

            try:
                decompress_file(filepath, extract_dir)
                success_count += 1
            except Exception as e:
                print(f"[sdb] Error extracting {file_id}: {e}")
                fail_count += 1

        total_timings['extraction'] = time.time() - t_start

    # Phase 3: Parse JSON files to CSV
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)  # Re-detect
    files_to_parse = []
    for json_path, file_id, data_type in json_files:
        expected_output = Path(csv_dir) / data_type / f"{file_id}.{ext}"
        if not expected_output.exists():
            files_to_parse.append((json_path, file_id, data_type))

    if files_to_parse:
        print("\n" + "="*60)
        print("PHASE 3: PARSING")
        print("="*60)

        t_start = time.time()
        workers = get_required(config, 'processing', 'parse_workers')
        parallel_mode = get_required(config, 'processing', 'parallel_mode')

        if parallel_mode and len(files_to_parse) > 1:
            print(f"[sdb] Parallel mode: {len(files_to_parse)} files with {workers} workers")

            parse_input = [(json_path, data_type) for json_path, file_id, data_type in files_to_parse]

            try:
                parser.parse_files_parallel(
                    files=parse_input,
                    output_dir=csv_dir,
                    platform_config=platform_config,
                    workers=workers
                )

                for json_path, file_id, data_type in files_to_parse:
                    success_count += 1

                    cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
                    if cleanup_temp and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[sdb] Removed: {Path(json_path).name}")

            except Exception as e:
                print(f"[sdb] Error in parallel parsing: {e}")
                fail_count += len(files_to_parse)
        else:
            # Sequential parsing
            for json_path, file_id, data_type in files_to_parse:
                try:
                    parser.parse_to_csv(
                        input_file=json_path,
                        output_dir=csv_dir,
                        data_type=data_type,
                        platform_config=platform_config
                    )

                    success_count += 1

                    cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
                    if cleanup_temp and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[sdb] Removed: {Path(json_path).name}")

                except Exception as e:
                    print(f"[sdb] Error parsing {file_id}: {e}")
                    fail_count += 1

        total_timings['parsing'] = time.time() - t_start

    # Final summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"[sdb] Successful: {success_count}")
    print(f"[sdb] Failed: {fail_count}")

    print(f"\n[sdb] Timing (minutes):")
    print(f"[sdb]   Extraction: {total_timings['extraction'] / 60:.2f}")
    print(f"[sdb]   Parsing:    {total_timings['parsing'] / 60:.2f}")
    total_time = sum(total_timings.values())
    print(f"[sdb]   Total:      {total_time / 60:.2f}")


def main():
    """Main entry point with optional watch mode."""
    config_dir = "/app/config"
    config = load_config(config_dir)
    watch_interval = get_required(config, 'processing', 'watch_interval')

    print(f"[sdb] Platform: {PLATFORM}")

    if watch_interval > 0:
        print(f"[sdb] Watch mode enabled: checking every {watch_interval} minutes")
        interval_seconds = watch_interval * 60
        while True:
            try:
                run_pipeline(config_dir)
            except Exception as e:
                print(f"[sdb] Pipeline error: {e}")
                print("[sdb] Will retry next interval...")

            print(f"\n[sdb] Next check in {watch_interval} minutes...")
            time.sleep(interval_seconds)
    else:
        run_pipeline(config_dir)


if __name__ == "__main__":
    main()
