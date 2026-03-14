"""
MongoDB ingestion profile orchestrator for social_data_bridge.
Handles raw JSON/NDJSON ingestion into MongoDB after extraction.

Supports two collection strategies:
  - per_file: Each input file becomes its own collection (e.g., Reddit monthly dumps)
  - per_data_type: All files of a data type merge into one collection

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import re
import time
from pathlib import Path
from typing import List, Dict, Tuple

from ..core.state import PipelineState
from ..core.decompress import decompress_file, is_compressed, strip_compression_extension
from ..core.config import (
    load_profile_config,
    load_platform_config as _load_platform_config,
    get_required,
    get_optional,
    validate_processing_config,
    validate_mongo_config,
    apply_env_overrides,
    ConfigurationError,
)
from ..db.mongo.ingest import (
    ensure_collection,
    mongoimport_file,
    create_index,
    record_ingested_file,
)


# Platform and source selection via environment variables
PLATFORM = os.environ.get('PLATFORM', 'reddit')
SOURCE = os.environ.get('SOURCE') or PLATFORM


def load_platform_config(config_dir: str) -> Dict:
    """Load platform configuration using centralized loader."""
    return _load_platform_config(config_dir, PLATFORM, source=SOURCE)


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """
    Load mongo_ingest profile configuration.

    Args:
        config_dir: Base configuration directory
        quiet: If True, suppress informational output

    Returns:
        Merged configuration dictionary
    """
    config = load_profile_config('mongo_ingest', config_dir, source=SOURCE, quiet=quiet)
    config = apply_env_overrides(config, 'mongo_ingest')
    validate_processing_config(config, 'mongo_ingest')
    validate_mongo_config(config)
    return config


# ---------------------------------------------------------------------------
# File detection (reused patterns from postgres_ingest)
# ---------------------------------------------------------------------------

def detect_dump_files(
    dumps_dir: str, data_types: List[str], file_patterns: Dict
) -> List[Tuple[str, str]]:
    """Detect compressed dump files in the dumps directory and subfolders."""
    dumps_path = Path(dumps_dir)
    files = []

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
                date_str = match.group(1) if match.groups() else filepath.name
                files.append((str(filepath), data_type, date_str))

    files.sort(key=lambda x: (x[2], x[1]))
    return [(f[0], f[1]) for f in files]


def detect_json_files(
    extracted_dir: str, data_types: List[str], file_patterns: Dict
) -> List[Tuple[str, str, str]]:
    """Detect decompressed JSON files in the extracted directory."""
    extracted_path = Path(extracted_dir)
    files = []

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
                    date_str = match.group(1) if match.groups() else filename
                    file_id = filename
                    files.append((str(filepath), file_id, data_type, date_str))

    files.sort(key=lambda x: (x[3], x[2]))
    return [(f[0], f[1], f[2]) for f in files]


def get_file_identifier(filepath: str) -> str:
    """Extract identifier from filepath, stripping compression extensions."""
    name = Path(filepath).name
    if is_compressed(name):
        return strip_compression_extension(name)
    return Path(filepath).stem


# ---------------------------------------------------------------------------
# Collection and database naming
# ---------------------------------------------------------------------------

def get_collection_name(
    file_id: str, data_type: str, strategy: str,
    file_patterns: Dict, collection_map: Dict = None,
) -> str:
    """
    Derive collection name from file and strategy.

    per_data_type: use explicit collection_map if provided, else data_type name.
    per_file: extract date/identifier portion from file_id using json pattern.
    """
    if strategy == 'per_data_type':
        if collection_map and data_type in collection_map:
            return collection_map[data_type]
        return data_type

    # per_file: extract identifier from filename using the json pattern
    pattern_str = file_patterns.get(data_type, {}).get('json', '')
    if pattern_str:
        match = re.match(pattern_str, file_id)
        if match and match.groups():
            return match.group(1)

    # Fallback: use full file_id
    return file_id


def get_db_name(platform_config: Dict, platform: str, data_type: str) -> str:
    """
    Get MongoDB database name from platform config.

    New format: 'mongo_db_name' (single name for all data types).
    Legacy format: 'mongo_db_name_template' with {platform}/{data_type} placeholders.
    """
    # New format: explicit db name
    if 'mongo_db_name' in platform_config:
        return platform_config['mongo_db_name']

    # Legacy format: template with placeholders
    template = platform_config.get('mongo_db_name_template', '{platform}_{data_type}')
    safe_platform = platform.replace('/', '_')
    return template.format(platform=safe_platform, data_type=data_type)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(config_dir: str = "/app/config"):
    """
    Run the MongoDB ingestion pipeline.

    Phases:
        1. Extract .zst files to JSON
        2. Ingest JSON files into MongoDB via mongoimport
        3. Create indexes on collections
    """
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir)

    db_config = config['database']
    proc_config = config['processing']

    # Get data types from profile config, fall back to platform config
    data_types = get_optional(config, 'processing', 'data_types', default=[])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    if not data_types:
        raise ConfigurationError("No data_types configured. Set in user.yaml or platform config.")

    file_patterns = platform_config.get('file_patterns', {})
    collection_strategy = platform_config.get('mongo_collection_strategy', 'per_file')
    collection_map = platform_config.get('mongo_collections', {})
    num_workers = get_required(config, 'processing', 'num_insertion_workers')

    host = db_config['host']
    port = db_config['port']
    user = db_config.get('user')
    password = db_config.get('password')

    print(f"[sdb] Profile: mongo_ingest")
    print(f"[sdb] Platform: {PLATFORM}")
    print(f"[sdb] Collection strategy: {collection_strategy}")
    print(f"[sdb] Data types: {data_types}")

    # Initialize state managers - one per data_type
    mongo_data_path = os.environ.get('MONGO_DATA_PATH', '/data/mongo')
    state_dir = f"{mongo_data_path}/state_tracking"
    os.makedirs(state_dir, exist_ok=True)

    # Build db_name function for state recovery
    def db_name_func(dt):
        return get_db_name(platform_config, PLATFORM, dt)

    states = {}
    total_processed = 0
    total_failed = 0

    for dt in data_types:
        state_file = f"{state_dir}/{SOURCE}_mongo_ingest_{dt}.json"
        states[dt] = PipelineState(
            state_file=state_file,
            db_config={
                'host': host,
                'port': port,
                'db_name_func': db_name_func,
                'user': user,
                'password': password,
            },
            data_types=[dt],
        )

        # If state is empty, try to recover from MongoDB metadata
        if states[dt].get_stats()['processed_count'] == 0:
            print(f"[sdb] No state for {dt}, attempting to recover from MongoDB...")
            states[dt].recover_from_mongodb()

        stats = states[dt].get_stats()
        total_processed += stats['processed_count']
        total_failed += stats['failed_count']

        # Handle interrupted processing
        interrupted_file = states[dt].get_in_progress()
        if interrupted_file:
            print(f"[sdb] Found interrupted file: {interrupted_file} (will be retried)")
            states[dt].clear_in_progress()

    print(f"[sdb] Previously processed: {total_processed} files")
    print(f"[sdb] Previously failed: {total_failed} files")

    # Paths
    dumps_dir = "/data/dumps"
    extracted_dir = "/data/extracted"
    log_dir = f"{mongo_data_path}/logs"

    # Detect files
    dump_files = detect_dump_files(dumps_dir, data_types, file_patterns)
    print(f"\n[sdb] Found {len(dump_files)} .zst files in {dumps_dir}")

    # Filter out already processed .zst files
    pending_zst_files = []
    skipped_count = 0
    for filepath, data_type in dump_files:
        file_id = get_file_identifier(filepath)
        if states[data_type].is_processed(file_id):
            skipped_count += 1
        else:
            pending_zst_files.append((filepath, data_type))

    if skipped_count > 0:
        print(f"[sdb] Skipping {skipped_count} already processed .zst files")

    # Check for existing JSON files
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)
    pending_json_files = [
        (p, fid, dt) for p, fid, dt in json_files
        if not states[dt].is_processed(fid)
    ]

    print(f"[sdb] Found {len(json_files)} JSON files in extracted directory")
    print(f"[sdb] {len(pending_json_files)} unprocessed JSON files to ingest")

    has_work = pending_zst_files or pending_json_files
    if not has_work:
        print("\n[sdb] No files to process. Exiting.")
        return

    total_timings = {'extraction': 0.0, 'ingestion': 0.0, 'indexing': 0.0}
    success_count = 0
    fail_count = 0
    cleanup_temp = get_required(config, 'processing', 'cleanup_temp')

    # =====================================================================
    # PHASE 1: EXTRACTION
    # =====================================================================
    if pending_zst_files:
        print("\n" + "=" * 60)
        print("PHASE 1: EXTRACTION")
        print("=" * 60)

        t_start = time.time()

        for filepath, data_type in pending_zst_files:
            file_id = get_file_identifier(filepath)
            extract_type_dir = f"{extracted_dir}/{data_type}"
            expected_json = Path(extract_type_dir) / file_id
            if not expected_json.exists():
                try:
                    decompress_file(filepath, extract_type_dir)
                except Exception as e:
                    print(f"[sdb] Error extracting {file_id}: {e}")
                    states[data_type].mark_failed(file_id, f"Extraction failed: {e}")
                    fail_count += 1

        total_timings['extraction'] = time.time() - t_start

    # =====================================================================
    # PHASE 2: INGESTION
    # =====================================================================
    # Re-detect JSON files (may have been created during extraction)
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)
    files_to_ingest = [
        (p, fid, dt) for p, fid, dt in json_files
        if not states[dt].is_processed(fid)
    ]

    if files_to_ingest:
        print("\n" + "=" * 60)
        print("PHASE 2: INGESTION")
        print("=" * 60)

        t_start = time.time()

        # Track collections created this run (for indexing phase)
        collections_by_db = {}  # {db_name: set(collection_names)}

        for json_path, file_id, data_type in files_to_ingest:
            db_name = get_db_name(platform_config, PLATFORM, data_type)
            collection_name = get_collection_name(
                file_id, data_type, collection_strategy, file_patterns, collection_map
            )

            try:
                states[data_type].mark_in_progress(file_id)

                # Ensure collection exists with zstd compression
                ensure_collection(db_name, collection_name, host, port, user=user, password=password)

                # Ingest via mongoimport
                print(f"[sdb] Ingesting {file_id} -> {db_name}.{collection_name}")
                mongoimport_file(
                    filepath=json_path,
                    db_name=db_name,
                    collection_name=collection_name,
                    host=host,
                    port=port,
                    num_workers=num_workers,
                    log_dir=log_dir,
                    user=user,
                    password=password,
                )

                # Record in metadata collection
                record_ingested_file(
                    db_name=db_name,
                    file_id=file_id,
                    data_type=data_type,
                    collection_name=collection_name,
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                )

                states[data_type].mark_completed(file_id)
                success_count += 1

                # Track collection for indexing
                if db_name not in collections_by_db:
                    collections_by_db[db_name] = {}
                if data_type not in collections_by_db[db_name]:
                    collections_by_db[db_name][data_type] = set()
                collections_by_db[db_name][data_type].add(collection_name)

                # Cleanup extracted JSON if configured
                if cleanup_temp and os.path.exists(json_path):
                    os.remove(json_path)
                    print(f"[sdb] Removed: {Path(json_path).name}")

            except Exception as e:
                print(f"[sdb] Error ingesting {file_id}: {e}")
                states[data_type].mark_failed(file_id, f"Ingestion failed: {e}")
                fail_count += 1

        total_timings['ingestion'] = time.time() - t_start

    else:
        collections_by_db = {}

    # =====================================================================
    # PHASE 3: INDEXES
    # =====================================================================
    create_indexes = get_required(config, 'processing', 'create_indexes')
    if create_indexes and success_count > 0 and collections_by_db:
        print("\n" + "=" * 60)
        print("CREATING INDEXES")
        print("=" * 60)

        # Get index config from profile config, fall back to platform config
        index_config = config.get('mongo_indexes', {})
        if not index_config:
            index_config = platform_config.get('mongo_indexes', {})

        t_start = time.time()

        for db_name, dt_collections in collections_by_db.items():
            for data_type, collection_names in dt_collections.items():
                index_fields = index_config.get(data_type, [])
                if not index_fields:
                    print(f"[sdb] No indexes configured for {data_type}, skipping")
                    continue

                for collection_name in sorted(collection_names):
                    print(f"[sdb] Creating {len(index_fields)} indexes on {db_name}.{collection_name}")
                    for field in index_fields:
                        try:
                            create_index(
                                db_name=db_name,
                                collection_name=collection_name,
                                field=field,
                                host=host,
                                port=port,
                                user=user,
                                password=password,
                            )
                        except Exception as e:
                            print(f"[sdb] Warning: Failed to create index on {field}: {e}")

        total_timings['indexing'] = time.time() - t_start

    # =====================================================================
    # SUMMARY
    # =====================================================================
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")

    total_processed = sum(states[dt].get_stats()['processed_count'] for dt in data_types)
    total_failed = sum(states[dt].get_stats()['failed_count'] for dt in data_types)
    print(f"Total processed: {total_processed}")
    print(f"Total failed: {total_failed}")

    print(f"\nTiming (minutes):")
    print(f"  Extraction: {total_timings['extraction'] / 60:.2f}")
    print(f"  Ingestion:  {total_timings['ingestion'] / 60:.2f}")
    print(f"  Indexing:   {total_timings['indexing'] / 60:.2f}")
    total_time = sum(total_timings.values())
    print(f"  Total:      {total_time / 60:.2f}")


def main():
    """Main entry point with optional watch mode."""
    config_dir = "/app/config"
    config = load_config(config_dir)
    watch_interval = get_required(config, 'processing', 'watch_interval')

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
