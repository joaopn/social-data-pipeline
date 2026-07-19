"""
StarRocks base table ingestion orchestrator for social_data_pipeline.
Ingests parsed Parquet/CSV files into StarRocks Primary Key tables.
Expects parsed files to already exist (run parse profile first).

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import time
from pathlib import Path
from typing import Dict

from ..core.state import PipelineState
from ..core.config import (
    load_profile_config,
    load_platform_config as _load_platform_config,
    load_db_config,
    get_optional,
    validate_processing_config,
    validate_starrocks_config,
    apply_env_overrides,
    ConfigurationError,
)
from ..db.starrocks.ingest import (
    compute_bucket_count,
    ensure_database_exists,
    table_exists,
    get_column_list,
    get_create_table_query,
    ingest_file,
    analyze_table,
    create_indexes,
    execute_query,
)
from .ml import detect_parsed_files, get_lingua_config, detect_lingua_files


# Platform and source selection via environment variables
PLATFORM = os.environ.get('PLATFORM', 'reddit')
SOURCE = os.environ.get('SOURCE') or PLATFORM


def load_platform_config(config_dir: str) -> Dict:
    """Load platform configuration using centralized loader."""
    return _load_platform_config(config_dir, PLATFORM, source=SOURCE)


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """Load sr_ingest profile configuration with env overrides and validation."""
    config = load_profile_config('sr_ingest', config_dir, source=SOURCE, quiet=quiet)
    config = apply_env_overrides(config, 'sr_ingest')
    validate_processing_config(config, 'sr_ingest')
    # Global table compression codec (config/db/starrocks.yaml). A per-source
    # profile override (already deep-merged into config['database']) wins; the
    # global value is only the fallback.
    config.setdefault('database', {})
    compression = (load_db_config('starrocks', config_dir) or {}).get('compression')
    if compression is not None:
        config['database'].setdefault('compression', compression)
    validate_starrocks_config(config)
    return config


def run_pipeline(config_dir: str = "/app/config"):
    """
    Run the StarRocks ingestion pipeline.

    Ingests parsed Parquet/CSV files into StarRocks Primary Key tables.
    Single code path for both new and existing tables — PK tables handle
    upsert/dedup natively.
    """
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir)

    db_config = config['database']
    password = db_config.get('password')
    compression = db_config.get('compression')

    # Get data types from profile config, fall back to platform config
    data_types = get_optional(config, 'processing', 'data_types', default=[])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    if not data_types:
        raise ConfigurationError("No data_types configured. Set in source config or platform config.")

    # File patterns and format from platform config
    file_patterns = platform_config.get('file_patterns', {})
    file_format = platform_config.get('file_format', 'csv')

    # Primary key and upsert ordering from platform config
    pk_column = platform_config.get('primary_key')
    order_field = platform_config.get('upsert_order_field')

    # Database name = source name (database-per-source, like MongoDB)
    database = SOURCE

    print("[sdp] Profile: sr_ingest")
    print(f"[sdp] Platform: {PLATFORM}")
    print(f"[sdp] Database: {database}")
    print(f"[sdp] Data types: {data_types}")

    # Build file prefixes from platform config for state recovery
    file_prefixes = {}
    for dt in data_types:
        if dt in file_patterns and 'prefix' in file_patterns[dt]:
            file_prefixes[dt] = file_patterns[dt]['prefix']

    # Initialize state managers — one per data_type for isolation
    state_base = os.environ.get('STARROCKS_DATA_PATH', '/data/database')
    state_dir = f"{state_base}/state_tracking"
    os.makedirs(state_dir, exist_ok=True)

    states = {}
    total_processed = 0
    total_failed = 0

    for dt in data_types:
        state_file = f"{state_dir}/{SOURCE}_sr_ingest_{dt}.json"
        states[dt] = PipelineState(
            state_file=state_file,
            db_config={
                'host': db_config['host'],
                'port': db_config['port'],
                'user': db_config['user'],
                'password': password,
                'database_name': database,
            },
            data_types=[dt],
            file_prefixes={dt: file_prefixes.get(dt)},
            state_field=platform_config.get('state_field'),
        )

        # If state is empty, try to recover from StarRocks
        if states[dt].get_stats()['processed_count'] == 0:
            print(f"[sdp] No state for {dt}, attempting to recover from StarRocks...")
            states[dt].recover_from_starrocks()

        stats = states[dt].get_stats()
        total_processed += stats['processed_count']
        total_failed += stats['failed_count']

        # Handle interrupted processing
        interrupted_file = states[dt].get_in_progress()
        if interrupted_file:
            print(f"[sdp] Found interrupted file: {interrupted_file} (will be retried)")
            states[dt].clear_in_progress()

    print(f"[sdp] Previously processed: {total_processed} files")
    print(f"[sdp] Previously failed: {total_failed} files")

    # Detect parsed files
    parsed_dir = "/data/parsed"

    # Check if we should prefer lingua files
    prefer_lingua = get_optional(config, 'processing', 'prefer_lingua', default=False)
    lingua_config = None
    parsed_source_map = {}

    if prefer_lingua:
        lingua_config = get_lingua_config(config_dir, source=SOURCE)
        if lingua_config:
            print(f"[sdp] Prefer lingua: enabled (suffix: {lingua_config['suffix']})")
            parsed_files, parsed_source_map = detect_lingua_files(data_types, lingua_config, file_format=file_format)
            lingua_count = sum(1 for src in parsed_source_map.values() if src == 'lingua')
            original_count = sum(1 for src in parsed_source_map.values() if src == 'original')
            print(f"[sdp] Found {lingua_count} lingua files, {original_count} original files")
        else:
            print("[sdp] Prefer lingua: enabled but lingua config not found, using original parsed files")
            parsed_files = detect_parsed_files(parsed_dir, data_types, file_patterns, file_format=file_format)
    else:
        parsed_files = detect_parsed_files(parsed_dir, data_types, file_patterns, file_format=file_format)

    files_to_ingest = [(p, fid, dt) for p, fid, dt in parsed_files if not states[dt].is_processed(fid)]

    print(f"[sdp] Found {len(parsed_files)} parsed files")
    print(f"[sdp] Pending ingestion: {len(files_to_ingest)} files")

    if not files_to_ingest:
        print("\n[sdp] No files to process. Exiting.")
        return

    # Ingestion phase
    print("\n" + "=" * 60)
    print("INGESTION")
    print("=" * 60)

    check_duplicates = get_optional(config, 'processing', 'check_duplicates', default=True)
    cleanup_temp = get_optional(config, 'processing', 'cleanup_temp', default=False)

    if check_duplicates and not pk_column:
        raise ConfigurationError(
            "check_duplicates is enabled but no primary_key is defined in platform config. "
            "Either set primary_key in platform.yaml or set check_duplicates: false in the sr profile."
        )

    # Ensure database exists
    ensure_database_exists(
        database=database,
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=password,
    )

    t_start = time.time()
    success_count = 0
    fail_count = 0

    data_types_with_files = set(dt for _, _, dt in files_to_ingest)

    for data_type in data_types:
        if data_type not in data_types_with_files:
            continue

        type_files = [(p, fid, dt) for p, fid, dt in files_to_ingest if dt == data_type]
        print(f"\n[sdp] Processing {data_type}: {len(type_files)} files")

        # Get columns from first file (to detect lingua columns)
        first_file = type_files[0][0]
        columns_list = get_column_list(data_type, platform_config, file=first_file)

        # Create table if not exists. pk_column may be None (no source PK):
        # get_create_table_query handles that by emitting the Duplicate Key
        # + DISTRIBUTED BY RANDOM shape.
        if not table_exists(
            table=data_type,
            database=database,
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=password,
        ):
            buckets = compute_bucket_count(platform_config, data_type)
            buckets_label = buckets if buckets is not None else "auto"
            print(f"[sdp] Creating table {database}.{data_type} (BUCKETS {buckets_label})")
            create_query = get_create_table_query(
                table=data_type,
                database=database,
                columns_list=columns_list,
                platform_config=platform_config,
                pk_column=pk_column,
                buckets=buckets,
                compression=compression,
            )
            execute_query(
                create_query,
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=password,
            )

        # Ingest each file
        for parsed_path, file_id, dt in type_files:
            try:
                states[data_type].mark_in_progress(file_id)

                row_count = ingest_file(
                    table=data_type,
                    database=database,
                    columns_list=columns_list,
                    file_path=parsed_path,
                    file_format=file_format,
                    check_duplicates=check_duplicates,
                    order_field=order_field,
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    password=password,
                )
                print(f"[sdp] Loaded {row_count} rows from {Path(parsed_path).name}")

                states[data_type].mark_completed(file_id)
                success_count += 1

                if cleanup_temp and os.path.exists(parsed_path):
                    os.remove(parsed_path)
                    print(f"[sdp] Removed: {Path(parsed_path).name}")

            except Exception as e:
                print(f"[sdp] Error ingesting {file_id}: {e}")
                states[data_type].mark_failed(file_id, f"Ingestion failed: {e}")
                fail_count += 1

    ingestion_time = time.time() - t_start

    # Analyze tables
    if success_count > 0:
        print("\n" + "=" * 60)
        print("ANALYZE")
        print("=" * 60)

        for data_type in data_types_with_files:
            try:
                analyze_table(
                    table=data_type,
                    database=database,
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    password=password,
                )
                print(f"[sdp] Analyzed {data_type}")
            except Exception as e:
                print(f"[sdp] Warning: Failed to analyze {data_type}: {e}")

    # Create indexes
    should_create_indexes = get_optional(config, 'processing', 'create_indexes', default=True)
    indexing_time = 0.0

    if should_create_indexes and success_count > 0:
        print("\n" + "=" * 60)
        print("CREATING INDEXES")
        print("=" * 60)

        # Get indexes: sr_indexes from profile, fall back to platform config sr_indexes, then indexes
        index_config = config.get('sr_indexes', {})
        if not index_config:
            index_config = platform_config.get('sr_indexes', {})
        if not index_config:
            index_config = platform_config.get('indexes', {})

        # Collect per-table plans up front so we can parallelize across tables.
        # Each table runs its own serial drain-submit-poll loop in create_indexes();
        # running multiple tables concurrently is safe because StarRocks' "one
        # schema change at a time" constraint is per-table, and BE-side memory
        # is bounded by alter_tablet_worker_count (set at sdp db setup).
        plan = {}
        for data_type in data_types_with_files:
            index_fields = index_config.get(data_type, [])
            if not index_fields:
                print(f"[sdp] No indexes configured for {data_type}, skipping")
                continue
            plan[data_type] = index_fields
            print(f"[sdp] Queued {len(index_fields)} BITMAP indexes on {database}.{data_type}")

        t_idx = time.time()
        if plan:
            import threading
            from concurrent.futures import ThreadPoolExecutor, as_completed

            print_lock = threading.Lock()

            def emit(msg):
                with print_lock:
                    print(msg, flush=True)

            poll_interval = get_optional(config, 'processing', 'index_poll_interval', default=10)

            def _run(data_type, fields):
                return create_indexes(
                    table=data_type,
                    database=database,
                    fields=fields,
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    password=password,
                    poll_interval=poll_interval,
                    line_prefix=f"[{data_type}] ",
                    print_fn=emit,
                )

            with ThreadPoolExecutor(max_workers=len(plan)) as pool:
                futures = {pool.submit(_run, dt, fs): dt for dt, fs in plan.items()}
                for fut in as_completed(futures):
                    data_type = futures[fut]
                    try:
                        created = fut.result()
                    except Exception as e:
                        print(f"[sdp] Warning: Failed to create indexes on {data_type}: {e}")
                        continue
                    if created:
                        print(f"[sdp] Created indexes on {data_type}: {', '.join(created)}")
        indexing_time = time.time() - t_idx

    # Final summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")

    total_processed = sum(states[dt].get_stats()['processed_count'] for dt in data_types)
    total_failed = sum(states[dt].get_stats()['failed_count'] for dt in data_types)
    print(f"Total processed: {total_processed}")
    print(f"Total failed: {total_failed}")

    print("\nTiming (minutes):")
    print(f"  Ingestion:  {ingestion_time / 60:.2f}")
    print(f"  Indexing:   {indexing_time / 60:.2f}")
    total_time = ingestion_time + indexing_time
    print(f"  Total:      {total_time / 60:.2f}")


def main():
    """Main entry point with optional watch mode."""
    config_dir = "/app/config"
    config = load_config(config_dir)
    watch_interval = get_optional(config, 'processing', 'watch_interval', default=0)

    if watch_interval > 0:
        print(f"[sdp] Watch mode enabled: checking every {watch_interval} minutes")
        interval_seconds = watch_interval * 60
        while True:
            try:
                run_pipeline(config_dir)
            except Exception as e:
                print(f"[sdp] Pipeline error: {e}")
                print("[sdp] Will retry next interval...")

            print(f"\n[sdp] Next check in {watch_interval} minutes...")
            time.sleep(interval_seconds)
    else:
        run_pipeline(config_dir)


if __name__ == "__main__":
    main()
