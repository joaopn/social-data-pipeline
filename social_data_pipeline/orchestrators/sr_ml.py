"""
StarRocks ML profile orchestrator for social_data_pipeline.
Handles ingestion of ML classifier outputs into StarRocks tables.
Expects classifier output files to already exist (run ml/lingua profiles first).

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple

from ..core.state import PipelineState
from ..core.config import (
    load_profile_config,
    load_platform_config as _load_platform_config,
    resolve_classifier_runs,
    apply_env_overrides,
    validate_processing_config,
    validate_starrocks_config,
)
from ..db.starrocks.ingest import (
    compute_bucket_count,
    ensure_database_exists,
    table_exists,
    ingest_file,
    analyze_table,
    execute_query,
    infer_classifier_schema,
    get_classifier_create_table_query,
)


def detect_classifier_csvs(
    output_dir: str,
    classifier_name: str,
    source_dir: str,
    suffix: str,
    data_types: List[str],
    file_patterns: Dict = None,
    file_format: str = 'csv'
) -> List[Tuple[str, str, str]]:
    """Detect classifier CSV/Parquet files for a given classifier.

    Returns list of (filepath, data_type, file_id) tuples, sorted by path.
    """
    base_path = Path(output_dir) / source_dir
    files = []

    ext = 'parquet' if file_format == 'parquet' else 'csv'

    patterns = {}
    for data_type in data_types:
        if file_patterns and data_type in file_patterns and file_format in file_patterns[data_type]:
            base_pattern = file_patterns[data_type][file_format]
            suffix_pattern = base_pattern.replace(rf'\.{ext}$', rf'{re.escape(suffix)}\.{ext}$')
            patterns[data_type] = re.compile(suffix_pattern)
        else:
            patterns[data_type] = re.compile(rf'^.+{re.escape(suffix)}\.{re.escape(ext)}$')

    for data_type in data_types:
        type_dir = base_path / data_type
        if not type_dir.is_dir():
            continue

        pattern = patterns.get(data_type)
        if not pattern:
            continue

        for filepath in type_dir.glob(f"*.{ext}"):
            if pattern.match(filepath.name):
                file_id = f"{classifier_name}/{filepath.stem}"
                files.append((str(filepath), data_type, file_id))

    return sorted(files, key=lambda x: x[0])


# Platform and source selection via environment variables
PLATFORM = os.environ.get('PLATFORM', 'reddit')
SOURCE = os.environ.get('SOURCE') or PLATFORM


def load_platform_config(config_dir: str) -> Dict:
    """Load platform configuration using centralized loader."""
    return _load_platform_config(config_dir, PLATFORM, source=SOURCE)


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """Load sr_ml profile configuration with env overrides and validation."""
    config = load_profile_config('sr_ml', config_dir, source=SOURCE, quiet=quiet)
    config = apply_env_overrides(config, 'sr_ml')
    validate_processing_config(config, 'sr_ml')
    validate_starrocks_config(config)
    return config


def run_pipeline(config_dir: str = "/app/config"):
    """Run the StarRocks ML classifier ingestion pipeline."""
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir)

    # Print config summary
    print("=" * 60)
    print("STARROCKS ML INGESTION")
    print("=" * 60)
    print("[sdp] Profile: sr_ml")
    print(f"[sdp] Platform: {PLATFORM}")

    db_config = config['database']
    password = db_config.get('password')
    proc_config = config.get('processing', {})
    # Optional ingestion-only overrides keyed by classifier name.
    # Supported: enabled (bool), source_dir (str), source_dir_ingest (lingua),
    # column_overrides (dict).
    ingestion_overrides = config.get('classifiers', {}) or {}

    output_dir = proc_config.get('output_dir', '/data/output')

    # Get data types from profile config, fall back to platform config
    data_types = proc_config.get('data_types', [])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    check_duplicates = proc_config.get('check_duplicates', True)
    parallel_ingestion = proc_config.get('parallel_ingestion', True)
    type_inference_rows = proc_config.get('type_inference_rows', 1000)

    # Read prefer_lingua from sr_ingest profile (mirrors PG pattern)
    try:
        sr_config = load_profile_config('sr_ingest', config_dir, source=SOURCE, quiet=True)
        prefer_lingua = sr_config.get('processing', {}).get('prefer_lingua', True)
    except Exception:
        prefer_lingua = True

    file_format = platform_config.get('file_format', 'csv')

    # Primary key and upsert ordering from platform config
    pk_column = platform_config.get('primary_key')
    order_field = platform_config.get('upsert_order_field')

    # Database name = source name (database-per-source, like sr_ingest)
    database = SOURCE

    # Resolve classifier runs from source's ml/lingua profiles + ingestion overrides.
    classifier_runs = resolve_classifier_runs(
        config_dir=config_dir,
        source=SOURCE,
        ingestion_overrides=ingestion_overrides,
        prefer_lingua=prefer_lingua,
    )

    single_classifier = os.environ.get('CLASSIFIER', '')
    if single_classifier:
        names = [run['name'] for run in classifier_runs]
        if single_classifier not in names:
            print(f"[sdp] CLASSIFIER='{single_classifier}' not in classifiers list: {names}")
            sys.exit(1)
        classifier_runs = [run for run in classifier_runs if run['name'] == single_classifier]
        print(f"[sdp] Running single classifier: {single_classifier}")

    print(f"[sdp] Database: {database}")
    print(f"[sdp] Output dir: {output_dir}")
    print(f"[sdp] Data types: {data_types}")
    print("[sdp] Classifiers:")
    for run in classifier_runs:
        scope = "all" if run['data_types'] is None else ",".join(run['data_types'])
        print(f"[sdp]   - {run['name']} (suffix: {run['suffix']}, data_types: {scope})")
    print(f"[sdp] Prefer lingua: {prefer_lingua} (from sr_ingest profile)")

    # Ensure database exists
    ensure_database_exists(
        database=database,
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=password,
    )

    # Initialize state files per data_type
    state_base = os.environ.get('STARROCKS_DATA_PATH', '/data/database')
    state_dir = f"{state_base}/state_tracking"
    os.makedirs(state_dir, exist_ok=True)

    states = {}
    for dt in data_types:
        state_file = f"{state_dir}/{SOURCE}_sr_ml_{dt}.json"
        states[dt] = PipelineState(state_file)

    total_success = 0
    total_fail = 0
    total_skipped = 0

    start_time = time.time()

    # Process each classifier
    for run in classifier_runs:
        classifier_name = run['name']
        source_dir = run['source_dir']
        suffix = run['suffix']
        column_overrides = run['column_overrides']
        scope = run['data_types']
        # Restrict file detection to the classifier's data_types scope.
        run_data_types = data_types if scope is None else [dt for dt in data_types if dt in set(scope)]

        scope_label = "all" if scope is None else ",".join(scope)
        print(f"\n{'=' * 60}")
        print(f"PROCESSING: {classifier_name} [data_types={scope_label}]")
        print(f"{'=' * 60}")
        print(f"[sdp] Source: {output_dir}/{source_dir}")
        print(f"[sdp] Suffix: {suffix}")

        # Detect classifier files
        files = detect_classifier_csvs(output_dir, classifier_name, source_dir, suffix, run_data_types, file_format=file_format)

        if not files:
            print(f"[sdp] {classifier_name}: No classified files found")
            continue

        print(f"[sdp] {classifier_name}: Found {len(files)} files")

        # Filter out already processed files
        pending_files = [(fp, dt, fid) for fp, dt, fid in files if not states[dt].is_processed(fid)]
        skip_count = len(files) - len(pending_files)

        if not pending_files:
            print(f"[sdp] {classifier_name}: All files already processed, skipping")
            total_skipped += skip_count
            continue

        # Group files by data_type
        files_by_type = {}
        for filepath, data_type, file_id in pending_files:
            if data_type not in files_by_type:
                files_by_type[data_type] = []
            files_by_type[data_type].append((filepath, file_id))

        success_count = 0
        fail_count = 0

        def ingest_classifier_type(dt):
            """Ingest classifier files for a single data type. Returns (success, fail)."""
            type_files = files_by_type[dt]
            table_name = f"{dt}{suffix}"
            local_success = 0
            local_fail = 0

            print(f"\n[sdp] Processing {table_name}: {len(type_files)} files")

            # Infer schema from first file
            first_file = type_files[0][0]
            print(f"[sdp] Inferring schema from {Path(first_file).name}...")
            column_list, column_types, _ = infer_classifier_schema(
                first_file, type_inference_rows, column_overrides
            )
            print(f"[sdp] Inferred {len(column_list)} columns")

            # Create table if not exists
            if not table_exists(
                table=table_name,
                database=database,
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=password,
            ):
                buckets = compute_bucket_count(platform_config, dt)
                buckets_label = buckets if buckets is not None else "auto"
                print(f"[sdp] Creating table {database}.{table_name} (BUCKETS {buckets_label})")
                create_query = get_classifier_create_table_query(
                    table=table_name,
                    database=database,
                    column_list=column_list,
                    column_types=column_types,
                    pk_column=pk_column,
                    buckets=buckets,
                )
                execute_query(
                    create_query,
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    password=password,
                )

            # Ingest each file
            for filepath, file_id in type_files:
                try:
                    states[dt].mark_in_progress(file_id)

                    row_count = ingest_file(
                        table=table_name,
                        database=database,
                        columns_list=column_list,
                        file_path=filepath,
                        file_format=file_format,
                        check_duplicates=check_duplicates,
                        order_field=order_field,
                        host=db_config['host'],
                        port=db_config['port'],
                        user=db_config['user'],
                        password=password,
                    )
                    print(f"[sdp] Loaded {row_count} rows from {Path(filepath).name}")

                    states[dt].mark_completed(file_id)
                    local_success += 1

                except Exception as e:
                    states[dt].mark_failed(file_id, str(e))
                    print(f"[sdp] ERROR {file_id}: {e}")
                    local_fail += 1

            # Analyze table after loading
            if local_success > 0:
                try:
                    analyze_table(
                        table=table_name,
                        database=database,
                        host=db_config['host'],
                        port=db_config['port'],
                        user=db_config['user'],
                        password=password,
                    )
                    print(f"[sdp] Analyzed {table_name}")
                except Exception as e:
                    print(f"[sdp] Warning: Failed to analyze {table_name}: {e}")

            return local_success, local_fail

        # Parallel ingestion for submissions + comments
        use_parallel = (
            parallel_ingestion
            and 'submissions' in files_by_type
            and 'comments' in files_by_type
        )

        if use_parallel:
            print("[sdp] Parallel ingestion enabled (submissions + comments concurrently)")

            with ThreadPoolExecutor(max_workers=2) as executor:
                future_submissions = executor.submit(ingest_classifier_type, 'submissions')
                future_comments = executor.submit(ingest_classifier_type, 'comments')

                sub_success, sub_fail = future_submissions.result()
                com_success, com_fail = future_comments.result()
                success_count += sub_success + com_success
                fail_count += sub_fail + com_fail
        else:
            for dt in sorted(files_by_type.keys()):
                local_success, local_fail = ingest_classifier_type(dt)
                success_count += local_success
                fail_count += local_fail

        print(f"[sdp] {classifier_name}: {success_count} success, {skip_count} skipped, {fail_count} failed")

        total_success += success_count
        total_fail += fail_count
        total_skipped += skip_count

    # Final summary
    elapsed = (time.time() - start_time) / 60
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print(f"Successful: {total_success}")
    print(f"Skipped:    {total_skipped}")
    print(f"Failed:     {total_fail}")
    print(f"Time:       {elapsed:.2f} minutes")


def main():
    """Main entry point with optional watch mode."""
    config_dir = "/app/config"
    config = load_config(config_dir)
    watch_interval = config.get('processing', {}).get('watch_interval', 0)

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
