"""
PostgreSQL ML profile orchestrator for social_data_bridge.
Handles ingestion of ML classifier outputs into PostgreSQL tables.
Expects classifier output CSVs to already exist (run ml/ml_cpu profiles first).

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from ..core.state import PipelineState
from ..core.config import (
    load_profile_config,
    load_platform_config as _load_platform_config,
    apply_env_overrides,
    validate_database_config,
    ConfigurationError,
)
from ..db.postgres.ingest import (
    ensure_database_exists, ensure_schema_exists, ensure_tablespaces, resolve_tablespace,
    ingest_classifier_csv, table_exists, infer_classifier_schema,
    # Fast initial load functions
    create_fast_load_classifier_table, fast_ingest_classifier_csv,
    delete_duplicates, finalize_fast_load_table,
)


# Platform and source selection via environment variables
PLATFORM = os.environ.get('PLATFORM', 'reddit')
SOURCE = os.environ.get('SOURCE') or PLATFORM


def load_platform_config(config_dir: str) -> Dict:
    """Load platform configuration using centralized loader."""
    return _load_platform_config(config_dir, PLATFORM, source=SOURCE)


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """
    Load postgres_ml profile configuration.
    
    Args:
        config_dir: Base configuration directory
        quiet: If True, suppress informational output
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    config = load_profile_config('postgres_ml', config_dir, source=SOURCE, quiet=quiet)
    
    # Apply environment variable overrides for database settings
    config = apply_env_overrides(config, 'postgres_ml')
    
    # Validate required config
    validate_database_config(config)
    
    return config


def detect_classifier_csvs(
    output_dir: str,
    classifier_name: str,
    source_dir: str,
    suffix: str,
    data_types: List[str],
    file_patterns: Dict = None
) -> List[Tuple[str, str, str]]:
    """
    Detect classifier CSV files for a given classifier.
    
    Args:
        output_dir: Base output directory (/data/output)
        classifier_name: Name of the classifier (e.g., 'lingua')
        source_dir: Subdirectory under output_dir
        suffix: File suffix pattern (e.g., '_lingua')
        data_types: List of data types to look for
        file_patterns: Optional dict of file patterns per data type from platform config
        
    Returns:
        List of (filepath, data_type, file_id) tuples
    """
    base_path = Path(output_dir) / source_dir
    files = []
    
    # Build patterns - use platform config if available, otherwise match any CSV with suffix
    patterns = {}
    for data_type in data_types:
        if file_patterns and data_type in file_patterns and 'csv' in file_patterns[data_type]:
            # Modify the platform pattern to include the classifier suffix
            base_pattern = file_patterns[data_type]['csv']
            # Replace .csv$ with {suffix}.csv$
            suffix_pattern = base_pattern.replace(r'\.csv$', rf'{re.escape(suffix)}\.csv$')
            patterns[data_type] = re.compile(suffix_pattern)
        else:
            # Fallback: match any file ending with suffix.csv
            patterns[data_type] = re.compile(rf'^.+{re.escape(suffix)}\.csv$')
    
    for data_type in data_types:
        type_dir = base_path / data_type
        if not type_dir.is_dir():
            continue
            
        pattern = patterns.get(data_type)
        if not pattern:
            continue
            
        for filepath in type_dir.glob("*.csv"):
            match = pattern.match(filepath.name)
            if match:
                file_id = f"{classifier_name}/{filepath.stem}"
                files.append((str(filepath), data_type, file_id))
    
    return sorted(files, key=lambda x: x[0])


def run_pipeline(config_dir: str = "/app/config"):
    """
    Run the postgres_ml ingestion pipeline.
    
    Args:
        config_dir: Base configuration directory
    """
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir)
    
    # Print config summary
    print("="*60)
    print("POSTGRES ML INGESTION")
    print("="*60)
    print(f"[sdb] Profile: postgres_ml")
    print(f"[sdb] Platform: {PLATFORM}")
    
    # Extract configuration
    db_config = config.get('database', {})
    password = db_config.get('password')
    proc_config = config.get('processing', {})
    classifiers_config = config.get('classifiers', {})
    
    # Get db_schema from profile config, fall back to platform config
    db_schema = db_config.get('schema')
    if not db_schema:
        db_schema = platform_config.get('db_schema')
    if not db_schema:
        raise ConfigurationError("No db_schema configured. Set in platform config or user.yaml.")
    db_config['schema'] = db_schema
    
    output_dir = proc_config.get('output_dir', '/data/output')
    
    # Get data types from profile config, fall back to platform config
    data_types = proc_config.get('data_types', [])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    check_duplicates = proc_config.get('check_duplicates', True)
    parallel_ingestion = proc_config.get('parallel_ingestion', True)
    type_inference_rows = proc_config.get('type_inference_rows', 1000)
    use_foreign_key = proc_config.get('use_foreign_key', True)
    # Read settings from postgres profile (tablespaces, prefer_lingua)
    try:
        postgres_config = load_profile_config('postgres_ingest', config_dir, source=SOURCE, quiet=True)
        prefer_lingua = postgres_config.get('processing', {}).get('prefer_lingua', True)
        tablespaces = postgres_config.get('tablespaces', {})
        table_tablespaces = postgres_config.get('table_tablespaces', {})
    except Exception:
        prefer_lingua = True  # Default to true if postgres config not found
        tablespaces = {}
        table_tablespaces = {}

    def get_tablespace(data_type):
        return resolve_tablespace(table_tablespaces.get(data_type))

    print(f"[sdb] Database: {db_config.get('name')}@{db_config.get('host')}:{db_config.get('port')}")
    print(f"[sdb] Schema: {db_config.get('schema')}")
    print(f"[sdb] Output dir: {output_dir}")
    print(f"[sdb] Data types: {data_types}")
    print(f"[sdb] Classifiers: {list(classifiers_config.keys())}")
    print(f"[sdb] Use foreign key: {use_foreign_key}")
    print(f"[sdb] Prefer lingua: {prefer_lingua} (from postgres profile)")
    if table_tablespaces:
        print(f"[sdb] Tablespace assignments: {table_tablespaces}")

    # Ensure database and schema exist
    ensure_database_exists(
        dbname=db_config['name'],
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=password
    )
    ensure_schema_exists(
        schema=db_config['schema'],
        dbname=db_config['name'],
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user'],
        password=password
    )

    # Create tablespaces if configured
    if tablespaces:
        ensure_tablespaces(
            tablespaces=tablespaces,
            dbname=db_config['name'],
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user'],
            password=password
        )

    # Initialize state files per data_type
    pgdata_path = os.environ.get('PGDATA_PATH', '/data/database')
    state_dir = f"{pgdata_path}/state_tracking"
    os.makedirs(state_dir, exist_ok=True)

    states = {}
    for dt in data_types:
        state_file = f"{state_dir}/{SOURCE}_postgres_ml_{dt}.json"
        states[dt] = PipelineState(state_file)
    
    total_success = 0
    total_fail = 0
    total_skipped = 0
    
    start_time = time.time()
    
    # Process each classifier
    for classifier_name, classifier_cfg in classifiers_config.items():
        if not classifier_cfg.get('enabled', True):
            print(f"\n[sdb] {classifier_name}: Skipped (disabled)")
            continue
        
        # Handle lingua classifier specially based on prefer_lingua setting
        if classifier_name == 'lingua':
            if prefer_lingua:
                # Lingua data is already in main table via postgres profile
                print(f"\n[sdb] {classifier_name}: Skipped (prefer_lingua=true, data in main table)")
                continue
            else:
                # Use lingua_ingest folder (minimal CSV for independent ingestion)
                source_dir = classifier_cfg.get('source_dir_ingest', 'lingua_ingest')
        else:
            source_dir = classifier_cfg.get('source_dir', classifier_name)
        
        suffix = classifier_cfg.get('suffix', f'_{classifier_name}')
        column_overrides = classifier_cfg.get('column_overrides', {})
        
        print(f"\n{'='*60}")
        print(f"PROCESSING: {classifier_name}")
        print(f"{'='*60}")
        print(f"[sdb] Source: {output_dir}/{source_dir}")
        print(f"[sdb] Suffix: {suffix}")
        
        # Detect CSV files
        files = detect_classifier_csvs(output_dir, classifier_name, source_dir, suffix, data_types)
        
        if not files:
            print(f"[sdb] {classifier_name}: No CSV files found")
            continue
        
        print(f"[sdb] {classifier_name}: Found {len(files)} CSV files")
        
        # Filter out already processed files
        pending_files = [(fp, dt, fid) for fp, dt, fid in files if not states[dt].is_processed(fid)]
        skip_count = len(files) - len(pending_files)
        
        if not pending_files:
            print(f"[sdb] {classifier_name}: All files already processed, skipping")
            total_skipped += skip_count
            continue
        
        # Group files by data_type
        files_by_type = {}
        for filepath, data_type, file_id in pending_files:
            if data_type not in files_by_type:
                files_by_type[data_type] = []
            files_by_type[data_type].append((filepath, file_id))
        
        # Check which tables exist for this classifier
        tables_existed = {}
        for dt in files_by_type.keys():
            table_name = f"{dt}{suffix}"
            tables_existed[dt] = table_exists(
                table=table_name,
                schema=db_config['schema'],
                dbname=db_config['name'],
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user'],
                password=password
            )
        
        # Determine load strategy per data type:
        # New tables use fast load (deferred PK, blind COPY, post-load dedup)
        # Existing tables use standard ON CONFLICT upsert
        fast_load_types = set()
        standard_load_types = set()

        for dt in files_by_type.keys():
            if not tables_existed[dt]:
                fast_load_types.add(dt)
            else:
                standard_load_types.add(dt)

        if fast_load_types:
            print(f"[sdb] Fast initial load for: {', '.join(sorted(fast_load_types))}")
        if standard_load_types:
            print(f"[sdb] Standard ON CONFLICT for: {', '.join(sorted(standard_load_types))}")
        
        success_count = 0
        fail_count = 0
        
        # =====================================================================
        # FAST INITIAL LOAD PATH (for classifier tables)
        # =====================================================================
        if fast_load_types:
            use_parallel_fast_load = (
                parallel_ingestion
                and 'submissions' in fast_load_types
                and 'comments' in fast_load_types
            )
            
            def fast_load_classifier_type(dt):
                """Fast load a single classifier data type. Returns (success_count, fail_count)."""
                type_files = files_by_type[dt]
                table_name = f"{dt}{suffix}"
                local_success = 0
                local_fail = 0
                
                print(f"\n[sdb] Fast loading {table_name}: {len(type_files)} files")
                
                # Get first CSV to infer schema
                first_csv = type_files[0][0]
                
                # Step 1: Infer schema from CSV
                print(f"[sdb] Inferring schema from {type_inference_rows} rows...")
                column_list, column_types, nullable_cols = infer_classifier_schema(
                    first_csv, type_inference_rows, column_overrides
                )
                print(f"[sdb] Inferred columns: {column_list}")
                
                # Step 2: Create table (no PK, no FK)
                create_fast_load_classifier_table(
                    table_name=table_name,
                    data_type=dt,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    column_list=column_list,
                    column_types=column_types,
                    tablespace=get_tablespace(dt),
                    password=password
                )
                
                # Step 3: Blind COPY all files
                for filepath, file_id in type_files:
                    try:
                        states[dt].mark_in_progress(file_id)
                        fast_ingest_classifier_csv(
                            csv_file=filepath,
                            table_name=table_name,
                            schema=db_config['schema'],
                            dbname=db_config['name'],
                            host=db_config['host'],
                            port=db_config['port'],
                            user=db_config['user'],
                            column_list=column_list,
                            nullable_cols=nullable_cols,
                            password=password
                        )
                        states[dt].mark_completed(file_id)
                        local_success += 1
                    except Exception as e:
                        print(f"[sdb] ERROR during COPY {file_id}: {e}")
                        states[dt].mark_failed(file_id, str(e))
                        local_fail += 1
                        raise  # Abort fast load on any failure
                
                # Step 4: Delete duplicates
                # Determine order column (retrieved_utc if present, else None)
                order_col = 'retrieved_utc' if 'retrieved_utc' in column_list else None
                delete_duplicates(
                    table=table_name,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    order_column=order_col,
                    password=password
                )
                
                # Step 5: Finalize (add PK, add FK if enabled)
                finalize_fast_load_table(
                    table=table_name,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    fk_reference_table=dt if use_foreign_key else None,
                    tablespace=get_tablespace(dt),
                    password=password
                )
                
                print(f"[sdb] Fast load completed for {table_name}")
                return local_success, local_fail
            
            if use_parallel_fast_load:
                print(f"[sdb] Parallel ingestion enabled (submissions + comments concurrently)")
                
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_submissions = executor.submit(fast_load_classifier_type, 'submissions')
                    future_comments = executor.submit(fast_load_classifier_type, 'comments')
                    
                    try:
                        sub_success, sub_fail = future_submissions.result()
                        com_success, com_fail = future_comments.result()
                        success_count += sub_success + com_success
                        fail_count += sub_fail + com_fail
                    except Exception as e:
                        print(f"[sdb] CRITICAL ERROR: {e}")
                        print(f"[sdb] Tables may be in inconsistent state. Manual recovery required.")
                        raise
            else:
                for dt in sorted(fast_load_types):
                    try:
                        local_success, local_fail = fast_load_classifier_type(dt)
                        success_count += local_success
                        fail_count += local_fail
                    except Exception as e:
                        print(f"[sdb] CRITICAL ERROR for {dt}{suffix}: {e}")
                        print(f"[sdb] Table may be in inconsistent state. Manual recovery required.")
                        raise
        
        # =====================================================================
        # STANDARD ON CONFLICT PATH
        # =====================================================================
        if standard_load_types:
            use_parallel_standard = (
                parallel_ingestion
                and 'submissions' in standard_load_types
                and 'comments' in standard_load_types
            )
            
            def ingest_classifier_type_files(dt):
                """Ingest files for a single data type. Returns (success_count, fail_count)."""
                type_files = files_by_type[dt]
                local_success = 0
                local_fail = 0

                for filepath, file_id in type_files:
                    try:
                        states[dt].mark_in_progress(file_id)

                        ingest_classifier_csv(
                            csv_file=filepath,
                            data_type=dt,
                            classifier_name=classifier_name,
                            dbname=db_config['name'],
                            schema=db_config['schema'],
                            host=db_config['host'],
                            port=db_config['port'],
                            user=db_config['user'],
                            check_duplicates=check_duplicates,
                            type_inference_rows=type_inference_rows,
                            column_overrides=column_overrides,
                            use_foreign_key=use_foreign_key,
                            suffix=suffix,
                            password=password
                        )

                        states[dt].mark_completed(file_id)
                        local_success += 1

                    except Exception as e:
                        states[dt].mark_failed(file_id, str(e))
                        print(f"[sdb] ERROR {file_id}: {e}")
                        local_fail += 1

                return local_success, local_fail
            
            if use_parallel_standard:
                print(f"[sdb] Parallel ingestion enabled (submissions + comments concurrently)")
                
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_submissions = executor.submit(ingest_classifier_type_files, 'submissions')
                    future_comments = executor.submit(ingest_classifier_type_files, 'comments')
                    
                    sub_success, sub_fail = future_submissions.result()
                    com_success, com_fail = future_comments.result()
                    success_count += sub_success + com_success
                    fail_count += sub_fail + com_fail
            else:
                for dt in sorted(standard_load_types):
                    local_success, local_fail = ingest_classifier_type_files(dt)
                    success_count += local_success
                    fail_count += local_fail
        
        print(f"[sdb] {classifier_name}: {success_count} success, {skip_count} skipped, {fail_count} failed")
        
        total_success += success_count
        total_fail += fail_count
        total_skipped += skip_count
    
    # Final summary
    elapsed = (time.time() - start_time) / 60
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Successful: {total_success}")
    print(f"Skipped:    {total_skipped}")
    print(f"Failed:     {total_fail}")
    print(f"Time:       {elapsed:.2f} minutes")


def main():
    """Main entry point with optional watch mode."""
    config_dir = os.environ.get('CONFIG_DIR', '/app/config')
    config = load_config(config_dir)
    watch_interval = config.get('processing', {}).get('watch_interval', 0)
    
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
