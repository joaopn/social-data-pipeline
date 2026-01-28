"""
PostgreSQL ML profile orchestrator for reddit_data_tools.
Handles ingestion of ML classifier outputs into PostgreSQL tables.
Expects classifier output CSVs to already exist (run ml/ml_cpu profiles first).
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
    apply_env_overrides,
    validate_database_config,
    ConfigurationError,
)
from ..db.postgres.ingest import (
    ensure_database_exists, ensure_schema_exists, ingest_classifier_csv,
    table_exists, infer_classifier_schema,
    # Fast initial load functions
    create_fast_load_classifier_table, fast_ingest_classifier_csv,
    delete_duplicates, finalize_fast_load_table,
)
from ..core.config import load_yaml_file


def load_services_config(config_dir: str) -> Dict:
    """
    Load services.yaml configuration.
    
    Args:
        config_dir: Directory containing services.yaml (postgres_ml profile directory)
        
    Returns:
        Services configuration dictionary
        
    Raises:
        ConfigurationError: If services.yaml is missing
    """
    config = load_yaml_file(Path(config_dir) / "services.yaml")
    if config is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/services.yaml")
    return config


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
    config = load_profile_config('postgres_ml', config_dir, quiet)
    
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
    data_types: List[str]
) -> List[Tuple[str, str, str]]:
    """
    Detect classifier CSV files for a given classifier.
    
    Args:
        output_dir: Base output directory (/data/output)
        classifier_name: Name of the classifier (e.g., 'lingua')
        source_dir: Subdirectory under output_dir
        suffix: File suffix pattern (e.g., '_lingua')
        data_types: List of data types to look for
        
    Returns:
        List of (filepath, data_type, file_id) tuples
    """
    base_path = Path(output_dir) / source_dir
    files = []
    
    patterns = {
        'submissions': re.compile(rf'^RS_(\d{{4}}-\d{{2}}){re.escape(suffix)}\.csv$'),
        'comments': re.compile(rf'^RC_(\d{{4}}-\d{{2}}){re.escape(suffix)}\.csv$')
    }
    
    for data_type in data_types:
        if data_type not in patterns:
            continue
            
        type_dir = base_path / data_type
        if not type_dir.is_dir():
            continue
            
        pattern = patterns[data_type]
        for filepath in type_dir.glob("*.csv"):
            match = pattern.match(filepath.name)
            if match:
                date_str = match.group(1)
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
    
    # Print config summary
    print("="*60)
    print("POSTGRES ML INGESTION")
    print("="*60)
    print(f"[CONFIG] Profile: postgres_ml")
    
    # Extract configuration
    db_config = config.get('database', {})
    proc_config = config.get('processing', {})
    classifiers_config = config.get('classifiers', {})
    
    output_dir = proc_config.get('output_dir', '/data/output')
    data_types = proc_config.get('data_types', ['submissions', 'comments'])
    check_duplicates = proc_config.get('check_duplicates', True)
    parallel_ingestion = proc_config.get('parallel_ingestion', True)
    type_inference_rows = proc_config.get('type_inference_rows', 1000)
    use_foreign_key = proc_config.get('use_foreign_key', True)
    fast_initial_load = proc_config.get('fast_initial_load', False)
    
    # Read prefer_lingua from postgres profile (controls lingua ingestion behavior)
    try:
        postgres_config = load_profile_config('postgres_ingest', config_dir, quiet=True)
        prefer_lingua = postgres_config.get('processing', {}).get('prefer_lingua', True)
    except Exception:
        prefer_lingua = True  # Default to true if postgres config not found
    
    print(f"[CONFIG] Database: {db_config.get('name')}@{db_config.get('host')}:{db_config.get('port')}")
    print(f"[CONFIG] Schema: {db_config.get('schema')}")
    print(f"[CONFIG] Output dir: {output_dir}")
    print(f"[CONFIG] Data types: {data_types}")
    print(f"[CONFIG] Classifiers: {list(classifiers_config.keys())}")
    print(f"[CONFIG] Use foreign key: {use_foreign_key}")
    print(f"[CONFIG] Fast initial load: {fast_initial_load}")
    print(f"[CONFIG] Prefer lingua: {prefer_lingua} (from postgres profile)")
    
    # Ensure database and schema exist
    ensure_database_exists(
        dbname=db_config['name'],
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user']
    )
    ensure_schema_exists(
        schema=db_config['schema'],
        dbname=db_config['name'],
        host=db_config['host'],
        port=db_config['port'],
        user=db_config['user']
    )
    
    # Initialize state
    state_file = f"{proc_config.get('state_dir', '/data/database')}/postgres_ml_state.json"
    state = PipelineState(state_file)
    
    total_success = 0
    total_fail = 0
    total_skipped = 0
    
    start_time = time.time()
    
    # Process each classifier
    for classifier_name, classifier_cfg in classifiers_config.items():
        if not classifier_cfg.get('enabled', True):
            print(f"\n[{classifier_name.upper()}] Skipped (disabled)")
            continue
        
        # Handle lingua classifier specially based on prefer_lingua setting
        if classifier_name == 'lingua':
            if prefer_lingua:
                # Lingua data is already in main table via postgres profile
                print(f"\n[{classifier_name.upper()}] Skipped (prefer_lingua=true, data in main table)")
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
        print(f"[{classifier_name.upper()}] Source: {output_dir}/{source_dir}")
        print(f"[{classifier_name.upper()}] Suffix: {suffix}")
        
        # Detect CSV files
        files = detect_classifier_csvs(output_dir, classifier_name, source_dir, suffix, data_types)
        
        if not files:
            print(f"[{classifier_name.upper()}] No CSV files found")
            continue
        
        print(f"[{classifier_name.upper()}] Found {len(files)} CSV files")
        
        # Filter out already processed files
        pending_files = [(fp, dt, fid) for fp, dt, fid in files if not state.is_processed(fid)]
        skip_count = len(files) - len(pending_files)
        
        if not pending_files:
            print(f"[{classifier_name.upper()}] All files already processed, skipping")
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
                user=db_config['user']
            )
        
        # Determine fast load eligibility per data_type
        fast_load_types = set()
        standard_load_types = set()
        
        if fast_initial_load:
            for dt in files_by_type.keys():
                if not tables_existed[dt]:
                    fast_load_types.add(dt)
                else:
                    standard_load_types.add(dt)
            
            if fast_load_types:
                print(f"[{classifier_name.upper()}] Fast initial load for: {', '.join(sorted(fast_load_types))}")
                print(f"[{classifier_name.upper()}] WARNING: If process fails, tables must be recreated!")
            if standard_load_types:
                print(f"[{classifier_name.upper()}] Standard ON CONFLICT for: {', '.join(sorted(standard_load_types))}")
        else:
            standard_load_types = set(files_by_type.keys())
        
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
                
                print(f"\n[{classifier_name.upper()}] Fast loading {table_name}: {len(type_files)} files")
                
                # Get first CSV to infer schema
                first_csv = type_files[0][0]
                
                # Step 1: Infer schema from CSV
                print(f"[{classifier_name.upper()}] Inferring schema from {type_inference_rows} rows...")
                column_list, column_types, nullable_cols = infer_classifier_schema(
                    first_csv, type_inference_rows, column_overrides
                )
                print(f"[{classifier_name.upper()}] Inferred columns: {column_list}")
                
                # Step 2: Create UNLOGGED table (no PK, no FK)
                create_fast_load_classifier_table(
                    table_name=table_name,
                    data_type=dt,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    column_list=column_list,
                    column_types=column_types
                )
                
                # Step 3: Blind COPY all files
                for filepath, file_id in type_files:
                    try:
                        state.mark_in_progress(file_id)
                        fast_ingest_classifier_csv(
                            csv_file=filepath,
                            table_name=table_name,
                            schema=db_config['schema'],
                            dbname=db_config['name'],
                            host=db_config['host'],
                            port=db_config['port'],
                            user=db_config['user'],
                            column_list=column_list,
                            nullable_cols=nullable_cols
                        )
                        state.mark_completed(file_id)
                        local_success += 1
                    except Exception as e:
                        print(f"[{classifier_name.upper()}] ERROR during COPY {file_id}: {e}")
                        state.mark_failed(file_id, str(e))
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
                    order_column=order_col
                )
                
                # Step 5: Finalize (add PK, add FK if enabled, VACUUM FREEZE, SET LOGGED)
                finalize_fast_load_table(
                    table=table_name,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    fk_reference_table=dt if use_foreign_key else None
                )
                
                print(f"[{classifier_name.upper()}] Fast load completed for {table_name}")
                return local_success, local_fail
            
            if use_parallel_fast_load:
                print(f"[{classifier_name.upper()}] Parallel ingestion enabled (submissions + comments concurrently)")
                
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_submissions = executor.submit(fast_load_classifier_type, 'submissions')
                    future_comments = executor.submit(fast_load_classifier_type, 'comments')
                    
                    try:
                        sub_success, sub_fail = future_submissions.result()
                        com_success, com_fail = future_comments.result()
                        success_count += sub_success + com_success
                        fail_count += sub_fail + com_fail
                    except Exception as e:
                        print(f"[{classifier_name.upper()}] CRITICAL ERROR: {e}")
                        print(f"[{classifier_name.upper()}] Tables may be in inconsistent state. Manual recovery required.")
                        raise
            else:
                for dt in sorted(fast_load_types):
                    try:
                        local_success, local_fail = fast_load_classifier_type(dt)
                        success_count += local_success
                        fail_count += local_fail
                    except Exception as e:
                        print(f"[{classifier_name.upper()}] CRITICAL ERROR for {dt}{suffix}: {e}")
                        print(f"[{classifier_name.upper()}] Table may be in inconsistent state. Manual recovery required.")
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
                        state.mark_in_progress(file_id)
                        
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
                            suffix=suffix
                        )
                        
                        state.mark_completed(file_id)
                        local_success += 1
                        
                    except Exception as e:
                        state.mark_failed(file_id, str(e))
                        print(f"[{classifier_name.upper()}] ERROR {file_id}: {e}")
                        local_fail += 1
                
                return local_success, local_fail
            
            if use_parallel_standard:
                print(f"[{classifier_name.upper()}] Parallel ingestion enabled (submissions + comments concurrently)")
                
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
        
        print(f"[{classifier_name.upper()}] Completed: {success_count} success, {skip_count} skipped, {fail_count} failed")
        
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
        print(f"[WATCH] Watch mode enabled: checking every {watch_interval} minutes")
        interval_seconds = watch_interval * 60
        while True:
            try:
                run_pipeline(config_dir)
            except Exception as e:
                print(f"[WATCH] Pipeline error: {e}")
                print("[WATCH] Will retry next interval...")
            
            print(f"\n[WATCH] Next check in {watch_interval} minutes...")
            time.sleep(interval_seconds)
    else:
        run_pipeline(config_dir)


if __name__ == "__main__":
    main()
