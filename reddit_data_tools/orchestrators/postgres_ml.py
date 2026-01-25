"""
PostgreSQL ML sidecar profile orchestrator for reddit_data_tools.
Handles ingestion of ML classifier outputs into PostgreSQL sidecar tables.
Expects classifier output CSVs to already exist (run ml/ml_cpu profiles first).
"""

import os
import re
import time
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
    table_exists, analyze_table, ensure_database_exists, ensure_schema_exists,
    ingest_sidecar_csv, rebuild_view_dynamic, get_column_list, load_services_config
)


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


def detect_sidecar_csvs(
    output_dir: str,
    sidecar_name: str,
    source_dir: str,
    suffix: str,
    data_types: List[str]
) -> List[Tuple[str, str, str]]:
    """
    Detect sidecar CSV files for a given classifier.
    
    Args:
        output_dir: Base output directory (/data/output)
        sidecar_name: Name of the sidecar (e.g., 'lingua')
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
                file_id = f"{sidecar_name}/{filepath.stem}"
                files.append((str(filepath), data_type, file_id))
    
    return sorted(files, key=lambda x: x[0])


def get_last_file_per_type(files: List[Tuple[str, str, str]]) -> Dict[str, str]:
    """Get the last (most recent) file for each data type."""
    last_files = {}
    for filepath, data_type, _ in files:
        last_files[data_type] = filepath
    return last_files


def main():
    """Main entry point for postgres_ml profile."""
    config_dir = os.environ.get('CONFIG_DIR', '/app/config')
    
    # Load configuration
    config = load_config(config_dir)
    
    # Print config summary
    print("="*60)
    print("POSTGRES ML SIDECAR INGESTION")
    print("="*60)
    print(f"[CONFIG] Profile: postgres_ml")
    
    # Extract configuration
    db_config = config.get('database', {})
    proc_config = config.get('processing', {})
    sidecars_config = config.get('sidecars', {})
    
    output_dir = proc_config.get('output_dir', '/data/output')
    data_types = proc_config.get('data_types', ['submissions', 'comments'])
    check_duplicates = proc_config.get('check_duplicates', True)
    type_inference_rows = proc_config.get('type_inference_rows', 1000)
    
    print(f"[CONFIG] Database: {db_config.get('name')}@{db_config.get('host')}:{db_config.get('port')}")
    print(f"[CONFIG] Schema: {db_config.get('schema')}")
    print(f"[CONFIG] Output dir: {output_dir}")
    print(f"[CONFIG] Data types: {data_types}")
    print(f"[CONFIG] Sidecars: {list(sidecars_config.keys())}")
    
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
    
    # Get base columns for each data type
    shared_config_dir = f"{config_dir}/shared"
    base_columns_cache = {}
    for data_type in data_types:
        base_columns_cache[data_type] = get_column_list(data_type, shared_config_dir)
    
    # Initialize state
    state_file = f"{proc_config.get('state_dir', '/data/database')}/postgres_ml_state.json"
    state = PipelineState(state_file)
    
    total_success = 0
    total_fail = 0
    total_skipped = 0
    tables_modified = set()
    
    start_time = time.time()
    
    # Process each sidecar
    for sidecar_name, sidecar_cfg in sidecars_config.items():
        if not sidecar_cfg.get('enabled', True):
            print(f"\n[{sidecar_name.upper()}] Skipped (disabled)")
            continue
        
        source_dir = sidecar_cfg.get('source_dir', sidecar_name)
        suffix = sidecar_cfg.get('suffix', f'_{sidecar_name}')
        table_columns = sidecar_cfg.get('table_columns', [])
        column_overrides = sidecar_cfg.get('column_overrides', {})
        
        print(f"\n{'='*60}")
        print(f"PROCESSING: {sidecar_name}")
        print(f"{'='*60}")
        print(f"[{sidecar_name.upper()}] Source: {output_dir}/{source_dir}")
        print(f"[{sidecar_name.upper()}] Suffix: {suffix}")
        print(f"[{sidecar_name.upper()}] Table columns: {table_columns}")
        
        # Detect CSV files
        files = detect_sidecar_csvs(output_dir, sidecar_name, source_dir, suffix, data_types)
        
        if not files:
            print(f"[{sidecar_name.upper()}] No CSV files found")
            continue
        
        print(f"[{sidecar_name.upper()}] Found {len(files)} CSV files")
        
        # Get last file per type for schema inference
        last_files = get_last_file_per_type(files)
        
        # Process each file
        success_count = 0
        fail_count = 0
        skip_count = 0
        
        for filepath, data_type, file_id in files:
            # Skip if already processed
            if state.is_processed(file_id):
                skip_count += 1
                continue
            
            try:
                state.mark_in_progress(file_id)
                
                ingest_sidecar_csv(
                    csv_file=filepath,
                    data_type=data_type,
                    sidecar_name=sidecar_name,
                    dbname=db_config['name'],
                    schema=db_config['schema'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    base_columns=base_columns_cache[data_type],
                    table_columns=table_columns,
                    check_duplicates=check_duplicates,
                    type_inference_rows=type_inference_rows,
                    column_overrides=column_overrides
                )
                
                state.mark_completed(file_id)
                success_count += 1
                tables_modified.add((data_type, sidecar_name))
                
            except Exception as e:
                state.mark_failed(file_id, str(e))
                print(f"[{sidecar_name.upper()}] ERROR {file_id}: {e}")
                fail_count += 1
        
        print(f"[{sidecar_name.upper()}] Completed: {success_count} success, {skip_count} skipped, {fail_count} failed")
        
        total_success += success_count
        total_fail += fail_count
        total_skipped += skip_count
    
    # Rebuild views for modified tables
    if tables_modified:
        print("\n" + "="*60)
        print("REBUILDING VIEWS")
        print("="*60)
        
        sidecar_config_dir = f"{config_dir}/postgres_ml"
        services_cfg = load_services_config(sidecar_config_dir)
        
        modified_data_types = set(dt for dt, _ in tables_modified)
        for data_type in modified_data_types:
            try:
                rebuild_view_dynamic(
                    data_type=data_type,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    sidecars_config=services_cfg.get('sidecars', {})
                )
            except Exception as e:
                print(f"[VIEW] Warning: Failed to rebuild view for {data_type}: {e}")
    
    # Final summary
    elapsed = (time.time() - start_time) / 60
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Successful: {total_success}")
    print(f"Skipped:    {total_skipped}")
    print(f"Failed:     {total_fail}")
    print(f"Time:       {elapsed:.2f} minutes")


if __name__ == "__main__":
    main()
