"""
PostgreSQL ML profile orchestrator for reddit_data_tools.
Handles ingestion of ML classifier outputs into PostgreSQL tables.
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
    ensure_database_exists, ensure_schema_exists, ingest_classifier_csv,
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


def main():
    """Main entry point for postgres_ml profile."""
    config_dir = os.environ.get('CONFIG_DIR', '/app/config')
    
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
    type_inference_rows = proc_config.get('type_inference_rows', 1000)
    use_foreign_key = proc_config.get('use_foreign_key', True)
    
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
                
                ingest_classifier_csv(
                    csv_file=filepath,
                    data_type=data_type,
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
                success_count += 1
                
            except Exception as e:
                state.mark_failed(file_id, str(e))
                print(f"[{classifier_name.upper()}] ERROR {file_id}: {e}")
                fail_count += 1
        
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


if __name__ == "__main__":
    main()
