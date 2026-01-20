"""
PostgreSQL database profile orchestrator for reddit_data_tools.
Handles CSV ingestion into PostgreSQL with indexing and view management.
Expects CSV files to already exist (run parse profile first).
"""

import os
import re
import time
import yaml
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict, Tuple

from ..core.state import PipelineState
from ..core.decompress import decompress_zst
from ..core.parse_csv import parse_to_csv, parse_files_parallel
from ..db.postgres.ingest import (
    ingest_csv, create_index, table_exists, analyze_table, rebuild_view,
    ensure_database_exists, ensure_schema_exists
)


def load_config(config_path: str = "/app/config/db_pg/pipeline.yaml") -> Dict:
    """Load pipeline configuration from YAML, checking for .local.yaml override first."""
    config_path = Path(config_path)
    local_path = config_path.with_suffix('.local.yaml')
    if local_path.exists():
        config_path = local_path
        print(f"[CONFIG] Using local override: {local_path.name}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Override database settings from environment variables
    if 'POSTGRES_PORT' in os.environ:
        config['database']['port'] = int(os.environ['POSTGRES_PORT'])
    if 'DB_NAME' in os.environ:
        config['database']['name'] = os.environ['DB_NAME']
    if 'DB_SCHEMA' in os.environ:
        config['database']['schema'] = os.environ['DB_SCHEMA']
    
    return config


def detect_dump_files(dumps_dir: str, data_types: List[str]) -> List[Tuple[str, str]]:
    """Detect .zst dump files in the dumps directory and subfolders."""
    dumps_path = Path(dumps_dir)
    files = []
    
    patterns = {
        'submissions': re.compile(r'^RS_(\d{4}-\d{2})\.zst$'),
        'comments': re.compile(r'^RC_(\d{4}-\d{2})\.zst$')
    }
    
    search_dirs = [dumps_path]
    for subfolder in ['submissions', 'comments']:
        subfolder_path = dumps_path / subfolder
        if subfolder_path.is_dir():
            search_dirs.append(subfolder_path)
    
    for search_dir in search_dirs:
        for filepath in search_dir.glob("*.zst"):
            filename = filepath.name
            
            for data_type in data_types:
                if data_type not in patterns:
                    continue
                    
                match = patterns[data_type].match(filename)
                if match:
                    date_str = match.group(1)
                    files.append((str(filepath), data_type, date_str))
                    break
    
    files.sort(key=lambda x: (x[2], x[1]))
    
    return [(f[0], f[1]) for f in files]


def get_file_identifier(filepath: str) -> str:
    """Extract identifier from filepath for state tracking."""
    return Path(filepath).stem


def detect_json_files(extracted_dir: str, data_types: List[str]) -> List[Tuple[str, str, str]]:
    """Detect decompressed JSON files in the extracted directory."""
    extracted_path = Path(extracted_dir)
    files = []
    
    patterns = {
        'submissions': re.compile(r'^RS_(\d{4}-\d{2})$'),
        'comments': re.compile(r'^RC_(\d{4}-\d{2})$')
    }
    
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
                    date_str = match.group(1)
                    file_id = filename
                    files.append((str(filepath), file_id, data_type, date_str))
    
    files.sort(key=lambda x: (x[3], x[2]))
    
    return [(f[0], f[1], f[2]) for f in files]


def detect_csv_files(csv_dir: str, data_types: List[str]) -> List[Tuple[str, str, str]]:
    """Detect parsed CSV files in the csv directory."""
    csv_base = Path(csv_dir)
    files = []
    
    patterns = {
        'submissions': re.compile(r'^RS_(\d{4}-\d{2})\.csv$'),
        'comments': re.compile(r'^RC_(\d{4}-\d{2})\.csv$')
    }
    
    for data_type in data_types:
        if data_type not in patterns:
            continue
            
        type_dir = csv_base / data_type
        if not type_dir.is_dir():
            continue
            
        for filepath in type_dir.glob("*.csv"):
            filename = filepath.name
            match = patterns[data_type].match(filename)
            if match:
                date_str = match.group(1)
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, date_str))
    
    files.sort(key=lambda x: (x[3], x[2]))
    
    return [(f[0], f[1], f[2]) for f in files]


def run_pipeline(config_path: str = "/app/config/db_pg/pipeline.yaml"):
    """
    Run the database ingestion pipeline.
    
    Handles full pipeline: extraction -> parsing -> ingestion -> indexing -> views
    """
    # Load configuration
    config = load_config(config_path)
    
    # Check for field config overrides
    config_dir = Path("/app/config/shared")
    if (config_dir / "reddit_field_list.local.yaml").exists():
        print("[CONFIG] Using local override: reddit_field_list.local.yaml")
    if (config_dir / "reddit_field_types.local.yaml").exists():
        print("[CONFIG] Using local override: reddit_field_types.local.yaml")
    
    db_config = config['database']
    proc_config = config['processing']
    data_types = proc_config['data_types']
    
    print(f"[CONFIG] Profile: db_pg")
    print(f"[CONFIG] Database: {db_config['name']}")
    print(f"[CONFIG] Schema: {db_config['schema']}")
    print(f"[CONFIG] Data types: {data_types}")
    
    # Initialize state manager with database config for recovery
    state = PipelineState(
        state_file="/data/database/pipeline_state.json",
        db_config={
            'name': db_config['name'],
            'user': db_config['user'],
            'host': db_config['host'],
            'port': db_config['port'],
            'schema': db_config['schema']
        }
    )
    
    # If state is empty, try to recover from database
    if state.get_stats()['processed_count'] == 0:
        print("[STATE] No state file found, attempting to recover from database...")
        state.recover_from_database()
    
    stats = state.get_stats()
    print(f"[STATE] Previously processed: {stats['processed_count']} files")
    print(f"[STATE] Previously failed: {stats['failed_count']} files")
    
    # Handle interrupted processing
    interrupted_file = state.get_in_progress()
    if interrupted_file:
        print(f"[STATE] Found interrupted file: {interrupted_file} (will be retried)")
        state.clear_in_progress()
    
    # Paths
    dumps_dir = "/data/dumps"
    extracted_dir = "/data/extracted"
    csv_dir = "/data/csv"
    
    # Detect files
    files = detect_dump_files(dumps_dir, data_types)
    print(f"\n[DETECT] Found {len(files)} .zst files in {dumps_dir}")
    
    # Filter out already processed .zst files
    pending_zst_files = []
    skipped_count = 0
    for filepath, data_type in files:
        file_id = get_file_identifier(filepath)
        if state.is_processed(file_id):
            skipped_count += 1
        else:
            pending_zst_files.append((filepath, data_type))
    
    if skipped_count > 0:
        print(f"[DETECT] Skipping {skipped_count} already processed .zst files")
    
    # Check for existing JSON/CSV files
    json_files = detect_json_files(extracted_dir, data_types)
    csv_files = detect_csv_files(csv_dir, data_types)
    pending_csv_files = [f for f in csv_files if not state.is_processed(f[1])]
    
    print(f"[DETECT] Found {len(json_files)} JSON files in extracted directory")
    print(f"[DETECT] Found {len(pending_csv_files)} unprocessed CSV files in csv directory")
    
    has_work = pending_zst_files or json_files or pending_csv_files
    
    if not has_work:
        print("\n[PIPELINE] No files to process. Exiting.")
        return
    
    # Check which tables exist before processing
    table_suffix = db_config.get('table_suffix', '')
    tables_existed_before = {}
    for data_type in data_types:
        base_table = f"{data_type}{table_suffix}"
        tables_existed_before[data_type] = table_exists(
            table=base_table,
            schema=db_config['schema'],
            dbname=db_config['name'],
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user']
        )
    
    is_initial_ingestion = not all(tables_existed_before.values())
    
    parallel_mode = proc_config.get('parallel_mode', False)
    workers = proc_config.get('parse_workers', 4)
    
    print(f"\n[PIPELINE] Mode: {'parallel' if parallel_mode else 'sequential'}")
    
    total_timings = {'extraction': 0.0, 'parsing': 0.0, 'ingestion': 0.0, 'indexing': 0.0, 'analyze': 0.0}
    success_count = 0
    fail_count = 0
    
    # Phase 1: Extract .zst files
    if pending_zst_files:
        print("\n" + "="*60)
        print("PHASE 1: EXTRACTION")
        print("="*60)
        
        t_start = time.time()
        
        for filepath, data_type in pending_zst_files:
            file_id = get_file_identifier(filepath)
            extract_type_dir = f"{extracted_dir}/{data_type}"
            expected_json = Path(extract_type_dir) / file_id
            if not expected_json.exists():
                try:
                    decompress_zst(filepath, extract_type_dir)
                except Exception as e:
                    print(f"[EXTRACT] Error extracting {file_id}: {e}")
                    state.mark_failed(file_id, f"Extraction failed: {e}")
                    fail_count += 1
        
        total_timings['extraction'] = time.time() - t_start
    
    # Phase 2: Parse JSON files to CSV
    json_files = detect_json_files(extracted_dir, data_types)
    files_to_parse = []
    for json_path, file_id, data_type in json_files:
        expected_csv = Path(f"{csv_dir}/{data_type}") / f"{file_id}.csv"
        if not expected_csv.exists():
            files_to_parse.append((json_path, file_id, data_type))
    
    if files_to_parse:
        print("\n" + "="*60)
        print(f"PHASE 2: PARSING")
        print("="*60)
        
        t_start = time.time()
        
        if parallel_mode and len(files_to_parse) > 1:
            print(f"[PARSE] Parallel mode: {len(files_to_parse)} files with {workers} workers")
            
            try:
                parse_input = [(json_path, data_type) for json_path, _, data_type in files_to_parse]
                parse_files_parallel(
                    files=parse_input,
                    output_dir=csv_dir,
                    config_dir="/app/config/shared",
                    workers=workers
                )
                
                # Cleanup JSON files
                if proc_config.get('cleanup_temp', True):
                    for json_path, file_id, _ in files_to_parse:
                        if os.path.exists(json_path):
                            os.remove(json_path)
                            print(f"[CLEANUP] Removed: {Path(json_path).name}")
            except Exception as e:
                print(f"[PARSE] Error in parallel parsing: {e}")
                for _, file_id, _ in files_to_parse:
                    state.mark_failed(file_id, f"Parsing failed: {e}")
                    fail_count += 1
        else:
            for json_path, file_id, data_type in files_to_parse:
                try:
                    parse_to_csv(
                        input_file=json_path,
                        output_dir=csv_dir,
                        data_type=data_type,
                        config_dir="/app/config/shared"
                    )
                    
                    if proc_config.get('cleanup_temp', True) and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[CLEANUP] Removed: {Path(json_path).name}")
                except Exception as e:
                    print(f"[PARSE] Error parsing {file_id}: {e}")
                    state.mark_failed(file_id, f"Parsing failed: {e}")
                    fail_count += 1
        
        total_timings['parsing'] = time.time() - t_start
    
    # Phase 3: Ingest CSV files to PostgreSQL
    csv_files = detect_csv_files(csv_dir, data_types)
    files_to_ingest = [(p, fid, dt) for p, fid, dt in csv_files if not state.is_processed(fid)]
    
    if files_to_ingest:
        print("\n" + "="*60)
        print("PHASE 3: INGESTION")
        print("="*60)
        
        t_start = time.time()
        
        table_suffix = db_config.get('table_suffix', '')
        parallel_ingestion = proc_config.get('parallel_ingestion', False)
        
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
        
        data_types_with_files = set(dt for _, _, dt in files_to_ingest)
        use_parallel_ingestion = (
            parallel_ingestion
            and 'submissions' in data_types_with_files
            and 'comments' in data_types_with_files
        )
        
        if use_parallel_ingestion:
            print("[INGEST] Parallel ingestion enabled")
            
            submissions_csvs = [(p, fid, dt) for p, fid, dt in files_to_ingest if dt == 'submissions']
            comments_csvs = [(p, fid, dt) for p, fid, dt in files_to_ingest if dt == 'comments']
            
            def ingest_data_type_files(files_list):
                local_success = 0
                local_fail = 0
                for csv_path, file_id, data_type in files_list:
                    try:
                        state.mark_in_progress(file_id)
                        base_table = f"{data_type}{table_suffix}"
                        ingest_csv(
                            csv_file=csv_path,
                            data_type=data_type,
                            dbname=db_config['name'],
                            schema=db_config['schema'],
                            table=base_table,
                            host=db_config['host'],
                            port=db_config['port'],
                            user=db_config['user'],
                            check_duplicates=proc_config.get('check_duplicates', True),
                            create_indexes=False,
                            config_dir="/app/config/shared"
                        )
                        
                        state.mark_completed(file_id)
                        local_success += 1
                        
                        if proc_config.get('cleanup_temp', True):
                            if os.path.exists(csv_path):
                                os.remove(csv_path)
                                print(f"[CLEANUP] Removed: {Path(csv_path).name}")
                                
                    except Exception as e:
                        print(f"[INGEST] Error ingesting {file_id}: {e}")
                        state.mark_failed(file_id, f"Ingestion failed: {e}")
                        local_fail += 1
                        if proc_config.get('cleanup_temp', True) and os.path.exists(csv_path):
                            os.remove(csv_path)
                
                return local_success, local_fail
            
            with ThreadPoolExecutor(max_workers=2) as executor:
                future_submissions = executor.submit(ingest_data_type_files, submissions_csvs)
                future_comments = executor.submit(ingest_data_type_files, comments_csvs)
                
                sub_success, sub_fail = future_submissions.result()
                com_success, com_fail = future_comments.result()
                
                success_count = sub_success + com_success
                fail_count += sub_fail + com_fail
        else:
            for csv_path, file_id, data_type in files_to_ingest:
                try:
                    state.mark_in_progress(file_id)
                    base_table = f"{data_type}{table_suffix}"
                    ingest_csv(
                        csv_file=csv_path,
                        data_type=data_type,
                        dbname=db_config['name'],
                        schema=db_config['schema'],
                        table=base_table,
                        host=db_config['host'],
                        port=db_config['port'],
                        user=db_config['user'],
                        check_duplicates=proc_config.get('check_duplicates', True),
                        create_indexes=False,
                        config_dir="/app/config/shared"
                    )
                    
                    state.mark_completed(file_id)
                    success_count += 1
                    
                    if proc_config.get('cleanup_temp', True) and os.path.exists(csv_path):
                        os.remove(csv_path)
                        print(f"[CLEANUP] Removed: {Path(csv_path).name}")
                except Exception as e:
                    print(f"[INGEST] Error ingesting {file_id}: {e}")
                    state.mark_failed(file_id, f"Ingestion failed: {e}")
                    fail_count += 1
                    if proc_config.get('cleanup_temp', True) and os.path.exists(csv_path):
                        os.remove(csv_path)
        
        total_timings['ingestion'] = time.time() - t_start
    
    # Create indexes
    if proc_config.get('create_indexes', True) and success_count > 0:
        print("\n" + "="*60)
        print("CREATING INDEXES")
        print("="*60)
        
        index_config = config.get('indexes', {})
        table_suffix = db_config.get('table_suffix', '')
        
        t_start = time.time()
        for data_type in data_types:
            base_table = f"{data_type}{table_suffix}"
            index_fields = index_config.get(data_type, [])
            if not index_fields:
                if data_type == 'submissions':
                    index_fields = ['author', 'subreddit', 'domain', 'created_utc']
                else:
                    index_fields = ['author', 'subreddit', 'link_id', 'created_utc']
            
            print(f"[INDEX] Creating indexes for {base_table}: {index_fields}")
            for field in index_fields:
                try:
                    created = create_index(
                        field=field,
                        table=base_table,
                        schema=db_config['schema'],
                        dbname=db_config['name'],
                        host=db_config['host'],
                        port=db_config['port'],
                        user=db_config['user']
                    )
                    if not created:
                        print(f"[INDEX] Already exists: idx_{base_table}_{field}")
                except Exception as e:
                    print(f"[INDEX] Warning: Failed to create index on {field}: {e}")
        total_timings['indexing'] = time.time() - t_start
    
    # Run analyze
    if success_count > 0:
        print("\n" + "="*60)
        print("ANALYZE")
        print("="*60)
        
        t_start = time.time()
        table_suffix = db_config.get('table_suffix', '')
        
        print("[ANALYZE] Running ANALYZE")
        
        for data_type in data_types:
            base_table = f"{data_type}{table_suffix}"
            try:
                analyze_table(
                    table=base_table,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user']
                )
            except Exception as e:
                print(f"[ANALYZE] Warning: Failed to analyze {base_table}: {e}")
        
        total_timings['analyze'] = time.time() - t_start
    
    # Create/rebuild views
    if success_count > 0:
        print("\n" + "="*60)
        print("CREATING VIEWS")
        print("="*60)
        
        for data_type in data_types:
            try:
                rebuild_view(
                    data_type=data_type,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    config_dir="/app/config/db_pg"
                )
            except Exception as e:
                print(f"[VIEW] Warning: Failed to create view for {data_type}: {e}")
    
    # Final summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    
    final_stats = state.get_stats()
    print(f"Total processed: {final_stats['processed_count']}")
    print(f"Total failed: {final_stats['failed_count']}")
    
    print(f"\nTiming (minutes):")
    print(f"  Extraction: {total_timings['extraction'] / 60:.2f}")
    print(f"  Parsing:    {total_timings['parsing'] / 60:.2f}")
    print(f"  Ingestion:  {total_timings['ingestion'] / 60:.2f}")
    print(f"  Indexing:   {total_timings['indexing'] / 60:.2f}")
    print(f"  Analyze:    {total_timings.get('analyze', 0.0) / 60:.2f}")
    total_time = sum(total_timings.values())
    print(f"  Total:      {total_time / 60:.2f}")


def main():
    """Main entry point with optional watch mode."""
    config = load_config()
    watch_interval = config.get('processing', {}).get('watch_interval', 0)
    
    if watch_interval > 0:
        print(f"[WATCH] Watch mode enabled: checking every {watch_interval} minutes")
        interval_seconds = watch_interval * 60
        while True:
            try:
                run_pipeline()
            except Exception as e:
                print(f"[WATCH] Pipeline error: {e}")
                print("[WATCH] Will retry next interval...")
            
            print(f"\n[WATCH] Next check in {watch_interval} minutes...")
            time.sleep(interval_seconds)
    else:
        run_pipeline()


if __name__ == "__main__":
    main()
