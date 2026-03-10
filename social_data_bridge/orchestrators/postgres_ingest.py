"""
PostgreSQL database profile orchestrator for social_data_bridge.
Handles CSV ingestion into PostgreSQL with indexing and view management.
Expects CSV files to already exist (run parse profile first).

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import List, Dict, Tuple, Optional

from ..core.state import PipelineState
from ..core.decompress import decompress_zst
from ..core.config import (
    load_profile_config,
    load_yaml_file,
    get_required,
    get_optional,
    validate_processing_config,
    validate_database_config,
    apply_env_overrides,
    ConfigurationError,
)
from ..db.postgres.ingest import (
    ingest_csv, create_index, table_exists, analyze_table,
    ensure_database_exists, ensure_schema_exists, ensure_tablespaces, resolve_tablespace,
    # Fast initial load functions
    create_fast_load_table, fast_ingest_csv, delete_duplicates, finalize_fast_load_table,
)
from .ml import detect_parsed_csv_files


# Platform selection via environment variable
PLATFORM = os.environ.get('PLATFORM', 'reddit')


def get_platform_parser():
    """
    Get the appropriate parser module for the current platform.
    
    Returns:
        Parser module with parse_to_csv and parse_files_parallel functions
    """
    if PLATFORM == 'reddit':
        from ..platforms.reddit import parser
        return parser
    elif PLATFORM == 'generic':
        from ..platforms.generic import parser
        return parser
    else:
        raise ConfigurationError(f"Unknown platform: {PLATFORM}. Supported: reddit, generic")


def get_platform_config_dir(config_dir: str) -> str:
    """Get the platform-specific config directory."""
    return f"{config_dir}/platforms/{PLATFORM}"


def load_platform_config(config_dir: str) -> Dict:
    """
    Load platform-specific configuration (file patterns, data types, indexes).
    
    Args:
        config_dir: Base configuration directory
        
    Returns:
        Platform configuration dictionary
    """
    platform_config_path = Path(get_platform_config_dir(config_dir)) / "platform.yaml"
    config = load_yaml_file(platform_config_path)
    if config is None:
        raise ConfigurationError(f"Platform config not found: {platform_config_path}")
    return config


def load_config(config_dir: str = "/app/config", quiet: bool = False) -> Dict:
    """
    Load postgres_ingest profile configuration.
    
    Loads base config files and merges user.yaml overrides if present.
    Environment variables can override database settings.
    
    Args:
        config_dir: Base configuration directory
        quiet: If True, suppress informational output
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    config = load_profile_config('postgres_ingest', config_dir, quiet)
    
    # Apply environment variable overrides for database settings
    config = apply_env_overrides(config, 'postgres_ingest')
    
    # Validate required config
    validate_processing_config(config, 'postgres_ingest')
    validate_database_config(config)
    
    return config


def detect_dump_files(dumps_dir: str, data_types: List[str], file_patterns: Dict) -> List[Tuple[str, str]]:
    """Detect .zst dump files in the dumps directory and subfolders."""
    dumps_path = Path(dumps_dir)
    files = []
    
    # Build patterns from platform config
    patterns = {}
    for data_type in data_types:
        if data_type in file_patterns and 'zst' in file_patterns[data_type]:
            patterns[data_type] = re.compile(file_patterns[data_type]['zst'])
    
    search_dirs = [dumps_path]
    for subfolder in data_types:
        subfolder_path = dumps_path / subfolder
        if subfolder_path.is_dir():
            search_dirs.append(subfolder_path)
    
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        for filepath in search_dir.glob("*.zst"):
            filename = filepath.name
            
            for data_type in data_types:
                if data_type not in patterns:
                    continue
                    
                match = patterns[data_type].match(filename)
                if match:
                    date_str = match.group(1) if match.groups() else filename
                    files.append((str(filepath), data_type, date_str))
                    break
    
    files.sort(key=lambda x: (x[2], x[1]))
    
    return [(f[0], f[1]) for f in files]


def get_file_identifier(filepath: str) -> str:
    """Extract identifier from filepath for state tracking."""
    return Path(filepath).stem


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
                    date_str = match.group(1) if match.groups() else filename
                    file_id = filename
                    files.append((str(filepath), file_id, data_type, date_str))
    
    files.sort(key=lambda x: (x[3], x[2]))
    
    return [(f[0], f[1], f[2]) for f in files]


def detect_csv_files(csv_dir: str, data_types: List[str], file_patterns: Dict) -> List[Tuple[str, str, str]]:
    """Detect parsed CSV files in the csv directory."""
    csv_base = Path(csv_dir)
    files = []
    
    # Build patterns from platform config
    patterns = {}
    for data_type in data_types:
        if data_type in file_patterns and 'csv' in file_patterns[data_type]:
            patterns[data_type] = re.compile(file_patterns[data_type]['csv'])
    
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
                date_str = match.group(1) if match.groups() else filename
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, date_str))
    
    files.sort(key=lambda x: (x[3], x[2]))
    
    return [(f[0], f[1], f[2]) for f in files]


def get_lingua_config(config_dir: str) -> Optional[Dict]:
    """
    Load lingua configuration from ml_cpu profile.
    
    Returns:
        Dict with 'suffix' and 'output_dir' keys, or None if config not found
    """
    try:
        ml_config = load_profile_config('ml_cpu', config_dir, quiet=True)
        lingua_config = ml_config.get('lingua', {})
        return {
            'suffix': lingua_config.get('suffix', '_lingua'),
            'output_dir': '/data/output/lingua'  # Standard output path for lingua
        }
    except Exception:
        return None


def detect_lingua_csv_files(
    data_types: List[str],
    lingua_config: Dict
) -> Tuple[List[Tuple[str, str, str]], Dict[str, str]]:
    """
    Detect CSV files in the lingua output directory only.
    Used when prefer_lingua is true; original CSV dir is not checked.
    
    Returns:
        Tuple of (list of (filepath, file_id, data_type), source_map with 'lingua')
    """
    lingua_output_dir = Path(lingua_config['output_dir'])
    lingua_suffix = lingua_config['suffix']
    files_with_name = []
    for data_type in data_types:
        type_dir = lingua_output_dir / data_type
        if type_dir.is_dir():
            for filepath in type_dir.glob(f"*{lingua_suffix}.csv"):
                file_id = filepath.stem
                if file_id.endswith(lingua_suffix):
                    file_id = file_id[:-len(lingua_suffix)]
                files_with_name.append((str(filepath), file_id, data_type, filepath.name))
    type_order = {dt: i for i, dt in enumerate(data_types)}
    files_with_name.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    result_files = [(f[0], f[1], f[2]) for f in files_with_name]
    source_map = {f[1]: 'lingua' for f in result_files}
    return result_files, source_map


def run_pipeline(config_dir: str = "/app/config"):
    """
    Run the database ingestion pipeline.
    
    Handles full pipeline: extraction -> parsing -> ingestion -> indexing -> views
    
    Args:
        config_dir: Base configuration directory
    """
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir)
    
    db_config = config['database']
    proc_config = config['processing']
    
    # Get db_schema from profile config, fall back to platform config
    db_schema = db_config.get('schema')
    if not db_schema:
        db_schema = platform_config.get('db_schema')
    if not db_schema:
        raise ConfigurationError("No db_schema configured. Set in platform config or user.yaml.")
    db_config['schema'] = db_schema
    
    # Get data types from profile config, fall back to platform config
    data_types = get_optional(config, 'processing', 'data_types', default=[])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    if not data_types:
        raise ConfigurationError("No data_types configured. Set in user.yaml or platform config.")
    
    # Get file patterns from platform config
    file_patterns = platform_config.get('file_patterns', {})
    
    # Get platform-specific config directory
    platform_config_dir = get_platform_config_dir(config_dir)
    
    # Tablespace configuration
    tablespaces = config.get('tablespaces', {})
    table_tablespaces = config.get('table_tablespaces', {})

    def get_tablespace(data_type):
        return resolve_tablespace(table_tablespaces.get(data_type))

    print(f"[sdb] Profile: postgres_ingest")
    print(f"[sdb] Platform: {PLATFORM}")
    print(f"[sdb] Database: {db_config['name']}")
    print(f"[sdb] Schema: {db_schema}")
    print(f"[sdb] Data types: {data_types}")
    if table_tablespaces:
        print(f"[sdb] Tablespace assignments: {table_tablespaces}")
    
    # Build file prefixes from platform config for state recovery
    file_prefixes = {}
    for dt in data_types:
        if dt in file_patterns and 'prefix' in file_patterns[dt]:
            file_prefixes[dt] = file_patterns[dt]['prefix']

    # Initialize state managers - one per data_type for isolation
    pgdata_path = os.environ.get('PGDATA_PATH', '/data/database')
    state_dir = f"{pgdata_path}/state_tracking"
    os.makedirs(state_dir, exist_ok=True)

    states = {}
    total_processed = 0
    total_failed = 0

    for dt in data_types:
        state_file = f"{state_dir}/{PLATFORM}_postgres_ingest_{dt}.json"
        states[dt] = PipelineState(
            state_file=state_file,
            db_config={
                'name': db_config['name'],
                'user': db_config['user'],
                'host': db_config['host'],
                'port': db_config['port'],
                'schema': db_config['schema']
            },
            data_types=[dt],
            file_prefixes={dt: file_prefixes.get(dt)}
        )

        # If state is empty, try to recover from database
        if states[dt].get_stats()['processed_count'] == 0:
            print(f"[sdb] No state for {dt}, attempting to recover from database...")
            states[dt].recover_from_database()

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
    csv_dir = "/data/csv"
    
    # Detect files
    files = detect_dump_files(dumps_dir, data_types, file_patterns)
    print(f"\n[sdb] Found {len(files)} .zst files in {dumps_dir}")
    
    # Filter out already processed .zst files
    pending_zst_files = []
    skipped_count = 0
    for filepath, data_type in files:
        file_id = get_file_identifier(filepath)
        if states[data_type].is_processed(file_id):
            skipped_count += 1
        else:
            pending_zst_files.append((filepath, data_type))
    
    if skipped_count > 0:
        print(f"[sdb] Skipping {skipped_count} already processed .zst files")
    
    # Check for existing JSON/CSV files
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)
    
    # Check if we should prefer lingua files
    prefer_lingua = get_optional(config, 'processing', 'prefer_lingua', default=False)
    lingua_config = None
    csv_source_map = {}
    
    if prefer_lingua:
        lingua_config = get_lingua_config(config_dir)
        if lingua_config:
            print(f"[sdb] Prefer lingua: enabled (suffix: {lingua_config['suffix']})")
            csv_files, csv_source_map = detect_lingua_csv_files(data_types, lingua_config)
            # Count sources
            lingua_count = sum(1 for src in csv_source_map.values() if src == 'lingua')
            original_count = sum(1 for src in csv_source_map.values() if src == 'original')
            print(f"[sdb] Found {lingua_count} lingua CSVs, {original_count} original CSVs (fallback)")
        else:
            print("[sdb] Prefer lingua: enabled but ml_cpu config not found, using original CSVs")
            csv_files = detect_csv_files(csv_dir, data_types, file_patterns)
    else:
        csv_files = detect_csv_files(csv_dir, data_types, file_patterns)
    
    pending_csv_files = [f for f in csv_files if not states[f[2]].is_processed(f[1])]
    
    print(f"[sdb] Found {len(json_files)} JSON files in extracted directory")
    print(f"[sdb] Found {len(pending_csv_files)} unprocessed CSV files to ingest")
    
    has_work = pending_zst_files or json_files or pending_csv_files
    
    if not has_work:
        print("\n[sdb] No files to process. Exiting.")
        return
    
    # Check which tables exist before processing
    tables_existed_before = {}
    for data_type in data_types:
        tables_existed_before[data_type] = table_exists(
            table=data_type,
            schema=db_config['schema'],
            dbname=db_config['name'],
            host=db_config['host'],
            port=db_config['port'],
            user=db_config['user']
        )
    
    is_initial_ingestion = not all(tables_existed_before.values())
    
    parallel_mode = get_required(config, 'processing', 'parallel_mode')
    workers = get_required(config, 'processing', 'parse_workers')
    
    print(f"\n[sdb] Mode: {'parallel' if parallel_mode else 'sequential'}")
    
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
                    print(f"[sdb] Error extracting {file_id}: {e}")
                    states[data_type].mark_failed(file_id, f"Extraction failed: {e}")
                    fail_count += 1
        
        total_timings['extraction'] = time.time() - t_start
    
    # Phase 2: Parse JSON files to CSV
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)
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
        
        # Get platform-specific parser
        parser = get_platform_parser()
        
        if parallel_mode and len(files_to_parse) > 1:
            print(f"[sdb] Parallel mode: {len(files_to_parse)} files with {workers} workers")
            
            try:
                parse_input = [(json_path, data_type) for json_path, _, data_type in files_to_parse]
                parser.parse_files_parallel(
                    files=parse_input,
                    output_dir=csv_dir,
                    config_dir=platform_config_dir,
                    workers=workers
                )
                
                # Cleanup JSON files
                cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
                if cleanup_temp:
                    for json_path, file_id, _ in files_to_parse:
                        if os.path.exists(json_path):
                            os.remove(json_path)
                            print(f"[sdb] Removed: {Path(json_path).name}")
            except Exception as e:
                print(f"[sdb] Error in parallel parsing: {e}")
                for _, file_id, data_type in files_to_parse:
                    states[data_type].mark_failed(file_id, f"Parsing failed: {e}")
                    fail_count += 1
        else:
            cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
            for json_path, file_id, data_type in files_to_parse:
                try:
                    parser.parse_to_csv(
                        input_file=json_path,
                        output_dir=csv_dir,
                        data_type=data_type,
                        config_dir=platform_config_dir
                    )
                    
                    if cleanup_temp and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[sdb] Removed: {Path(json_path).name}")
                except Exception as e:
                    print(f"[sdb] Error parsing {file_id}: {e}")
                    states[data_type].mark_failed(file_id, f"Parsing failed: {e}")
                    fail_count += 1
        
        total_timings['parsing'] = time.time() - t_start
    
    # Phase 3: Ingest CSV files to PostgreSQL
    # Re-detect CSV files (may have been created during parsing phase)
    if prefer_lingua and lingua_config:
        csv_files, csv_source_map = detect_lingua_csv_files(data_types, lingua_config)
    else:
        csv_files = detect_csv_files(csv_dir, data_types, file_patterns)
    files_to_ingest = [(p, fid, dt) for p, fid, dt in csv_files if not states[dt].is_processed(fid)]
    
    if files_to_ingest:
        print("\n" + "="*60)
        print("PHASE 3: INGESTION")
        print("="*60)
        
        # Log source breakdown if prefer_lingua is enabled
        if prefer_lingua and csv_source_map:
            ingest_from_lingua = sum(1 for p, fid, dt in files_to_ingest if csv_source_map.get(fid) == 'lingua')
            ingest_from_original = sum(1 for p, fid, dt in files_to_ingest if csv_source_map.get(fid) == 'original')
            print(f"[sdb] Sources: {ingest_from_lingua} lingua, {ingest_from_original} original (fallback)")
        
        t_start = time.time()
        
        parallel_ingestion = get_required(config, 'processing', 'parallel_ingestion')
        check_duplicates = get_required(config, 'processing', 'check_duplicates')
        cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
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

        # Create tablespaces if configured
        if tablespaces:
            ensure_tablespaces(
                tablespaces=tablespaces,
                dbname=db_config['name'],
                host=db_config['host'],
                port=db_config['port'],
                user=db_config['user']
            )

        data_types_with_files = set(dt for _, _, dt in files_to_ingest)
        
        # Determine load strategy per data type:
        # New tables use fast load (deferred PK, blind COPY, post-load dedup)
        # Existing tables use standard ON CONFLICT upsert
        fast_load_types = set()
        standard_load_types = set()

        for dt in data_types_with_files:
            if not tables_existed_before.get(dt, True):
                fast_load_types.add(dt)
            else:
                standard_load_types.add(dt)

        if fast_load_types:
            print(f"[sdb] Fast initial load for: {', '.join(sorted(fast_load_types))}")
        if standard_load_types:
            print(f"[sdb] Standard ON CONFLICT load for: {', '.join(sorted(standard_load_types))}")
        
        # =====================================================================
        # FAST INITIAL LOAD PATH
        # =====================================================================
        if fast_load_types:
            use_parallel_fast_load = (
                parallel_ingestion
                and 'submissions' in fast_load_types
                and 'comments' in fast_load_types
            )
            
            def fast_load_data_type(data_type):
                """Fast load a single data type. Returns (success_count, fail_count)."""
                type_files = [(p, fid, dt) for p, fid, dt in files_to_ingest if dt == data_type]
                if not type_files:
                    return 0, 0
                
                local_success = 0
                local_fail = 0
                
                print(f"\n[sdb] Processing {data_type}: {len(type_files)} files")
                
                # Get first CSV to determine lingua columns
                first_csv = type_files[0][0]
                
                # Step 1: Create table (no PK)
                create_fast_load_table(
                    data_type=data_type,
                    dbname=db_config['name'],
                    schema=db_config['schema'],
                    table=data_type,
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    config_dir=platform_config_dir,
                    csv_file=first_csv,
                    tablespace=get_tablespace(data_type)
                )
                
                # Step 2: Blind COPY all files
                for csv_path, file_id, dt in type_files:
                    try:
                        states[data_type].mark_in_progress(file_id)
                        fast_ingest_csv(
                            csv_file=csv_path,
                            data_type=data_type,
                            dbname=db_config['name'],
                            schema=db_config['schema'],
                            table=data_type,
                            host=db_config['host'],
                            port=db_config['port'],
                            user=db_config['user'],
                            config_dir=platform_config_dir
                        )
                        states[data_type].mark_completed(file_id)
                        local_success += 1

                        if cleanup_temp and os.path.exists(csv_path):
                            os.remove(csv_path)
                            print(f"[sdb] Removed: {Path(csv_path).name}")
                    except Exception as e:
                        print(f"[sdb] Error during COPY {file_id}: {e}")
                        states[data_type].mark_failed(file_id, f"Fast ingestion failed: {e}")
                        local_fail += 1
                        raise  # Abort fast load on any failure
                
                # Step 3: Delete duplicates
                delete_duplicates(
                    table=data_type,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user']
                )
                
                # Step 4: Finalize (add PK)
                finalize_fast_load_table(
                    table=data_type,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user'],
                    tablespace=get_tablespace(data_type)
                )
                
                print(f"[sdb] Completed {data_type}")
                return local_success, local_fail
            
            if use_parallel_fast_load:
                print("[sdb] Parallel ingestion enabled (submissions + comments concurrently)")
                
                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_submissions = executor.submit(fast_load_data_type, 'submissions')
                    future_comments = executor.submit(fast_load_data_type, 'comments')
                    
                    try:
                        sub_success, sub_fail = future_submissions.result()
                        com_success, com_fail = future_comments.result()
                        success_count += sub_success + com_success
                        fail_count += sub_fail + com_fail
                    except Exception as e:
                        print(f"[sdb] CRITICAL ERROR: {e}")
                        print("[sdb] Database may be in inconsistent state. Manual recovery required.")
                        raise
            else:
                for data_type in sorted(fast_load_types):
                    try:
                        local_success, local_fail = fast_load_data_type(data_type)
                        success_count += local_success
                        fail_count += local_fail
                    except Exception as e:
                        print(f"[sdb] CRITICAL ERROR for {data_type}: {e}")
                        print("[sdb] Database may be in inconsistent state. Manual recovery required.")
                        raise

        # =====================================================================
        # STANDARD ON CONFLICT PATH
        # =====================================================================
        if standard_load_types:
            standard_files = [(p, fid, dt) for p, fid, dt in files_to_ingest if dt in standard_load_types]
            
            use_parallel_ingestion = (
                parallel_ingestion
                and 'submissions' in standard_load_types
                and 'comments' in standard_load_types
            )
            
            if use_parallel_ingestion:
                print("[sdb] Parallel ingestion enabled")
                
                submissions_csvs = [(p, fid, dt) for p, fid, dt in standard_files if dt == 'submissions']
                comments_csvs = [(p, fid, dt) for p, fid, dt in standard_files if dt == 'comments']
                
                def ingest_data_type_files(files_list):
                    local_success = 0
                    local_fail = 0
                    for csv_path, file_id, data_type in files_list:
                        try:
                            states[data_type].mark_in_progress(file_id)
                            ingest_csv(
                                csv_file=csv_path,
                                data_type=data_type,
                                dbname=db_config['name'],
                                schema=db_config['schema'],
                                table=data_type,
                                host=db_config['host'],
                                port=db_config['port'],
                                user=db_config['user'],
                                check_duplicates=check_duplicates,
                                create_indexes=False,
                                config_dir=platform_config_dir,
                                tablespace=get_tablespace(data_type)
                            )

                            states[data_type].mark_completed(file_id)
                            local_success += 1

                            if cleanup_temp and os.path.exists(csv_path):
                                os.remove(csv_path)
                                print(f"[sdb] Removed: {Path(csv_path).name}")

                        except Exception as e:
                            print(f"[sdb] Error ingesting {file_id}: {e}")
                            states[data_type].mark_failed(file_id, f"Ingestion failed: {e}")
                            local_fail += 1
                            if cleanup_temp and os.path.exists(csv_path):
                                os.remove(csv_path)

                    return local_success, local_fail

                with ThreadPoolExecutor(max_workers=2) as executor:
                    future_submissions = executor.submit(ingest_data_type_files, submissions_csvs)
                    future_comments = executor.submit(ingest_data_type_files, comments_csvs)
                    
                    sub_success, sub_fail = future_submissions.result()
                    com_success, com_fail = future_comments.result()
                    
                    success_count += sub_success + com_success
                    fail_count += sub_fail + com_fail
            else:
                for csv_path, file_id, data_type in standard_files:
                    try:
                        states[data_type].mark_in_progress(file_id)
                        ingest_csv(
                            csv_file=csv_path,
                            data_type=data_type,
                            dbname=db_config['name'],
                            schema=db_config['schema'],
                            table=data_type,
                            host=db_config['host'],
                            port=db_config['port'],
                            user=db_config['user'],
                            check_duplicates=check_duplicates,
                            create_indexes=False,
                            config_dir=platform_config_dir,
                            tablespace=get_tablespace(data_type)
                        )

                        states[data_type].mark_completed(file_id)
                        success_count += 1

                        if cleanup_temp and os.path.exists(csv_path):
                            os.remove(csv_path)
                            print(f"[sdb] Removed: {Path(csv_path).name}")
                    except Exception as e:
                        print(f"[sdb] Error ingesting {file_id}: {e}")
                        states[data_type].mark_failed(file_id, f"Ingestion failed: {e}")
                        fail_count += 1
                        if cleanup_temp and os.path.exists(csv_path):
                            os.remove(csv_path)
        
        total_timings['ingestion'] = time.time() - t_start
    
    # Create indexes
    create_indexes = get_required(config, 'processing', 'create_indexes')
    if create_indexes and success_count > 0:
        print("\n" + "="*60)
        print("CREATING INDEXES")
        print("="*60)

        # Get indexes from profile config, fall back to platform config
        index_config = config.get('indexes', {})
        if not index_config:
            index_config = platform_config.get('indexes', {})

        # Get parallelism settings from config
        parallel_index_workers = get_optional(config, 'processing', 'parallel_index_workers', default=8)

        t_start = time.time()
        for data_type in data_types:
            index_fields = index_config.get(data_type, [])
            if not index_fields:
                print(f"[sdb] No indexes configured for {data_type}, skipping")
                continue

            print(f"[sdb] Creating {len(index_fields)} indexes on {db_config['schema']}.{data_type} (workers_per_build={parallel_index_workers})")
            for i, field in enumerate(index_fields):
                try:
                    create_index(
                        field=field,
                        table=data_type,
                        schema=db_config['schema'],
                        dbname=db_config['name'],
                        host=db_config['host'],
                        port=db_config['port'],
                        user=db_config['user'],
                        quiet=(i > 0),  # Only log session config for first index
                        parallel_workers=parallel_index_workers,
                        tablespace=get_tablespace(data_type)
                    )
                except Exception as e:
                    print(f"[sdb] Warning: Failed to create index on {field}: {e}")
        total_timings['indexing'] = time.time() - t_start
    
    # Run analyze
    if success_count > 0:
        print("\n" + "="*60)
        print("ANALYZE")
        print("="*60)
        
        t_start = time.time()
        
        print("[sdb] Running ANALYZE")
        
        for data_type in data_types:
            try:
                analyze_table(
                    table=data_type,
                    schema=db_config['schema'],
                    dbname=db_config['name'],
                    host=db_config['host'],
                    port=db_config['port'],
                    user=db_config['user']
                )
            except Exception as e:
                print(f"[sdb] Warning: Failed to analyze {data_type}: {e}")
        
        total_timings['analyze'] = time.time() - t_start
    
    # Final summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")

    # Aggregate stats from all state managers
    total_processed = sum(states[dt].get_stats()['processed_count'] for dt in data_types)
    total_failed = sum(states[dt].get_stats()['failed_count'] for dt in data_types)
    print(f"Total processed: {total_processed}")
    print(f"Total failed: {total_failed}")
    
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
