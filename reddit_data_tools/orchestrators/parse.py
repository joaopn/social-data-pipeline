"""
Parse profile orchestrator for reddit_data_tools.
Handles file detection, decompression, and JSON to CSV parsing.
Does NOT run classifiers or database ingestion.
"""

import os
import re
import sys
import time
import yaml
from pathlib import Path
from typing import List, Dict, Tuple

from ..core.state import PipelineState
from ..core.decompress import decompress_zst
from ..core.parse_csv import parse_to_csv, parse_files_parallel, load_yaml_file


def load_config(config_path: str = "/app/config/parse/pipeline.yaml", quiet: bool = False) -> Dict:
    """Load pipeline configuration from YAML, checking for .local.yaml override first."""
    config_path = Path(config_path)
    local_path = config_path.with_suffix('.local.yaml')
    if local_path.exists():
        config_path = local_path
        if not quiet:
            print(f"[CONFIG] Using local override: {local_path.name}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def detect_dump_files(dumps_dir: str, data_types: List[str]) -> List[Tuple[str, str]]:
    """
    Detect .zst dump files in the dumps directory and subfolders.
    
    Searches in:
        - dumps_dir/ (root)
        - dumps_dir/submissions/
        - dumps_dir/comments/
    
    Returns:
        List of tuples (filepath, data_type) sorted: submissions first, then comments,
        alphabetically within each type
    """
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
        if not search_dir.exists():
            continue
        for filepath in search_dir.glob("*.zst"):
            filename = filepath.name
            
            for data_type in data_types:
                if data_type not in patterns:
                    continue
                    
                match = patterns[data_type].match(filename)
                if match:
                    files.append((str(filepath), data_type, filename))
                    break
    
    type_order = {'submissions': 0, 'comments': 1}
    files.sort(key=lambda x: (type_order.get(x[1], 99), x[2]))
    
    return [(f[0], f[1]) for f in files]


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
                    file_id = filename
                    files.append((str(filepath), file_id, data_type, filename))
    
    type_order = {'submissions': 0, 'comments': 1}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def detect_parsed_csv_files(csv_dir: str, data_types: List[str]) -> List[Tuple[str, str, str]]:
    """Detect parsed CSV files in the CSV directory."""
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
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, filename))
    
    type_order = {'submissions': 0, 'comments': 1}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def get_file_identifier(filepath: str) -> str:
    """Extract identifier from filepath for state tracking (e.g., RC_2023-01)."""
    return Path(filepath).stem


def run_pipeline(config_path: str = "/app/config/parse/pipeline.yaml"):
    """
    Run the parse pipeline.
    
    Pipeline phases:
    1. Detect input sources (.zst files, existing JSON)
    2. Decompress .zst files
    3. Parse JSON to CSV
    """
    # Load configuration
    config = load_config(config_path)
    
    # Check for field config overrides
    config_dir = Path("/app/config/shared")
    if (config_dir / "reddit_field_list.local.yaml").exists():
        print("[CONFIG] Using local override: reddit_field_list.local.yaml")
    if (config_dir / "reddit_field_types.local.yaml").exists():
        print("[CONFIG] Using local override: reddit_field_types.local.yaml")
    
    proc_config = config['processing']
    data_types = proc_config['data_types']
    
    print(f"[CONFIG] Profile: parse")
    print(f"[CONFIG] Data types: {data_types}")
    
    # Initialize state manager
    state = PipelineState(state_file="/data/output/parse_state.json")
    
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
    
    # Phase 1: Detect input sources
    print("\n" + "="*60)
    print("PHASE 1: INPUT DETECTION")
    print("="*60)
    
    zst_files = detect_dump_files(dumps_dir, data_types)
    print(f"[DETECT] Found {len(zst_files)} .zst files in {dumps_dir}")
    
    json_files = detect_json_files(extracted_dir, data_types)
    parsed_csv_files = detect_parsed_csv_files(csv_dir, data_types)
    
    print(f"[DETECT] Found {len(json_files)} JSON files in extracted directory")
    print(f"[DETECT] Found {len(parsed_csv_files)} CSV files in csv directory")
    
    # Filter out already processed files
    pending_zst = [(p, dt) for p, dt in zst_files if not state.is_processed(get_file_identifier(p))]
    pending_json = [(p, fid, dt) for p, fid, dt in json_files if not state.is_processed(fid)]
    
    print(f"[DETECT] Pending: {len(pending_zst)} .zst, {len(pending_json)} JSON")
    
    if not (pending_zst or pending_json):
        print("\n[PIPELINE] No files to process. Exiting.")
        return
    
    total_timings = {'extraction': 0.0, 'parsing': 0.0}
    success_count = 0
    fail_count = 0
    
    # Phase 2: Extract .zst files
    if pending_zst:
        print("\n" + "="*60)
        print("PHASE 2: EXTRACTION")
        print("="*60)
        
        t_start = time.time()
        
        for filepath, data_type in pending_zst:
            file_id = get_file_identifier(filepath)
            extract_dir = f"{extracted_dir}/{data_type}"
            expected_json = Path(extract_dir) / file_id
            
            if not expected_json.exists():
                try:
                    state.mark_in_progress(file_id)
                    decompress_zst(filepath, extract_dir)
                except Exception as e:
                    print(f"[EXTRACT] Error extracting {file_id}: {e}")
                    state.mark_failed(file_id, f"Extraction failed: {e}")
                    fail_count += 1
        
        total_timings['extraction'] = time.time() - t_start
    
    # Phase 3: Parse JSON files to CSV
    json_files = detect_json_files(extracted_dir, data_types)  # Re-detect
    files_to_parse = []
    for json_path, file_id, data_type in json_files:
        expected_csv = Path(csv_dir) / data_type / f"{file_id}.csv"
        if not expected_csv.exists():
            files_to_parse.append((json_path, file_id, data_type))
    
    if files_to_parse:
        print("\n" + "="*60)
        print("PHASE 3: PARSING")
        print("="*60)
        
        t_start = time.time()
        workers = proc_config.get('parse_workers', 4)
        parallel_mode = proc_config.get('parallel_mode', False)
        
        if parallel_mode and len(files_to_parse) > 1:
            print(f"[PARSE] Parallel mode: {len(files_to_parse)} files with {workers} workers")
            
            parse_input = [(json_path, data_type) for json_path, file_id, data_type in files_to_parse]
            
            try:
                parse_files_parallel(
                    files=parse_input,
                    output_dir=csv_dir,
                    config_dir="/app/config/shared",
                    workers=workers
                )
                
                # Mark files as completed and cleanup
                for json_path, file_id, data_type in files_to_parse:
                    state.mark_completed(file_id)
                    success_count += 1
                    
                    if proc_config.get('cleanup_temp', True):
                        if os.path.exists(json_path):
                            os.remove(json_path)
                            print(f"[CLEANUP] Removed: {Path(json_path).name}")
                            
            except Exception as e:
                print(f"[PARSE] Error in parallel parsing: {e}")
                for _, file_id, _ in files_to_parse:
                    state.mark_failed(file_id, f"Parsing failed: {e}")
                    fail_count += 1
        else:
            # Sequential parsing
            for json_path, file_id, data_type in files_to_parse:
                try:
                    state.mark_in_progress(file_id)
                    parse_to_csv(
                        input_file=json_path,
                        output_dir=csv_dir,
                        data_type=data_type,
                        config_dir="/app/config/shared"
                    )
                    
                    state.mark_completed(file_id)
                    success_count += 1
                    
                    if proc_config.get('cleanup_temp', True) and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[CLEANUP] Removed: {Path(json_path).name}")
                        
                except Exception as e:
                    print(f"[PARSE] Error parsing {file_id}: {e}")
                    state.mark_failed(file_id, f"Parsing failed: {e}")
                    fail_count += 1
        
        total_timings['parsing'] = time.time() - t_start
    
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
