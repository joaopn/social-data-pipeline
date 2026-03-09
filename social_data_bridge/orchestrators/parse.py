"""
Parse profile orchestrator for social-data-bridge.
Handles file detection, decompression, and JSON to CSV parsing.
Does NOT run classifiers or database ingestion.

Platform selection via PLATFORM env var (default: reddit).
"""

import os
import re
import sys
import time
from pathlib import Path
from typing import List, Dict, Tuple

from ..core.decompress import decompress_zst
from ..core.config import (
    load_profile_config,
    load_yaml_file,
    get_required,
    get_optional,
    validate_processing_config,
    ConfigurationError,
)


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
    Load platform-specific configuration (file patterns, data types).
    
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
    Load parse profile configuration.
    
    Loads base config files and merges user.yaml overrides if present.
    
    Args:
        config_dir: Base configuration directory
        quiet: If True, suppress informational output
        
    Returns:
        Merged configuration dictionary
        
    Raises:
        ConfigurationError: If required config is missing
    """
    config = load_profile_config('parse', config_dir, quiet)
    validate_processing_config(config, 'parse')
    return config


def detect_dump_files(dumps_dir: str, data_types: List[str], file_patterns: Dict) -> List[Tuple[str, str]]:
    """
    Detect .zst dump files in the dumps directory and subfolders.
    
    Searches in:
        - dumps_dir/ (root)
        - dumps_dir/<data_type>/
    
    Returns:
        List of tuples (filepath, data_type) sorted alphabetically
    """
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
                    files.append((str(filepath), data_type, filename))
                    break
    
    # Sort by data type index then filename
    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[1], 99), x[2]))
    
    return [(f[0], f[1]) for f in files]


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
                    file_id = filename
                    files.append((str(filepath), file_id, data_type, filename))
    
    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def detect_parsed_csv_files(csv_dir: str, data_types: List[str], file_patterns: Dict) -> List[Tuple[str, str, str]]:
    """Detect parsed CSV files in the CSV directory."""
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
                file_id = filepath.stem
                files.append((str(filepath), file_id, data_type, filename))
    
    type_order = {dt: i for i, dt in enumerate(data_types)}
    files.sort(key=lambda x: (type_order.get(x[2], 99), x[3]))
    return [(f[0], f[1], f[2]) for f in files]


def get_file_identifier(filepath: str) -> str:
    """Extract identifier from filepath (e.g., RC_2023-01 from RC_2023-01.zst)."""
    return Path(filepath).stem


def run_pipeline(config_dir: str = "/app/config"):
    """
    Run the parse pipeline.
    
    Pipeline phases:
    1. Detect input sources (.zst files, existing JSON)
    2. Decompress .zst files
    3. Parse JSON to CSV
    
    Args:
        config_dir: Base configuration directory
    """
    # Load configuration
    config = load_config(config_dir)
    platform_config = load_platform_config(config_dir)
    
    # Get platform-specific parser
    parser = get_platform_parser()
    platform_config_dir = get_platform_config_dir(config_dir)
    
    proc_config = config['processing']
    
    # Get data types from profile config, fall back to platform config
    data_types = get_optional(config, 'processing', 'data_types', default=[])
    if not data_types:
        data_types = platform_config.get('data_types', [])
    if not data_types:
        raise ConfigurationError("No data_types configured. Set in user.yaml or platform config.")
    
    # Get file patterns from platform config
    file_patterns = platform_config.get('file_patterns', {})
    
    print(f"[sdb] Profile: parse")
    print(f"[sdb] Platform: {PLATFORM}")
    print(f"[sdb] Data types: {data_types}")

    # Paths
    dumps_dir = "/data/dumps"
    extracted_dir = "/data/extracted"
    csv_dir = "/data/csv"
    
    # Phase 1: Detect input sources
    print("\n" + "="*60)
    print("PHASE 1: INPUT DETECTION")
    print("="*60)
    
    zst_files = detect_dump_files(dumps_dir, data_types, file_patterns)
    print(f"[sdb] Found {len(zst_files)} .zst files in {dumps_dir}")
    
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)
    parsed_csv_files = detect_parsed_csv_files(csv_dir, data_types, file_patterns)
    
    print(f"[sdb] Found {len(json_files)} JSON files in extracted directory")
    print(f"[sdb] Found {len(parsed_csv_files)} CSV files in csv directory")
    
    # Filter out .zst files that already have extracted JSON
    existing_json_ids = {fid for _, fid, _ in json_files}
    existing_csv_ids = {fid for _, fid, _ in parsed_csv_files}
    pending_zst = [(p, dt) for p, dt in zst_files
                   if get_file_identifier(p) not in existing_json_ids
                   and get_file_identifier(p) not in existing_csv_ids]

    # Filter out JSON files that already have parsed CSV
    pending_json = [(p, fid, dt) for p, fid, dt in json_files if fid not in existing_csv_ids]

    print(f"[sdb] Pending: {len(pending_zst)} .zst, {len(pending_json)} JSON")
    
    if not (pending_zst or pending_json):
        print("\n[sdb] No files to process. Exiting.")
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

            try:
                decompress_zst(filepath, extract_dir)
                success_count += 1
            except Exception as e:
                print(f"[sdb] Error extracting {file_id}: {e}")
                fail_count += 1
        
        total_timings['extraction'] = time.time() - t_start
    
    # Phase 3: Parse JSON files to CSV
    json_files = detect_json_files(extracted_dir, data_types, file_patterns)  # Re-detect
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
        workers = get_required(config, 'processing', 'parse_workers')
        parallel_mode = get_required(config, 'processing', 'parallel_mode')
        
        if parallel_mode and len(files_to_parse) > 1:
            print(f"[sdb] Parallel mode: {len(files_to_parse)} files with {workers} workers")
            
            parse_input = [(json_path, data_type) for json_path, file_id, data_type in files_to_parse]
            
            try:
                parser.parse_files_parallel(
                    files=parse_input,
                    output_dir=csv_dir,
                    config_dir=platform_config_dir,
                    workers=workers
                )

                for json_path, file_id, data_type in files_to_parse:
                    success_count += 1

                    cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
                    if cleanup_temp and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[sdb] Removed: {Path(json_path).name}")

            except Exception as e:
                print(f"[sdb] Error in parallel parsing: {e}")
                fail_count += len(files_to_parse)
        else:
            # Sequential parsing
            for json_path, file_id, data_type in files_to_parse:
                try:
                    parser.parse_to_csv(
                        input_file=json_path,
                        output_dir=csv_dir,
                        data_type=data_type,
                        config_dir=platform_config_dir
                    )

                    success_count += 1

                    cleanup_temp = get_required(config, 'processing', 'cleanup_temp')
                    if cleanup_temp and os.path.exists(json_path):
                        os.remove(json_path)
                        print(f"[sdb] Removed: {Path(json_path).name}")

                except Exception as e:
                    print(f"[sdb] Error parsing {file_id}: {e}")
                    fail_count += 1
        
        total_timings['parsing'] = time.time() - t_start
    
    # Final summary
    print("\n" + "="*60)
    print("PIPELINE COMPLETE")
    print("="*60)
    print(f"[sdb] Successful: {success_count}")
    print(f"[sdb] Failed: {fail_count}")
    
    print(f"\n[sdb] Timing (minutes):")
    print(f"[sdb]   Extraction: {total_timings['extraction'] / 60:.2f}")
    print(f"[sdb]   Parsing:    {total_timings['parsing'] / 60:.2f}")
    total_time = sum(total_timings.values())
    print(f"[sdb]   Total:      {total_time / 60:.2f}")


def main():
    """Main entry point with optional watch mode."""
    config_dir = "/app/config"
    config = load_config(config_dir)
    watch_interval = get_required(config, 'processing', 'watch_interval')
    
    print(f"[sdb] Platform: {PLATFORM}")
    
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
