"""
JSON to CSV parsing for Reddit data dumps.
"""

import json
import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Any, Optional, Tuple

from .config import load_yaml_file, ConfigurationError


def escape_string(value: str) -> str:
    """Escape special characters in a string value."""
    if isinstance(value, str):
        return value.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\u0000', '')
    return value


def quote_field(field: Any) -> str:
    """Quote a field value for CSV formatting."""
    if field is None:
        return ''
    elif isinstance(field, str) and field:
        escaped_field = field.replace('"', '""')
        return f'"{escaped_field}"'
    return str(field)


def get_nested_data(data: Dict, field: str) -> Any:
    """Retrieve nested data from a dictionary using dot notation."""
    fields = field.split('.')
    for key in fields:
        if data is not None and key in data:
            data = data[key]
        else:
            return None
    return data


def enforce_data_type(key: str, value: Any, data_types: Dict) -> Any:
    """Enforce a specific data type for a value based on the configuration."""
    if not data_types:
        return value

    def cast_value(data_type, val):
        if data_type in ('integer', 'bigint'):
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        elif data_type == 'boolean':
            return val in (True, 'True', 'true', 1)
        elif data_type == 'float':
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        elif data_type in ('char', 'varchar', 'text'):
            val = str(val)
            if data_type in ('char', 'varchar'):
                max_length = data_types[key][1]
                return val[:max_length]
            return val
        else:
            return None

    data_type = data_types.get(key)
    if data_type:
        return cast_value(data_type[0] if isinstance(data_type, list) else data_type, value)
    else:
        return value


# Mandatory fields that are always included (not in YAML config)
MANDATORY_FIELDS = ['id', 'retrieved_utc']
MANDATORY_FIELD_TYPES = {
    'dataset': ['char', 7],
    'id': ['varchar', 7],
    'retrieved_utc': 'integer'
}


def base36_to_int(value: str) -> Optional[int]:
    """
    Convert a base 36 string to base 10 integer.
    
    Reddit IDs are base 36 (digits 0-9 and letters a-z).
    
    Args:
        value: Base 36 string (e.g., 'abc123')
        
    Returns:
        Base 10 integer, or None if conversion fails
    """
    if not value:
        return None
    try:
        return int(value, 36)
    except (ValueError, TypeError):
        return None


def determine_removal_status(data: Dict) -> Tuple[bool, str]:
    """
    Determine if content is deleted/removed and the removal type.
    
    Uses a waterfall algorithm (first match wins) checking sources in order
    of reliability. Outputs canonical Arctic Shift removal_type values.
    
    Priority order:
        1. _meta.removal_type (Arctic Shift 2023-11+, ground truth)
        2. _meta.was_deleted_later (marks deleted, continues to find type)
        3. removed_by_category (gold standard, 2018+)
        4. spam boolean flag (2020+ API)
        5. removed boolean flag (2020+ API)
        6. banned_by field (legacy, 2008-2018)
        7. Text content markers ([deleted], [removed])
        8. author == '[deleted]'
    
    Returns:
        Tuple of (is_deleted: bool, removal_type: str)
        
    Canonical removal_type values (from Arctic Shift _meta):
        - 'deleted' - User deleted their own content
        - 'author' - Author deleted (synonym for 'deleted')
        - 'moderator' - Removed by subreddit moderator
        - 'reddit' - Removed by Reddit admin/spam filter
        - 'automod_filtered' - Removed by AutoModerator
        - 'content_takedown' - Legal/DMCA takedown
        - 'copyright_takedown' - Copyright-specific takedown
        - 'community_ops' - Reddit Community Operations
        - '' - Not removed
    """
    meta = data.get('_meta', {})
    
    # Track if content was deleted (may be set early, type determined later)
    was_deleted_later = False
    
    # Priority 1: _meta.removal_type (Arctic Shift 2023-11+, ground truth)
    # Pass through as-is since it already uses canonical values
    if meta:
        meta_removal_type = meta.get('removal_type', '')
        if meta_removal_type:
            return True, meta_removal_type
        
        # Priority 2: _meta.was_deleted_later (mark deleted, continue to find type)
        if meta.get('was_deleted_later'):
            was_deleted_later = True
    
    # Priority 3: removed_by_category (gold standard, 2018+)
    removed_by_category = data.get('removed_by_category')
    if removed_by_category:
        category_lower = removed_by_category.lower()
        
        # Map to canonical values
        if category_lower in ('deleted', 'author'):
            return True, 'deleted'
        elif category_lower == 'moderator':
            return True, 'moderator'
        elif category_lower == 'reddit':
            return True, 'reddit'
        elif category_lower in ('anti_evil_ops', 'admin'):
            return True, 'reddit'
        elif category_lower == 'automod_filtered':
            return True, 'automod_filtered'
        elif category_lower == 'content_takedown':
            return True, 'content_takedown'
        elif category_lower == 'copyright_takedown':
            return True, 'copyright_takedown'
        elif category_lower == 'community_ops':
            return True, 'community_ops'
        else:
            # Unknown category, default to moderator
            return True, 'moderator'
    
    # Priority 4: spam boolean flag (2020+ API)
    if data.get('spam') is True:
        return True, 'reddit'
    
    # Priority 5: removed boolean flag (2020+ API)
    if data.get('removed') is True:
        return True, 'moderator'
    
    # Priority 6: banned_by field (legacy, 2008-2018)
    banned_by = data.get('banned_by')
    if banned_by is not None and banned_by != '' and banned_by is not False:
        # True (boolean) = Reddit spam filter (shadowban)
        if banned_by is True:
            return True, 'reddit'
        
        banned_by_str = str(banned_by).lower()
        if banned_by_str == 'true':
            return True, 'reddit'
        elif banned_by_str == 'automoderator':
            return True, 'automod_filtered'
        else:
            # Specific moderator username or other string
            return True, 'moderator'
    
    # Priority 7: Text content markers (body for comments, selftext for submissions)
    body = data.get('body', '')
    selftext = data.get('selftext', '')
    
    if body == '[removed]' or selftext == '[removed]':
        return True, 'moderator'
    
    if body == '[deleted]' or selftext == '[deleted]':
        return True, 'deleted'
    
    # Priority 8: author == '[deleted]'
    author = data.get('author', '')
    if author == '[deleted]':
        return True, 'deleted'
    
    # If _meta.was_deleted_later was set but we couldn't determine type
    if was_deleted_later:
        return True, 'deleted'
    
    # Not removed
    return False, ''


def get_all_columns(data_type: str, fields_to_extract: List[str]) -> List[str]:
    """
    Get the full column list for CSV header.
    
    Column order: [dataset, id, retrieved_utc, ...fields from YAML...]
    """
    return ['dataset'] + MANDATORY_FIELDS + fields_to_extract


def transform_json(data: Dict, dataset: str, data_type_config: Dict, fields_to_extract: List[str]) -> List:
    """
    Transform JSON data into a list of extracted values.
    
    CSV column order: [dataset, id, retrieved_utc, ...fields from YAML...]
    
    Handles both old and new Reddit data formats:
    - Uses retrieved_on as retrieved_utc if retrieved_utc is not present (old format)
    - For new format (2023-11+), uses _meta.retrieved_2nd_on as retrieved_utc when available
    - Detects deletions/removals from multiple fields across different Reddit API eras
    """
    meta = data.get('_meta', {})
    
    # Handle retrieved_utc field (mandatory, with fallback for old format)
    data['retrieved_utc'] = data.get('retrieved_utc', data.get('retrieved_on', ''))
    
    # For new format (2023-11+), use second retrieval time if available
    if meta and meta.get('retrieved_2nd_on'):
        data['retrieved_utc'] = meta['retrieved_2nd_on']
    
    # Determine deletion status and removal type
    is_deleted, removal_type = determine_removal_status(data)
    
    # Add deletion status to data
    data['is_deleted'] = is_deleted
    data['removal_type'] = removal_type
    
    # Compute id10 only if requested in field list (base 36 id to base 10)
    if 'id10' in fields_to_extract:
        data['id10'] = base36_to_int(data.get('id', ''))
    
    # Normalize text fields
    data['subreddit'] = (data.get('subreddit') or '').lower()
    data['author'] = (data.get('author') or '').lower()

    # Merge mandatory field types with config types
    all_types = {**MANDATORY_FIELD_TYPES, **data_type_config}
    
    # Build extracted list: [dataset, id, retrieved_utc, ...yaml fields...]
    extracted = [dataset]
    
    # Add mandatory fields first (id, retrieved_utc)
    for field in MANDATORY_FIELDS:
        value = get_nested_data(data, field)
        if isinstance(value, str):
            value = escape_string(value)
        value = enforce_data_type(field, value, all_types)
        extracted.append(value)
    
    # Add user-configured fields from YAML
    for field in fields_to_extract:
        value = get_nested_data(data, field)
        if isinstance(value, str):
            value = escape_string(value)
        last_key = field.split('.')[-1]
        value = enforce_data_type(last_key, value, all_types)
        extracted.append(value)
    
    return extracted


def process_single_file(
    input_file: str,
    output_file: str,
    data_type: str,
    data_type_config: Dict,
    fields_to_extract: List[str]
) -> tuple:
    """
    Process a single JSON file and write to CSV with headers.
    
    Uses a .temp file during writing and renames to final name on success.
    This ensures partial files from interrupted runs are not mistaken as complete.
    
    Args:
        input_file: Path to decompressed JSON file
        output_file: Path for output CSV file
        data_type: 'submissions' or 'comments'
        data_type_config: Field type configuration
        fields_to_extract: List of fields to extract
        
    Returns:
        Tuple of (input_size, output_file)
    """
    prefix = "RS_" if data_type == "submissions" else "RC_"
    dataset = Path(input_file).name.replace(prefix, "")
    
    output_path = Path(output_file)
    temp_path = output_path.with_suffix(output_path.suffix + '.temp')
    
    # Clean up any leftover temp file from interrupted run
    if temp_path.exists():
        print(f"[PARSE] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()
    
    line_count = 0
    error_count = 0
    
    # Get column names for header
    columns = get_all_columns(data_type, fields_to_extract)
    header_row = ','.join(columns)
    
    try:
        with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, \
             open(temp_path, 'w', newline='', encoding='utf-8') as outfile:
            
            # Write header row
            outfile.write(header_row + '\n')
            
            for line in infile:
                cleaned_line = line.replace('\x00', '')
                if not cleaned_line.strip():
                    continue
                try:
                    data = json.loads(cleaned_line)
                    csv_data = transform_json(data, dataset, data_type_config, fields_to_extract)
                    csv_row = ','.join(map(quote_field, csv_data))
                    outfile.write(csv_row + '\n')
                    line_count += 1
                except json.JSONDecodeError as e:
                    error_count += 1
                    logging.error(f"Failed to decode line in {input_file}: {cleaned_line[:100]}... Error: {e}")
                    continue
        
        # Rename temp file to final output path on success
        temp_path.rename(output_path)
        
    except Exception:
        # Clean up temp file on failure
        if temp_path.exists():
            temp_path.unlink()
        raise
    
    input_size = os.path.getsize(input_file)
    output_size = os.path.getsize(output_file)
    
    print(f"[PARSE] {Path(input_file).name} -> {Path(output_file).name}")
    print(f"[PARSE] Rows: {line_count:,}, Errors: {error_count}, Output: {output_size / (1024**3):.2f} GB")
    
    return input_size, output_file


def parse_to_csv(
    input_file: str,
    output_dir: str,
    data_type: str,
    config_dir: str,
    use_type_subdir: bool = True
) -> str:
    """
    Parse a decompressed Reddit JSON file to CSV with headers.
    
    Args:
        input_file: Path to decompressed JSON file (e.g., RC_2023-01)
        output_dir: Directory for output CSV file (or base dir if use_type_subdir=True)
        data_type: 'submissions' or 'comments'
        config_dir: Directory containing configuration files (shared config dir)
        use_type_subdir: If True, output to output_dir/data_type/ (default: True)
        
    Returns:
        Path to the output CSV file
        
    Raises:
        ConfigurationError: If config files are missing
    """
    config_dir = Path(config_dir)
    output_dir = Path(output_dir)
    if use_type_subdir:
        output_dir = output_dir / data_type
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load configuration
    field_types = load_yaml_file(config_dir / "reddit_field_types.yaml")
    field_list = load_yaml_file(config_dir / "reddit_field_list.yaml")
    
    if field_types is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/reddit_field_types.yaml")
    if field_list is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/reddit_field_list.yaml")
    
    fields_to_extract = field_list.get(data_type, [])
    if not fields_to_extract:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")
    
    # Configure logging
    log_filename = output_dir / f"parsing_errors_{data_type}.log"
    logging.basicConfig(
        filename=str(log_filename),
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )
    
    # Determine output filename
    input_path = Path(input_file)
    output_file = output_dir / f"{input_path.name}.csv"
    
    # Process the file
    _, output_path = process_single_file(
        input_file=str(input_path),
        output_file=str(output_file),
        data_type=data_type,
        data_type_config=field_types,
        fields_to_extract=fields_to_extract
    )
    
    # Clean up empty log file
    try:
        if log_filename.exists() and log_filename.stat().st_size == 0:
            log_filename.unlink()
    except Exception:
        pass
    
    return output_path


def _parse_file_worker(args: Tuple[str, str, str, str]) -> Tuple[str, str, str]:
    """
    Worker function for parallel parsing. Must be at module level for pickling.
    
    Args:
        args: Tuple of (input_file, output_dir, data_type, config_dir)
        
    Returns:
        Tuple of (input_file, csv_path, data_type)
    """
    input_file, output_dir, data_type, config_dir = args
    csv_path = parse_to_csv(input_file, output_dir, data_type, config_dir)
    return input_file, csv_path, data_type


def parse_files_parallel(
    files: List[Tuple[str, str]],
    output_dir: str,
    config_dir: str,
    workers: int
) -> List[Tuple[str, str]]:
    """
    Parse multiple JSON files to CSV in parallel.
    
    Args:
        files: List of tuples (input_file, data_type)
        output_dir: Directory for output CSV files
        config_dir: Directory containing configuration files (shared config dir)
        workers: Number of parallel workers
        
    Returns:
        List of tuples (csv_path, data_type) in the same order as input
    """
    if not files:
        return []
    
    print(f"[PARSE] Starting parallel parsing with up to {workers} workers for {len(files)} files")
    
    # Prepare arguments for workers
    worker_args = [
        (input_file, output_dir, data_type, config_dir)
        for input_file, data_type in files
    ]
    
    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        futures = [executor.submit(_parse_file_worker, args) for args in worker_args]
        
        # Collect results in order
        for future in futures:
            try:
                input_file, csv_path, data_type = future.result()
                results.append((csv_path, data_type))
            except Exception as e:
                print(f"[PARSE] Error in parallel parsing: {e}")
                raise
    
    print(f"[PARSE] Parallel parsing complete: {len(results)} files processed")
    return results
