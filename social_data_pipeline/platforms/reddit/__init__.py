"""
Reddit platform parser.

Handles Reddit-specific data formats including:
- Waterfall algorithm for deletion detection
- Base-36 ID conversion
- Old/new format compatibility (retrieved_on vs retrieved_utc)
"""

from .parser import (
    transform_json,
    parse_to_csv,
    parse_files_parallel,
    process_single_file,
    get_all_columns,
    determine_removal_status,
    base36_to_int,
    MANDATORY_FIELDS,
    MANDATORY_FIELD_TYPES,
)

__all__ = [
    'transform_json',
    'parse_to_csv',
    'parse_files_parallel',
    'process_single_file',
    'get_all_columns',
    'determine_removal_status',
    'base36_to_int',
    'MANDATORY_FIELDS',
    'MANDATORY_FIELD_TYPES',
]
