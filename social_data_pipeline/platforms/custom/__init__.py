"""
Custom platform parser.

A simple parser for arbitrary JSON/NDJSON data without platform-specific logic.
Used by all custom/* platforms. Uses shared utilities from core/parser.py.
"""

from .parser import (
    transform_json,
    parse_to_csv,
    parse_files_parallel,
    process_single_file,
)

__all__ = [
    'transform_json',
    'parse_to_csv',
    'parse_files_parallel',
    'process_single_file',
]
