"""
Core modules shared across all profiles.
"""

from .decompress import decompress_zst, decompress_file, detect_compression, is_compressed, strip_compression_extension
from .parser import (
    escape_string,
    quote_field,
    get_nested_data,
    enforce_data_type,
    flatten_record,
    write_csv_row,
)
from .state import PipelineState
from .config import (
    ConfigurationError,
    deep_merge,
    load_yaml_file,
    load_profile_config,
    get_required,
    get_optional,
    validate_processing_config,
    validate_database_config,
    validate_classifier_config,
    apply_env_overrides,
)

__all__ = [
    'decompress_zst',
    'decompress_file',
    'detect_compression',
    'is_compressed',
    'strip_compression_extension',
    'escape_string',
    'quote_field',
    'get_nested_data',
    'enforce_data_type',
    'flatten_record',
    'write_csv_row',
    'PipelineState',
    'ConfigurationError',
    'deep_merge',
    'load_yaml_file',
    'load_profile_config',
    'get_required',
    'get_optional',
    'validate_processing_config',
    'validate_database_config',
    'validate_classifier_config',
    'apply_env_overrides',
]
