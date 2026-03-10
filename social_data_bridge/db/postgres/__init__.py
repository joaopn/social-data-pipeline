"""
PostgreSQL database module.
"""

from .ingest import (
    ingest_csv,
    create_index,
    table_exists,
    analyze_table,
    ensure_database_exists,
    ensure_schema_exists,
    ensure_tablespaces,
    resolve_tablespace,
    get_column_list,
    ingest_classifier_csv,
    infer_classifier_schema,
    # Fast initial load functions
    create_fast_load_table,
    fast_ingest_csv,
    delete_duplicates,
    finalize_fast_load_table,
    create_fast_load_classifier_table,
    fast_ingest_classifier_csv,
)

__all__ = [
    'ingest_csv',
    'create_index',
    'table_exists',
    'analyze_table',
    'ensure_database_exists',
    'ensure_schema_exists',
    'ensure_tablespaces',
    'resolve_tablespace',
    'get_column_list',
    'ingest_classifier_csv',
    'infer_classifier_schema',
    'create_fast_load_table',
    'fast_ingest_csv',
    'delete_duplicates',
    'finalize_fast_load_table',
    'create_fast_load_classifier_table',
    'fast_ingest_classifier_csv',
]
