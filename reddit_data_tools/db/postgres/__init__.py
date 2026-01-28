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
    get_column_list,
    ingest_classifier_csv,
    infer_classifier_schema,
    # Fast initial load functions (main tables)
    create_fast_load_table,
    fast_ingest_csv,
    delete_duplicates,
    finalize_fast_load_table,
    # Fast initial load functions (classifier tables)
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
    'get_column_list',
    'ingest_classifier_csv',
    'infer_classifier_schema',
    # Fast initial load functions (main tables)
    'create_fast_load_table',
    'fast_ingest_csv',
    'delete_duplicates',
    'finalize_fast_load_table',
    # Fast initial load functions (classifier tables)
    'create_fast_load_classifier_table',
    'fast_ingest_classifier_csv',
]
