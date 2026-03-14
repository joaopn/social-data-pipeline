"""
CSV to PostgreSQL ingestion for social_data_bridge.
"""

import os
import psycopg
import logging
from pathlib import Path
from typing import List, Optional, Dict

from ...core.config import ConfigurationError


def _connect(dbname, user, host, port, password=None, **kwargs):
    """Create a psycopg connection, including password if provided."""
    params = dict(dbname=dbname, user=user, host=host, port=port, **kwargs)
    if password:
        params['password'] = password
    return psycopg.connect(**params)


# Mandatory fields always included (in this order at start of columns)
MANDATORY_FIELDS = ['dataset', 'id', 'retrieved_utc']

# Mandatory field SQL definitions
MANDATORY_FIELD_SQL = {
    'dataset': 'character(7) NOT NULL',
    'id': 'character varying(7) PRIMARY KEY',
    'retrieved_utc': 'integer'
}

# All TEXT fields use STORAGE EXTERNAL (uncompressed TOAST)
# This disables PostgreSQL compression - use filesystem compression (ZFS, BTRFS) instead

# Tablespace container path convention
TABLESPACE_BASE_PATH = '/data/tablespace'


def resolve_tablespace(name: Optional[str]) -> Optional[str]:
    """Return tablespace name for SQL clauses, or None for default (pgdata)."""
    if name is None or name == 'pgdata':
        return None
    return name


def ensure_tablespaces(
    tablespaces: Dict[str, str],
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
):
    """
    Create configured tablespaces if they don't already exist.
    Skips 'pgdata' (the default PostgreSQL tablespace).

    Args:
        tablespaces: Dict of tablespace_name -> host_path
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
    """
    if not tablespaces:
        return

    with _connect(dbname, user, host, port, password, autocommit=True) as conn:
        with conn.cursor() as curr:
            for ts_name in tablespaces:
                if ts_name == 'pgdata':
                    continue
                curr.execute(
                    "SELECT 1 FROM pg_tablespace WHERE spcname = %s", (ts_name,)
                )
                if curr.fetchone() is None:
                    location = f"{TABLESPACE_BASE_PATH}/{ts_name}"
                    # CREATE TABLESPACE doesn't support parameterized LOCATION
                    curr.execute(
                        f"CREATE TABLESPACE {ts_name} LOCATION '{location}'"
                    )
                    print(f"[sdb] Created tablespace: {ts_name} -> {location}")
                else:
                    print(f"[sdb] Tablespace exists: {ts_name}")


def configure_ingestion_session(
    cur,
    quiet: bool = False,
    parallel_workers: int = 8
) -> Dict[str, any]:
    """
    Configure PostgreSQL session for optimal bulk ingestion.

    Uses ONLY session-level settings that automatically clean up when the
    connection closes. No ALTER SYSTEM calls - if the script crashes, the
    server state is unaffected.

    Settings applied (session-level only):
    - wal_compression = 'zstd' (3-4x WAL size reduction)
    - maintenance_work_mem = shared_buffers / (1 + parallel_workers)
    - max_parallel_maintenance_workers (for parallel index builds)
    - maintenance_io_concurrency = 500 (for NVMe)
    - synchronous_commit = off
    - temp_file_limit = -1

    Note: max_wal_size cannot be set at session level. For large ingestions,
    users should increase it in postgresql.conf (recommend 4x shared_buffers).

    Args:
        cur: Active psycopg cursor. The connection MUST stay open during the
             entire ingestion for settings to remain active.
        quiet: If True, suppress log output (useful for repeated calls).
        parallel_workers: max_parallel_maintenance_workers per index build.

    Returns:
        Dict with applied settings for logging/verification
    """
    settings = {}

    # Get current hardware constraints
    cur.execute("SELECT setting::bigint FROM pg_settings WHERE name = 'shared_buffers'")
    shared_buffers_pages = cur.fetchone()[0]
    shared_buffers_gb = (shared_buffers_pages * 8192) / (1024**3)  # pages to GB

    # Calculate maintenance memory to stay within shared_buffers
    # Each index build uses: maintenance_work_mem * (1 + parallel_workers)
    # Sequential builds: only 1 index at a time
    total_processes = 1 + parallel_workers
    maintenance_mem_mb = max(256, int((shared_buffers_gb * 1024) / total_processes))

    # Session-level settings (auto-cleanup when connection closes)
    cur.execute("SET synchronous_commit = 'off'")
    cur.execute("SET temp_file_limit = '-1'")
    cur.execute(f"SET maintenance_work_mem = '{maintenance_mem_mb}MB'")
    cur.execute(f"SET max_parallel_maintenance_workers = {parallel_workers}")
    cur.execute("SET wal_compression = 'zstd'")
    cur.execute("SET maintenance_io_concurrency = 500")

    settings['maintenance_work_mem'] = f'{maintenance_mem_mb}MB'
    settings['max_parallel_maintenance_workers'] = parallel_workers
    settings['wal_compression'] = 'zstd'
    settings['maintenance_io_concurrency'] = 500

    if not quiet:
        # Log applied settings
        print(f"[sdb] Session configured for ingestion:")
        print(f"      shared_buffers = {shared_buffers_gb:.1f}GB (detected)")
        for key, value in settings.items():
            print(f"      {key} = {value}")

        # Warn if max_wal_size is low (can't change at session level)
        cur.execute("SELECT setting FROM pg_settings WHERE name = 'max_wal_size'")
        max_wal_size = cur.fetchone()[0]
        recommended_wal_gb = max(16, min(300, int(shared_buffers_gb * 4)))
        cur.execute("SELECT pg_size_bytes(%s)", (max_wal_size,))
        current_wal_bytes = cur.fetchone()[0]
        if current_wal_bytes < recommended_wal_gb * 1024**3:
            print(f"      [!] max_wal_size = {max_wal_size} (consider {recommended_wal_gb}GB in postgresql.conf)")

    return settings



def yaml_type_to_sql(type_def) -> str:
    """
    Convert YAML type definition to PostgreSQL type.
    
    Args:
        type_def: Can be 'integer', 'boolean', 'float', 'text', 
                  or ['char', N] / ['varchar', N]
    
    Returns:
        PostgreSQL column type string
    """
    if isinstance(type_def, list):
        type_name, length = type_def[0], type_def[1]
        if type_name == 'char':
            return f'character({length})'
        elif type_name == 'varchar':
            return f'character varying({length})'
    elif type_def == 'integer':
        return 'integer'
    elif type_def == 'bigint':
        return 'bigint'
    elif type_def == 'boolean':
        return 'boolean'
    elif type_def == 'float':
        return 'real'
    elif type_def == 'text':
        return 'TEXT STORAGE EXTERNAL'
    
    # Default to TEXT STORAGE EXTERNAL for unknown types (no compression)
    return 'TEXT STORAGE EXTERNAL'


def get_column_list(data_type: str, platform_config: Dict, csv_file: str = None) -> List[str]:
    """
    Get ordered list of columns for a data type.

    Order: [dataset, id, retrieved_utc, ...fields from platform config..., (lingua fields if applicable)]

    Args:
        data_type: Data type key (e.g., 'submissions', 'comments')
        platform_config: Loaded platform configuration dict (must contain 'fields')
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are appended

    Returns:
        List of column names in order

    Raises:
        ConfigurationError: If no fields configured for the data type
    """
    yaml_fields = platform_config.get('fields', {}).get(data_type, [])
    if not yaml_fields:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")

    # Mandatory fields first, then YAML fields
    columns = MANDATORY_FIELDS + yaml_fields

    # Append lingua columns if this is a lingua file
    if csv_file and 'lingua' in csv_file:
        columns = columns + ['lang', 'lang_prob', 'lang2', 'lang2_prob', 'lang_chars']

    return columns


def get_create_table_query(
    data_type: str,
    schema: str,
    table: str,
    platform_config: Dict,
    csv_file: str = None,
    unlogged: bool = False,
    include_pk: bool = True,
    tablespace: str = None
) -> str:
    """
    Generate CREATE TABLE query dynamically from platform configuration.

    All TEXT fields use STORAGE EXTERNAL (uncompressed TOAST) for external
    filesystem compression (ZFS, BTRFS).

    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        platform_config: Loaded platform configuration dict (must contain 'fields' and 'field_types')
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are included
        unlogged: If True, create UNLOGGED table (faster, no WAL, no crash recovery)
        include_pk: If True, include PRIMARY KEY on id column
        tablespace: Optional tablespace name (None = default pg_default)

    Returns:
        CREATE TABLE SQL query

    Raises:
        ConfigurationError: If platform config is missing required keys
    """

    full_table = f"{schema}.{table}"

    # Get field types from platform config
    field_types = platform_config.get('field_types', {})
    if not field_types:
        raise ConfigurationError("No field_types configured in platform config")

    # Get column list (includes lingua columns if csv_file is a lingua file)
    columns = get_column_list(data_type, platform_config, csv_file)
    
    # Build column definitions
    col_defs = []
    for col in columns:
        if col == 'id':
            # Handle id column with optional PRIMARY KEY
            if include_pk:
                col_defs.append(f"    {col} {MANDATORY_FIELD_SQL[col]}")
            else:
                col_defs.append(f"    {col} character varying(7)")
        elif col in MANDATORY_FIELD_SQL:
            col_defs.append(f"    {col} {MANDATORY_FIELD_SQL[col]}")
        elif col in field_types:
            sql_type = yaml_type_to_sql(field_types[col])
            col_defs.append(f"    {col} {sql_type}")
        else:
            # Default to TEXT for unknown fields
            col_defs.append(f"    {col} TEXT")

    columns_sql = ",\n".join(col_defs)

    # Build CREATE statement
    create_type = "UNLOGGED TABLE" if unlogged else "TABLE"
    tablespace_clause = f"\n        TABLESPACE {tablespace}" if tablespace else ""

    return f"""
        CREATE {create_type} IF NOT EXISTS {full_table}
        (
{columns_sql}
        ){tablespace_clause};"""


def get_ingest_query(
    data_type: str,
    schema: str,
    table: str,
    check_duplicates: bool,
    platform_config: Dict,
    csv_file: str = None
) -> str:
    """
    Generate COPY/INSERT query dynamically from platform configuration.

    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        check_duplicates: Whether to handle duplicate IDs
        platform_config: Loaded platform configuration dict (must contain 'fields')
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are included

    Returns:
        SQL query for data ingestion
    """

    full_table = f"{schema}.{table}"
    temp_table = f"temp_{data_type}"

    # Get column list from platform config (includes lingua columns if csv_file is a lingua file)
    columns_list = get_column_list(data_type, platform_config, csv_file)
    columns = ", ".join(columns_list)
    
    # Fields to update on conflict (all except 'id' which is the primary key)
    update_fields = [col for col in columns_list if col != 'id']
    update_set = ",\n                ".join(
        f"{col} = EXCLUDED.{col}" for col in update_fields
    )
    
    # Build COPY options - add FORCE_NULL for lingua columns that may be empty (empty string -> NULL)
    copy_options = "FORMAT csv, HEADER true, DELIMITER ','"
    if csv_file and 'lingua' in csv_file:
        copy_options += ", FORCE_NULL (lang, lang_prob, lang2, lang2_prob, lang_chars)"
    
    if not check_duplicates:
        return f"""
            COPY {full_table}({columns})
            FROM '%s'
            WITH ({copy_options});
            """
    else:
        return f"""
            CREATE TEMPORARY TABLE {temp_table}
            AS SELECT * FROM {full_table} LIMIT 0;

            COPY {temp_table}({columns})
            FROM '%s'
            WITH ({copy_options});

            WITH latest_rows AS (
                SELECT DISTINCT ON (id)
                    {columns}
                FROM {temp_table}
                ORDER BY id, retrieved_utc DESC
            )
            INSERT INTO {full_table}
                ({columns})
            SELECT
                {columns}
            FROM
                latest_rows
            ON CONFLICT (id) DO UPDATE SET
                {update_set}
            WHERE
                {full_table}.retrieved_utc < EXCLUDED.retrieved_utc;

            DROP TABLE {temp_table};
            """


def execute_query(
    query: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None,
    args: List[str] = None
):
    """Execute a query, optionally with arguments."""

    with _connect(dbname, user, host, port, password) as conn:
        with conn.cursor() as curr:
            if args is None or len(args) == 0:
                try:
                    curr.execute(query)
                    conn.commit()
                except Exception as e:
                    logging.error("Exception occurred", exc_info=True)
                    raise
            else:
                for arg in args:
                    try:
                        curr.execute(query % arg)
                        conn.commit()
                    except Exception as e:
                        logging.error(f"Exception occurred with arg {arg}", exc_info=True)
                        raise


def index_exists(
    index_name: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
) -> bool:
    """Check if an index exists in the database."""
    query = "SELECT 1 FROM pg_indexes WHERE indexname = %s"
    with _connect(dbname, user, host, port, password) as conn:
        with conn.cursor() as curr:
            curr.execute(query, (index_name,))
            return curr.fetchone() is not None


def table_exists(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
) -> bool:
    """Check if a table exists in the database."""
    query = """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """
    try:
        with _connect(dbname, user, host, port, password) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, table))
                return curr.fetchone() is not None
    except Exception:
        return False


def analyze_table(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
):
    """Run ANALYZE on a table. Used after initial bulk load."""
    full_table = f"{schema}.{table}"

    with _connect(dbname, user, host, port, password, autocommit=True) as conn:
        with conn.cursor() as curr:
            curr.execute(f"ANALYZE {full_table}")
    
    print(f"[sdb] ANALYZE complete: {full_table}")


# =============================================================================
# Fast Initial Load Functions
# =============================================================================
# Creates tables without PK for fast bulk COPY, then adds PK after all data
# is loaded. Avoids per-row index maintenance during ingestion.

def delete_duplicates(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None,
    order_column: Optional[str] = 'retrieved_utc'
) -> int:
    """
    Delete duplicate rows keeping the best one per id.
    
    Uses ctid + ROW_NUMBER() window function. No temporary index needed -
    PostgreSQL's external merge sort handles this efficiently for rare duplicates.
    
    Args:
        table: Table name
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        order_column: Column to use for ordering duplicates (keeps highest value).
                      If None, arbitrary row is kept per id.
        
    Returns:
        Number of rows deleted
    """
    full_table = f"{schema}.{table}"
    
    # Build ORDER BY clause
    if order_column:
        order_by = f"id, {order_column} DESC"
    else:
        order_by = "id"
    
    query = f"""
        DELETE FROM {full_table}
        WHERE ctid IN (
            SELECT ctid FROM (
                SELECT ctid, ROW_NUMBER() OVER (
                    PARTITION BY id ORDER BY {order_by}
                ) as rn
                FROM {full_table}
            ) sub WHERE rn > 1
        );
    """
    
    print(f"[sdb] Removing duplicates from {full_table}...")
    
    with _connect(dbname, user, host, port, password) as conn:
        with conn.cursor() as curr:
            curr.execute(query)
            deleted_count = curr.rowcount
            conn.commit()
    
    print(f"[sdb] Deleted {deleted_count} duplicate rows from {full_table}")
    return deleted_count


def finalize_fast_load_table(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None,
    fk_reference_table: Optional[str] = None,
    tablespace: str = None
):
    """
    Finalize table after fast initial load: add PK, optionally add FK.

    Args:
        table: Table name
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        fk_reference_table: If provided, add FK constraint referencing this table (same schema)
        tablespace: Optional tablespace name for PK index (None = default)
    """
    full_table = f"{schema}.{table}"

    # Add PRIMARY KEY
    print(f"[sdb] Adding PRIMARY KEY to {full_table}...")
    add_pk_query = f"ALTER TABLE {full_table} ADD PRIMARY KEY (id);"

    with _connect(dbname, user, host, port, password) as conn:
        with conn.cursor() as curr:
            configure_ingestion_session(curr)
            if tablespace:
                curr.execute(f"SET default_tablespace = '{tablespace}'")
            curr.execute(add_pk_query)
            conn.commit()
    print(f"[sdb] PRIMARY KEY added to {full_table}")

    # Add FOREIGN KEY (if reference table provided)
    if fk_reference_table:
        ref_table = f"{schema}.{fk_reference_table}"
        if table_exists(fk_reference_table, schema, dbname, host, port, user, password):
            print(f"[sdb] Adding FOREIGN KEY to {full_table} -> {ref_table}...")
            fk_name = f"fk_{table}_id"
            add_fk_query = f"ALTER TABLE {full_table} ADD CONSTRAINT {fk_name} FOREIGN KEY (id) REFERENCES {ref_table}(id);"

            with _connect(dbname, user, host, port, password) as conn:
                with conn.cursor() as curr:
                    curr.execute(add_fk_query)
                    conn.commit()
            print(f"[sdb] FOREIGN KEY added to {full_table}")
        else:
            print(f"[sdb] Warning: Reference table {ref_table} not found, skipping FK constraint")


def fast_ingest_csv(
    csv_file: str,
    data_type: str,
    dbname: str,
    schema: str,
    table: str,
    host: str,
    port: int,
    user: str,
    platform_config: Dict,
    password: str = None
):
    """
    Fast ingest a single CSV file using blind COPY (no duplicate checking).

    Part of fast initial load - just appends data to existing table (no PK).
    Deduplication happens after all files are loaded.

    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name
        table: Table name
        host: Database host
        port: Database port
        user: Database user
        platform_config: Loaded platform configuration dict
    """
    print(f"[sdb] COPY: {csv_file}")

    # Use existing get_ingest_query with check_duplicates=False for blind COPY
    copy_query = get_ingest_query(data_type, schema, table, check_duplicates=False, platform_config=platform_config, csv_file=csv_file)
    execute_query(copy_query, dbname, host, port, user, password, args=[csv_file])


def create_fast_load_table(
    data_type: str,
    dbname: str,
    schema: str,
    table: str,
    host: str,
    port: int,
    user: str,
    platform_config: Dict,
    password: str = None,
    csv_file: str = None,
    tablespace: str = None
):
    """
    Create table without PK for fast initial load (PK added after all data is loaded).

    Args:
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name
        table: Table name
        host: Database host
        port: Database port
        user: Database user
        platform_config: Loaded platform configuration dict
        csv_file: Optional CSV file path for lingua column detection
        tablespace: Optional tablespace name (None = default pg_default)
    """
    ts_label = f" on tablespace {tablespace}" if tablespace else ""
    print(f"[sdb] Creating table {schema}.{table} (no PK){ts_label}...")

    # Ensure database and schema exist
    ensure_database_exists(dbname, host, port, user, password)
    ensure_schema_exists(schema, dbname, host, port, user, password)

    create_query = get_create_table_query(data_type, schema, table, platform_config, csv_file, include_pk=False, tablespace=tablespace)
    execute_query(create_query, dbname, host, port, user, password)

    print(f"[sdb] Created table {schema}.{table} (no PK)")


def create_fast_load_classifier_table(
    table_name: str,
    data_type: str,
    schema: str,
    dbname: str,
    host: str,
    port: int,
    user: str,
    column_list: List[str],
    column_types: Dict[str, str],
    password: str = None,
    tablespace: str = None
):
    """
    Create classifier table without PK/FK for fast initial load.

    Args:
        table_name: Classifier table name (e.g., 'submissions_lingua')
        data_type: 'submissions' or 'comments' (for FK reference later)
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        column_list: List of column names in CSV order
        column_types: Dict of column_name -> sql_type (excludes 'id')
        tablespace: Optional tablespace name (None = default pg_default)
    """
    ts_label = f" on tablespace {tablespace}" if tablespace else ""
    print(f"[sdb] Creating table {schema}.{table_name} (no PK, no FK){ts_label}...")

    ensure_schema_exists(schema, dbname, host, port, user, password)

    create_query = get_classifier_create_table_query(
        table_name, data_type, schema, column_list, column_types,
        use_foreign_key=False, include_pk=False, tablespace=tablespace
    )
    execute_query(create_query, dbname, host, port, user, password)

    print(f"[sdb] Created table {schema}.{table_name} (no PK, no FK)")


def fast_ingest_classifier_csv(
    csv_file: str,
    table_name: str,
    schema: str,
    dbname: str,
    host: str,
    port: int,
    user: str,
    column_list: List[str],
    password: str = None,
    nullable_cols: Optional[List[str]] = None
):
    """
    Fast ingest a single classifier CSV file using blind COPY (no duplicate checking).
    
    Args:
        csv_file: Path to the CSV file
        table_name: Classifier table name (e.g., 'submissions_lingua')
        schema: Schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        column_list: List of column names in table order
        nullable_cols: Columns that may have empty strings to treat as NULL
    """
    print(f"[sdb] COPY: {csv_file}")
    
    copy_query = get_classifier_ingest_query(
        table_name, schema, column_list, check_duplicates=False, nullable_cols=nullable_cols
    )
    execute_query(copy_query, dbname, host, port, user, password, args=[csv_file])


def create_index(
    field: str,
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None,
    quiet: bool = False,
    parallel_workers: int = 8,
    tablespace: str = None
) -> bool:
    """Create an index on a table field. Returns True if created, False if already existed."""

    full_table = f"{schema}.{table}"
    index_name = f"idx_{table}_{field}"

    # Check if index already exists
    if index_exists(index_name, dbname, host, port, user, password):
        return False

    tablespace_clause = f" TABLESPACE {tablespace}" if tablespace else ""
    query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table} ({field}){tablespace_clause};"

    print(f"[sdb] Creating index: {index_name}")

    with _connect(dbname, user, host, port, password) as conn:
        with conn.cursor() as curr:
            # Configure session for optimal index creation (maintenance_work_mem, etc.)
            configure_ingestion_session(curr, quiet=quiet, parallel_workers=parallel_workers)
            try:
                curr.execute(query)
                conn.commit()
                print(f"[sdb] Created index: {index_name}")
                return True
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                raise


def ensure_database_exists(
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
):
    """Create database if it doesn't exist."""

    with _connect('postgres', user, host, port, password, autocommit=True) as conn:
        with conn.cursor() as curr:
            # Check if database exists first
            curr.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (dbname,)
            )
            if curr.fetchone() is None:
                curr.execute(f"CREATE DATABASE {dbname}")
                print(f"[sdb] Created database: {dbname}")


def ensure_schema_exists(
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
):
    """Create schema if it doesn't exist. Thread-safe with exception handling."""

    if schema == 'public':
        return  # public schema always exists

    with _connect(dbname, user, host, port, password, autocommit=True) as conn:
        with conn.cursor() as curr:
            try:
                curr.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            except psycopg.errors.UniqueViolation:
                # Schema was created by another concurrent connection - this is fine
                pass


def ingest_csv(
    csv_file: str,
    data_type: str,
    dbname: str,
    schema: str,
    table: Optional[str],
    host: str,
    port: int,
    user: str,
    check_duplicates: bool,
    create_indexes: bool,
    platform_config: Dict,
    password: str = None,
    index_fields: Optional[List[str]] = None,
    tablespace: str = None
):
    """
    Ingest a CSV file into PostgreSQL.

    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name
        table: Table name (None to use data_type)
        host: Database host (hostname or IP)
        port: Database port
        user: Database user
        check_duplicates: Whether to handle duplicates
        create_indexes: Whether to create indexes after ingestion
        platform_config: Loaded platform configuration dict
        index_fields: Fields to index (uses defaults if None)
        tablespace: Optional tablespace name (None = default pg_default)
    """
    if table is None:
        table = data_type

    print(f"[sdb] Starting ingestion: {csv_file} -> {schema}.{table}")

    # Ensure database and schema exist
    ensure_database_exists(dbname, host, port, user, password)
    ensure_schema_exists(schema, dbname, host, port, user, password)

    # Create table if needed (schema from platform config, includes lingua columns if applicable)
    create_query = get_create_table_query(data_type, schema, table, platform_config, csv_file, tablespace=tablespace)
    execute_query(create_query, dbname, host, port, user, password)

    # Ingest data (columns from platform config, includes lingua columns if applicable)
    ingest_query = get_ingest_query(data_type, schema, table, check_duplicates, platform_config, csv_file)
    execute_query(ingest_query, dbname, host, port, user, password, args=[csv_file])

    # Create indexes if requested
    if create_indexes:
        if index_fields is None:
            # No default index fields - must be configured per platform
            index_fields = []

        for field in index_fields:
            try:
                create_index(field, table, schema, dbname, host, port, user, password, tablespace=tablespace)
            except Exception as e:
                print(f"[sdb] Warning: Failed to create index on {field}: {e}")




# =============================================================================
# Classifier Table Functions
# =============================================================================

def infer_sql_type(values: List[str]) -> tuple:
    """
    Infer SQL type from a list of sample values.
    
    Priority: integer > real > boolean > text
    
    Args:
        values: List of string values from CSV
        
    Returns:
        Tuple of (sql_type, has_empty) where has_empty indicates column has empty values
    """
    has_int = False
    has_float = False
    has_bool = False
    has_empty = False
    
    for val in values:
        if not val or val == "":
            has_empty = True
            continue
        
        # Try integer
        try:
            int(val)
            has_int = True
            continue
        except ValueError:
            pass
        
        # Try float
        try:
            float(val)
            has_float = True
            continue
        except ValueError:
            pass
        
        # Try boolean
        if val.lower() in ('true', 'false'):
            has_bool = True
            continue
        
        # If we hit a non-numeric, non-boolean value, it's text
        return ('text', has_empty)
    
    # Determine type based on what we found
    if has_float:
        return ('real', has_empty)
    if has_int:
        return ('integer', has_empty)
    if has_bool:
        return ('boolean', has_empty)
    
    # All empty or no values - default to text
    return ('text', has_empty)


def infer_classifier_schema(
    csv_file: str,
    n_rows: int = 1000,
    column_overrides: Optional[Dict[str, str]] = None
) -> tuple:
    """
    Infer column list and types for classifier table from CSV data.
    
    Reads N rows from CSV and infers SQL types for all columns.
    Returns columns in CSV header order (important for COPY).
    
    Args:
        csv_file: Path to CSV file
        n_rows: Number of rows to sample for type inference
        column_overrides: Optional dict of column_name -> sql_type overrides
        
    Returns:
        Tuple of (column_list, column_types_dict, nullable_cols) where:
        - column_list: List of column names in CSV order
        - column_types_dict: Dict of column_name -> sql_type
        - nullable_cols: List of columns that have empty values (need FORCE_NULL)
    """
    import csv
    
    column_overrides = column_overrides or {}
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        if not header:
            raise ValueError(f"CSV file has no header: {csv_file}")
        
        # Keep all columns in original order
        all_cols = list(header)
        
        if not all_cols:
            raise ValueError(f"No columns found in CSV: {csv_file}")
        
        # Collect sample values for each column (except 'id' which is always varchar)
        samples: Dict[str, List[str]] = {col: [] for col in all_cols if col != 'id'}
        
        for i, row in enumerate(reader):
            if i >= n_rows:
                break
            for col in samples:
                samples[col].append(row.get(col, ""))
    
    # Infer types (id is always varchar(7) PRIMARY KEY, handled separately)
    column_types = {}
    nullable_cols = []
    for col in all_cols:
        if col == 'id':
            continue  # id handled separately in table creation
        if col in column_overrides:
            column_types[col] = yaml_type_to_sql(column_overrides[col])
        else:
            sql_type, has_empty = infer_sql_type(samples[col])
            column_types[col] = sql_type
            # Track columns with empty values that aren't text (need FORCE_NULL for COPY)
            if has_empty and sql_type != 'text':
                nullable_cols.append(col)
    
    return all_cols, column_types, nullable_cols


def get_classifier_create_table_query(
    table_name: str,
    data_type: str,
    schema: str,
    column_list: List[str],
    column_types: Dict[str, str],
    use_foreign_key: bool = True,
    unlogged: bool = False,
    include_pk: bool = True,
    tablespace: str = None
) -> str:
    """
    Generate CREATE TABLE query for a classifier output table.

    Classifier tables have:
    - id VARCHAR(7) PRIMARY KEY (unless include_pk=False)
    - Optional FOREIGN KEY to main table (submissions/comments)
    - All other columns from CSV with inferred types

    Args:
        table_name: Full table name (e.g., 'submissions_lingua')
        data_type: 'submissions' or 'comments' (for FK reference)
        schema: Database schema name
        column_list: List of column names in CSV order
        column_types: Dict of column_name -> sql_type (excludes 'id')
        use_foreign_key: If True, add FK constraint to main table
        unlogged: If True, create UNLOGGED table (faster, no WAL, no crash recovery)
        include_pk: If True, include PRIMARY KEY on id column
        tablespace: Optional tablespace name (None = default pg_default)

    Returns:
        CREATE TABLE SQL query
    """
    full_table = f"{schema}.{table_name}"
    main_table = f"{schema}.{data_type}"
    
    # Build column definitions in CSV order
    col_defs = []
    for col in column_list:
        if col == 'id':
            if include_pk:
                col_defs.append("    id character varying(7) PRIMARY KEY")
            else:
                col_defs.append("    id character varying(7)")
        else:
            sql_type = column_types.get(col, 'text')
            col_defs.append(f"    {col} {sql_type}")
    
    # Add foreign key constraint if enabled (only if also including PK)
    if use_foreign_key and include_pk:
        col_defs.append(f"    CONSTRAINT fk_{table_name}_id FOREIGN KEY (id) REFERENCES {main_table}(id)")

    columns_sql = ",\n".join(col_defs)

    # Build CREATE statement
    create_type = "UNLOGGED TABLE" if unlogged else "TABLE"
    tablespace_clause = f"\n        TABLESPACE {tablespace}" if tablespace else ""

    return f"""
        CREATE {create_type} IF NOT EXISTS {full_table}
        (
{columns_sql}
        ){tablespace_clause};"""


def get_classifier_ingest_query(
    table_name: str,
    schema: str,
    column_list: List[str],
    check_duplicates: bool,
    nullable_cols: Optional[List[str]] = None
) -> str:
    """
    Generate COPY/INSERT query for classifier output table.
    
    Mirrors the base table ingestion pattern exactly.
    
    Args:
        table_name: Full table name (e.g., 'submissions_lingua')
        schema: Database schema name
        column_list: List of column names in CSV order
        check_duplicates: Whether to handle duplicate IDs
        nullable_cols: Columns that may have empty strings to treat as NULL
        
    Returns:
        SQL query for data ingestion
    """
    full_table = f"{schema}.{table_name}"
    temp_table = f"temp_{table_name}"
    
    columns = ", ".join(column_list)
    
    # Build COPY options - FORCE_NULL for columns with empty values
    copy_options = "FORMAT csv, HEADER true, DELIMITER ',', NULL ''"
    if nullable_cols:
        force_null_cols = ", ".join(nullable_cols)
        copy_options += f", FORCE_NULL ({force_null_cols})"
    
    # Fields to update on conflict (all except 'id' which is the primary key)
    update_fields = [col for col in column_list if col != 'id']
    update_set = ",\n                ".join(
        f"{col} = EXCLUDED.{col}" for col in update_fields
    )
    
    # Determine ORDER BY clause for deduplication
    # Use retrieved_utc if available to prefer most recent version
    if 'retrieved_utc' in column_list:
        order_by = "id, retrieved_utc DESC"
    else:
        order_by = "id"
    
    if not check_duplicates:
        return f"""
            COPY {full_table}({columns})
            FROM '%s'
            WITH ({copy_options});
            """
    else:
        # Only add WHERE clause if retrieved_utc exists (for preferring newer versions)
        if 'retrieved_utc' in column_list:
            where_clause = f"""
            WHERE
                {full_table}.retrieved_utc < EXCLUDED.retrieved_utc"""
        else:
            where_clause = ""
        
        return f"""
            CREATE TEMPORARY TABLE {temp_table}
            AS SELECT * FROM {full_table} LIMIT 0;

            COPY {temp_table}({columns})
            FROM '%s'
            WITH ({copy_options});

            WITH latest_rows AS (
                SELECT DISTINCT ON (id)
                    {columns}
                FROM {temp_table}
                ORDER BY {order_by}
            )
            INSERT INTO {full_table}
                ({columns})
            SELECT
                {columns}
            FROM
                latest_rows
            ON CONFLICT (id) DO UPDATE SET
                {update_set}{where_clause};

            DROP TABLE {temp_table};
            """


def get_table_column_list(
    table_name: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    password: str = None
) -> List[str]:
    """
    Get column names for an existing table in ordinal position order.
    
    Args:
        table_name: Table name (e.g., 'submissions_lingua')
        schema: Database schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        
    Returns:
        List of column names in table order
    """
    
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """
    
    columns = []
    try:
        with _connect(dbname, user, host, port, password) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, table_name))
                columns = [row[0] for row in curr.fetchall()]
    except Exception as e:
        print(f"[sdb] Warning: Could not query columns for {table_name}: {e}")
    
    return columns


def ingest_classifier_csv(
    csv_file: str,
    data_type: str,
    classifier_name: str,
    dbname: str,
    schema: str,
    host: str,
    port: int,
    user: str,
    password: str = None,
    check_duplicates: bool = True,
    type_inference_rows: int = 1000,
    column_overrides: Optional[Dict[str, str]] = None,
    use_foreign_key: bool = True,
    suffix: Optional[str] = None
):
    """
    Ingest a classifier CSV file into PostgreSQL.
    
    Auto-infers column types from CSV data. Creates table if needed.
    Ingests the full CSV file directly via COPY (identical to base table ingestion).
    
    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        classifier_name: Name of the classifier (e.g., 'lingua') - used for logging
        dbname: Database name
        schema: Schema name
        host: Database host
        port: Database port
        user: Database user
        check_duplicates: Whether to handle duplicates
        type_inference_rows: Number of rows to sample for type inference
        column_overrides: Optional column type overrides
        use_foreign_key: If True, add FK constraint to main table (default: True)
        suffix: Suffix for table name (e.g., '_lingua'). If None, uses _{classifier_name}
    """
    # Determine table suffix
    if suffix is None:
        suffix = f"_{classifier_name}"
    
    # Table name: {data_type}{suffix} (e.g., submissions_lingua)
    table_name = f"{data_type}{suffix}"
    
    print(f"[sdb] Starting ingestion: {csv_file} -> {schema}.{table_name}")
    
    # Ensure schema exists
    ensure_schema_exists(schema, dbname, host, port, user, password)

    # Check if table exists FIRST to avoid unnecessary CSV inference
    if not table_exists(table_name, schema, dbname, host, port, user, password):
        # Table doesn't exist - infer schema from CSV and create it
        print(f"[sdb] Inferring schema from {type_inference_rows} rows...")
        column_list, column_types, nullable_cols = infer_classifier_schema(
            csv_file, type_inference_rows, column_overrides
        )
        
        if nullable_cols:
            print(f"[sdb] Columns with empty values (using FORCE_NULL): {nullable_cols}")
        
        print(f"[sdb] Inferred columns: {column_list}")
        
        # Check if main table exists when FK is requested
        main_table_exists = table_exists(data_type, schema, dbname, host, port, user, password)
        actual_use_fk = use_foreign_key and main_table_exists
        
        if use_foreign_key and not main_table_exists:
            print(f"[sdb] Warning: Main table {schema}.{data_type} not found, creating without FK")
        
        # Create table
        create_query = get_classifier_create_table_query(
            table_name, data_type, schema, column_list, column_types, 
            use_foreign_key=actual_use_fk
        )
        execute_query(create_query, dbname, host, port, user, password)
        fk_status = " (with FK)" if actual_use_fk else " (no FK)"
        print(f"[sdb] Created table: {schema}.{table_name}{fk_status}")
    else:
        # Table exists - get schema from database (no CSV inference needed)
        column_list = get_table_column_list(
            table_name, schema, dbname, host, port, user, password
        )
        # No need to determine nullable_cols - if there was an issue, first file would have failed
        nullable_cols = []
    
    # Ingest data (COPY entire CSV directly, just like base table)
    ingest_query = get_classifier_ingest_query(
        table_name, schema, column_list, check_duplicates, nullable_cols
    )
    execute_query(ingest_query, dbname, host, port, user, password, args=[csv_file])
    
    print(f"[sdb] Ingestion complete: {schema}.{table_name}")
