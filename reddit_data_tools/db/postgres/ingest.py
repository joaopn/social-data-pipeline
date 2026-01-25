"""
CSV to PostgreSQL ingestion for Reddit data.
"""

import psycopg
import logging
from pathlib import Path
from typing import List, Optional, Dict

from ...core.config import load_yaml_file, ConfigurationError


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


def load_services_config(config_dir: str) -> Dict:
    """
    Load services.yaml configuration.
    
    Args:
        config_dir: Directory containing services.yaml (postgres profile directory)
        
    Returns:
        Services configuration dictionary
        
    Raises:
        ConfigurationError: If services.yaml is missing
    """
    config = load_yaml_file(Path(config_dir) / "services.yaml")
    if config is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/services.yaml")
    return config


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


def get_column_list(data_type: str, config_dir: str) -> List[str]:
    """
    Get ordered list of columns for a data type.
    
    Order: [dataset, id, retrieved_utc, ...fields from YAML...]
    
    Args:
        data_type: 'submissions' or 'comments'
        config_dir: Directory containing reddit_field_list.yaml
        
    Returns:
        List of column names in order
        
    Raises:
        ConfigurationError: If config file is missing or data type not configured
    """
    field_list = load_yaml_file(Path(config_dir) / "reddit_field_list.yaml")
    if field_list is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/reddit_field_list.yaml")
    
    yaml_fields = field_list.get(data_type, [])
    if not yaml_fields:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")
    
    # Mandatory fields first, then YAML fields
    return MANDATORY_FIELDS + yaml_fields


def get_create_table_query(
    data_type: str, 
    schema: str, 
    table: str,
    config_dir: str
) -> str:
    """
    Generate CREATE TABLE query dynamically from YAML configuration.
    
    All TEXT fields use STORAGE EXTERNAL (uncompressed TOAST) for external
    filesystem compression (ZFS, BTRFS).
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        config_dir: Directory containing config files
        
    Returns:
        CREATE TABLE SQL query
        
    Raises:
        ConfigurationError: If config files are missing
    """
    
    full_table = f"{schema}.{table}"
    
    # Load field types
    field_types = load_yaml_file(Path(config_dir) / "reddit_field_types.yaml")
    if field_types is None:
        raise ConfigurationError(f"Required config file not found: {config_dir}/reddit_field_types.yaml")
    
    # Get column list
    columns = get_column_list(data_type, config_dir)
    
    # Build column definitions
    col_defs = []
    for col in columns:
        if col in MANDATORY_FIELD_SQL:
            col_defs.append(f"    {col} {MANDATORY_FIELD_SQL[col]}")
        elif col in field_types:
            sql_type = yaml_type_to_sql(field_types[col])
            col_defs.append(f"    {col} {sql_type}")
        else:
            # Default to TEXT for unknown fields
            col_defs.append(f"    {col} TEXT")
    
    columns_sql = ",\n".join(col_defs)
    
    return f"""
        CREATE TABLE IF NOT EXISTS {full_table}
        (
{columns_sql}
        );"""


def get_ingest_query(
    data_type: str, 
    schema: str, 
    table: str, 
    check_duplicates: bool,
    config_dir: str
) -> str:
    """
    Generate COPY/INSERT query dynamically from YAML configuration.
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        check_duplicates: Whether to handle duplicate IDs
        config_dir: Directory containing config files
        
    Returns:
        SQL query for data ingestion
    """
    
    full_table = f"{schema}.{table}"
    temp_table = f"temp_{data_type}"
    
    # Get column list from YAML
    columns_list = get_column_list(data_type, config_dir)
    columns = ", ".join(columns_list)
    
    # Fields to update on conflict (all except 'id' which is the primary key)
    update_fields = [col for col in columns_list if col != 'id']
    update_set = ",\n                ".join(
        f"{col} = EXCLUDED.{col}" for col in update_fields
    )
    
    if not check_duplicates:
        return f"""
            COPY {full_table}({columns})
            FROM '%s'
            DELIMITER ','
            QUOTE '"'
            HEADER
            CSV;
            """
    else:
        return f"""
            CREATE TEMPORARY TABLE {temp_table}
            AS SELECT * FROM {full_table} LIMIT 0;

            COPY {temp_table}({columns})
            FROM '%s'
            WITH (FORMAT csv, HEADER true, DELIMITER ',');

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
    args: List[str] = None
):
    """Execute a query, optionally with arguments."""
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
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
    user: str = 'postgres'
) -> bool:
    """Check if an index exists in the database."""
    query = "SELECT 1 FROM pg_indexes WHERE indexname = %s"
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            curr.execute(query, (index_name,))
            return curr.fetchone() is not None


def table_exists(
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> bool:
    """Check if a table exists in the database."""
    query = """
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = %s AND table_name = %s
    """
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
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
    user: str = 'postgres'
):
    """Run ANALYZE on a table. Used after initial bulk load."""
    full_table = f"{schema}.{table}"
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port, autocommit=True) as conn:
        with conn.cursor() as curr:
            curr.execute(f"ANALYZE {full_table}")
    
    print(f"[ANALYZE] ANALYZE complete: {full_table}")




def create_index(
    field: str,
    table: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> bool:
    """Create an index on a table field. Returns True if created, False if already existed."""
    
    full_table = f"{schema}.{table}"
    index_name = f"idx_{table}_{field}"
    
    # Check if index already exists
    if index_exists(index_name, dbname, host, port, user):
        return False
    
    query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {full_table} ({field});"
    
    print(f"[INDEX] Creating: {index_name}")
    
    with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
        with conn.cursor() as curr:
            try:
                curr.execute(query)
                conn.commit()
                print(f"[INDEX] Created: {index_name}")
                return True
            except Exception as e:
                logging.error("Exception occurred", exc_info=True)
                raise


def ensure_database_exists(
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
):
    """Create database if it doesn't exist."""
    
    with psycopg.connect(
        dbname='postgres',
        user=user,
        host=host,
        port=port,
        autocommit=True
    ) as conn:
        with conn.cursor() as curr:
            # Check if database exists first
            curr.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (dbname,)
            )
            if curr.fetchone() is None:
                curr.execute(f"CREATE DATABASE {dbname}")
                print(f"[DB] Created database: {dbname}")


def ensure_schema_exists(
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
):
    """Create schema if it doesn't exist. Thread-safe with exception handling."""
    
    if schema == 'public':
        return  # public schema always exists
    
    with psycopg.connect(
        dbname=dbname,
        user=user,
        host=host,
        port=port,
        autocommit=True
    ) as conn:
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
    config_dir: str,
    index_fields: Optional[List[str]] = None
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
        config_dir: Directory containing YAML configuration files
        index_fields: Fields to index (uses defaults if None)
    """
    if table is None:
        table = data_type
    
    print(f"[INGEST] Starting ingestion: {csv_file} -> {schema}.{table}")
    
    # Ensure database and schema exist
    ensure_database_exists(dbname, host, port, user)
    ensure_schema_exists(schema, dbname, host, port, user)
    
    # Create table if needed (schema from YAML)
    create_query = get_create_table_query(data_type, schema, table, config_dir)
    execute_query(create_query, dbname, host, port, user)
    
    # Ingest data (columns from YAML)
    ingest_query = get_ingest_query(data_type, schema, table, check_duplicates, config_dir)
    execute_query(ingest_query, dbname, host, port, user, args=[csv_file])
    
    # Create indexes if requested
    if create_indexes:
        if index_fields is None:
            if data_type == 'submissions':
                index_fields = ['author', 'subreddit', 'domain', 'created_utc']
            else:
                index_fields = ['author', 'subreddit', 'link_id', 'created_utc']
        
        for field in index_fields:
            try:
                create_index(field, table, schema, dbname, host, port, user)
            except Exception as e:
                print(f"[INDEX] Warning: Failed to create index on {field}: {e}")


def get_existing_sidecars(
    data_type: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> List[str]:
    """
    Query database for existing sidecar tables for a data type.
    
    Looks for tables matching pattern: {data_type}_base_{sidecar_name}
    (e.g., submissions_base_lingua, comments_base_emotions)
    
    Returns list of sidecar names (without the data_type_base_ prefix).
    """
    base_prefix = f"{data_type}_base_"
    
    query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_name LIKE %s
    """
    
    sidecars = []
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                # Match tables like submissions_base_*
                curr.execute(query, (schema, f"{base_prefix}%"))
                for row in curr.fetchall():
                    table_name = row[0]
                    # Extract sidecar name (e.g., "submissions_base_lingua" -> "lingua")
                    sidecar_name = table_name[len(base_prefix):]
                    sidecars.append(sidecar_name)
    except Exception as e:
        print(f"[VIEW] Warning: Could not query for sidecars: {e}")
    
    return sidecars


def get_view_query(
    data_type: str,
    schema: str,
    sidecars_config: Dict,
    existing_sidecars: List[str]
) -> str:
    """
    Generate CREATE OR REPLACE VIEW query with dynamic LEFT JOINs.
    
    Only includes sidecars that:
    1. Exist in the database (in existing_sidecars list)
    2. Are enabled in config (sidecars_config[name].get('enabled', True))
    
    When no sidecars exist, creates a simple passthrough view.
    Sidecar tables use naming: {data_type}_base_{sidecar_name}
    """
    base_table = f"{schema}.{data_type}_base"
    view_name = f"{schema}.{data_type}"
    
    select_cols = ["base.*"]
    joins = []
    
    # Add only sidecars that exist in DB AND are enabled in config
    for name, cfg in sidecars_config.items():
        if name in existing_sidecars and cfg.get('enabled', True):
            sidecar_table = f"{schema}.{data_type}_base_{name}"
            # Use short alias (first 3 chars of sidecar name)
            alias = name[:3]
            
            # Add sidecar columns (id is not included as it's the join key)
            for col in cfg.get('columns', {}).keys():
                select_cols.append(f"{alias}.{col}")
            
            joins.append(f"LEFT JOIN {sidecar_table} {alias} ON base.id = {alias}.id")
    
    join_clause = '\n        '.join(joins) if joins else ''
    
    return f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT {', '.join(select_cols)}
        FROM {base_table} base
        {join_clause};
    """


def rebuild_view(
    data_type: str,
    schema: str,
    dbname: str,
    host: str,
    port: int,
    user: str,
    config_dir: str
):
    """
    Rebuild the view for a data type, including any existing sidecars.
    
    This is idempotent - can be called multiple times safely.
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        config_dir: Directory containing services.yaml
    """
    # Load sidecar configuration
    services_config = load_services_config(config_dir)
    sidecars_config = services_config.get('sidecars', {})
    
    # Check which sidecars actually exist in the database
    existing_sidecars = get_existing_sidecars(
        data_type, schema, dbname, host, port, user
    )
    
    if existing_sidecars:
        print(f"[VIEW] Found sidecars for {data_type}: {existing_sidecars}")
    
    # Generate and execute the view query
    view_query = get_view_query(data_type, schema, sidecars_config, existing_sidecars)
    
    print(f"[VIEW] Creating view: {schema}.{data_type}")
    execute_query(view_query, dbname, host, port, user)
    print(f"[VIEW] View created: {schema}.{data_type}")


# =============================================================================
# Sidecar Table Functions
# =============================================================================

def infer_sql_type(values: List[str]) -> str:
    """
    Infer SQL type from a list of sample values.
    
    Priority: integer > real > boolean > text
    
    Args:
        values: List of string values from CSV
        
    Returns:
        SQL type string
    """
    has_int = False
    has_float = False
    has_bool = False
    
    for val in values:
        if not val or val == "":
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
        return 'text'
    
    # Determine type based on what we found
    if has_float:
        return 'real'
    if has_int:
        return 'integer'
    if has_bool:
        return 'boolean'
    
    # All empty or no values - default to text
    return 'text'


def infer_sidecar_schema(
    csv_file: str,
    base_columns: List[str],
    table_columns: Optional[List[str]] = None,
    n_rows: int = 1000,
    column_overrides: Optional[Dict[str, str]] = None
) -> Dict[str, str]:
    """
    Infer column types for sidecar table from CSV data.
    
    Reads N rows from CSV, identifies columns to include, and infers their SQL types.
    
    Args:
        csv_file: Path to CSV file
        base_columns: List of base table column names (used to identify classifier columns)
        table_columns: Optional list of base columns to include (empty/None = classifier cols only)
        n_rows: Number of rows to sample for type inference
        column_overrides: Optional dict of column_name -> sql_type overrides
        
    Returns:
        Dict of column_name -> sql_type for sidecar columns
    """
    import csv
    
    base_cols_set = set(base_columns)
    table_cols_set = set(table_columns) if table_columns else set()
    column_overrides = column_overrides or {}
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        if not header:
            raise ValueError(f"CSV file has no header: {csv_file}")
        
        # Columns to include:
        # 1. Classifier columns (not in base columns, not 'id')
        # 2. Base columns explicitly listed in table_columns
        sidecar_cols = []
        for c in header:
            if c == 'id':
                continue
            if c not in base_cols_set:
                # Classifier column - always include
                sidecar_cols.append(c)
            elif c in table_cols_set:
                # Base column explicitly requested
                sidecar_cols.append(c)
        
        if not sidecar_cols:
            raise ValueError(f"No sidecar columns found in CSV: {csv_file}")
        
        # Collect sample values for each sidecar column
        samples: Dict[str, List[str]] = {col: [] for col in sidecar_cols}
        
        for i, row in enumerate(reader):
            if i >= n_rows:
                break
            for col in sidecar_cols:
                samples[col].append(row.get(col, ""))
    
    # Infer types
    schema = {}
    for col in sidecar_cols:
        if col in column_overrides:
            schema[col] = yaml_type_to_sql(column_overrides[col])
        else:
            schema[col] = infer_sql_type(samples[col])
    
    return schema


def get_sidecar_create_table_query(
    data_type: str,
    sidecar_name: str,
    schema: str,
    column_schema: Dict[str, str]
) -> str:
    """
    Generate CREATE TABLE query for a sidecar table.
    
    Sidecar tables have:
    - id VARCHAR(7) PRIMARY KEY (FK to base table)
    - Classifier output columns with inferred types
    
    Table naming: {data_type}_base_{sidecar_name} (e.g., submissions_base_lingua)
    
    Args:
        data_type: 'submissions' or 'comments'
        sidecar_name: Name of the sidecar (e.g., 'lingua', 'toxic_roberta')
        schema: Database schema name
        column_schema: Dict of column_name -> sql_type
        
    Returns:
        CREATE TABLE SQL query
    """
    table_name = f"{data_type}_base_{sidecar_name}"
    full_table = f"{schema}.{table_name}"
    base_table = f"{schema}.{data_type}_base"
    
    # Build column definitions
    col_defs = ["    id character varying(7) PRIMARY KEY"]
    for col, sql_type in column_schema.items():
        col_defs.append(f"    {col} {sql_type}")
    
    columns_sql = ",\n".join(col_defs)
    
    return f"""
        CREATE TABLE IF NOT EXISTS {full_table}
        (
{columns_sql}
        );"""


def ingest_sidecar_from_csv(
    csv_file: str,
    data_type: str,
    sidecar_name: str,
    schema: str,
    columns: List[str],
    dbname: str,
    host: str,
    port: int,
    user: str,
    check_duplicates: bool = True
):
    """
    Ingest selected columns from CSV into sidecar table.
    
    Uses miller (mlr) to filter columns to a temp file, then PostgreSQL
    COPY FROM for maximum throughput. No Python in the data path.
    
    Args:
        csv_file: Path to CSV file
        data_type: 'submissions' or 'comments'
        sidecar_name: Name of the sidecar
        schema: Database schema name
        columns: List of column names to ingest (including 'id')
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        check_duplicates: Whether to handle duplicate IDs
    """
    import subprocess
    import os
    
    table_name = f"{data_type}_base_{sidecar_name}"
    full_table = f"{schema}.{table_name}"
    temp_table = f"temp_{table_name}"
    columns_str = ", ".join(columns)
    mlr_columns = ",".join(columns)
    
    # Temp file in writable shared volume (postgres container needs access)
    temp_dir = "/data/database/tmp"
    os.makedirs(temp_dir, exist_ok=True)
    csv_basename = os.path.basename(csv_file)
    temp_csv = f"{temp_dir}/{csv_basename}.filtered.tmp"
    
    # Fields to update on conflict (all except 'id')
    update_fields = [col for col in columns if col != 'id']
    update_set = ",\n                ".join(f"{col} = EXCLUDED.{col}" for col in update_fields)
    
    try:
        # Filter columns with miller (fast, native)
        mlr_cmd = ['mlr', '--csv', 'cut', '-f', mlr_columns, csv_file]
        with open(temp_csv, 'w') as f:
            result = subprocess.run(mlr_cmd, stdout=f, stderr=subprocess.PIPE, check=True)
        
        # Verify file was created
        if not os.path.exists(temp_csv):
            raise RuntimeError(f"Miller failed to create temp file: {temp_csv}")
        if os.path.getsize(temp_csv) == 0:
            raise RuntimeError(f"Miller produced empty file: {temp_csv}")
        
        # Build ingestion query using PostgreSQL COPY FROM file
        if not check_duplicates:
            query = f"""
                COPY {full_table}({columns_str})
                FROM '{temp_csv}'
                WITH (FORMAT csv, HEADER true);
            """
        else:
            query = f"""
                CREATE TEMPORARY TABLE {temp_table}
                AS SELECT * FROM {full_table} LIMIT 0;

                COPY {temp_table}({columns_str})
                FROM '{temp_csv}'
                WITH (FORMAT csv, HEADER true);

                INSERT INTO {full_table}
                    ({columns_str})
                SELECT
                    {columns_str}
                FROM
                    {temp_table}
                ON CONFLICT (id) DO UPDATE SET
                    {update_set};

                DROP TABLE {temp_table};
            """
        
        execute_query(query, dbname, host, port, user)
    finally:
        # Clean up temp file
        if os.path.exists(temp_csv):
            os.remove(temp_csv)


def get_sidecar_table_columns(
    data_type: str,
    sidecar_name: str,
    schema: str,
    dbname: str,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres'
) -> List[str]:
    """
    Get column names for an existing sidecar table (excluding 'id').
    
    Args:
        data_type: 'submissions' or 'comments'
        sidecar_name: Name of the sidecar
        schema: Database schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        
    Returns:
        List of column names (excluding 'id')
    """
    table_name = f"{data_type}_base_{sidecar_name}"
    
    query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s AND column_name != 'id'
        ORDER BY ordinal_position
    """
    
    columns = []
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, table_name))
                columns = [row[0] for row in curr.fetchall()]
    except Exception as e:
        print(f"[SIDECAR] Warning: Could not query columns for {table_name}: {e}")
    
    return columns


def ingest_sidecar_csv(
    csv_file: str,
    data_type: str,
    sidecar_name: str,
    dbname: str,
    schema: str,
    host: str,
    port: int,
    user: str,
    base_columns: List[str],
    table_columns: Optional[List[str]] = None,
    check_duplicates: bool = True,
    type_inference_rows: int = 1000,
    column_overrides: Optional[Dict[str, str]] = None
):
    """
    Ingest a sidecar CSV file into PostgreSQL.
    
    Auto-infers column types from CSV data. Creates table if needed.
    
    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        sidecar_name: Name of the sidecar (e.g., 'lingua')
        dbname: Database name
        schema: Schema name
        host: Database host
        port: Database port
        user: Database user
        base_columns: List of base table column names (to identify classifier cols)
        table_columns: Optional list of base columns to include (empty = classifier cols only)
        check_duplicates: Whether to handle duplicates
        type_inference_rows: Number of rows to sample for type inference
        column_overrides: Optional column type overrides
    """
    table_name = f"{data_type}_base_{sidecar_name}"
    
    print(f"[SIDECAR] Starting ingestion: {csv_file} -> {schema}.{table_name}")
    
    # Ensure schema exists
    ensure_schema_exists(schema, dbname, host, port, user)
    
    # Check if table exists
    if not table_exists(table_name, schema, dbname, host, port, user):
        # Infer schema from CSV
        print(f"[SIDECAR] Inferring schema from {type_inference_rows} rows...")
        column_schema = infer_sidecar_schema(
            csv_file, base_columns, table_columns, type_inference_rows, column_overrides
        )
        print(f"[SIDECAR] Inferred columns: {list(column_schema.keys())}")
        
        # Create table
        create_query = get_sidecar_create_table_query(
            data_type, sidecar_name, schema, column_schema
        )
        execute_query(create_query, dbname, host, port, user)
        print(f"[SIDECAR] Created table: {schema}.{table_name}")
        
        sidecar_columns = list(column_schema.keys())
    else:
        # Get existing columns from database
        sidecar_columns = get_sidecar_table_columns(
            data_type, sidecar_name, schema, dbname, host, port, user
        )
    
    # Ingest data (id + sidecar columns only)
    columns_to_ingest = ['id'] + sidecar_columns
    ingest_sidecar_from_csv(
        csv_file=csv_file,
        data_type=data_type,
        sidecar_name=sidecar_name,
        schema=schema,
        columns=columns_to_ingest,
        dbname=dbname,
        host=host,
        port=port,
        user=user,
        check_duplicates=check_duplicates
    )
    
    print(f"[SIDECAR] Ingestion complete: {schema}.{table_name}")


def rebuild_view_dynamic(
    data_type: str,
    schema: str,
    dbname: str,
    host: str,
    port: int,
    user: str,
    sidecars_config: Optional[Dict] = None
):
    """
    Rebuild views for a data type, dynamically discovering sidecar columns from DB.
    
    Creates individual views for each sidecar: {data_type}_{sidecar_name}
    (e.g., submissions_lingua) that join base table with that sidecar's columns.
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        dbname: Database name
        host: Database host
        port: Database port
        user: Database user
        sidecars_config: Optional config to check 'enabled' status and view_columns
    """
    sidecars_config = sidecars_config or {}
    
    # Check which sidecars actually exist in the database
    existing_sidecars = get_existing_sidecars(
        data_type, schema, dbname, host, port, user
    )
    
    if existing_sidecars:
        print(f"[VIEW] Found sidecars for {data_type}: {existing_sidecars}")
    
    base_table = f"{schema}.{data_type}_base"
    
    # Get base table columns to avoid duplicates in view
    base_columns = set()
    try:
        query = """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, f"{data_type}_base"))
                base_columns = {row[0] for row in curr.fetchall()}
    except Exception as e:
        print(f"[VIEW] Warning: Could not get base columns: {e}")
    
    # Create individual view for each sidecar
    for sidecar_name in existing_sidecars:
        # Check if enabled in config (default: True)
        cfg = sidecars_config.get(sidecar_name, {})
        if not cfg.get('enabled', True):
            continue
        
        sidecar_table = f"{schema}.{data_type}_base_{sidecar_name}"
        view_name = f"{schema}.{data_type}_{sidecar_name}"
        
        # Get actual columns from database
        db_columns = get_sidecar_table_columns(
            data_type, sidecar_name, schema, dbname, host, port, user
        )
        
        # Filter columns based on view_columns config (empty/missing = all columns)
        view_columns = cfg.get('view_columns', [])
        if view_columns:
            # Only include columns that are both in config and exist in DB
            columns = [col for col in view_columns if col in db_columns]
        else:
            # Include all columns from DB
            columns = db_columns
        
        # Exclude columns already in base table (avoid duplicates since we use base.*)
        columns = [col for col in columns if col not in base_columns]
        
        # Build select: base.* + sidecar columns (only non-base columns)
        select_cols = ["base.*"]
        for col in columns:
            select_cols.append(f"sc.{col}")
        
        view_query = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT {', '.join(select_cols)}
            FROM {base_table} base
            LEFT JOIN {sidecar_table} sc ON base.id = sc.id;
        """
        
        print(f"[VIEW] Creating view: {view_name}")
        execute_query(view_query, dbname, host, port, user)
        print(f"[VIEW] View created: {view_name}")
    
    # Create main combined view joining all enabled sidecars
    main_view_name = f"{schema}.{data_type}"
    main_select_cols = ["base.*"]
    main_joins = []
    seen_columns = set(base_columns)  # Track columns already included
    
    for sidecar_name in existing_sidecars:
        cfg = sidecars_config.get(sidecar_name, {})
        if not cfg.get('enabled', True):
            continue
        
        sidecar_table = f"{schema}.{data_type}_base_{sidecar_name}"
        alias = sidecar_name[:3]
        
        # Get columns and filter by view_columns config
        db_columns = get_sidecar_table_columns(
            data_type, sidecar_name, schema, dbname, host, port, user
        )
        
        view_columns = cfg.get('view_columns', [])
        if view_columns:
            columns = [col for col in view_columns if col in db_columns]
        else:
            columns = db_columns
        
        # Exclude already-seen columns (base columns + columns from previous sidecars)
        columns = [col for col in columns if col not in seen_columns]
        
        for col in columns:
            main_select_cols.append(f"{alias}.{col}")
            seen_columns.add(col)
        
        main_joins.append(f"LEFT JOIN {sidecar_table} {alias} ON base.id = {alias}.id")
    
    if main_joins:
        join_clause = '\n        '.join(main_joins)
        main_view_query = f"""
            CREATE OR REPLACE VIEW {main_view_name} AS
            SELECT {', '.join(main_select_cols)}
            FROM {base_table} base
            {join_clause};
        """
        
        print(f"[VIEW] Creating main view: {main_view_name}")
        execute_query(main_view_query, dbname, host, port, user)
        print(f"[VIEW] Main view created: {main_view_name}")
