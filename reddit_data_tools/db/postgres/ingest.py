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


def get_column_list(data_type: str, config_dir: str, csv_file: str = None) -> List[str]:
    """
    Get ordered list of columns for a data type.
    
    Order: [dataset, id, retrieved_utc, ...fields from YAML..., (lingua fields if applicable)]
    
    Args:
        data_type: 'submissions' or 'comments'
        config_dir: Directory containing reddit_field_list.yaml
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are appended
        
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
    columns = MANDATORY_FIELDS + yaml_fields
    
    # Append lingua columns if this is a lingua file
    if csv_file and 'lingua' in csv_file:
        columns = columns + ['lang', 'lang_prob', 'lang2', 'lang2_prob']
    
    return columns


def get_create_table_query(
    data_type: str, 
    schema: str, 
    table: str,
    config_dir: str,
    csv_file: str = None
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
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are included
        
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
    
    # Get column list (includes lingua columns if csv_file is a lingua file)
    columns = get_column_list(data_type, config_dir, csv_file)
    
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
    config_dir: str,
    csv_file: str = None
) -> str:
    """
    Generate COPY/INSERT query dynamically from YAML configuration.
    
    Args:
        data_type: 'submissions' or 'comments'
        schema: Database schema name
        table: Table name
        check_duplicates: Whether to handle duplicate IDs
        config_dir: Directory containing config files
        csv_file: Optional CSV file path - if contains 'lingua', lingua columns are included
        
    Returns:
        SQL query for data ingestion
    """
    
    full_table = f"{schema}.{table}"
    temp_table = f"temp_{data_type}"
    
    # Get column list from YAML (includes lingua columns if csv_file is a lingua file)
    columns_list = get_column_list(data_type, config_dir, csv_file)
    columns = ", ".join(columns_list)
    
    # Fields to update on conflict (all except 'id' which is the primary key)
    update_fields = [col for col in columns_list if col != 'id']
    update_set = ",\n                ".join(
        f"{col} = EXCLUDED.{col}" for col in update_fields
    )
    
    # Build COPY options - add FORCE_NULL for lingua probability columns (empty string -> NULL)
    copy_options = "FORMAT csv, HEADER true, DELIMITER ','"
    if csv_file and 'lingua' in csv_file:
        copy_options += ", FORCE_NULL (lang_prob, lang2_prob)"
    
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
    
    # Create table if needed (schema from YAML, includes lingua columns if applicable)
    create_query = get_create_table_query(data_type, schema, table, config_dir, csv_file)
    execute_query(create_query, dbname, host, port, user)
    
    # Ingest data (columns from YAML, includes lingua columns if applicable)
    ingest_query = get_ingest_query(data_type, schema, table, check_duplicates, config_dir, csv_file)
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
    use_foreign_key: bool = True
) -> str:
    """
    Generate CREATE TABLE query for a classifier output table.
    
    Classifier tables have:
    - id VARCHAR(7) PRIMARY KEY
    - Optional FOREIGN KEY to main table (submissions/comments)
    - All other columns from CSV with inferred types
    
    Args:
        table_name: Full table name (e.g., 'submissions_lingua')
        data_type: 'submissions' or 'comments' (for FK reference)
        schema: Database schema name
        column_list: List of column names in CSV order
        column_types: Dict of column_name -> sql_type (excludes 'id')
        use_foreign_key: If True, add FK constraint to main table
        
    Returns:
        CREATE TABLE SQL query
    """
    full_table = f"{schema}.{table_name}"
    main_table = f"{schema}.{data_type}"
    
    # Build column definitions in CSV order
    col_defs = []
    for col in column_list:
        if col == 'id':
            col_defs.append("    id character varying(7) PRIMARY KEY")
        else:
            sql_type = column_types.get(col, 'text')
            col_defs.append(f"    {col} {sql_type}")
    
    # Add foreign key constraint if enabled
    if use_foreign_key:
        col_defs.append(f"    CONSTRAINT fk_{table_name}_id FOREIGN KEY (id) REFERENCES {main_table}(id)")
    
    columns_sql = ",\n".join(col_defs)
    
    return f"""
        CREATE TABLE IF NOT EXISTS {full_table}
        (
{columns_sql}
        );"""


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
    user: str = 'postgres'
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
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                curr.execute(query, (schema, table_name))
                columns = [row[0] for row in curr.fetchall()]
    except Exception as e:
        print(f"[INGEST] Warning: Could not query columns for {table_name}: {e}")
    
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
    
    print(f"[{classifier_name.upper()}] Starting ingestion: {csv_file} -> {schema}.{table_name}")
    
    # Ensure schema exists
    ensure_schema_exists(schema, dbname, host, port, user)
    
    # Always infer schema to get nullable_cols for FORCE_NULL (even if table exists)
    print(f"[{classifier_name.upper()}] Inferring schema from {type_inference_rows} rows...")
    column_list, column_types, nullable_cols = infer_classifier_schema(
        csv_file, type_inference_rows, column_overrides
    )
    
    if nullable_cols:
        print(f"[{classifier_name.upper()}] Columns with empty values (using FORCE_NULL): {nullable_cols}")
    
    # Check if table exists
    if not table_exists(table_name, schema, dbname, host, port, user):
        print(f"[{classifier_name.upper()}] Inferred columns: {column_list}")
        
        # Check if main table exists when FK is requested
        main_table_exists = table_exists(data_type, schema, dbname, host, port, user)
        actual_use_fk = use_foreign_key and main_table_exists
        
        if use_foreign_key and not main_table_exists:
            print(f"[{classifier_name.upper()}] Warning: Main table {schema}.{data_type} not found, creating without FK")
        
        # Create table
        create_query = get_classifier_create_table_query(
            table_name, data_type, schema, column_list, column_types, 
            use_foreign_key=actual_use_fk
        )
        execute_query(create_query, dbname, host, port, user)
        fk_status = " (with FK)" if actual_use_fk else " (no FK)"
        print(f"[{classifier_name.upper()}] Created table: {schema}.{table_name}{fk_status}")
    else:
        # Get existing columns from database (in table order)
        column_list = get_table_column_list(
            table_name, schema, dbname, host, port, user
        )
    
    # Ingest data (COPY entire CSV directly, just like base table)
    ingest_query = get_classifier_ingest_query(
        table_name, schema, column_list, check_duplicates, nullable_cols
    )
    execute_query(ingest_query, dbname, host, port, user, args=[csv_file])
    
    print(f"[{classifier_name.upper()}] Ingestion complete: {schema}.{table_name}")


