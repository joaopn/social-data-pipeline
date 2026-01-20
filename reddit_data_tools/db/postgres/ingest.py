"""
CSV to PostgreSQL ingestion for Reddit data.
"""

import psycopg
import logging
import yaml
from pathlib import Path
from typing import List, Optional, Dict


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


def load_yaml_file(file_path: str) -> Optional[Dict]:
    """Load a YAML configuration file, checking for .local.yaml override first."""
    file_path = Path(file_path)
    local_path = file_path.with_suffix('.local.yaml')
    if local_path.exists():
        file_path = local_path
    
    with open(file_path, 'r') as file:
        try:
            return yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(f"Error loading YAML file {file_path}: {exc}")
            return None


def load_services_config(config_dir: str = "/app/config") -> Dict:
    """Load services.yaml configuration with .local override support."""
    config = load_yaml_file(Path(config_dir) / "services.yaml")
    if config is None:
        return {"sidecars": {}}
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
        return 'double precision'
    elif type_def == 'text':
        return 'TEXT STORAGE EXTERNAL'
    
    # Default to TEXT STORAGE EXTERNAL for unknown types (no compression)
    return 'TEXT STORAGE EXTERNAL'


def get_column_list(data_type: str, config_dir: str = "/app/config") -> List[str]:
    """
    Get ordered list of columns for a data type.
    
    Order: [dataset, id, retrieved_utc, ...fields from YAML...]
    """
    field_list = load_yaml_file(Path(config_dir) / "reddit_field_list.yaml")
    if field_list is None:
        raise RuntimeError("Failed to load reddit_field_list.yaml")
    
    yaml_fields = field_list.get(data_type, [])
    if not yaml_fields:
        raise ValueError(f"No fields configured for data type: {data_type}")
    
    # Mandatory fields first, then YAML fields
    return MANDATORY_FIELDS + yaml_fields


def get_create_table_query(
    data_type: str, 
    schema: str, 
    table: str,
    config_dir: str = "/app/config"
) -> str:
    """
    Generate CREATE TABLE query dynamically from YAML configuration.
    
    All TEXT fields use STORAGE EXTERNAL (uncompressed TOAST) for external
    filesystem compression (ZFS, BTRFS).
    """
    
    full_table = f"{schema}.{table}"
    
    # Load field types
    field_types = load_yaml_file(Path(config_dir) / "reddit_field_types.yaml")
    if field_types is None:
        raise RuntimeError("Failed to load reddit_field_types.yaml")
    
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
    config_dir: str = "/app/config"
) -> str:
    """
    Generate COPY/INSERT query dynamically from YAML configuration.
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
    schema: str = 'public',
    table: Optional[str] = None,
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    check_duplicates: bool = True,
    create_indexes: bool = True,
    index_fields: Optional[List[str]] = None,
    config_dir: str = "/app/config"
):
    """
    Ingest a CSV file into PostgreSQL.
    
    Args:
        csv_file: Path to the CSV file
        data_type: 'submissions' or 'comments'
        dbname: Database name
        schema: Schema name (default: 'public')
        table: Table name (default: data_type)
        host: Database host (hostname or IP)
        port: Database port
        user: Database user
        check_duplicates: Whether to handle duplicates
        create_indexes: Whether to create indexes after ingestion
        index_fields: Fields to index (uses defaults if None)
        config_dir: Directory containing YAML configuration files
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
    
    Looks for tables matching pattern: {data_type}_{sidecar_name}
    (e.g., submissions_language, comments_embeddings)
    
    Returns list of sidecar names (without the data_type prefix).
    """
    base_prefix = f"{data_type}_"
    
    query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = %s 
        AND table_name LIKE %s
        AND table_name != %s
    """
    
    sidecars = []
    try:
        with psycopg.connect(dbname=dbname, user=user, host=host, port=port) as conn:
            with conn.cursor() as curr:
                # Match tables like submissions_* but exclude submissions_base
                curr.execute(query, (schema, f"{base_prefix}%", f"{data_type}_base"))
                for row in curr.fetchall():
                    table_name = row[0]
                    # Extract sidecar name (e.g., "submissions_language" -> "language")
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
    """
    base_table = f"{schema}.{data_type}_base"
    view_name = f"{schema}.{data_type}"
    
    select_cols = ["base.*"]
    joins = []
    
    # Add only sidecars that exist in DB AND are enabled in config
    for name, cfg in sidecars_config.items():
        if name in existing_sidecars and cfg.get('enabled', True):
            sidecar_table = f"{schema}.{data_type}_{name}"
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
    host: str = '127.0.0.1',
    port: int = 5432,
    user: str = 'postgres',
    config_dir: str = "/app/config"
):
    """
    Rebuild the view for a data type, including any existing sidecars.
    
    This is idempotent - can be called multiple times safely.
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
