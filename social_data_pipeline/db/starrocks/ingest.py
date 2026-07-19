"""
Parquet/CSV to StarRocks ingestion for social_data_pipeline.
Uses INSERT INTO ... SELECT FROM FILES() for server-side file reading.
StarRocks Primary Key tables handle upsert/dedup natively.
"""

import os
import logging
from typing import List, Dict

import mysql.connector

from ...core.config import ConfigurationError


def _sr_server_path(container_path: str) -> str:
    """Translate ingestion container path to StarRocks BE-visible path.

    The ingestion container is started by ``sdp run`` with source-specific mounts
    (e.g. /mnt/data/parsed/reddit -> /data/parsed).  The StarRocks container is
    started independently by ``sdp db start`` with parent-level mounts
    (e.g. /mnt/data/parsed -> /data/parsed, set via PARSED_PATH/OUTPUT_PATH in
    .env).  StarRocks BE reads files from its own filesystem, so paths must
    include the source subdirectory.
    """
    source = os.environ.get('SOURCE', '')
    if not source:
        return container_path

    for prefix in ('/data/parsed/', '/data/output/'):
        if container_path.startswith(prefix):
            return f"{prefix}{source}/{container_path[len(prefix):]}"
    return container_path


def _connect(host, port, user, password=None, database=None):
    """MySQL protocol connection factory for StarRocks."""
    params = dict(host=host, port=port, user=user)
    if password:
        params['password'] = password
    if database:
        params['database'] = database
    return mysql.connector.connect(**params)


def compute_bucket_count(platform_config, data_type):
    """Resolve the BUCKETS value for a data_type from platform_config.

    Reads `sr_buckets` from the source's platform.yaml, accepting either form:
        sr_buckets: 256             # uniform — all tables get 256
        sr_buckets:                 # per-data-type override
            submissions: 128
            comments: 512

    Returns None if no value is configured (caller leaves out the BUCKETS
    clause, letting SR fall back to its auto-bucketing default).
    """
    value = platform_config.get('sr_buckets')
    if value is None:
        return None

    if isinstance(value, dict):
        resolved = value.get(data_type)
        if resolved is None:
            return None
        return max(1, int(resolved))

    return max(1, int(value))


def yaml_type_to_sr_sql(type_def) -> str:
    """Map YAML type definitions to StarRocks column types."""
    # StarRocks STRING caps at 65,533 bytes; VARCHAR(1048576) is the 1 MB max.
    # Real-world dumps (e.g. Reddit selftext) exceed 65 KB, so use VARCHAR(1 MB).
    if isinstance(type_def, list):
        type_name, length = type_def[0], type_def[1]
        if type_name == 'char':
            return f'CHAR({length})'
        elif type_name == 'varchar':
            return f'VARCHAR({length})'
    elif type_def == 'integer':
        return 'INT'
    elif type_def == 'bigint':
        return 'BIGINT'
    elif type_def == 'boolean':
        return 'BOOLEAN'
    elif type_def == 'float':
        return 'FLOAT'
    elif type_def == 'text':
        return 'VARCHAR(1048576)'
    return 'VARCHAR(1048576)'


def get_column_list(data_type: str, platform_config: Dict, file: str = None) -> List[str]:
    """Get ordered list of columns for a data type.

    Order: [mandatory_fields..., ...fields from platform config..., (lingua fields if applicable)]

    Args:
        data_type: Data type key (e.g., 'submissions', 'comments')
        platform_config: Loaded platform configuration dict (must contain 'fields')
        file: Optional file path — if contains 'lingua', lingua columns are appended
    """
    yaml_fields = platform_config.get('fields', {}).get(data_type, [])
    if not yaml_fields:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")

    mandatory_fields = platform_config.get('mandatory_fields', [])
    columns = mandatory_fields + yaml_fields

    # Append lingua columns if this is a lingua file
    if file and 'lingua' in file:
        columns = columns + ['lang', 'lang_prob', 'lang2', 'lang2_prob', 'lang_chars']

    return columns


def _compression_property(compression):
    """Render the `"compression" = "<codec>"` PROPERTIES fragment, or None.

    Returns None when no codec is configured, so callers omit the property
    entirely and StarRocks applies its own default (LZ4) — existing installs
    without a configured codec stay unchanged.
    """
    if not compression:
        return None
    return f'"compression" = "{compression}"'


def _properties_clause(*fragments):
    """Assemble a PROPERTIES(...) clause from `"key" = "value"` fragments.

    None fragments are dropped (used for the conditional compression property).
    Returns an empty string when nothing remains, so a table that had no
    PROPERTIES stays without one.
    """
    kept = [f for f in fragments if f]
    if not kept:
        return ""
    return f"PROPERTIES({', '.join(kept)})"


def get_create_table_query(table, database, columns_list, platform_config, pk_column,
                           buckets=None, compression=None):
    """Build StarRocks CREATE TABLE statement.

    With a primary key: PRIMARY KEY (pk) table + DISTRIBUTED BY HASH(pk) —
    native upsert/dedup on duplicate keys.

    Without a primary key (custom platforms that opted out): Duplicate Key
    table (default model when no key clause is supplied) + DISTRIBUTED BY
    RANDOM. Re-runs append rows; no source-level dedup. Matches the shape
    `get_classifier_create_table_query` already uses for ML output tables.

    Args:
        pk_column: Source column name, or None to use the no-PK model.
        buckets: Explicit bucket count. If None, SR's auto-bucketing default
            is used (typically too low for single-BE clusters at TB scale —
            callers should pass an explicit value).
        compression: Table compression codec (e.g. 'ZSTD', 'LZ4'). None omits
            the property, leaving StarRocks' own default (LZ4).
    """
    field_types = platform_config.get('field_types', {})

    if pk_column is None:
        col_defs = [
            f"    `{c}` {yaml_type_to_sr_sql(field_types.get(c, 'text'))}"
            for c in columns_list
        ]
        columns_sql = ",\n".join(col_defs)
        distribute_clause = "DISTRIBUTED BY RANDOM"
        if buckets is not None:
            distribute_clause += f" BUCKETS {int(buckets)}"
        properties = _properties_clause(
            '"replication_num" = "1"',
            _compression_property(compression),
        )
        query = (
            f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
            f"{columns_sql}\n"
            f") {distribute_clause}\n"
            f"{properties}"
        )
        return query

    # StarRocks requires PK column to be first in the schema
    ordered_cols = [pk_column] + [c for c in columns_list if c != pk_column]

    col_defs = []
    for col in ordered_cols:
        col_type = yaml_type_to_sr_sql(field_types.get(col, 'text'))
        not_null = " NOT NULL" if col == pk_column else ""
        col_defs.append(f"    `{col}` {col_type}{not_null}")

    columns_sql = ",\n".join(col_defs)

    distribute_clause = f"DISTRIBUTED BY HASH(`{pk_column}`)"
    if buckets is not None:
        distribute_clause += f" BUCKETS {int(buckets)}"

    properties = _properties_clause(
        '"enable_persistent_index" = "true"',
        '"replication_num" = "1"',
        _compression_property(compression),
    )
    query = (
        f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
        f"{columns_sql}\n"
        f") PRIMARY KEY (`{pk_column}`)\n"
        f"{distribute_clause}\n"
        f"{properties}"
    )
    return query


def get_ingest_query(table, database, columns_list, file_path, file_format='parquet',
                     check_duplicates=False, order_field=None):
    """Build INSERT INTO ... SELECT FROM FILES() statement.

    StarRocks BE reads files directly from its mounted volumes.
    merge_condition enables conditional upsert (keep row with higher order_field).
    """
    columns = ", ".join(f"`{c}`" for c in columns_list)
    sr_path = _sr_server_path(file_path)

    # Build FILES() parameters
    files_params = f'"path" = "file://{sr_path}", "format" = "{file_format}"'

    if file_format == 'csv':
        files_params += ', "csv.column_separator" = ","'
        files_params += ', "csv.row_delimiter" = "\\n"'
        files_params += ', "csv.skip_header" = "1"'

    # Build merge_condition for conditional upsert
    properties = ""
    if check_duplicates and order_field and order_field in columns_list:
        properties = f' PROPERTIES("merge_condition" = "{order_field}")'

    query = (
        f"INSERT INTO `{database}`.`{table}` ({columns}){properties}\n"
        f"SELECT {columns}\n"
        f"FROM FILES({files_params})"
    )
    return query


def execute_query(query, host, port, user, password=None, database=None):
    """Execute a SQL query and return results or rowcount."""
    conn = _connect(host, port, user, password, database)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        if cursor.with_rows:
            result = cursor.fetchall()
        else:
            result = cursor.rowcount
        conn.commit()
        return result
    finally:
        cursor.close()
        conn.close()


def ensure_database_exists(database, host, port, user, password=None):
    """Create StarRocks database if it does not exist."""
    query = f"CREATE DATABASE IF NOT EXISTS `{database}`"
    execute_query(query, host, port, user, password)


def table_exists(table, database, host, port, user, password=None):
    """Check if a table exists in the StarRocks database."""
    query = (
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s"
    )
    conn = _connect(host, port, user, password, database)
    cursor = conn.cursor()
    try:
        cursor.execute(query, (database, table))
        count = cursor.fetchone()[0]
        return count > 0
    finally:
        cursor.close()
        conn.close()


def ingest_file(table, database, columns_list, file_path, file_format='parquet',
                check_duplicates=False, order_field=None,
                host='127.0.0.1', port=9030, user='root', password=None):
    """Ingest a single file into a StarRocks Primary Key table.

    Returns the number of rows loaded.
    """
    query = get_ingest_query(table, database, columns_list, file_path,
                             file_format, check_duplicates, order_field)

    conn = _connect(host, port, user, password, database)
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        row_count = cursor.rowcount
        conn.commit()
        return row_count
    except Exception as e:
        logging.error("Failed to ingest %s: %s", file_path, e)
        raise
    finally:
        cursor.close()
        conn.close()


def analyze_table(table, database, host, port, user, password=None):
    """Collect statistics for the query optimizer."""
    query = f"ANALYZE TABLE `{database}`.`{table}`"
    execute_query(query, host, port, user, password, database)


_ALTER_TERMINAL_STATES = {'FINISHED', 'CANCELLED'}


def _show_alter_column_jobs(database, host, port, user, password):
    """Return SHOW ALTER TABLE COLUMN rows as a list (empty if none)."""
    rows = execute_query(f"SHOW ALTER TABLE COLUMN FROM `{database}`",
                         host, port, user, password, database)
    return rows if isinstance(rows, list) else []


def _wait_for_active_alter_job(database, table, host, port, user, password, poll_interval,
                               line_prefix='', print_fn=None):
    """Block until no non-terminal alter job exists for `table`.

    StarRocks rejects CREATE INDEX while another schema change on the same
    table is running; we must drain any in-flight job (e.g. from an earlier
    interrupted run) before submitting the next one.
    """
    import time
    emit = print_fn or (lambda m: print(m, flush=True))
    last_progress = None
    while True:
        rows = _show_alter_column_jobs(database, host, port, user, password)
        active = [r for r in rows if r[1] == table and r[9] not in _ALTER_TERMINAL_STATES]
        if not active:
            return
        row = max(active, key=lambda r: int(r[0]))
        progress = row[11]
        if progress != last_progress:
            emit(f"[sdp]   {line_prefix}Waiting on alter job {row[0]}: "
                 f"state={row[9]} progress={progress}")
            last_progress = progress
        time.sleep(poll_interval)


def _poll_alter_job(database, job_id, host, port, user, password, poll_interval,
                    line_prefix='', print_fn=None):
    """Poll one alter JobId until it reaches FINISHED or CANCELLED. Returns (state, msg)."""
    import time
    emit = print_fn or (lambda m: print(m, flush=True))
    last_progress = None
    while True:
        rows = _show_alter_column_jobs(database, host, port, user, password)
        row = next((r for r in rows if int(r[0]) == job_id), None)
        if row is None:
            return 'FINISHED', ''
        state = row[9]
        progress = row[11]
        if state in _ALTER_TERMINAL_STATES:
            return state, row[10]
        if progress != last_progress:
            emit(f"[sdp]   {line_prefix}Alter job {job_id}: state={state} progress={progress}")
            last_progress = progress
        time.sleep(poll_interval)


def create_indexes(table, database, fields, host, port, user, password=None,
                   poll_interval=10.0, line_prefix='', print_fn=None):
    """Create BITMAP indexes on multiple columns, serializing per table.

    StarRocks only allows one schema change per table at a time. For each
    field we drain any in-flight alter job, submit CREATE INDEX, then poll
    the resulting alter job until it reaches FINISHED (or CANCELLED — in
    which case we log a warning and move on to the next field). No timeout:
    large tables can take a long time and bailing early just leaks half-built
    indexes. Optional `line_prefix` / `print_fn` are used when multiple tables
    run in parallel so each table's progress lines are distinguishable and
    stdout writes can be serialized.
    """
    emit = print_fn or (lambda m: print(m, flush=True))
    existing = set()
    rows = execute_query(f"SHOW INDEXES FROM `{database}`.`{table}`",
                         host, port, user, password, database)
    if isinstance(rows, list):
        existing = {row[2] for row in rows}

    created = []
    for field in fields:
        index_name = f"idx_{table}_{field}"
        if index_name in existing:
            continue

        _wait_for_active_alter_job(database, table, host, port, user, password, poll_interval,
                                   line_prefix=line_prefix, print_fn=emit)

        emit(f"[sdp]   {line_prefix}Building {index_name} ...")
        execute_query(
            f"CREATE INDEX `{index_name}` ON `{database}`.`{table}` (`{field}`) USING BITMAP",
            host, port, user, password, database)

        rows = _show_alter_column_jobs(database, host, port, user, password)
        matching = [int(r[0]) for r in rows if r[1] == table]
        if not matching:
            logging.warning("Submitted %s but no alter job found on %s.%s",
                            index_name, database, table)
            continue
        job_id = max(matching)

        state, msg = _poll_alter_job(database, job_id, host, port, user, password, poll_interval,
                                     line_prefix=line_prefix, print_fn=emit)
        if state == 'FINISHED':
            created.append(field)
            emit(f"[sdp]   {line_prefix}Built {index_name}")
        else:
            logging.warning("BITMAP index %s on %s.%s ended in state %s: %s",
                            index_name, database, table, state, msg)

    return created


def _arrow_type_to_sr_sql(arrow_type) -> str:
    """Map a PyArrow type to a StarRocks SQL type."""
    import pyarrow as pa
    if pa.types.is_integer(arrow_type):
        if arrow_type.bit_width <= 32:
            return 'INT'
        return 'BIGINT'
    if pa.types.is_floating(arrow_type):
        if arrow_type.bit_width <= 32:
            return 'FLOAT'
        return 'DOUBLE'
    if pa.types.is_boolean(arrow_type):
        return 'BOOLEAN'
    return 'STRING'


def _infer_sr_type(values: list) -> tuple:
    """Infer StarRocks SQL type from a list of sample string values.

    Priority: INT > FLOAT > BOOLEAN > STRING.

    Returns:
        Tuple of (sr_type, has_empty)
    """
    has_int = False
    has_float = False
    has_bool = False
    has_empty = False

    for val in values:
        if not val or val == "":
            has_empty = True
            continue

        try:
            int(val)
            has_int = True
            continue
        except ValueError:
            pass

        try:
            float(val)
            has_float = True
            continue
        except ValueError:
            pass

        if val.lower() in ('true', 'false'):
            has_bool = True
            continue

        return ('STRING', has_empty)

    if has_float:
        return ('FLOAT', has_empty)
    if has_int:
        return ('INT', has_empty)
    if has_bool:
        return ('BOOLEAN', has_empty)
    return ('STRING', has_empty)


def infer_classifier_schema(file_path, n_rows=1000, column_overrides=None):
    """Infer column names and StarRocks types from a classifier output file.

    Parquet: reads typed schema from file metadata (no sampling needed).
    CSV: samples N rows and infers types (integer > float > boolean > string).

    Args:
        file_path: Path to CSV or Parquet file
        n_rows: Number of rows to sample for type inference (CSV only)
        column_overrides: Optional dict of column_name -> sr_type overrides

    Returns:
        Tuple of (column_list, column_types_dict, nullable_cols)
    """
    column_overrides = column_overrides or {}

    if file_path.endswith('.parquet'):
        import pyarrow.parquet as pq
        schema = pq.read_schema(file_path)
        columns = [field.name for field in schema]
        types = {}
        for field in schema:
            if field.name in column_overrides:
                types[field.name] = column_overrides[field.name]
            else:
                types[field.name] = _arrow_type_to_sr_sql(field.type)
        return columns, types, []

    import csv

    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames

        if not header:
            raise ValueError(f"CSV file has no header: {file_path}")

        all_cols = list(header)
        samples: Dict[str, list] = {col: [] for col in all_cols}

        for i, row in enumerate(reader):
            if i >= n_rows:
                break
            for col in samples:
                samples[col].append(row.get(col, ""))

    column_types = {}
    nullable_cols = []
    for col in all_cols:
        if col in column_overrides:
            column_types[col] = column_overrides[col]
        else:
            sr_type, has_empty = _infer_sr_type(samples[col])
            column_types[col] = sr_type
            if has_empty and sr_type != 'STRING':
                nullable_cols.append(col)

    return all_cols, column_types, nullable_cols


def get_classifier_create_table_query(table, database, column_list, column_types, pk_column=None,
                                      buckets=None, compression=None):
    """Build CREATE TABLE for a classifier output table.

    Uses pre-inferred column types (from infer_classifier_schema) rather than
    platform_config field_types. No foreign key constraint (SR FKs are
    non-enforced optimizer hints only).

    Args:
        buckets: Explicit bucket count. If None, falls back to SR's
            auto-bucketing (same caveat as get_create_table_query).
        compression: Table compression codec (e.g. 'ZSTD', 'LZ4'). None omits
            the property, leaving StarRocks' own default (LZ4).
    """
    if pk_column:
        ordered_cols = [pk_column] + [c for c in column_list if c != pk_column]
    else:
        ordered_cols = list(column_list)

    col_defs = []
    for col in ordered_cols:
        col_type = column_types.get(col, 'STRING')
        not_null = " NOT NULL" if col == pk_column else ""
        col_defs.append(f"    `{col}` {col_type}{not_null}")

    columns_sql = ",\n".join(col_defs)

    if pk_column:
        distribute_clause = f"DISTRIBUTED BY HASH(`{pk_column}`)"
        if buckets is not None:
            distribute_clause += f" BUCKETS {int(buckets)}"
        properties = _properties_clause(
            '"enable_persistent_index" = "true"',
            '"replication_num" = "1"',
            _compression_property(compression),
        )
        query = (
            f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
            f"{columns_sql}\n"
            f") PRIMARY KEY (`{pk_column}`)\n"
            f"{distribute_clause}\n"
            f"{properties}"
        )
    else:
        distribute_clause = "DISTRIBUTED BY RANDOM"
        if buckets is not None:
            distribute_clause += f" BUCKETS {int(buckets)}"
        # replication_num=1 to match every other table shape (base PK, base
        # no-PK, classifier PK all set it). Without it StarRocks falls back to
        # default_replication_num=3, and CREATE fails on single-BE clusters —
        # which is every deployment this project targets. The base no-PK table
        # already sets it; this branch historically omitted PROPERTIES entirely.
        properties = _properties_clause(
            '"replication_num" = "1"',
            _compression_property(compression),
        )
        query = (
            f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` (\n"
            f"{columns_sql}\n"
            f") {distribute_clause}\n"
            f"{properties}"
        )
    return query
