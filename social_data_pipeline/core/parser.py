"""
Generic JSON to CSV/Parquet parsing utilities.

This module contains shared utilities used by all platform-specific parsers.
Platform-specific logic (like Reddit's waterfall algorithm) lives in platforms/*.
"""

from pathlib import Path
from typing import Dict, List, Any, Optional


def escape_string(value: str) -> str:
    """Escape special characters in a string value for CSV output."""
    if isinstance(value, str):
        return value.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\u0000', '')
    return value


def quote_field(field: Any) -> str:
    """Quote a field value for CSV formatting."""
    if field is None:
        return ''
    elif isinstance(field, str) and field:
        escaped_field = field.replace('"', '""')
        return f'"{escaped_field}"'
    return str(field)


def get_nested_data(data: Dict, field: str) -> Any:
    """
    Retrieve nested data from a dictionary using dot notation.
    
    Supports:
    - Simple nested access: 'user.name' -> data['user']['name']
    - Array index access: 'items.0.id' -> data['items'][0]['id']
    - Arrays are converted to pipe-separated strings
    
    Args:
        data: Source dictionary
        field: Dot-separated field path (e.g., 'user.profile.name')
        
    Returns:
        The nested value, or None if not found.
        Arrays are returned as pipe-separated strings.
    """
    keys = field.split('.')
    current = data
    
    for key in keys:
        if current is None:
            return None
        if isinstance(current, dict) and key in current:
            current = current[key]
        elif isinstance(current, list) and key.isdigit():
            # Array index access
            idx = int(key)
            current = current[idx] if 0 <= idx < len(current) else None
        else:
            return None
    
    # Convert arrays to pipe-separated string
    if isinstance(current, list):
        return '|'.join(str(item) for item in current if item is not None)
    
    return current


def enforce_data_type(key: str, value: Any, data_types: Dict) -> Any:
    """
    Enforce a specific data type for a value based on the configuration.
    
    Args:
        key: Field name (used to look up type and max length)
        value: Value to cast
        data_types: Dict mapping field names to types. Types can be:
            - 'integer', 'bigint': Cast to int
            - 'float': Cast to float
            - 'boolean': Cast to bool
            - 'text': Cast to str
            - ['char', N] or ['varchar', N]: Cast to str with max length N
            
    Returns:
        The value cast to the appropriate type, or None if conversion fails.
    """
    if not data_types:
        return value

    def cast_value(data_type, val):
        if data_type in ('integer', 'bigint'):
            try:
                return int(val)
            except (ValueError, TypeError):
                return None
        elif data_type == 'boolean':
            return val in (True, 'True', 'true', 1)
        elif data_type == 'float':
            try:
                return float(val)
            except (ValueError, TypeError):
                return None
        elif data_type in ('char', 'varchar', 'text'):
            if val is None:
                return None
            val = str(val)
            if data_type in ('char', 'varchar') and key in data_types:
                type_def = data_types[key]
                if isinstance(type_def, list) and len(type_def) > 1:
                    max_length = type_def[1]
                    return val[:max_length]
            return val
        else:
            return None

    data_type = data_types.get(key)
    if data_type:
        return cast_value(data_type[0] if isinstance(data_type, list) else data_type, value)
    else:
        return value


def flatten_record(record: Dict, fields: List[str], types: Dict) -> List[Any]:
    """
    Generic JSON to CSV row conversion.
    
    Extracts specified fields from a JSON record, applies type enforcement,
    and returns a list suitable for CSV output.
    
    Args:
        record: Source JSON object as dict
        fields: List of field names to extract (supports dot notation)
        types: Field type definitions for type enforcement
        
    Returns:
        List of extracted and type-enforced values
    """
    row = []
    for field in fields:
        value = get_nested_data(record, field)
        if isinstance(value, str):
            value = escape_string(value)
        last_key = field.split('.')[-1]
        value = enforce_data_type(last_key, value, types)
        row.append(value)
    return row


def write_csv_row(values: List[Any]) -> str:
    """Convert a list of values to a CSV row string."""
    return ','.join(map(quote_field, values))


# ---------------------------------------------------------------------------
# Parquet support
# ---------------------------------------------------------------------------

def escape_string_parquet(value: str) -> str:
    """Escape string for parquet output — only strips null bytes."""
    if isinstance(value, str):
        return value.replace('\u0000', '')
    return value


def yaml_type_to_polars(type_def):
    """Map a YAML type definition to a Polars dtype.

    Args:
        type_def: 'integer', 'bigint', 'float', 'boolean', 'text',
                  or ['char', N] / ['varchar', N]

    Returns:
        Polars DataType
    """
    import polars as pl
    if isinstance(type_def, list):
        return pl.Utf8
    mapping = {
        'integer': pl.Int64,
        'bigint': pl.Int64,
        'float': pl.Float64,
        'boolean': pl.Boolean,
        'text': pl.Utf8,
        'char': pl.Utf8,
        'varchar': pl.Utf8,
    }
    return mapping.get(type_def, pl.Utf8)


def build_parquet_schema(columns: List[str], field_types: Dict) -> dict:
    """Build a column→dtype mapping for a Polars DataFrame from YAML field_types."""
    import polars as pl
    schema = {}
    for col in columns:
        type_def = field_types.get(col)
        schema[col] = yaml_type_to_polars(type_def) if type_def else pl.Utf8
    return schema


PARQUET_ROW_GROUP_SIZE = 1_000_000
"""Default number of rows per Parquet row-group.

Matches the conventional 1M-row size used by Spark/Hive/DuckDB.
Configurable per-source via ``parquet_row_group_size`` in platform.yaml."""


class BatchedParquetWriter:
    """Incrementally writes row dicts to a Parquet file via PyArrow row-groups.

    Accumulates rows up to *batch_size*, converts to a Polars DataFrame,
    writes as a row-group through PyArrow's ParquetWriter, then discards the
    batch.  This keeps memory bounded regardless of total file size.

    Usage::

        writer = BatchedParquetWriter(columns, field_types, output_path)
        for row_dict in stream:
            writer.append(row_dict)
        writer.close()          # flushes remaining rows + atomic rename
    """

    def __init__(
        self,
        columns: List[str],
        field_types: Dict,
        output_path: str,
        batch_size: int = PARQUET_ROW_GROUP_SIZE,
    ):
        self._columns = columns
        self._schema = build_parquet_schema(columns, field_types)
        self._output_path = Path(output_path)
        self._temp_path = self._output_path.with_suffix(
            self._output_path.suffix + '.temp'
        )
        self._batch_size = batch_size
        self._buffer: List[Dict[str, Any]] = []
        self._pq_writer = None
        self._total_rows = 0

        if self._temp_path.exists():
            self._temp_path.unlink()

    # ------------------------------------------------------------------

    def append(self, row_dict: Dict[str, Any]) -> None:
        """Add a single row dict.  Flushes automatically when batch is full."""
        self._buffer.append(row_dict)
        if len(self._buffer) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        if not self._buffer:
            return

        import polars as pl
        import pyarrow.parquet as pq

        df = pl.DataFrame(self._buffer, schema=self._schema, orient='row')
        table = df.to_arrow()

        if self._pq_writer is None:
            self._pq_writer = pq.ParquetWriter(str(self._temp_path), table.schema)
        self._pq_writer.write_table(table)

        self._total_rows += len(self._buffer)
        self._buffer.clear()

    def close(self) -> int:
        """Flush remaining rows, close writer, atomic-rename to final path.

        Returns the total number of rows written.
        """
        try:
            self._flush()
            if self._pq_writer is not None:
                self._pq_writer.close()
                self._pq_writer = None
            if self._temp_path.exists():
                self._temp_path.rename(self._output_path)
        except Exception:
            self.cleanup()
            raise
        return self._total_rows

    def cleanup(self) -> None:
        """Close writer and remove temp file (for error paths)."""
        if self._pq_writer is not None:
            self._pq_writer.close()
            self._pq_writer = None
        if self._temp_path.exists():
            self._temp_path.unlink()


def write_parquet_file(
    rows: List[Dict[str, Any]],
    columns: List[str],
    field_types: Dict,
    output_path: str,
) -> int:
    """Write a list of row dicts as a Parquet file with proper dtypes.

    Uses a temp file + atomic rename.  Returns the number of rows written.

    NOTE: For large files, prefer ``BatchedParquetWriter`` which streams
    rows to disk incrementally without holding everything in memory.
    """
    import polars as pl
    output_path = Path(output_path)
    temp_path = output_path.with_suffix(output_path.suffix + '.temp')

    if temp_path.exists():
        temp_path.unlink()

    schema = build_parquet_schema(columns, field_types)

    try:
        df = pl.DataFrame(rows, schema=schema, orient='row')
        df.write_parquet(temp_path)
        temp_path.rename(output_path)
    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise

    return len(rows)


def flatten_record_parquet(record: Dict, fields: List[str], types: Dict) -> List[Any]:
    """Like flatten_record but uses parquet escaping (null-byte strip only)."""
    row = []
    for field in fields:
        value = get_nested_data(record, field)
        if isinstance(value, str):
            value = escape_string_parquet(value)
        last_key = field.split('.')[-1]
        value = enforce_data_type(last_key, value, types)
        row.append(value)
    return row
