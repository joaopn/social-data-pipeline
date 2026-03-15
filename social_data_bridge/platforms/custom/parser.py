"""
Custom platform JSON to CSV/Parquet parsing.

This module provides a simple parser for arbitrary JSON/NDJSON data
without any platform-specific transformation logic.
Used by all custom/* platforms.
"""

import json
import os
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Tuple

from ...core.config import ConfigurationError
from ...core.parser import (
    escape_string,
    quote_field,
    flatten_record,
    flatten_record_parquet,
    write_parquet_file,
)


def transform_json(data: Dict, dataset: str, data_type_config: Dict, fields_to_extract: List[str], file_format: str = 'csv') -> List:
    """
    Transform JSON data into a list of extracted values.

    Generic transformation - no platform-specific logic.
    """
    _flatten = flatten_record_parquet if file_format == 'parquet' else flatten_record
    return [dataset] + _flatten(data, fields_to_extract, data_type_config)


def process_single_file(
    input_file: str,
    output_file: str,
    data_type: str,
    data_type_config: Dict,
    fields_to_extract: List[str],
    file_format: str = 'csv',
) -> tuple:
    """
    Process a single JSON file and write to CSV or Parquet.

    Uses a .temp file during writing and renames to final name on success.
    """
    dataset = Path(input_file).stem

    output_path = Path(output_file)
    temp_path = output_path.with_suffix(output_path.suffix + '.temp')

    # Clean up any leftover temp file from interrupted run
    if temp_path.exists():
        print(f"[sdb] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()

    line_count = 0
    error_count = 0

    # Get column names for header
    columns = ['dataset'] + fields_to_extract

    try:
        if file_format == 'parquet':
            # Parquet: accumulate rows as dicts, write with Polars at end
            rows = []
            with open(input_file, 'r', encoding='utf-8', errors='replace') as infile:
                for line in infile:
                    cleaned_line = line.replace('\x00', '')
                    if not cleaned_line.strip():
                        continue
                    try:
                        data = json.loads(cleaned_line)
                        values = transform_json(data, dataset, data_type_config, fields_to_extract, file_format='parquet')
                        rows.append(dict(zip(columns, values)))
                        line_count += 1
                    except json.JSONDecodeError as e:
                        error_count += 1
                        logging.error(f"Failed to decode line in {input_file}: {cleaned_line[:100]}... Error: {e}")
                        continue

            write_parquet_file(rows, columns, data_type_config, str(output_path))

        else:
            # CSV: original row-by-row write
            header_row = ','.join(columns)
            with open(input_file, 'r', encoding='utf-8', errors='replace') as infile, \
                 open(temp_path, 'w', newline='', encoding='utf-8') as outfile:

                outfile.write(header_row + '\n')

                for line in infile:
                    cleaned_line = line.replace('\x00', '')
                    if not cleaned_line.strip():
                        continue
                    try:
                        data = json.loads(cleaned_line)
                        csv_data = transform_json(data, dataset, data_type_config, fields_to_extract)
                        csv_row = ','.join(map(quote_field, csv_data))
                        outfile.write(csv_row + '\n')
                        line_count += 1
                    except json.JSONDecodeError as e:
                        error_count += 1
                        logging.error(f"Failed to decode line in {input_file}: {cleaned_line[:100]}... Error: {e}")
                        continue

            temp_path.rename(output_path)

    except Exception:
        if temp_path.exists():
            temp_path.unlink()
        raise

    input_size = os.path.getsize(input_file)
    output_size = os.path.getsize(output_file)

    print(f"[sdb] {Path(input_file).name} -> {Path(output_file).name}")
    print(f"[sdb] Rows: {line_count:,}, Errors: {error_count}, Output: {output_size / (1024**3):.2f} GB")

    return input_size, output_file


def parse_to_csv(
    input_file: str,
    output_dir: str,
    data_type: str,
    platform_config: Dict,
    use_type_subdir: bool = True
) -> str:
    """
    Parse a JSON/NDJSON file to CSV or Parquet.

    Output format is determined by platform_config['file_format'] (default: 'csv').
    """
    output_dir = Path(output_dir)
    if use_type_subdir:
        output_dir = output_dir / data_type
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract fields and types from platform config
    field_types = platform_config.get('field_types', {})
    if not field_types:
        raise ConfigurationError("No field_types configured in platform config")

    fields_to_extract = platform_config.get('fields', {}).get(data_type, [])
    if not fields_to_extract:
        raise ConfigurationError(f"No fields configured for data type: {data_type}")

    file_format = platform_config.get('file_format', 'csv')

    # Configure logging
    log_filename = output_dir / f"parsing_errors_{data_type}.log"
    logging.basicConfig(
        filename=str(log_filename),
        level=logging.ERROR,
        format='%(asctime)s:%(levelname)s:%(message)s'
    )

    # Determine output filename
    input_path = Path(input_file)
    ext = '.parquet' if file_format == 'parquet' else '.csv'
    output_file = output_dir / f"{input_path.name}{ext}"

    # Process the file
    _, output_path = process_single_file(
        input_file=str(input_path),
        output_file=str(output_file),
        data_type=data_type,
        data_type_config=field_types,
        fields_to_extract=fields_to_extract,
        file_format=file_format,
    )

    # Clean up empty log file
    try:
        if log_filename.exists() and log_filename.stat().st_size == 0:
            log_filename.unlink()
    except Exception:
        pass

    return output_path


def _parse_file_worker(args: Tuple[str, str, str, Dict]) -> Tuple[str, str, str]:
    """Worker function for parallel parsing."""
    input_file, output_dir, data_type, platform_config = args
    csv_path = parse_to_csv(input_file, output_dir, data_type, platform_config)
    return input_file, csv_path, data_type


def parse_files_parallel(
    files: List[Tuple[str, str]],
    output_dir: str,
    platform_config: Dict,
    workers: int
) -> List[Tuple[str, str]]:
    """Parse multiple JSON files to CSV/Parquet in parallel."""
    if not files:
        return []

    print(f"[sdb] Starting parallel parsing with up to {workers} workers for {len(files)} files")

    worker_args = [
        (input_file, output_dir, data_type, platform_config)
        for input_file, data_type in files
    ]

    results = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_parse_file_worker, args) for args in worker_args]

        for future in futures:
            try:
                input_file, csv_path, data_type = future.result()
                results.append((csv_path, data_type))
            except Exception as e:
                print(f"[sdb] Error in parallel parsing: {e}")
                raise

    print(f"[sdb] Parallel parsing complete: {len(results)} files processed")
    return results
