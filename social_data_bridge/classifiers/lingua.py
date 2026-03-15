"""
Lingua language detection for social_data_bridge pipeline.
Uses Polars for vectorized I/O and text processing.
Uses Lingua's native Rust parallelism via Rayon for detection.
Processes large files in batches to avoid memory blow-up.
Supports both CSV and Parquet file formats.
"""

import os
import time
from pathlib import Path
from typing import List, Dict, Tuple

import polars as pl

# Note: RAYON_NUM_THREADS must be set BEFORE importing Lingua.
# The orchestrator sets this from config before importing this module.
try:
    from lingua import Language, LanguageDetectorBuilder
    LINGUA_AVAILABLE = True
except ImportError:
    LINGUA_AVAILABLE = False
    Language = None


# Output columns this classifier adds
OUTPUT_COLUMNS = ['lang', 'lang_prob', 'lang2', 'lang2_prob', 'lang_chars']

# Mandatory fields for ingest output (required for ON CONFLICT resolution)
INGEST_MANDATORY_FIELDS = ['id', 'dataset', 'retrieved_utc']


_cached_detector = None
_cached_config_key = None


def _get_detector(languages: List[str], low_accuracy: bool, num_workers: int, quiet: bool = False):
    """Get or create cached Lingua detector."""
    global _cached_detector, _cached_config_key

    if not LINGUA_AVAILABLE:
        raise ImportError("Lingua not available. Install: pip install lingua-language-detector")

    config_key = (tuple(sorted(languages)), low_accuracy)

    if _cached_detector is not None and _cached_config_key == config_key:
        return _cached_detector

    lang_enums = []
    for lang_code in languages:
        lang_upper = lang_code.upper()
        if hasattr(Language, lang_upper):
            lang_enums.append(getattr(Language, lang_upper))
        else:
            if not quiet:
                print(f"[sdb] Warning: Unknown language '{lang_code}', skipping")

    if not lang_enums:
        raise ValueError("No valid languages specified in config")

    builder = LanguageDetectorBuilder.from_languages(*lang_enums)

    if low_accuracy:
        builder = builder.with_low_accuracy_mode()

    _cached_detector = builder.with_preloaded_language_models().build()
    _cached_config_key = config_key

    return _cached_detector


def get_text_columns(data_type: str, config: Dict) -> List[str]:
    """Get text columns for the given data type."""
    text_columns_config = config.get('text_columns', {})
    defaults = {'submissions': ['title', 'selftext'], 'comments': ['body']}
    return text_columns_config.get(data_type, defaults.get(data_type, []))


def _build_text_expr(text_columns: List[str], remove_strings: List[str], remove_patterns: List[str]) -> pl.Expr:
    """Build a Polars expression for text cleaning."""
    if len(text_columns) == 1:
        text_expr = pl.col(text_columns[0]).fill_null("")
    else:
        col_exprs_nullable = [
            pl.when(pl.col(c).fill_null("") == "")
            .then(None)
            .otherwise(pl.col(c))
            for c in text_columns
        ]
        text_expr = pl.concat_str(col_exprs_nullable, separator=" ", ignore_nulls=True)

    for s in remove_strings:
        text_expr = text_expr.str.replace_all(s, "", literal=True)

    for pattern in remove_patterns:
        text_expr = text_expr.str.replace_all(pattern, "")

    text_expr = text_expr.str.replace_all("\n", " ", literal=True)
    text_expr = text_expr.str.replace_all("\\n", " ", literal=True)
    text_expr = text_expr.str.replace_all(r"\s+", " ")
    text_expr = text_expr.str.strip_chars()

    return text_expr


def _get_ingest_columns(config: Dict) -> List[str]:
    """Get list of columns to include in ingest output."""
    columns = list(INGEST_MANDATORY_FIELDS)

    extra_fields = config.get('fields', [])
    if extra_fields:
        for field in extra_fields:
            if field not in columns:
                columns.append(field)

    columns.extend(OUTPUT_COLUMNS)

    return columns


def _get_ingest_output_path(output_path: str) -> Path:
    """Convert lingua output path to lingua_ingest output path."""
    output_path = Path(output_path)

    parts = list(output_path.parts)
    for i, part in enumerate(parts):
        if part == 'lingua':
            parts[i] = 'lingua_ingest'
            break

    return Path(*parts)


def _detect_format(filepath: str) -> str:
    """Detect file format from extension."""
    return 'parquet' if Path(filepath).suffix == '.parquet' else 'csv'


def _get_expected_rows(input_path: str, file_format: str) -> int:
    """Get expected row count from input file."""
    if file_format == 'parquet':
        import pyarrow.parquet as pq
        return pq.ParquetFile(input_path).metadata.num_rows
    else:
        return pl.scan_csv(input_path).select(pl.len()).collect().item()


def _create_batched_reader(input_path: str, file_format: str, batch_size: int):
    """Create a batched reader that yields DataFrames.

    Returns an iterator of pl.DataFrame batches.
    """
    if file_format == 'parquet':
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(input_path)
        def _iter():
            for batch in pf.iter_batches(batch_size=batch_size):
                yield pl.from_arrow(batch)
        return _iter()
    else:
        # CSV: use Polars batched reader, wrapped as a simple iterator
        reader = pl.read_csv_batched(input_path, batch_size=batch_size)
        def _iter():
            while True:
                batches = reader.next_batches(1)
                if not batches:
                    break
                yield batches[0]
        return _iter()


class _OutputWriter:
    """Abstraction for batched output writing (CSV append or Parquet row-groups)."""

    def __init__(self, temp_path: Path, file_format: str):
        self.temp_path = temp_path
        self.file_format = file_format
        self._first_batch = True
        self._pq_writer = None

    def write_batch(self, df: pl.DataFrame):
        if self.file_format == 'parquet':
            import pyarrow.parquet as pq
            table = df.to_arrow()
            if self._pq_writer is None:
                self._pq_writer = pq.ParquetWriter(str(self.temp_path), table.schema)
            self._pq_writer.write_table(table)
        else:
            if self._first_batch:
                df.write_csv(self.temp_path)
            else:
                with open(self.temp_path, 'ab') as f:
                    df.write_csv(f, include_header=False)
        self._first_batch = False

    def close(self):
        if self._pq_writer is not None:
            self._pq_writer.close()
            self._pq_writer = None

    def cleanup(self):
        self.close()
        if self.temp_path.exists():
            self.temp_path.unlink()


def process_csv(input_csv: str, output_csv: str, data_type: str, config: Dict) -> int:
    """Process input file with language detection using Polars and Lingua.

    Supports both CSV and Parquet formats (detected from file extension).
    """
    if not LINGUA_AVAILABLE:
        raise ImportError("Lingua not available")

    languages = config.get('languages', [])
    if not languages:
        raise ValueError("No languages specified in config")

    low_accuracy = config.get('low_accuracy', False)
    num_workers = config.get('workers', 8)
    batch_size = config.get('batch_size', 2_000_000)
    text_columns = get_text_columns(data_type, config)
    remove_strings = config.get('remove_strings', [])
    remove_patterns = config.get('remove_patterns', [])
    min_chars = config['min_chars']
    prefer_lingua = config.get('prefer_lingua', True)
    output_ingest = not prefer_lingua

    os.environ['POLARS_MAX_THREADS'] = str(num_workers)

    input_path = Path(input_csv)
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_format = _detect_format(input_csv)

    # Get expected row count
    try:
        expected_rows = _get_expected_rows(input_csv, file_format)
    except Exception as e:
        raise RuntimeError(f"Failed to get row count from input file: {e}")

    temp_path = output_path.with_suffix(output_path.suffix + '.temp')

    if temp_path.exists():
        print(f"[{input_path.stem}] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()

    # Ingest output setup
    ingest_path = None
    ingest_writer = None
    ingest_columns = None
    if output_ingest:
        ingest_path = _get_ingest_output_path(output_csv)
        ingest_path.parent.mkdir(parents=True, exist_ok=True)
        ingest_temp_path = ingest_path.with_suffix(ingest_path.suffix + '.temp')
        ingest_columns = _get_ingest_columns(config)

        if ingest_temp_path.exists():
            print(f"[{input_path.stem}] Removing incomplete ingest temp file: {ingest_temp_path.name}")
            ingest_temp_path.unlink()
    else:
        ingest_temp_path = None

    detector = _get_detector(languages, low_accuracy, num_workers, quiet=True)
    text_expr = _build_text_expr(text_columns, remove_strings, remove_patterns)

    total_rows = 0
    total_detected = 0
    detect_time = 0.0
    start_time = time.time()

    # Create writers
    main_writer = _OutputWriter(temp_path, file_format)
    if output_ingest and ingest_temp_path:
        ingest_writer = _OutputWriter(ingest_temp_path, file_format)

    print(f"[{input_path.stem}] Starting" + (" (with ingest output)" if output_ingest else ""))

    try:
        reader = _create_batched_reader(input_csv, file_format, batch_size)

        for df in reader:
            # Accumulate small batches up to batch_size
            # (PyArrow iter_batches already returns correct sizes; CSV reader may return smaller)
            n_rows = len(df)

            df_with_text = df.with_columns([
                text_expr.alias("_text"),
                text_expr.str.len_chars().alias("_len"),
            ])

            valid_expr = pl.col("_len") >= min_chars
            df_with_text = df_with_text.with_columns([
                valid_expr.alias("_valid"),
                pl.when(valid_expr)
                .then(pl.col("_len").cast(pl.Utf8))
                .otherwise(pl.lit(None) if file_format == 'parquet' else pl.lit(""))
                .alias("_lang_chars"),
            ])

            valid_mask = df_with_text["_valid"]
            valid_df = df_with_text.filter(valid_mask)
            valid_texts = valid_df["_text"].to_list()

            empty = None if file_format == 'parquet' else ""
            lang1_col = [empty] * n_rows
            prob1_col = [empty] * n_rows
            lang2_col = [empty] * n_rows
            prob2_col = [empty] * n_rows

            if valid_texts:
                valid_indices = valid_mask.arg_true().to_list()
                valid_lengths = valid_df["_len"].to_list()

                sorted_order = sorted(range(len(valid_texts)), key=lambda i: valid_lengths[i], reverse=True)
                sorted_texts = [valid_texts[i] for i in sorted_order]

                t0 = time.time()
                sorted_results = detector.compute_language_confidence_values_in_parallel(sorted_texts)
                detect_time += time.time() - t0

                for sorted_i, res in enumerate(sorted_results):
                    if res:
                        original_i = sorted_order[sorted_i]
                        idx = valid_indices[original_i]
                        r0 = res[0]
                        lang1 = r0.language.iso_code_639_1.name.lower()
                        lang1_col[idx] = lang1
                        prob1_col[idx] = f"{r0.value:.4f}"

                        if len(res) > 1:
                            r1 = res[1]
                            lang2_col[idx] = r1.language.iso_code_639_1.name.lower()
                            prob2_col[idx] = f"{r1.value:.4f}"

                total_detected += len(valid_texts)

            df_out = df.with_columns([
                pl.Series("lang", lang1_col),
                pl.Series("lang_prob", prob1_col),
                pl.Series("lang2", lang2_col),
                pl.Series("lang2_prob", prob2_col),
                df_with_text["_lang_chars"].alias("lang_chars"),
            ])

            main_writer.write_batch(df_out)

            if ingest_writer and ingest_columns:
                available_ingest_cols = [c for c in ingest_columns if c in df_out.columns]
                df_ingest = df_out.select(available_ingest_cols)
                ingest_writer.write_batch(df_ingest)

            total_rows += n_rows

    except Exception as e:
        main_writer.cleanup()
        if ingest_writer:
            ingest_writer.cleanup()
        raise RuntimeError(f"Error during batch processing: {e}")

    # Close writers and rename temp files
    main_writer.close()
    if temp_path.exists():
        temp_path.rename(output_path)

    if ingest_writer:
        ingest_writer.close()
        if ingest_temp_path and ingest_temp_path.exists():
            ingest_temp_path.rename(ingest_path)

    # Validate output row count matches input
    if total_rows != expected_rows:
        if output_path.exists():
            output_path.unlink()
        if output_ingest and ingest_path and ingest_path.exists():
            ingest_path.unlink()
        raise RuntimeError(
            f"Row count mismatch! Input: {expected_rows:,} rows, Output: {total_rows:,} rows. "
            f"Processed only {total_rows/expected_rows*100:.1f}% of input. "
            f"This indicates the batched reader stopped early - possibly due to memory pressure, "
            f"file corruption, or resource contention from parallel processing."
        )

    elapsed = time.time() - start_time
    rate = total_detected / elapsed if elapsed > 0 else 0
    skipped = total_rows - total_detected
    skip_pct = (skipped / total_rows * 100) if total_rows > 0 else 0

    print(f"[{input_path.stem}] Finished ({skip_pct:.0f}% skipped). detect={detect_time:.1f}s, total={elapsed:.1f}s ({rate:.0f} msg/s)")

    return total_rows
