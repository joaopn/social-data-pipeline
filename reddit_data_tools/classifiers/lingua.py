"""
Lingua language detection for reddit_data_tools pipeline.
Uses Polars for vectorized CSV I/O and text processing.
Uses Lingua's native Rust parallelism via Rayon for detection.
Processes large files in batches to avoid memory blow-up.
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
OUTPUT_COLUMNS = ['lang', 'lang_prob', 'lang2', 'lang2_prob']


def _build_lingua_validity_expr(text_col: str = "_text") -> pl.Expr:
    """
    Build a Polars expression for Lingua validity filtering.
    
    Implements a heuristic derived from Lingua's official test suite
    (https://github.com/pemistahl/lingua#4-how-good-is-it):
    
    - 3+ words: SAFE - Lingua can reliably detect language
    - 2 words with ≥10 chars: SAFE - Lingua tests on pairs ≥10 chars
    - 1 word with ≥5 chars: SAFE - Lingua tests on words ≥5 chars
    - Otherwise: SKIP - Too short for reliable detection
    
    Args:
        text_col: Name of the cleaned text column to check
        
    Returns:
        Polars boolean expression for validity
    """
    text = pl.col(text_col)
    char_len = text.str.len_chars()
    
    # Count words by splitting on whitespace
    # str.split returns a list, we count its length
    word_count = text.str.split(" ").list.len()
    
    # Build validity conditions:
    # Case A: 3+ words -> always valid
    case_a = word_count >= 3
    
    # Case B: 2 words AND >= 8 chars
    case_b = (word_count == 2) & (char_len >= 10)
    
    # Case C: 1 word AND >= 5 chars
    case_c = (word_count == 1) & (char_len >= 5)
    
    return case_a | case_b | case_c

# Cached detector (reused across files)
_cached_detector = None
_cached_config_key = None


def _get_detector(languages: List[str], low_accuracy: bool, num_workers: int, quiet: bool = False):
    """
    Get or create cached Lingua detector.
    Detector is reused if config hasn't changed.
    
    Note: RAYON_NUM_THREADS is set at module import time. The num_workers parameter
    is used for cache key consistency but cannot change thread count after Rayon init.
    
    Args:
        languages: List of language codes to detect
        low_accuracy: Use low accuracy mode
        num_workers: Number of workers (for logging only)
        quiet: If True, suppress all logging (used in parallel mode)
    """
    global _cached_detector, _cached_config_key
    
    if not LINGUA_AVAILABLE:
        raise ImportError("Lingua not available. Install: pip install lingua-language-detector")
    
    # Create a hashable key for the current config (excludes workers since it can't change)
    config_key = (tuple(sorted(languages)), low_accuracy)
    
    # Return cached detector if config matches
    if _cached_detector is not None and _cached_config_key == config_key:
        return _cached_detector
    
    # Convert language codes to Language enum
    lang_enums = []
    for lang_code in languages:
        lang_upper = lang_code.upper()
        if hasattr(Language, lang_upper):
            lang_enums.append(getattr(Language, lang_upper))
        else:
            if not quiet:
                print(f"[LINGUA] Warning: Unknown language '{lang_code}', skipping")
    
    if not lang_enums:
        raise ValueError("No valid languages specified in config")
    
    builder = LanguageDetectorBuilder.from_languages(*lang_enums)
    
    if low_accuracy:
        builder = builder.with_low_accuracy_mode()
    
    # Preload models for efficient parallel processing
    _cached_detector = builder.with_preloaded_language_models().build()
    _cached_config_key = config_key
    
    return _cached_detector


def get_text_columns(data_type: str, config: Dict) -> List[str]:
    """Get text columns for the given data type."""
    text_columns_config = config.get('text_columns', {})
    defaults = {'submissions': ['title', 'selftext'], 'comments': ['body']}
    return text_columns_config.get(data_type, defaults.get(data_type, []))


def _build_text_expr(text_columns: List[str], remove_strings: List[str], remove_patterns: List[str]) -> pl.Expr:
    """
    Build a Polars expression for text cleaning.
    Matches the behavior of the original _clean_text function exactly.
    """
    # Build expression for each column, replacing null/empty with None marker
    # Then filter out empty parts when joining (to match old behavior)
    if len(text_columns) == 1:
        # Single column: just clean nulls
        text_expr = pl.col(text_columns[0]).fill_null("")
    else:
        # Multiple columns: concatenate non-empty values with space
        # Old code: parts = [row[idx] for idx in text_indices if idx < len(row) and row[idx]]
        # This skips empty strings, so we need to replicate that
        col_exprs = []
        for c in text_columns:
            # Replace null with empty, then we'll handle empty in join
            col_exprs.append(pl.col(c).fill_null(""))
        
        # Use concat_str with ignore_nulls - but empty strings aren't null
        # We need a different approach: replace empty with null, then concat with ignore_nulls
        col_exprs_nullable = [
            pl.when(pl.col(c).fill_null("") == "")
            .then(None)
            .otherwise(pl.col(c))
            for c in text_columns
        ]
        text_expr = pl.concat_str(col_exprs_nullable, separator=" ", ignore_nulls=True)
    
    # Remove exact strings (literal matching)
    for s in remove_strings:
        text_expr = text_expr.str.replace_all(s, "", literal=True)
    
    # Remove regex patterns
    for pattern in remove_patterns:
        text_expr = text_expr.str.replace_all(pattern, "")
    
    # Clean whitespace - match old behavior exactly:
    # text.replace('\n', ' ').replace('\\n', ' ').strip()
    # Note: '\\n' in Python string is literal backslash-n, not newline
    text_expr = text_expr.str.replace_all("\n", " ", literal=True)  # actual newline
    text_expr = text_expr.str.replace_all("\\n", " ", literal=True)  # literal \n
    text_expr = text_expr.str.replace_all(r"\s+", " ")  # collapse multiple spaces
    text_expr = text_expr.str.strip_chars()
    
    return text_expr


def process_csv(input_csv: str, output_csv: str, data_type: str, config: Dict) -> int:
    """
    Process CSV file with language detection using Polars and Lingua.
    
    Uses Polars for vectorized I/O and text cleaning (Rust, parallel).
    Uses Lingua for language detection (Rust/Rayon, parallel).
    Processes in batches to handle very large files (100GB+).
    
    Uses a .temp file during writing and renames to final name on success.
    This ensures partial files from interrupted runs are not mistaken as complete.
    
    Args:
        input_csv: Input CSV path
        output_csv: Output CSV path  
        data_type: 'submissions' or 'comments'
        config: Classifier config dict
        
    Returns:
        Number of rows processed
    """
    if not LINGUA_AVAILABLE:
        raise ImportError("Lingua not available")
    
    # Extract config
    languages = config.get('languages', [])
    if not languages:
        raise ValueError("No languages specified in config. Add 'languages' list to classifiers.yaml")
    
    low_accuracy = config.get('low_accuracy', False)
    num_workers = config.get('workers', 8)
    batch_size = config.get('batch_size', 2_000_000)
    text_columns = get_text_columns(data_type, config)
    remove_strings = config.get('remove_strings', [])
    remove_patterns = config.get('remove_patterns', [])
    
    # Set Polars thread count (same as Lingua/Rayon)
    os.environ['POLARS_MAX_THREADS'] = str(num_workers)
    
    input_path = Path(input_csv)
    output_path = Path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Use temp file during writing, rename on success
    temp_path = output_path.with_suffix(output_path.suffix + '.temp')
    
    # Clean up any leftover temp file from interrupted run
    if temp_path.exists():
        print(f"[{input_path.stem}] Removing incomplete temp file: {temp_path.name}")
        temp_path.unlink()
    
    # Get cached detector (or build if first call / config changed)
    # quiet=True because initialization logging is handled by orchestrator
    detector = _get_detector(languages, low_accuracy, num_workers, quiet=True)
    
    # Build text expression once (reused per batch)
    text_expr = _build_text_expr(text_columns, remove_strings, remove_patterns)
    
    # Initialize counters
    total_rows = 0
    total_detected = 0
    detect_time = 0.0
    read_time = 0.0
    clean_time = 0.0
    unpack_time = 0.0
    write_time = 0.0
    stats = {}
    first_batch = True
    start_time = time.time()
    batch_num = 0
    
    # Use batched CSV reader for streaming large files
    # Note: Polars batch_size may not be respected (known issue), so we accumulate
    reader = pl.read_csv_batched(input_csv, batch_size=batch_size)
    
    print(f"[{input_path.stem}] Starting")
    
    while True:
        t0 = time.time()
        # Read multiple small batches and concatenate to reach target batch_size
        accumulated = []
        accumulated_rows = 0
        while accumulated_rows < batch_size:
            batches = reader.next_batches(1)
            if not batches:
                break
            accumulated.append(batches[0])
            accumulated_rows += len(batches[0])
        
        if not accumulated:
            break
        
        # Concatenate accumulated batches
        if len(accumulated) == 1:
            df = accumulated[0]
        else:
            df = pl.concat(accumulated)
        n_rows = len(df)
        read_time += time.time() - t0
        
        # Compute cleaned text and length in one pass (vectorized, Rust)
        # Length is used for Rayon load balancing (sorting), not for filtering
        t0 = time.time()
        df_with_text = df.with_columns([
            text_expr.alias("_text"),
            text_expr.str.len_chars().alias("_len"),
        ])
        
        # Apply word-based validity filter (based on Lingua benchmarks)
        # - 3+ words: valid
        # - 2 words with ≥8 chars: valid
        # - 1 word with ≥5 chars: valid
        validity_expr = _build_lingua_validity_expr("_text")
        df_with_text = df_with_text.with_columns([validity_expr.alias("_valid")])
        
        # Filter to valid rows and extract texts
        valid_mask = df_with_text["_valid"]
        valid_df = df_with_text.filter(valid_mask)
        valid_texts = valid_df["_text"].to_list()
        clean_time += time.time() - t0
        
        # Initialize result columns with empty values
        lang1_col = [""] * n_rows
        prob1_col = [""] * n_rows
        lang2_col = [""] * n_rows
        prob2_col = [""] * n_rows
        
        if valid_texts:
            # Get original indices for valid rows
            valid_indices = valid_mask.arg_true().to_list()
            valid_lengths = valid_df["_len"].to_list()
            
            # Sort texts by length (descending) for better Rayon load balancing
            # Long texts first ensures they start processing early, short texts fill gaps
            sorted_order = sorted(range(len(valid_texts)), key=lambda i: valid_lengths[i], reverse=True)
            sorted_texts = [valid_texts[i] for i in sorted_order]
            
            # Lingua detection (Rayon parallel)
            t0 = time.time()
            sorted_results = detector.compute_language_confidence_values_in_parallel(sorted_texts)
            detect_time += time.time() - t0
            
            # Map results back - unsort and assign to original indices
            t0 = time.time()
            for sorted_i, res in enumerate(sorted_results):
                if res:
                    original_i = sorted_order[sorted_i]
                    idx = valid_indices[original_i]
                    r0 = res[0]
                    lang1 = r0.language.iso_code_639_1.name.lower()
                    lang1_col[idx] = lang1
                    prob1_col[idx] = f"{r0.value:.4f}"
                    stats[lang1] = stats.get(lang1, 0) + 1
                    
                    if len(res) > 1:
                        r1 = res[1]
                        lang2_col[idx] = r1.language.iso_code_639_1.name.lower()
                        prob2_col[idx] = f"{r1.value:.4f}"
            unpack_time += time.time() - t0
            
            total_detected += len(valid_texts)
        
        # Add result columns and write
        t0 = time.time()
        df_out = df.with_columns([
            pl.Series("lang", lang1_col),
            pl.Series("lang_prob", prob1_col),
            pl.Series("lang2", lang2_col),
            pl.Series("lang2_prob", prob2_col),
        ])
        
        # Write batch to temp file
        if first_batch:
            df_out.write_csv(temp_path)
            first_batch = False
        else:
            with open(temp_path, 'ab') as f:
                df_out.write_csv(f, include_header=False)
        write_time += time.time() - t0
        
        total_rows += n_rows
        batch_num += 1
    
    # Rename temp file to final output path on success
    if temp_path.exists():
        temp_path.rename(output_path)
    
    # Final stats
    elapsed = time.time() - start_time
    rate = total_detected / elapsed if elapsed > 0 else 0
    skipped = total_rows - total_detected
    skip_pct = (skipped / total_rows * 100) if total_rows > 0 else 0
    
    print(f"[{input_path.stem}] Finished ({skip_pct:.0f}% skipped). detect={detect_time:.1f}s, total={elapsed:.1f}s ({rate:.0f} msg/s)")
    
    return total_rows
