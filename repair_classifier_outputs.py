#!/usr/bin/env python3
"""Repair transformer classifier output parquet files.

Historical classifier outputs have two defects:
  1. Wrong dtypes: score columns (and, in CSV-era files, every column) were
     written as strings. Score columns become Float32; passthrough columns
     are cast to the types declared in the source's platform.yaml
     (field_types), the pipeline's single source of truth.
  2. Missing retrieved_utc: files classified after commit a8350a5 dropped the
     column. It is re-joined from the matching lingua output file on `id`.

Column order is normalized to: mandatory_fields, upsert_order_field, remaining
passthrough columns in file order, then score columns in file order.
Files with LABEL_* score columns (broken model metadata) are skipped and
reported — they contain no recoverable data.

Reads from INPUT_DIR/<data_type>/*.parquet, writes the repaired files under
OUTPUT_DIR/<data_type>/ with the same names (never in place). Existing
output files are skipped, so interrupted runs can be resumed. Row counts are
verified after every write.

Usage:
    python repair_classifier_outputs.py \
        /mnt/datasets/sdp/output/reddit/go_emotions \
        /mnt/datasets/sdp/output/reddit/go_emotions_repaired \
        --platform-config config/sources/reddit/platform.yaml \
        --lingua-dir /mnt/datasets/sdp/output/reddit/lingua \
        --suffix _emotions_en
"""

import argparse
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import yaml

# Columns appended by the lingua classifier (may appear in outputs downstream
# of lingua). Everything not in the platform universe or here is a score.
LINGUA_COLUMNS = {
    'lang': pl.String,
    'lang_prob': pl.Float32,
    'lang2': pl.String,
    'lang2_prob': pl.Float32,
    'lang_chars': pl.Int64,
}


def yaml_type_to_polars(type_def) -> pl.DataType:
    """Map platform.yaml field_types entries to polars dtypes.

    Integer types map to Int64 to match what the parse profile writes.
    """
    if isinstance(type_def, list):
        return pl.String  # ['char', n] / ['varchar', n]
    return {
        'integer': pl.Int64,
        'bigint': pl.Int64,
        'boolean': pl.Boolean,
        'float': pl.Float64,
    }.get(type_def, pl.String)  # 'text' and anything else


def load_platform_types(platform_yaml: Path) -> tuple:
    """Return (target_dtypes, ordered_priority_cols) from platform.yaml.

    target_dtypes covers every column the platform can pass through a
    classifier (mandatory_fields + all per-data-type fields + lingua columns).
    ordered_priority_cols = mandatory_fields + upsert_order_field, used to
    normalize output column order.
    """
    cfg = yaml.safe_load(platform_yaml.read_text())
    field_types = cfg.get('field_types', {})
    mandatory = cfg.get('mandatory_fields', [])
    order_field = cfg.get('upsert_order_field')

    universe = list(mandatory)
    for fields in (cfg.get('fields') or {}).values():
        universe += fields or []
    if order_field:
        universe.append(order_field)

    targets = {c: yaml_type_to_polars(field_types.get(c, 'text')) for c in universe}
    targets.update(LINGUA_COLUMNS)

    priority = list(mandatory)
    if order_field and order_field not in priority:
        priority.append(order_field)
    return targets, priority


def parquet_row_count(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


class LinguaCursor:
    """Forward-only cursor supplying retrieved_utc from a lingua output file.

    The classifier output is a row-filtered subset of the same parsed input
    the lingua run consumed, and both preserve input row order — so the
    classifier's ids are an ordered subsequence of the lingua file's ids
    (verified on real data). That lets each classifier batch be satisfied
    from a small sliding window of lingua rows.

    A whole-file hash join is NOT a viable alternative here: polars falls
    back to materializing the joined result, which at these row counts
    (~265M) reached 122 GB RSS and got the process OOM-killed. This cursor
    holds only the current window (tens of MB).

    Self-validating: if the subsequence assumption is ever violated (an id
    is missing or out of order), it raises rather than emitting a wrong
    retrieved_utc.
    """

    def __init__(self, path: Path, read_rows: int = 2_000_000):
        self._batches = pq.ParquetFile(path).iter_batches(
            batch_size=read_rows, columns=["id", "retrieved_utc"])
        self._window = None
        self._name = path.name

    def _extend(self) -> bool:
        try:
            batch = next(self._batches)
        except StopIteration:
            return False
        df = pl.from_arrow(batch)
        self._window = df if self._window is None else pl.concat([self._window, df])
        return True

    def take(self, ids: pl.Series) -> pl.Series:
        """Return retrieved_utc aligned row-for-row with `ids`."""
        if len(ids) == 0:
            return pl.Series("retrieved_utc", [], dtype=pl.Int64)

        last = ids[-1]
        # Grow the window until this batch's final id is inside it. Because
        # ids are an ordered subsequence, every earlier id in the batch is
        # then also within the window.
        while self._window is None or not (self._window["id"] == last).any():
            if not self._extend():
                raise ValueError(
                    f"{self._name} exhausted before matching classifier id {last!r} — "
                    "classifier/lingua row order or id sets do not correspond")

        out = pl.DataFrame({"id": ids}).join(
            self._window, on="id", how="left", maintain_order="left")
        if out["retrieved_utc"].null_count():
            raise ValueError(
                f"classifier ids not found in {self._name} window — "
                "classifier/lingua row order or id sets do not correspond")

        # Advance past the matched region so the window stays small.
        pos = self._window.with_row_index("_i").filter(pl.col("id") == last)["_i"][0]
        self._window = self._window.slice(pos + 1)
        return out["retrieved_utc"]


def build_cast_expr(name: str, current: pl.DataType, target: pl.DataType) -> pl.Expr:
    """Cast expression for one column; empty strings become null first.

    Casts are strict: unexpected junk in a numeric column aborts the file
    instead of silently producing nulls.
    """
    col = pl.col(name)
    if current == target:
        return col
    if current == pl.String:
        col = pl.when(pl.col(name) == "").then(None).otherwise(pl.col(name))
    return col.cast(target, strict=True).alias(name)


def repair_file(in_path: Path, out_path: Path, lingua_file: Path,
                target_dtypes: dict, priority_cols: list,
                batch_rows: int = 1_000_000, compression: str = "zstd") -> str:
    """Repair one classifier parquet file. Returns a status string.

    Reads batches with pyarrow, casts/joins per batch with polars, and
    writes through pyarrow's ParquetWriter (dictionary encoding on,
    ~batch_rows rows per row group). Rounded scores have at most ~10k
    distinct values, so dictionary pages keep them near ~2 bytes/row —
    polars' sink_parquet writes them PLAIN (4 bytes/row) in many small
    row groups, which inflates the output well past the all-string input.

    Memory is bounded by ~batch_rows, not by file size: the retype path
    streams one buffer at a time, and the enrich path pulls retrieved_utc
    from a LinguaCursor whose sliding window is trimmed after every batch.
    Measured peak on a 38M-row enrich: ~4 GB (the previous whole-file join
    reached 122 GB on a 265M-row file and was OOM-killed). Lower
    batch_rows if you need a smaller footprint.
    """
    pf = pq.ParquetFile(in_path)
    schema = dict(pl.scan_parquet(in_path).collect_schema())
    cols = list(schema)

    if any(c.startswith("LABEL_") for c in cols):
        return "SKIP_LABEL"

    passthrough = [c for c in cols if c in target_dtypes]
    scores = [c for c in cols if c not in target_dtypes]
    if not scores:
        raise ValueError(f"No score columns found in {in_path.name}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_suffix(".parquet.temp")
    enriched = False

    if "retrieved_utc" in target_dtypes and "retrieved_utc" not in cols:
        if not lingua_file.is_file():
            raise FileNotFoundError(f"No lingua file to enrich from: {lingua_file}")
        lingua_schema = pl.scan_parquet(lingua_file).collect_schema()
        if "retrieved_utc" not in lingua_schema.names():
            raise ValueError(f"retrieved_utc missing from lingua file {lingua_file.name}")
        passthrough.append("retrieved_utc")
        schema["retrieved_utc"] = lingua_schema["retrieved_utc"]
        enriched = True

    exprs = {c: build_cast_expr(c, schema[c], target_dtypes[c]) for c in passthrough}
    exprs.update({c: build_cast_expr(c, schema[c], pl.Float32) for c in scores})

    # Canonical order: mandatory + order field first, then remaining
    # passthrough in file order, then scores in file order.
    ordered = [c for c in priority_cols if c in exprs]
    ordered += [c for c in passthrough if c not in ordered]
    ordered += scores
    ordered_exprs = [exprs[c] for c in ordered]

    writer = None
    rows_out = 0
    cursor = LinguaCursor(lingua_file) if enriched else None
    try:
        def write_df(df: pl.DataFrame):
            nonlocal writer, rows_out
            table = df.select(ordered_exprs).to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(temp_path, table.schema, compression=compression)
            writer.write_table(table)
            rows_out += len(table)

        # Coalesce reader batches to batch_rows so output row groups stay large.
        buf, buf_rows = [], 0
        for batch in pf.iter_batches(batch_size=batch_rows):
            df = pl.from_arrow(batch)
            if cursor is not None:
                df = df.with_columns(cursor.take(df["id"]).alias("retrieved_utc"))
            buf.append(df)
            buf_rows += len(df)
            if buf_rows >= batch_rows:
                write_df(pl.concat(buf))
                buf, buf_rows = [], 0
        if buf:
            write_df(pl.concat(buf))
        if writer is not None:
            writer.close()
            writer = None
        rows_in = parquet_row_count(in_path)
        if rows_in != rows_out:
            raise ValueError(
                f"Row count mismatch for {in_path.name}: {rows_in} in, {rows_out} out")
        temp_path.rename(out_path)
    except BaseException:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)
        raise

    return "ENRICHED" if enriched else "RETYPED"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("input_dir", type=Path,
                    help="Classifier output dir with <data_type>/ subdirs (e.g. .../output/reddit/go_emotions)")
    ap.add_argument("output_dir", type=Path,
                    help="Destination dir; <data_type>/ structure is mirrored. Must differ from input_dir.")
    ap.add_argument("--platform-config", type=Path, required=True,
                    help="Source platform.yaml providing field_types (dtype authority) and column order")
    ap.add_argument("--lingua-dir", type=Path, required=True,
                    help="Lingua output dir used as the retrieved_utc source for enrichment")
    ap.add_argument("--suffix", required=True,
                    help="Classifier filename suffix (e.g. _emotions_en), stripped to find the lingua file")
    ap.add_argument("--lingua-suffix", default="_lingua",
                    help="Lingua filename suffix (default: _lingua)")
    ap.add_argument("--compression", default="zstd", choices=["zstd", "snappy"],
                    help="Parquet codec for repaired files (default: zstd, ~25%% smaller than snappy)")
    args = ap.parse_args()

    if args.output_dir.resolve() == args.input_dir.resolve():
        print("Error: output_dir must differ from input_dir (files are never repaired in place).")
        return 1

    target_dtypes, priority_cols = load_platform_types(args.platform_config)

    data_types = sorted(p.name for p in args.input_dir.iterdir() if p.is_dir())
    if not data_types:
        print(f"Error: no <data_type> subdirectories in {args.input_dir}")
        return 1

    counts = {"RETYPED": 0, "ENRICHED": 0, "SKIP_LABEL": 0, "SKIP_EXISTS": 0,
              "REDO_BAD": 0, "FAILED": 0}
    failures = []
    start = time.time()

    # A SIGKILL (e.g. the OOM killer) bypasses the in-process cleanup, so a
    # previous run can leave partial temps behind. They are never valid output.
    stale = [p for dt in data_types
             for p in (args.output_dir / dt).glob("*.temp")] if args.output_dir.is_dir() else []
    for p in stale:
        print(f"  removing stale temp from an interrupted run: {p.name}")
        p.unlink(missing_ok=True)

    for data_type in data_types:
        files = sorted((args.input_dir / data_type).glob("*.parquet"))
        print(f"== {data_type}: {len(files)} files")
        for f in files:
            out_path = args.output_dir / data_type / f.name
            if out_path.exists():
                # Don't trust an existing output blindly: a run killed mid-write,
                # or one from an older/buggy version, can leave a file that is
                # present but wrong. Re-do anything whose row count doesn't match
                # the source. (Cheap: parquet footer read, no data scan.)
                try:
                    ok = parquet_row_count(out_path) == parquet_row_count(f)
                except Exception:
                    ok = False
                if ok:
                    counts["SKIP_EXISTS"] += 1
                    continue
                print(f"  {f.name}: existing output is incomplete/unreadable — redoing")
                counts["REDO_BAD"] += 1
                out_path.unlink(missing_ok=True)
            if not f.stem.endswith(args.suffix):
                print(f"  {f.name}: does not end with '{args.suffix}', skipping")
                continue
            base = f.stem[: -len(args.suffix)]
            lingua_file = args.lingua_dir / data_type / f"{base}{args.lingua_suffix}.parquet"
            t0 = time.time()
            try:
                status = repair_file(f, out_path, lingua_file, target_dtypes, priority_cols,
                                     compression=args.compression)
            except Exception as e:
                counts["FAILED"] += 1
                failures.append((f, e))
                print(f"  {f.name}: FAILED — {e}")
                continue
            counts[status] += 1
            if status == "SKIP_LABEL":
                print(f"  {f.name}: LABEL_* columns (broken output), skipped — reclassify this file")
            else:
                print(f"  {f.name}: {status.lower()} in {time.time() - t0:.1f}s")

    print(f"\nDone in {(time.time() - start) / 60:.1f} min: "
          f"{counts['RETYPED']} retyped, {counts['ENRICHED']} enriched, "
          f"{counts['SKIP_LABEL']} broken (LABEL_*), {counts['SKIP_EXISTS']} already done, "
          f"{counts['REDO_BAD']} redone (bad existing), {counts['FAILED']} failed")
    if failures:
        print("\nFailed files:")
        for f, e in failures:
            print(f"  {f}: {e}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
