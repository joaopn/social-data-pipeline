#!/usr/bin/env python3
"""Convert CSV / CSV.zst classifier outputs to typed Parquet.

The pre-parquet classifier runs (multilingual_sentiment, xlmr_toxicity) wrote
CSV, where every column is text. They already carry the full passthrough field
set (dataset, id, retrieved_utc, author, subreddit) — nothing needs joining in
from lingua — so this is a pure format + type + naming conversion:

  * types    : passthrough columns cast per the source's platform.yaml
               (field_types), score columns to Float32, "" -> null.
  * names    : score columns normalized to snake_case ("Very Negative" ->
               "very_negative") so they match the other classifiers and need
               no backticks in SQL. Disable with --keep-score-names.
  * order    : mandatory_fields, upsert_order_field, remaining passthrough,
               then scores — identical to repair_classifier_outputs.py output.
  * encoding : zstd + dictionary, ~1M-row row groups.

Reads INPUT_DIR/<data_type>/*.csv[.zst], writes OUTPUT_DIR/<data_type>/*.parquet.
Where a month exists as both .csv and .csv.zst (they are byte-identical), one
is chosen via --prefer. Source files are never modified or deleted.

Memory is bounded by ~batch_rows regardless of file size: the CSV is streamed
through pyarrow's incremental reader and written row-group by row-group.

Existing outputs are skipped, so an interrupted run resumes. Writes go to a
.temp and are atomically renamed, so a final file is complete by construction;
a SIGKILL (e.g. the OOM killer) can only leave a .temp, which is cleaned up on
the next run.

Usage:
    python convert_csv_classifier_outputs.py \
        /mnt/datasets/sdp/output/reddit/multilingual_sentiment \
        /mnt/datasets/sdp/output/reddit/multilingual_sentiment_v2 \
        --platform-config config/sources/reddit/platform.yaml
"""

import argparse
import csv as csvmod
import subprocess
import sys
import time
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from repair_classifier_outputs import build_cast_expr, load_platform_types
# Shared with the transformer classifier's output-column naming, so converted
# files and freshly produced pipeline files carry identical score columns.
from social_data_pipeline.classifiers.base import normalize_score_name


def open_csv_stream(path: Path):
    """Return (binary file object, proc-or-None) streaming decompressed CSV."""
    if path.suffix == ".zst":
        proc = subprocess.Popen(["zstd", "-dc", str(path)], stdout=subprocess.PIPE)
        return proc.stdout, proc
    return path.open("rb"), None


def close_csv_stream(fobj, proc) -> None:
    try:
        fobj.close()
    except Exception:
        pass
    if proc is not None:
        proc.kill()
        proc.wait()


def read_header(path: Path) -> list:
    """Read just the header row (kills the decompressor immediately after)."""
    fobj, proc = open_csv_stream(path)
    try:
        line = fobj.readline().decode("utf-8", "replace")
    finally:
        close_csv_stream(fobj, proc)
    if not line.strip():
        raise ValueError(f"empty file: {path.name}")
    return next(csvmod.reader([line.rstrip("\r\n")]))


def output_name(in_path: Path) -> str:
    """RC_2005-12_sentiment.csv[.zst] -> RC_2005-12_sentiment.parquet"""
    name = in_path.name
    for ext in (".csv.zst", ".csv"):
        if name.endswith(ext):
            return name[: -len(ext)] + ".parquet"
    raise ValueError(f"not a csv/csv.zst file: {name}")


def convert_file(in_path: Path, out_path: Path, target_dtypes: dict,
                 priority_cols: list, batch_rows: int = 1_000_000,
                 compression: str = "zstd", normalize: bool = True,
                 block_size: int = 64 << 20) -> int:
    """Convert one CSV/CSV.zst to typed parquet. Returns rows written."""
    header = read_header(in_path)

    passthrough = [c for c in header if c in target_dtypes]
    scores = [c for c in header if c not in target_dtypes]
    if not scores:
        raise ValueError(f"No score columns found in {in_path.name} (header={header})")

    renamed = {c: (normalize_score_name(c) if normalize else c) for c in scores}
    clashes = [c for c in scores if renamed[c] in target_dtypes]
    if clashes:
        raise ValueError(
            f"{in_path.name}: normalized score name collides with a platform field: {clashes}")

    # Read every column as text and cast explicitly — same semantics as the
    # parquet repair path (strict casts, "" -> null), rather than trusting
    # pyarrow's per-block type inference.
    col_types = {c: pa.string() for c in header}

    exprs = {c: build_cast_expr(c, pl.String, target_dtypes[c]) for c in passthrough}
    for c in scores:
        exprs[c] = build_cast_expr(c, pl.String, pl.Float32).alias(renamed[c])

    ordered = [c for c in priority_cols if c in exprs]
    ordered += [c for c in passthrough if c not in ordered]
    ordered += scores
    ordered_exprs = [exprs[c] for c in ordered]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_suffix(".parquet.temp")

    fobj, proc = open_csv_stream(in_path)
    writer = None
    rows_out = 0
    try:
        reader = pacsv.open_csv(
            fobj,
            read_options=pacsv.ReadOptions(block_size=block_size),
            convert_options=pacsv.ConvertOptions(column_types=col_types),
        )

        def write_df(df: pl.DataFrame):
            nonlocal writer, rows_out
            table = df.select(ordered_exprs).to_arrow()
            if writer is None:
                writer = pq.ParquetWriter(temp_path, table.schema, compression=compression)
            writer.write_table(table)
            rows_out += len(table)

        buf, buf_rows = [], 0
        for batch in reader:
            buf.append(pl.from_arrow(batch))
            buf_rows += batch.num_rows
            if buf_rows >= batch_rows:
                write_df(pl.concat(buf))
                buf, buf_rows = [], 0
        if buf:
            write_df(pl.concat(buf))

        if writer is not None:
            writer.close()
            writer = None
        elif rows_out == 0:
            raise ValueError(f"{in_path.name}: no data rows")

        # The writer reports what it wrote; confirm the file agrees.
        if pq.ParquetFile(temp_path).metadata.num_rows != rows_out:
            raise ValueError(f"{in_path.name}: parquet row count disagrees with rows written")
        temp_path.rename(out_path)
    except BaseException:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)
        raise
    finally:
        close_csv_stream(fobj, proc)

    return rows_out


def select_sources(data_dir: Path, prefer: str) -> list:
    """One input file per month; .csv and .csv.zst duplicates resolved by `prefer`."""
    by_stem = {}
    for f in sorted(data_dir.iterdir()):
        n = f.name
        if n.endswith(".csv.zst"):
            by_stem.setdefault(n[: -len(".csv.zst")], {})["zst"] = f
        elif n.endswith(".csv"):
            by_stem.setdefault(n[: -len(".csv")], {})["csv"] = f
    other = "csv" if prefer == "zst" else "zst"
    chosen, dupes = [], 0
    for _, forms in sorted(by_stem.items()):
        if len(forms) > 1:
            dupes += 1
        chosen.append(forms.get(prefer) or forms[other])
    return chosen, dupes


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("input_dir", type=Path,
                    help="Classifier output dir with <data_type>/ subdirs of csv/csv.zst")
    ap.add_argument("output_dir", type=Path,
                    help="Destination dir; <data_type>/ structure is mirrored. Must differ from input_dir.")
    ap.add_argument("--platform-config", type=Path, required=True,
                    help="Source platform.yaml providing field_types (dtype authority) and column order")
    ap.add_argument("--prefer", default="zst", choices=["zst", "csv"],
                    help="Which copy to read when a month exists as both (default: zst)")
    ap.add_argument("--keep-score-names", action="store_true",
                    help="Keep score column names verbatim instead of normalizing to snake_case")
    ap.add_argument("--compression", default="zstd", choices=["zstd", "snappy"],
                    help="Parquet codec (default: zstd)")
    args = ap.parse_args()

    if args.output_dir.resolve() == args.input_dir.resolve():
        print("Error: output_dir must differ from input_dir (sources are never modified).")
        return 1

    target_dtypes, priority_cols = load_platform_types(args.platform_config)
    normalize = not args.keep_score_names

    data_types = sorted(p.name for p in args.input_dir.iterdir() if p.is_dir())
    if not data_types:
        print(f"Error: no <data_type> subdirectories in {args.input_dir}")
        return 1

    # A SIGKILL bypasses in-process cleanup, so a prior run can leave partial
    # temps. They are never valid output.
    if args.output_dir.is_dir():
        for dt in data_types:
            for p in (args.output_dir / dt).glob("*.temp"):
                print(f"  removing stale temp from an interrupted run: {p.name}")
                p.unlink(missing_ok=True)

    counts = {"CONVERTED": 0, "SKIP_EXISTS": 0, "REDO_BAD": 0, "FAILED": 0}
    failures = []
    total_rows = 0
    start = time.time()

    for data_type in data_types:
        sources, dupes = select_sources(args.input_dir / data_type, args.prefer)
        note = f" ({dupes} also exist as the other format; reading .{args.prefer})" if dupes else ""
        print(f"== {data_type}: {len(sources)} months{note}")

        for src in sources:
            out_path = args.output_dir / data_type / output_name(src)
            if out_path.exists():
                # Final files are complete by construction (atomic rename after
                # close), so only verify the parquet is readable and non-empty.
                try:
                    ok = pq.ParquetFile(out_path).metadata.num_rows > 0
                except Exception:
                    ok = False
                if ok:
                    counts["SKIP_EXISTS"] += 1
                    continue
                print(f"  {src.name}: existing output unreadable/empty — redoing")
                counts["REDO_BAD"] += 1
                out_path.unlink(missing_ok=True)

            t0 = time.time()
            try:
                n = convert_file(src, out_path, target_dtypes, priority_cols,
                                 compression=args.compression, normalize=normalize)
            except Exception as e:
                counts["FAILED"] += 1
                failures.append((src, e))
                print(f"  {src.name}: FAILED — {e}")
                continue
            counts["CONVERTED"] += 1
            total_rows += n
            print(f"  {src.name}: {n:,} rows in {time.time() - t0:.1f}s")

    print(f"\nDone in {(time.time() - start) / 60:.1f} min: "
          f"{counts['CONVERTED']} converted ({total_rows:,} rows), "
          f"{counts['SKIP_EXISTS']} already done, "
          f"{counts['REDO_BAD']} redone (bad existing), {counts['FAILED']} failed")
    if failures:
        print("\nFailed files:")
        for f, e in failures:
            print(f"  {f}: {e}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
