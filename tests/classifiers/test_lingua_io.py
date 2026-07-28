"""Tests for lingua's batched file readers (format-agnostic I/O layer).

These cover `_create_batched_reader` only, which needs polars but not the
lingua Rust backend — the module imports cleanly without it.
"""

import polars as pl
import pytest

from social_data_pipeline.classifiers.lingua import (
    _create_batched_reader,
    _detect_format,
    _get_expected_rows,
)


ROWS = 2500
BATCH = 1000


@pytest.fixture
def csv_file(tmp_path):
    path = tmp_path / "input.csv"
    pl.DataFrame({
        "id": [f"i{i}" for i in range(ROWS)],
        "body": [f"text {i}" for i in range(ROWS)],
    }).write_csv(path)
    return path


@pytest.fixture
def parquet_file(tmp_path, csv_file):
    path = tmp_path / "input.parquet"
    pl.read_csv(csv_file).write_parquet(path)
    return path


# ============================================================================
# _create_batched_reader
# ============================================================================

@pytest.mark.parametrize("fmt", ["csv", "parquet"])
def test_yields_dataframes_bounded_by_batch_size(request, fmt):
    path = request.getfixturevalue(f"{fmt}_file")
    batches = list(_create_batched_reader(str(path), fmt, BATCH))

    assert all(isinstance(b, pl.DataFrame) for b in batches)
    assert all(len(b) <= BATCH for b in batches)
    assert sum(len(b) for b in batches) == ROWS


@pytest.mark.parametrize("fmt", ["csv", "parquet"])
def test_row_total_matches_expected_rows(request, fmt):
    path = request.getfixturevalue(f"{fmt}_file")
    streamed = sum(len(b) for b in _create_batched_reader(str(path), fmt, BATCH))

    assert streamed == _get_expected_rows(str(path), fmt)


def test_both_formats_stream_identical_content(csv_file, parquet_file):
    from_csv = pl.concat(list(_create_batched_reader(str(csv_file), "csv", BATCH)))
    from_parquet = pl.concat(
        list(_create_batched_reader(str(parquet_file), "parquet", BATCH))
    )

    assert from_csv.columns == ["id", "body"]
    assert from_csv.equals(from_parquet)


def test_single_batch_when_batch_size_exceeds_file(csv_file):
    batches = list(_create_batched_reader(str(csv_file), "csv", ROWS * 2))

    assert len(batches) == 1
    assert len(batches[0]) == ROWS


def test_empty_file_yields_no_rows(tmp_path):
    path = tmp_path / "empty.csv"
    pl.DataFrame({"id": [], "body": []}).write_csv(path)

    batches = list(_create_batched_reader(str(path), "csv", BATCH))

    assert sum(len(b) for b in batches) == 0


# ============================================================================
# _detect_format
# ============================================================================

@pytest.mark.parametrize("name,expected", [
    ("f.parquet", "parquet"),
    ("f.csv", "csv"),
    ("f_lingua.parquet", "parquet"),
    ("f.txt", "csv"),
])
def test_detect_format(name, expected):
    assert _detect_format(name) == expected
