"""Tests for parsing a table's existing bloom_filter_columns.

Bug class:
  `ALTER TABLE ... SET ("bloom_filter_columns" = ...)` REPLACES the whole list.
  Every write is therefore a read-modify-write, and a read that wrongly reports
  "nothing set" silently drops every existing bloom filter column on the table.
  So the parser must handle the real SHOW CREATE TABLE spacing, and an
  unreadable result must raise rather than degrade to an empty set.
"""

import sys
import types

import pytest

# mysql-connector lives in the container image, not the test env.
for _name in ("mysql", "mysql.connector"):
    sys.modules.setdefault(_name, types.ModuleType(_name))
sys.modules["mysql"].connector = sys.modules["mysql.connector"]

from social_data_pipeline.db.starrocks import ingest  # noqa: E402


def _ddl(properties: str) -> str:
    """A SHOW CREATE TABLE body with the given PROPERTIES block."""
    return (
        'CREATE TABLE `comments_emotions_en` (\n'
        '  `id` varchar(7) NOT NULL COMMENT "",\n'
        '  `author` varchar(1048576) NULL COMMENT ""\n'
        ') ENGINE=OLAP \n'
        'PRIMARY KEY(`id`)\n'
        'DISTRIBUTED BY HASH(`id`) BUCKETS 256 \n'
        f'PROPERTIES (\n{properties}\n);'
    )


class TestGetBloomFilterColumns:
    def _patch(self, monkeypatch, rows):
        monkeypatch.setattr(ingest, "execute_query", lambda *a, **k: rows)

    def test_absent_property_is_empty_set(self, monkeypatch):
        self._patch(monkeypatch, [["comments_emotions_en", _ddl('"compression" = "ZSTD"')]])
        assert ingest.get_bloom_filter_columns(
            "comments_emotions_en", "reddit", "h", 9030, "root") == set()

    def test_single_column(self, monkeypatch):
        self._patch(monkeypatch, [["t", _ddl('"bloom_filter_columns" = "author"')]])
        assert ingest.get_bloom_filter_columns("t", "reddit", "h", 9030, "root") == {"author"}

    def test_multiple_columns_with_spaces(self, monkeypatch):
        self._patch(monkeypatch, [["t", _ddl('"bloom_filter_columns" = "author, subreddit"')]])
        assert ingest.get_bloom_filter_columns("t", "reddit", "h", 9030, "root") == {
            "author", "subreddit"}

    def test_alongside_other_properties(self, monkeypatch):
        props = ('"compression" = "ZSTD",\n"bloom_filter_columns" = "author,subreddit",\n'
                 '"replication_num" = "1"')
        self._patch(monkeypatch, [["t", _ddl(props)]])
        assert ingest.get_bloom_filter_columns("t", "reddit", "h", 9030, "root") == {
            "author", "subreddit"}

    def test_empty_property_value(self, monkeypatch):
        self._patch(monkeypatch, [["t", _ddl('"bloom_filter_columns" = ""')]])
        assert ingest.get_bloom_filter_columns("t", "reddit", "h", 9030, "root") == set()

    @pytest.mark.parametrize("rows", [[], None, "not a list", [["only-one-column"]]])
    def test_unreadable_result_raises(self, monkeypatch, rows):
        # Never degrade to an empty set: the caller would then write a
        # replacement list that drops existing bloom columns.
        self._patch(monkeypatch, rows)
        with pytest.raises(RuntimeError, match="refusing to modify"):
            ingest.get_bloom_filter_columns("t", "reddit", "h", 9030, "root")


class TestSetBloomFilterColumns:
    def test_noop_when_already_present(self, monkeypatch):
        monkeypatch.setattr(ingest, "get_bloom_filter_columns", lambda *a, **k: {"author"})
        called = []
        monkeypatch.setattr(ingest, "execute_query",
                            lambda *a, **k: called.append(a) or [])
        added = ingest.set_bloom_filter_columns(
            "t", "reddit", ["author"], "h", 9030, "root", print_fn=lambda m: None)
        assert added == []
        assert called == [], "no ALTER should be submitted when nothing changes"

    def test_merges_rather_than_replaces(self, monkeypatch):
        statements = []
        monkeypatch.setattr(ingest, "get_bloom_filter_columns",
                            lambda *a, **k: {"author", "subreddit"})
        monkeypatch.setattr(ingest, "_wait_for_active_alter_job", lambda *a, **k: None)
        monkeypatch.setattr(ingest, "_show_alter_column_jobs", lambda *a, **k: [])
        monkeypatch.setattr(ingest, "execute_query",
                            lambda q, *a, **k: statements.append(q) or [])
        # Existing columns must appear in the SET, or they are dropped.
        with pytest.raises(RuntimeError):
            # verify step re-reads via the patched get_bloom_filter_columns,
            # which still reports the old set -> mismatch -> raise
            ingest.set_bloom_filter_columns(
                "t", "reddit", ["dataset"], "h", 9030, "root", print_fn=lambda m: None)
        assert any("author" in s and "subreddit" in s and "dataset" in s
                   for s in statements), statements

    def test_raises_when_property_did_not_take_effect(self, monkeypatch):
        monkeypatch.setattr(ingest, "get_bloom_filter_columns", lambda *a, **k: set())
        monkeypatch.setattr(ingest, "_wait_for_active_alter_job", lambda *a, **k: None)
        monkeypatch.setattr(ingest, "_show_alter_column_jobs", lambda *a, **k: [])
        monkeypatch.setattr(ingest, "execute_query", lambda *a, **k: [])
        with pytest.raises(RuntimeError, match="did not take effect"):
            ingest.set_bloom_filter_columns(
                "t", "reddit", ["author"], "h", 9030, "root", print_fn=lambda m: None)
