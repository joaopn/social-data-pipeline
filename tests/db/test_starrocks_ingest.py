"""Tests for StarRocks ingestion pure-logic functions."""

import sys
from unittest.mock import MagicMock

# mysql-connector-python is not in test deps — stub it before importing ingest
sys.modules.setdefault('mysql', MagicMock())
sys.modules.setdefault('mysql.connector', MagicMock())

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from social_data_pipeline.core.config import ConfigurationError
from social_data_pipeline.db.starrocks.ingest import (
    yaml_type_to_sr_sql,
    get_column_list,
    get_create_table_query,
    get_classifier_create_table_query,
    get_ingest_query,
    infer_classifier_schema,
    compute_bucket_count,
    _sr_server_path,
    _arrow_type_to_sr_sql,
    _infer_sr_type,
)


# ── yaml_type_to_sr_sql ──────────────────────────────────────────────────────

class TestYamlTypeToSrSql:
    def test_char_with_length(self):
        assert yaml_type_to_sr_sql(['char', 7]) == 'CHAR(7)'

    def test_varchar_with_length(self):
        assert yaml_type_to_sr_sql(['varchar', 255]) == 'VARCHAR(255)'

    def test_integer(self):
        assert yaml_type_to_sr_sql('integer') == 'INT'

    def test_bigint(self):
        assert yaml_type_to_sr_sql('bigint') == 'BIGINT'

    def test_boolean(self):
        assert yaml_type_to_sr_sql('boolean') == 'BOOLEAN'

    def test_float(self):
        assert yaml_type_to_sr_sql('float') == 'FLOAT'

    def test_text(self):
        assert yaml_type_to_sr_sql('text') == 'VARCHAR(1048576)'

    def test_unknown_type_defaults_to_varchar_max(self):
        assert yaml_type_to_sr_sql('blob') == 'VARCHAR(1048576)'

    def test_unknown_list_type_defaults_to_varchar_max(self):
        assert yaml_type_to_sr_sql(['unknown', 10]) == 'VARCHAR(1048576)'


# ── _arrow_type_to_sr_sql ────────────────────────────────────────────────────

class TestArrowTypeToSrSql:
    def test_int32(self):
        assert _arrow_type_to_sr_sql(pa.int32()) == 'INT'

    def test_int16(self):
        assert _arrow_type_to_sr_sql(pa.int16()) == 'INT'

    def test_int64(self):
        assert _arrow_type_to_sr_sql(pa.int64()) == 'BIGINT'

    def test_float32(self):
        assert _arrow_type_to_sr_sql(pa.float32()) == 'FLOAT'

    def test_float64(self):
        assert _arrow_type_to_sr_sql(pa.float64()) == 'DOUBLE'

    def test_boolean(self):
        assert _arrow_type_to_sr_sql(pa.bool_()) == 'BOOLEAN'

    def test_string(self):
        assert _arrow_type_to_sr_sql(pa.string()) == 'STRING'

    def test_large_string(self):
        assert _arrow_type_to_sr_sql(pa.large_string()) == 'STRING'

    def test_timestamp_defaults_to_string(self):
        assert _arrow_type_to_sr_sql(pa.timestamp('us')) == 'STRING'


# ── _infer_sr_type ────────────────────────────────────────────────────────────

class TestInferSrType:
    def test_integers(self):
        assert _infer_sr_type(['1', '2', '3']) == ('INT', False)

    def test_floats(self):
        assert _infer_sr_type(['1.5', '2.7', '3.0']) == ('FLOAT', False)

    def test_booleans(self):
        assert _infer_sr_type(['true', 'false', 'True']) == ('BOOLEAN', False)

    def test_strings(self):
        assert _infer_sr_type(['hello', 'world']) == ('STRING', False)

    def test_mixed_int_float_returns_float(self):
        assert _infer_sr_type(['1', '2.5', '3']) == ('FLOAT', False)

    def test_mixed_with_string_returns_string(self):
        assert _infer_sr_type(['1', 'hello', '3']) == ('STRING', False)

    def test_empty_values_detected(self):
        sr_type, has_empty = _infer_sr_type(['1', '', '3'])
        assert sr_type == 'INT'
        assert has_empty is True

    def test_all_empty_returns_string(self):
        assert _infer_sr_type(['', '', '']) == ('STRING', True)

    def test_none_values_are_empty(self):
        sr_type, has_empty = _infer_sr_type([None, '1', '2'])
        assert sr_type == 'INT'
        assert has_empty is True

    def test_negative_integers(self):
        assert _infer_sr_type(['-1', '-2', '3']) == ('INT', False)

    def test_negative_floats(self):
        assert _infer_sr_type(['-1.5', '2.7']) == ('FLOAT', False)

    def test_single_value_int(self):
        assert _infer_sr_type(['42']) == ('INT', False)

    def test_empty_list_returns_string(self):
        assert _infer_sr_type([]) == ('STRING', False)


# ── _sr_server_path ───────────────────────────────────────────────────────────

class TestSrServerPath:
    def test_parsed_path_with_source(self, monkeypatch):
        monkeypatch.setenv('SOURCE', 'reddit')
        assert _sr_server_path('/data/parsed/RS_2024-01.parquet') == \
            '/data/parsed/reddit/RS_2024-01.parquet'

    def test_output_path_with_source(self, monkeypatch):
        monkeypatch.setenv('SOURCE', 'reddit')
        assert _sr_server_path('/data/output/lingua/RS_2024-01.parquet') == \
            '/data/output/reddit/lingua/RS_2024-01.parquet'

    def test_no_source_returns_unchanged(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        path = '/data/parsed/RS_2024-01.parquet'
        assert _sr_server_path(path) == path

    def test_non_matching_prefix_unchanged(self, monkeypatch):
        monkeypatch.setenv('SOURCE', 'reddit')
        path = '/data/dumps/file.zst'
        assert _sr_server_path(path) == path

    def test_nested_subdirectory(self, monkeypatch):
        monkeypatch.setenv('SOURCE', 'hf_dataset')
        assert _sr_server_path('/data/parsed/submissions/RS_2024-01.parquet') == \
            '/data/parsed/hf_dataset/submissions/RS_2024-01.parquet'


# ── get_column_list ───────────────────────────────────────────────────────────

class TestGetColumnList:
    PLATFORM = {
        'mandatory_fields': ['dataset', 'id', 'retrieved_utc'],
        'fields': {
            'submissions': ['title', 'selftext', 'score'],
            'comments': ['body', 'score'],
        },
    }

    def test_mandatory_fields_first(self):
        cols = get_column_list('submissions', self.PLATFORM)
        assert cols[:3] == ['dataset', 'id', 'retrieved_utc']

    def test_data_type_fields_appended(self):
        cols = get_column_list('submissions', self.PLATFORM)
        assert cols == ['dataset', 'id', 'retrieved_utc', 'title', 'selftext', 'score']

    def test_different_data_type(self):
        cols = get_column_list('comments', self.PLATFORM)
        assert cols == ['dataset', 'id', 'retrieved_utc', 'body', 'score']

    def test_lingua_columns_appended(self):
        cols = get_column_list('submissions', self.PLATFORM, file='/data/output/lingua/RS_2024-01.parquet')
        assert cols[-5:] == ['lang', 'lang_prob', 'lang2', 'lang2_prob', 'lang_chars']

    def test_lingua_not_appended_for_regular_file(self):
        cols = get_column_list('submissions', self.PLATFORM, file='/data/parsed/RS_2024-01.parquet')
        assert 'lang' not in cols

    def test_no_file_means_no_lingua(self):
        cols = get_column_list('submissions', self.PLATFORM)
        assert 'lang' not in cols

    def test_missing_data_type_raises(self):
        with pytest.raises(ConfigurationError, match="No fields configured"):
            get_column_list('nonexistent', self.PLATFORM)

    def test_no_mandatory_fields_still_works(self):
        platform = {'fields': {'comments': ['body', 'score']}}
        cols = get_column_list('comments', platform)
        assert cols == ['body', 'score']


# ── get_create_table_query ────────────────────────────────────────────────────

class TestGetCreateTableQuery:
    PLATFORM = {
        'field_types': {
            'id': ['char', 7],
            'dataset': ['char', 7],
            'retrieved_utc': 'integer',
            'title': 'text',
            'score': 'integer',
        },
    }
    COLUMNS = ['dataset', 'id', 'retrieved_utc', 'title', 'score']

    def test_pk_column_first(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        lines = query.split('\n')
        # First column definition should be the PK
        assert '`id`' in lines[1]

    def test_pk_column_not_null(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert '`id` CHAR(7) NOT NULL' in query

    def test_non_pk_columns_nullable(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        # title should not have NOT NULL
        assert '`title` VARCHAR(1048576)\n' in query or '`title` VARCHAR(1048576),' in query

    def test_distributed_by_hash(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert 'DISTRIBUTED BY HASH(`id`)' in query

    def test_primary_key_clause(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert 'PRIMARY KEY (`id`)' in query

    def test_persistent_index_property(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert '"enable_persistent_index" = "true"' in query

    def test_replication_num_one(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert '"replication_num" = "1"' in query

    def test_create_if_not_exists(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert 'CREATE TABLE IF NOT EXISTS `reddit`.`submissions`' in query

    def test_type_mapping_applied(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS, self.PLATFORM, 'id')
        assert 'CHAR(7)' in query  # id and dataset
        assert 'INT' in query       # retrieved_utc, score
        assert 'VARCHAR(1048576)' in query  # title (text → VARCHAR 1 MB)

    def test_unknown_field_type_defaults_to_varchar_max(self):
        cols = ['id', 'mystery_field']
        platform = {'field_types': {'id': ['char', 7]}}
        query = get_create_table_query('tbl', 'db', cols, platform, 'id')
        assert '`mystery_field` VARCHAR(1048576)' in query

    def test_compression_property_when_set(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS,
                                       self.PLATFORM, 'id', compression='ZSTD')
        assert '"compression" = "ZSTD"' in query
        # Appended after the existing PK properties, none of which are lost.
        assert '"enable_persistent_index" = "true"' in query
        assert '"replication_num" = "1"' in query

    def test_compression_omitted_when_none(self):
        query = get_create_table_query('submissions', 'reddit', self.COLUMNS,
                                       self.PLATFORM, 'id')
        assert 'compression' not in query
        # PK PROPERTIES stay byte-identical to the pre-compression output.
        assert ('PROPERTIES("enable_persistent_index" = "true", '
                '"replication_num" = "1")') in query


class TestGetCreateTableQueryNoPK:
    """Custom platforms that opted out of a source PK use the Duplicate Key
    table model + DISTRIBUTED BY RANDOM. Re-runs append rows; no source-level
    dedup. Mirrors the shape get_classifier_create_table_query uses."""

    PLATFORM = {'field_types': {'text': 'text', 'score': 'integer'}}
    COLUMNS = ['text', 'score']

    def test_no_primary_key_clause(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        assert 'PRIMARY KEY' not in query

    def test_distributed_by_random(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        assert 'DISTRIBUTED BY RANDOM' in query
        assert 'DISTRIBUTED BY HASH' not in query

    def test_no_not_null_on_any_column(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        assert 'NOT NULL' not in query

    def test_columns_preserve_declared_order(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        text_idx = query.index('`text`')
        score_idx = query.index('`score`')
        assert text_idx < score_idx

    def test_no_persistent_index_property(self):
        # enable_persistent_index is a PK-table property; Duplicate Key tables
        # reject it. Only replication_num should be present.
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        assert 'enable_persistent_index' not in query
        assert '"replication_num" = "1"' in query

    def test_buckets_clause_when_supplied(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None, buckets=32)
        assert 'DISTRIBUTED BY RANDOM BUCKETS 32' in query

    def test_create_if_not_exists(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        assert 'CREATE TABLE IF NOT EXISTS `hf`.`tweets`' in query

    def test_compression_property_when_set(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM,
                                       None, compression='ZSTD')
        assert '"compression" = "ZSTD"' in query
        # Compression is appended after replication_num, which is preserved.
        assert '"replication_num" = "1"' in query

    def test_compression_omitted_when_none(self):
        query = get_create_table_query('tweets', 'hf', self.COLUMNS, self.PLATFORM, None)
        assert 'compression' not in query
        # No-PK PROPERTIES stay byte-identical to the pre-compression output.
        assert 'PROPERTIES("replication_num" = "1")' in query


# ── get_ingest_query ──────────────────────────────────────────────────────────

class TestGetIngestQuery:
    COLUMNS = ['id', 'title', 'score']

    def test_parquet_format(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet')
        assert '"format" = "parquet"' in query
        assert 'csv' not in query.lower().split('format')[0]  # no csv params

    def test_csv_format_params(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.csv', file_format='csv')
        assert '"format" = "csv"' in query
        assert '"csv.column_separator" = ","' in query
        assert '"csv.row_delimiter" = "\\n"' in query
        assert '"csv.skip_header" = "1"' in query

    def test_insert_into_correct_table(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet')
        assert 'INSERT INTO `reddit`.`submissions`' in query

    def test_columns_backticked(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet')
        assert '`id`, `title`, `score`' in query

    def test_select_from_files(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet')
        assert 'SELECT `id`, `title`, `score`' in query
        assert 'FROM FILES(' in query

    def test_file_path_in_query(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet')
        assert '"path" = "file:///data/parsed/RS_2024-01.parquet"' in query

    def test_merge_condition_with_order_field(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet',
                                 check_duplicates=True, order_field='score')
        assert 'PROPERTIES("merge_condition" = "score")' in query

    def test_merge_condition_skipped_without_check_duplicates(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet',
                                 check_duplicates=False, order_field='score')
        assert 'merge_condition' not in query

    def test_merge_condition_skipped_without_order_field(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet',
                                 check_duplicates=True, order_field=None)
        assert 'merge_condition' not in query

    def test_merge_condition_skipped_when_field_not_in_columns(self, monkeypatch):
        monkeypatch.delenv('SOURCE', raising=False)
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet',
                                 check_duplicates=True, order_field='not_a_column')
        assert 'merge_condition' not in query

    def test_server_path_translation(self, monkeypatch):
        monkeypatch.setenv('SOURCE', 'reddit')
        query = get_ingest_query('submissions', 'reddit', self.COLUMNS,
                                 '/data/parsed/RS_2024-01.parquet')
        assert 'file:///data/parsed/reddit/RS_2024-01.parquet' in query


# ── get_classifier_create_table_query ─────────────────────────────────────────

class TestGetClassifierCreateTableQuery:
    def test_with_primary_key(self):
        cols = ['id', 'toxic_score', 'label']
        types = {'id': 'CHAR(7)', 'toxic_score': 'FLOAT', 'label': 'STRING'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types, pk_column='id')
        assert 'PRIMARY KEY (`id`)' in query
        assert 'DISTRIBUTED BY HASH(`id`)' in query
        assert '`id` CHAR(7) NOT NULL' in query
        assert '"enable_persistent_index" = "true"' in query

    def test_without_primary_key(self):
        cols = ['toxic_score', 'label']
        types = {'toxic_score': 'FLOAT', 'label': 'STRING'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types)
        assert 'DISTRIBUTED BY RANDOM' in query
        assert 'PRIMARY KEY' not in query

    def test_pk_column_ordered_first(self):
        cols = ['toxic_score', 'id', 'label']
        types = {'id': 'CHAR(7)', 'toxic_score': 'FLOAT', 'label': 'STRING'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types, pk_column='id')
        lines = query.split('\n')
        # First column def should be id
        assert '`id`' in lines[1]

    def test_missing_type_defaults_to_string(self):
        cols = ['id', 'unknown_col']
        types = {'id': 'CHAR(7)'}
        query = get_classifier_create_table_query('tbl', 'db', cols, types, pk_column='id')
        assert '`unknown_col` STRING' in query

    def test_create_if_not_exists(self):
        cols = ['id', 'score']
        types = {'id': 'CHAR(7)', 'score': 'FLOAT'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types, pk_column='id')
        assert 'CREATE TABLE IF NOT EXISTS `reddit`.`toxic`' in query

    def test_compression_property_with_pk(self):
        cols = ['id', 'score']
        types = {'id': 'CHAR(7)', 'score': 'FLOAT'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types,
                                                  pk_column='id', compression='ZSTD')
        assert '"compression" = "ZSTD"' in query
        assert '"enable_persistent_index" = "true"' in query

    def test_compression_property_without_pk(self):
        cols = ['toxic_score', 'label']
        types = {'toxic_score': 'FLOAT', 'label': 'STRING'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types,
                                                  compression='ZSTD')
        # Duplicate Key table carries replication_num, plus compression appended.
        assert 'PROPERTIES("replication_num" = "1", "compression" = "ZSTD")' in query

    def test_no_pk_sets_replication_num_when_compression_none(self):
        cols = ['toxic_score', 'label']
        types = {'toxic_score': 'FLOAT', 'label': 'STRING'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types)
        # replication_num=1 is always present (CREATE fails on single-BE
        # clusters without it); no compression property when unset.
        assert 'PROPERTIES("replication_num" = "1")' in query
        assert 'compression' not in query

    def test_pk_no_compression_properties_unchanged(self):
        cols = ['id', 'score']
        types = {'id': 'CHAR(7)', 'score': 'FLOAT'}
        query = get_classifier_create_table_query('toxic', 'reddit', cols, types, pk_column='id')
        assert 'compression' not in query
        assert ('PROPERTIES("enable_persistent_index" = "true", '
                '"replication_num" = "1")') in query


# ── infer_classifier_schema ───────────────────────────────────────────────────

class TestInferClassifierSchema:
    def test_csv_integer_inference(self, tmp_path):
        f = tmp_path / "classifier.csv"
        f.write_text("id,score\nabc,1\ndef,2\nghi,3\n")
        columns, types, nullable = infer_classifier_schema(str(f))
        assert columns == ['id', 'score']
        assert types['id'] == 'STRING'
        assert types['score'] == 'INT'
        assert nullable == []

    def test_csv_float_inference(self, tmp_path):
        f = tmp_path / "classifier.csv"
        f.write_text("id,probability\nabc,0.95\ndef,0.12\n")
        columns, types, nullable = infer_classifier_schema(str(f))
        assert types['probability'] == 'FLOAT'

    def test_csv_boolean_inference(self, tmp_path):
        f = tmp_path / "classifier.csv"
        f.write_text("id,is_toxic\nabc,true\ndef,false\n")
        columns, types, nullable = infer_classifier_schema(str(f))
        assert types['is_toxic'] == 'BOOLEAN'

    def test_csv_empty_values_mark_nullable(self, tmp_path):
        f = tmp_path / "classifier.csv"
        f.write_text("id,score\nabc,1\ndef,\nghi,3\n")
        columns, types, nullable = infer_classifier_schema(str(f))
        assert types['score'] == 'INT'
        assert 'score' in nullable

    def test_csv_string_empty_not_nullable(self, tmp_path):
        """STRING columns with empty values are NOT marked nullable (empty string is valid)."""
        f = tmp_path / "classifier.csv"
        f.write_text("id,label\nabc,good\ndef,\nghi,bad\n")
        columns, types, nullable = infer_classifier_schema(str(f))
        assert types['label'] == 'STRING'
        assert 'label' not in nullable

    def test_csv_column_overrides(self, tmp_path):
        f = tmp_path / "classifier.csv"
        f.write_text("id,score\nabc,1\ndef,2\n")
        columns, types, nullable = infer_classifier_schema(str(f), column_overrides={'score': 'BIGINT'})
        assert types['score'] == 'BIGINT'

    def test_csv_no_header_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("")
        with pytest.raises(ValueError, match="no header"):
            infer_classifier_schema(str(f))

    def test_csv_sampling_limit(self, tmp_path):
        f = tmp_path / "large.csv"
        lines = ["id,val"] + [f"r{i},{i}" for i in range(2000)]
        f.write_text("\n".join(lines) + "\n")
        columns, types, nullable = infer_classifier_schema(str(f), n_rows=10)
        assert types['val'] == 'INT'
        assert columns == ['id', 'val']

    def test_parquet_schema_inference(self, tmp_path):
        f = tmp_path / "classifier.parquet"
        table = pa.table({
            'id': pa.array(['abc', 'def'], type=pa.string()),
            'score': pa.array([1, 2], type=pa.int32()),
            'probability': pa.array([0.9, 0.1], type=pa.float64()),
        })
        pq.write_table(table, str(f))
        columns, types, nullable = infer_classifier_schema(str(f))
        assert columns == ['id', 'score', 'probability']
        assert types['id'] == 'STRING'
        assert types['score'] == 'INT'
        assert types['probability'] == 'DOUBLE'
        assert nullable == []

    def test_parquet_column_overrides(self, tmp_path):
        f = tmp_path / "classifier.parquet"
        table = pa.table({
            'id': pa.array(['abc'], type=pa.string()),
            'score': pa.array([1], type=pa.int32()),
        })
        pq.write_table(table, str(f))
        columns, types, nullable = infer_classifier_schema(str(f), column_overrides={'score': 'BIGINT'})
        assert types['score'] == 'BIGINT'

    def test_parquet_int64_maps_to_bigint(self, tmp_path):
        f = tmp_path / "classifier.parquet"
        table = pa.table({
            'big_id': pa.array([10**15, 10**16], type=pa.int64()),
        })
        pq.write_table(table, str(f))
        columns, types, nullable = infer_classifier_schema(str(f))
        assert types['big_id'] == 'BIGINT'

    def test_parquet_bool_maps_to_boolean(self, tmp_path):
        f = tmp_path / "classifier.parquet"
        table = pa.table({
            'flag': pa.array([True, False], type=pa.bool_()),
        })
        pq.write_table(table, str(f))
        columns, types, nullable = infer_classifier_schema(str(f))
        assert types['flag'] == 'BOOLEAN'


# ── BUCKETS clause (get_create_table_query / get_classifier_create_table_query) ──

class TestBucketsClause:
    PLATFORM = {'field_types': {'id': ['char', 7]}}

    def test_create_table_no_buckets_when_not_passed(self):
        q = get_create_table_query('t', 'db', ['id'], self.PLATFORM, 'id')
        assert 'BUCKETS' not in q

    def test_create_table_buckets_emitted(self):
        q = get_create_table_query('t', 'db', ['id'], self.PLATFORM, 'id', buckets=128)
        assert 'DISTRIBUTED BY HASH(`id`) BUCKETS 128' in q

    def test_create_table_buckets_coerced_to_int(self):
        q = get_create_table_query('t', 'db', ['id'], self.PLATFORM, 'id', buckets='64')
        assert 'BUCKETS 64' in q

    def test_classifier_create_with_pk_buckets(self):
        q = get_classifier_create_table_query(
            't', 'db', ['id', 'score'],
            {'id': 'CHAR(7)', 'score': 'FLOAT'},
            pk_column='id', buckets=32,
        )
        assert 'DISTRIBUTED BY HASH(`id`) BUCKETS 32' in q

    def test_classifier_create_without_pk_buckets(self):
        q = get_classifier_create_table_query(
            't', 'db', ['score'], {'score': 'FLOAT'},
            buckets=16,
        )
        assert 'DISTRIBUTED BY RANDOM BUCKETS 16' in q

    def test_classifier_create_without_pk_no_buckets(self):
        q = get_classifier_create_table_query(
            't', 'db', ['score'], {'score': 'FLOAT'},
        )
        assert 'DISTRIBUTED BY RANDOM' in q
        assert 'BUCKETS' not in q


# ── compute_bucket_count ─────────────────────────────────────────────────────

class TestComputeBucketCount:
    def test_int_value_uniform_across_data_types(self):
        platform = {'sr_buckets': 256}
        assert compute_bucket_count(platform, 'submissions') == 256
        assert compute_bucket_count(platform, 'comments') == 256

    def test_dict_value_per_data_type(self):
        platform = {'sr_buckets': {'submissions': 128, 'comments': 512}}
        assert compute_bucket_count(platform, 'submissions') == 128
        assert compute_bucket_count(platform, 'comments') == 512

    def test_dict_unknown_data_type_returns_none(self):
        platform = {'sr_buckets': {'submissions': 128}}
        assert compute_bucket_count(platform, 'comments') is None

    def test_missing_key_returns_none(self):
        assert compute_bucket_count({}, 'submissions') is None

    def test_zero_clamps_to_one(self):
        assert compute_bucket_count({'sr_buckets': 0}, 'anything') == 1

    def test_negative_clamps_to_one(self):
        assert compute_bucket_count({'sr_buckets': -5}, 'anything') == 1

    def test_string_numeric_is_coerced(self):
        # YAML can parse numerics as strings depending on quoting
        assert compute_bucket_count({'sr_buckets': '64'}, 'anything') == 64
