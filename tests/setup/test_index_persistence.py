"""Tests for `sdp db create-indexes` → platform.yaml persistence routing.

Bug class:
  `_interactive_pg_indexes` / `_interactive_sr_indexes` offer every base table in
  the database, which includes classifier tables (`comments_lingua`, …). Before
  routing existed, those were written back under `indexes` / `sr_indexes` keyed
  by the classifier table name — keys nothing reads: the base ingestion profiles
  only iterate `data_types`. The saved config looked authoritative and rebuilt
  nothing after a table drop.

  The routing rule is exact set membership in `data_types`, so these tests pin
  both directions plus the merge semantics.
"""

from __future__ import annotations

import yaml

import sdp


def _write_platform(tmp_path, source="reddit", **extra):
    """Create config/sources/<source>/platform.yaml under a fake CONFIG_DIR."""
    source_dir = tmp_path / "sources" / source
    source_dir.mkdir(parents=True)
    config = {"db_schema": source, "data_types": ["submissions", "comments"]}
    config.update(extra)
    (source_dir / "platform.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
    return source_dir / "platform.yaml"


def _read(path):
    return yaml.safe_load(path.read_text())


class TestPersistIndexRouting:
    """Base tables → indexes/sr_indexes; classifier tables → ml_indexes/sr_ml_indexes."""

    def test_base_table_routes_to_base_key(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={"comments": ["author"]},
            mongo_created={},
            sr_created={"comments": ["subreddit"]},
        )

        config = _read(path)
        assert config["indexes"] == {"comments": ["author"]}
        assert config["sr_indexes"] == {"comments": ["subreddit"]}
        assert "ml_indexes" not in config
        assert "sr_ml_indexes" not in config

    def test_classifier_table_routes_to_ml_key(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={"comments_lingua": ["lang"]},
            mongo_created={},
            sr_created={"comments_toxic_roberta": ["toxic"]},
        )

        config = _read(path)
        assert config["ml_indexes"] == {"comments_lingua": ["lang"]}
        assert config["sr_ml_indexes"] == {"comments_toxic_roberta": ["toxic"]}
        assert "indexes" not in config
        assert "sr_indexes" not in config

    def test_mixed_batch_splits_by_table(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={"comments": ["author"], "comments_lingua": ["lang"]},
            mongo_created={},
            sr_created={"submissions": ["dataset"], "submissions_lingua": ["lang"]},
        )

        config = _read(path)
        assert config["indexes"] == {"comments": ["author"]}
        assert config["ml_indexes"] == {"comments_lingua": ["lang"]}
        assert config["sr_indexes"] == {"submissions": ["dataset"]}
        assert config["sr_ml_indexes"] == {"submissions_lingua": ["lang"]}

    def test_merges_into_existing_without_duplicates(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(
            tmp_path,
            indexes={"comments": ["author"]},
            ml_indexes={"comments_lingua": ["lang"]},
        )

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={"comments": ["author", "subreddit"], "comments_lingua": ["lang", "lang2"]},
            mongo_created={},
            sr_created={},
        )

        config = _read(path)
        assert config["indexes"] == {"comments": ["author", "subreddit"]}
        assert config["ml_indexes"] == {"comments_lingua": ["lang", "lang2"]}

    def test_mongo_always_uses_mongo_indexes(self, tmp_path, monkeypatch):
        # Mongo has no ML ingestion profile; its dict is keyed by data type and
        # must never be split.
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={},
            mongo_created={"comments": ["author"]},
            sr_created={},
        )

        assert _read(path)["mongo_indexes"] == {"comments": ["author"]}

    def test_typed_sr_spec_is_written(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={},
            mongo_created={},
            sr_created={"comments": {"bitmap": ["subreddit"], "bloomfilter": ["author"]}},
        )

        assert _read(path)["sr_indexes"] == {
            "comments": {"bitmap": ["subreddit"], "bloomfilter": ["author"]}
        }

    def test_bitmap_only_stays_flat(self, tmp_path, monkeypatch):
        # No reason to churn existing configs into the per-type shape when
        # nothing needs it.
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={},
            mongo_created={},
            sr_created={"comments": {"bitmap": ["subreddit"], "bloomfilter": []}},
        )

        assert _read(path)["sr_indexes"] == {"comments": ["subreddit"]}

    def test_bloomfilter_promotes_stored_list_without_losing_it(self, tmp_path, monkeypatch):
        # A stored plain list means bitmap. Adding a bloom filter must preserve
        # it rather than raise AttributeError after the build has already run.
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path, sr_indexes={"comments": ["dataset", "subreddit"]})

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={},
            mongo_created={},
            sr_created={"comments": {"bitmap": [], "bloomfilter": ["author"]}},
        )

        assert _read(path)["sr_indexes"] == {
            "comments": {"bitmap": ["dataset", "subreddit"], "bloomfilter": ["author"]}
        }

    def test_merges_into_stored_typed_spec_without_duplicates(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(
            tmp_path,
            sr_ml_indexes={"comments_lingua": {"bitmap": ["lang"], "bloomfilter": ["author"]}},
        )

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={},
            mongo_created={},
            sr_created={"comments_lingua": {"bitmap": ["lang", "lang2"],
                                            "bloomfilter": ["author"]}},
        )

        assert _read(path)["sr_ml_indexes"] == {
            "comments_lingua": {"bitmap": ["lang", "lang2"], "bloomfilter": ["author"]}
        }

    def test_typed_spec_routes_classifier_tables_to_ml_key(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={},
            mongo_created={},
            sr_created={
                "comments": {"bitmap": [], "bloomfilter": ["author"]},
                "comments_lingua": {"bitmap": [], "bloomfilter": ["lang"]},
            },
        )

        config = _read(path)
        assert config["sr_indexes"] == {"comments": {"bitmap": [], "bloomfilter": ["author"]}}
        assert config["sr_ml_indexes"] == {
            "comments_lingua": {"bitmap": [], "bloomfilter": ["lang"]}
        }

    def test_unrelated_keys_survive(self, tmp_path, monkeypatch):
        monkeypatch.setattr(sdp, "CONFIG_DIR", tmp_path)
        path = _write_platform(tmp_path, primary_key="id", sr_buckets=8)

        sdp._persist_indexes_to_config(
            "reddit",
            pg_created={"comments_lingua": ["lang"]},
            mongo_created={},
            sr_created={},
        )

        config = _read(path)
        assert config["primary_key"] == "id"
        assert config["sr_buckets"] == 8
        assert config["data_types"] == ["submissions", "comments"]
