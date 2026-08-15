"""Tests for source-setup config generators."""

import pytest
import yaml

from social_data_pipeline.setup.source import (
    carry_over_unprompted,
    generate_platform_yaml,
    generate_postgres_yaml,
    generate_postgres_ml_yaml,
    generate_reddit_platform_yaml,
    generate_starrocks_yaml,
    generate_sr_ml_yaml,
)


def _base_settings(**overrides):
    settings = {
        "source_name": "mydata",
        "data_types": ["events"],
        "dumps_path": "./data/dumps/mydata",
        "extracted_path": "./data/extracted/mydata",
        "parsed_path": "./data/parsed/mydata",
        "output_path": "./data/output/mydata",
        "custom_file_patterns": {
            "events": {
                "dump": r"^events_.+\.zst$",
                "json": r"^events_.+$",
                "csv": r"^events_.+\.csv$",
                "parquet": r"^events_.+\.parquet$",
                "prefix": "events_",
                "compression": "zst",
                "dump_glob": "events_*.zst",
            }
        },
    }
    settings.update(overrides)
    return settings


class TestGeneratePlatformYaml:
    def test_writes_fields_when_provided(self):
        """Happy path: explicit fields land in the generated YAML."""
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "user.name", "score"]}
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["fields"]["events"] == ["timestamp", "user.name", "score"]

    def test_raises_when_no_fields(self):
        """Regression: empty/missing fields used to silently produce an
        empty fields list, breaking parse with "No fields configured for
        data type". Now the generator refuses.
        """
        settings = _base_settings()  # no custom_fields
        with pytest.raises(ValueError, match="No fields configured"):
            generate_platform_yaml(settings)

    def test_raises_when_fields_dict_empty(self):
        """Defensive: a dict with no entries is also rejected."""
        settings = _base_settings(custom_fields={})
        with pytest.raises(ValueError, match="No fields configured"):
            generate_platform_yaml(settings)


class TestProfileGeneratorsNoPrimaryKey:
    """When a CUSTOM source has no primary_key, db-profile generators must
    emit check_duplicates: false. Otherwise the runtime guards
    (postgres_ingest.py 'check_duplicates is enabled but no primary_key'
    and sr_ingest.py's equivalent) would raise on the first run, and the SR
    ingest path would also emit invalid PROPERTIES("merge_condition") SQL
    against a Duplicate Key table.

    Reddit must NOT get this injection — its primary_key lives in the
    template (not in settings), so a naive `not settings.get("primary_key")`
    check would silently flip Reddit's check_duplicates to false and break
    the standard ON CONFLICT path on overlapping-ID re-ingest. The platform
    gate prevents that.
    """

    def _custom(self, primary_key=None):
        return {"data_types": ["events"], "platform": "custom/mydata", "primary_key": primary_key}

    def _reddit(self):
        # Reddit settings: no "primary_key" key (template owns it).
        return {"data_types": ["submissions", "comments"], "platform": "reddit"}

    # ---- custom platform without PK → check_duplicates: false ----

    def test_postgres_yaml_disables_check_duplicates_when_custom_no_pk(self):
        out = yaml.safe_load(generate_postgres_yaml(self._custom(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_postgres_yaml_omits_check_duplicates_when_custom_pk_set(self):
        out = yaml.safe_load(generate_postgres_yaml(self._custom(primary_key="id")))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_starrocks_yaml_disables_check_duplicates_when_custom_no_pk(self):
        out = yaml.safe_load(generate_starrocks_yaml(self._custom(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_starrocks_yaml_omits_check_duplicates_when_custom_pk_set(self):
        out = yaml.safe_load(generate_starrocks_yaml(self._custom(primary_key="id")))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_postgres_ml_yaml_disables_check_duplicates_when_custom_no_pk(self):
        out = yaml.safe_load(generate_postgres_ml_yaml(self._custom(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_sr_ml_yaml_disables_check_duplicates_when_custom_no_pk(self):
        out = yaml.safe_load(generate_sr_ml_yaml(self._custom(primary_key=None)))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    # ---- Reddit → check_duplicates LEFT to base default ----

    def test_postgres_yaml_does_not_touch_reddit_check_duplicates(self):
        out = yaml.safe_load(generate_postgres_yaml(self._reddit()))
        assert "check_duplicates" not in out["pipeline"]["processing"], (
            "Reddit's postgres.yaml must inherit check_duplicates: true from the base "
            "pipeline.yaml — overriding to false breaks the ON CONFLICT upsert path that "
            "Reddit needs for overlapping-ID re-ingest (Arctic Shift schema drift)."
        )

    def test_starrocks_yaml_does_not_touch_reddit_check_duplicates(self):
        out = yaml.safe_load(generate_starrocks_yaml(self._reddit()))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_postgres_ml_yaml_does_not_touch_reddit_check_duplicates(self):
        out = yaml.safe_load(generate_postgres_ml_yaml(self._reddit()))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_sr_ml_yaml_does_not_touch_reddit_check_duplicates(self):
        out = yaml.safe_load(generate_sr_ml_yaml(self._reddit()))
        assert "check_duplicates" not in out["pipeline"]["processing"]


class TestPlatformYamlNoPrimaryKey:
    """primary_key is optional in the generated platform.yaml — absent means
    'no source-level dedup, db autogenerates / Duplicate Key model'."""

    def test_omitted_when_settings_primary_key_is_none(self):
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "text"]},
            primary_key=None,
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert "primary_key" not in config

    def test_omitted_when_settings_primary_key_is_empty_string(self):
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "text"]},
            primary_key="",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert "primary_key" not in config

    def test_emitted_when_settings_primary_key_set(self):
        settings = _base_settings(
            custom_fields={"events": ["timestamp", "text"]},
            primary_key="timestamp",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["primary_key"] == "timestamp"


class TestPlatformYamlMandatoryDataset:
    """The custom parser always prepends a `dataset` column. Without
    mandatory_fields: [dataset] in platform.yaml, downstream PG/SR
    get_column_list returns N columns while the parsed Parquet has N+1,
    and PG's pg_parquet COPY fails on the count mismatch. Reddit is
    immune (its template sets mandatory_fields). This pins the fix for
    custom platforms (HF and non-HF alike).
    """

    def test_non_hf_custom_emits_dataset_in_mandatory_fields(self):
        settings = _base_settings(custom_fields={"events": ["id", "score"]})
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["mandatory_fields"] == ["dataset"]

    def test_non_hf_custom_emits_dataset_in_field_types(self):
        settings = _base_settings(custom_fields={"events": ["id", "score"]})
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["field_types"]["dataset"] == "text"

    def test_hf_custom_emits_dataset_in_mandatory_fields(self):
        # HF path provides custom_field_types (derived from HF metadata).
        settings = _base_settings(
            custom_fields={"events": ["text", "label"]},
            custom_field_types={"text": "text", "label": "integer"},
            hf_dataset="some/dataset",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["mandatory_fields"] == ["dataset"]

    def test_hf_custom_emits_dataset_in_field_types_when_absent_from_hf(self):
        # HF metadata won't include `dataset` (parser-synthesized column);
        # generator must inject it so PG/SR DDL knows the type.
        settings = _base_settings(
            custom_fields={"events": ["text"]},
            custom_field_types={"text": "text"},
            hf_dataset="some/dataset",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["field_types"]["dataset"] == "text"

    def test_hf_custom_preserves_existing_dataset_type_if_provided(self):
        # Defensive: if some upstream ever provides `dataset` via
        # custom_field_types, don't clobber it.
        settings = _base_settings(
            custom_fields={"events": ["text"]},
            custom_field_types={"dataset": ["char", 7], "text": "text"},
            hf_dataset="some/dataset",
        )
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["field_types"]["dataset"] == ["char", 7]


class TestGenerateRedditPlatformYaml:
    """Reddit's platform.yaml is built by copying config/templates/reddit.yaml
    and injecting per-source paths + file_format. The template's
    mandatory_fields / primary_key / state_field / field_types must survive
    the round trip — if any of those gets clobbered or dropped, Reddit
    ingest fails downstream in non-obvious ways (PG fast-load assumes a PK,
    parser dedup waterfall keys on state_field, etc.).
    """

    def _reddit_settings(self, **overrides):
        s = {
            "source_name": "reddit",
            "dumps_path": "./data/dumps/reddit",
            "extracted_path": "./data/extracted/reddit",
            "parsed_path": "./data/parsed/reddit",
            "output_path": "./data/output/reddit",
        }
        s.update(overrides)
        return s

    def test_mandatory_fields_preserved_from_template(self):
        config = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        # Must contain at least `dataset` (the per-dump lineage column that
        # the parser writes) — losing it would break PG/SR get_column_list.
        assert "mandatory_fields" in config
        assert "dataset" in config["mandatory_fields"]

    def test_primary_key_preserved_from_template(self):
        config = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        # Reddit's ON CONFLICT path keys on id; if the template-derived PK
        # gets dropped, dedup falls back to fast-load every run.
        assert config.get("primary_key") == "id"

    def test_state_field_preserved_from_template(self):
        config = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        # state_field drives Arctic-Shift deletion-waterfall + DB-state
        # recovery via DISTINCT dataset. Losing it silently re-processes all
        # files on resume.
        assert config.get("state_field") == "dataset"

    def test_field_types_preserved_from_template(self):
        config = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        # A representative slice — the full type table lives in the template;
        # just verify it round-trips with at least the PK + ordering fields.
        ft = config.get("field_types", {})
        assert "id" in ft
        assert "dataset" in ft
        assert "retrieved_utc" in ft

    def test_paths_injected_from_settings(self):
        s = self._reddit_settings(
            dumps_path="/mnt/dumps/r",
            extracted_path="/mnt/extracted/r",
            parsed_path="/mnt/parsed/r",
            output_path="/mnt/output/r",
        )
        config = yaml.safe_load(generate_reddit_platform_yaml(s))
        assert config["paths"] == {
            "dumps": "/mnt/dumps/r",
            "extracted": "/mnt/extracted/r",
            "parsed": "/mnt/parsed/r",
            "output": "/mnt/output/r",
        }

    def test_file_format_defaults_to_parquet(self):
        config = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        assert config["file_format"] == "parquet"

    def test_file_format_respects_setting(self):
        s = self._reddit_settings(file_format="csv")
        config = yaml.safe_load(generate_reddit_platform_yaml(s))
        assert config["file_format"] == "csv"

    def test_parquet_row_group_size_written_only_when_set(self):
        without = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        assert "parquet_row_group_size" not in without

        with_val = yaml.safe_load(generate_reddit_platform_yaml(
            self._reddit_settings(parquet_row_group_size=500_000)
        ))
        assert with_val["parquet_row_group_size"] == 500_000

    def test_sr_buckets_template_default_preserved_when_unset(self):
        # Reddit's template ships with `sr_buckets: 256`. When the operator
        # doesn't override, the template value must round-trip — losing it
        # silently flips SR to auto-bucketing (too low for big datasets).
        config = yaml.safe_load(generate_reddit_platform_yaml(self._reddit_settings()))
        assert config["sr_buckets"] == 256

    def test_sr_buckets_explicit_setting_overrides_template(self):
        config = yaml.safe_load(generate_reddit_platform_yaml(
            self._reddit_settings(sr_buckets=64)
        ))
        assert config["sr_buckets"] == 64

    def test_sr_buckets_zero_is_written(self):
        # `if settings.get("sr_buckets") is not None` — explicit 0 should
        # still override (e.g. operator override for tiny dev datasets).
        config = yaml.safe_load(generate_reddit_platform_yaml(
            self._reddit_settings(sr_buckets=0)
        ))
        assert config["sr_buckets"] == 0


class TestGeneratePostgresYamlOptionalSettings:
    """Conditional writes in generate_postgres_yaml. Each was added to
    expose an operator knob; silently dropping any of them after a refactor
    would degrade behavior without an error message.
    """

    def _reddit(self, **kw):
        s = {"data_types": ["comments"], "platform": "reddit"}
        s.update(kw)
        return s

    def test_pg_prefer_lingua_written_only_when_set(self):
        without = yaml.safe_load(generate_postgres_yaml(self._reddit()))
        assert "prefer_lingua" not in without["pipeline"]["processing"]

        with_true = yaml.safe_load(generate_postgres_yaml(self._reddit(pg_prefer_lingua=True)))
        assert with_true["pipeline"]["processing"]["prefer_lingua"] is True

        with_false = yaml.safe_load(generate_postgres_yaml(self._reddit(pg_prefer_lingua=False)))
        assert with_false["pipeline"]["processing"]["prefer_lingua"] is False

    def test_pg_parallel_index_workers_written_only_when_set(self):
        without = yaml.safe_load(generate_postgres_yaml(self._reddit()))
        assert "parallel_index_workers" not in without["pipeline"]["processing"]

        with_val = yaml.safe_load(generate_postgres_yaml(
            self._reddit(pg_parallel_index_workers=4)
        ))
        assert with_val["pipeline"]["processing"]["parallel_index_workers"] == 4

    def test_tablespaces_written_only_when_set(self):
        without = yaml.safe_load(generate_postgres_yaml(self._reddit()))
        assert "tablespaces" not in without["pipeline"]

        ts = {"fast": "/mnt/nvme"}
        with_val = yaml.safe_load(generate_postgres_yaml(self._reddit(tablespaces=ts)))
        assert with_val["pipeline"]["tablespaces"] == ts

    def test_table_tablespaces_written_only_when_set(self):
        without = yaml.safe_load(generate_postgres_yaml(self._reddit()))
        assert "table_tablespaces" not in without["pipeline"]

        tt = {"comments": "fast"}
        with_val = yaml.safe_load(generate_postgres_yaml(self._reddit(table_tablespaces=tt)))
        assert with_val["pipeline"]["table_tablespaces"] == tt


class TestGenerateStarrocksYamlOptionalSettings:
    def _reddit(self, **kw):
        s = {"data_types": ["comments"], "platform": "reddit"}
        s.update(kw)
        return s

    def test_sr_prefer_lingua_written_only_when_set(self):
        without = yaml.safe_load(generate_starrocks_yaml(self._reddit()))
        assert "prefer_lingua" not in without["pipeline"]["processing"]

        with_true = yaml.safe_load(generate_starrocks_yaml(self._reddit(sr_prefer_lingua=True)))
        assert with_true["pipeline"]["processing"]["prefer_lingua"] is True


class TestPlatformGateDefensiveBehavior:
    """The check_duplicates: false gate keys on settings['platform']
    starting with 'custom/'. Cover the failure modes:
      - missing 'platform' key → defaults to "" → not-custom → no override
        (safe: Reddit-like behavior; if a custom platform reaches this with
        no platform key, the runtime no-PK guard will fail loud instead of
        silently disabling dedup)
      - platform="reddit" → no override (the B3 contract)
      - platform="custom/x" → override fires when no primary_key
    """

    def test_missing_platform_key_defaults_to_no_override(self):
        # No 'platform' key, no 'primary_key' key. Generator should NOT
        # override check_duplicates — the safe default for an unknown
        # platform is the base pipeline.yaml's true.
        out = yaml.safe_load(generate_postgres_yaml({"data_types": ["x"]}))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_reddit_platform_with_falsy_primary_key_no_override(self):
        # Explicit primary_key=None for reddit — still must not override.
        out = yaml.safe_load(generate_postgres_yaml({
            "data_types": ["x"], "platform": "reddit", "primary_key": None,
        }))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_custom_platform_with_falsy_primary_key_overrides(self):
        out = yaml.safe_load(generate_postgres_yaml({
            "data_types": ["x"], "platform": "custom/anything", "primary_key": None,
        }))
        assert out["pipeline"]["processing"]["check_duplicates"] is False

    def test_custom_platform_with_truthy_primary_key_no_override(self):
        out = yaml.safe_load(generate_postgres_yaml({
            "data_types": ["x"], "platform": "custom/anything", "primary_key": "id",
        }))
        assert "check_duplicates" not in out["pipeline"]["processing"]

    def test_gate_applies_consistently_across_all_four_generators(self):
        # Spot-check that all four generators agree on the gate.
        custom_no_pk = {"data_types": ["x"], "platform": "custom/anything"}
        reddit = {"data_types": ["x"], "platform": "reddit"}

        for gen in (generate_postgres_yaml, generate_postgres_ml_yaml,
                    generate_starrocks_yaml, generate_sr_ml_yaml):
            custom_out = yaml.safe_load(gen(custom_no_pk))
            reddit_out = yaml.safe_load(gen(reddit))
            assert custom_out["pipeline"]["processing"]["check_duplicates"] is False, (
                f"{gen.__name__} must disable check_duplicates for custom platforms with no PK"
            )
            assert "check_duplicates" not in reddit_out["pipeline"]["processing"], (
                f"{gen.__name__} must NOT override check_duplicates for Reddit"
            )


class TestGeneratePlatformYamlConditionalWrites:
    """generate_platform_yaml's conditional-write logic. Each conditional
    is a small contract that could regress silently in a refactor.
    """

    def _custom(self, **overrides):
        s = _base_settings(custom_fields={"events": ["a", "b"]})
        s.update(overrides)
        return s

    # ---- file_format / input_format / input_csv_delimiter / parquet_row_group_size ----

    def test_file_format_defaults_to_parquet(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert config["file_format"] == "parquet"

    def test_file_format_respects_setting(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom(file_format="csv")))
        assert config["file_format"] == "csv"

    def test_input_format_omitted_when_default_ndjson(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "input_format" not in config

    def test_input_format_omitted_when_explicit_ndjson(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom(input_format="ndjson")))
        assert "input_format" not in config

    def test_input_format_written_when_non_default(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom(input_format="csv")))
        assert config["input_format"] == "csv"

    def test_input_format_written_when_parquet(self):
        # HF flow sets input_format=parquet — must round-trip.
        config = yaml.safe_load(generate_platform_yaml(self._custom(input_format="parquet")))
        assert config["input_format"] == "parquet"

    def test_input_csv_delimiter_omitted_when_default_comma(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "input_csv_delimiter" not in config

    def test_input_csv_delimiter_written_when_non_default(self):
        config = yaml.safe_load(generate_platform_yaml(
            self._custom(input_format="csv", input_csv_delimiter="\t")
        ))
        assert config["input_csv_delimiter"] == "\t"

    def test_parquet_row_group_size_written_only_when_set(self):
        without = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "parquet_row_group_size" not in without

        with_val = yaml.safe_load(generate_platform_yaml(
            self._custom(parquet_row_group_size=500_000)
        ))
        assert with_val["parquet_row_group_size"] == 500_000

    # ---- db_schema ----

    def test_db_schema_defaults_to_source_name(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        # source_name in _base_settings is "mydata"
        assert config["db_schema"] == "mydata"

    def test_db_schema_respects_explicit_value(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom(db_schema="analytics")))
        assert config["db_schema"] == "analytics"

    # ---- paths block ----

    def test_paths_block_uses_settings_values(self):
        s = self._custom(
            dumps_path="/x/d", extracted_path="/x/e",
            parsed_path="/x/p", output_path="/x/o",
        )
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["paths"] == {
            "dumps": "/x/d", "extracted": "/x/e",
            "parsed": "/x/p", "output": "/x/o",
        }

    # ---- HF-specific keys ----

    def test_hf_keys_round_trip_when_set(self):
        s = self._custom(
            hf_dataset="org/dataset",
            hf_config_map={"events": ["train", "test"]},
        )
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["hf_dataset"] == "org/dataset"
        assert config["hf_config_map"] == {"events": ["train", "test"]}

    def test_hf_keys_omitted_when_unset(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "hf_dataset" not in config
        assert "hf_config_map" not in config

    # ---- Mongo-specific keys ----

    def test_mongo_collection_strategy_written_with_db_name(self):
        # When mongo_collection_strategy is set, mongo_db_name must also be
        # written (default = source_name) — runtime requires both.
        s = self._custom(mongo_collection_strategy="per_file")
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["mongo_collection_strategy"] == "per_file"
        assert config["mongo_db_name"] == "mydata"  # default from source_name

    def test_mongo_collection_strategy_with_explicit_db_name(self):
        s = self._custom(
            mongo_collection_strategy="per_data_type",
            mongo_db_name="my_explicit_db",
        )
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["mongo_db_name"] == "my_explicit_db"

    def test_mongo_collections_round_trip(self):
        s = self._custom(
            mongo_collection_strategy="per_data_type",
            mongo_collections={"events": "events_coll"},
        )
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["mongo_collections"] == {"events": "events_coll"}

    def test_mongo_exclude_fields_round_trip(self):
        s = self._custom(mongo_exclude_fields=["embeddings", "raw_payload"])
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["mongo_exclude_fields"] == ["embeddings", "raw_payload"]

    def test_mongo_keys_omitted_when_unset(self):
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "mongo_collection_strategy" not in config
        assert "mongo_db_name" not in config
        assert "mongo_collections" not in config
        assert "mongo_exclude_fields" not in config

    # ---- Indexes ----

    def test_indexes_always_emitted_even_if_empty(self):
        # Runtime code at db/postgres/ingest.py reads platform_config['indexes']
        # via .get with default {}; an absent key is fine for runtime but the
        # generator's current contract is to always emit, even as {}. Pin it.
        config = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "indexes" in config
        assert config["indexes"] == {}

    def test_custom_indexes_round_trip(self):
        s = self._custom(custom_indexes={"events": ["author", "created_at"]})
        config = yaml.safe_load(generate_platform_yaml(s))
        assert config["indexes"] == {"events": ["author", "created_at"]}

    def test_custom_sr_indexes_written_only_when_set(self):
        without = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "sr_indexes" not in without

        with_val = yaml.safe_load(generate_platform_yaml(
            self._custom(custom_sr_indexes={"events": ["author"]})
        ))
        assert with_val["sr_indexes"] == {"events": ["author"]}

    def test_custom_ml_indexes_written_only_when_set(self):
        without = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "ml_indexes" not in without

        with_val = yaml.safe_load(generate_platform_yaml(
            self._custom(custom_ml_indexes={"events_lingua": ["lang"]})
        ))
        assert with_val["ml_indexes"] == {"events_lingua": ["lang"]}

    def test_custom_sr_ml_indexes_written_only_when_set(self):
        without = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "sr_ml_indexes" not in without

        with_val = yaml.safe_load(generate_platform_yaml(
            self._custom(custom_sr_ml_indexes={"events_lingua": ["lang"]})
        ))
        assert with_val["sr_ml_indexes"] == {"events_lingua": ["lang"]}

    def test_custom_mongo_indexes_written_only_when_set(self):
        without = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "mongo_indexes" not in without

        with_val = yaml.safe_load(generate_platform_yaml(
            self._custom(custom_mongo_indexes={"events": ["author"]})
        ))
        assert with_val["mongo_indexes"] == {"events": ["author"]}

    # ---- sr_buckets ----

    def test_sr_buckets_written_only_when_set(self):
        without = yaml.safe_load(generate_platform_yaml(self._custom()))
        assert "sr_buckets" not in without

        with_val = yaml.safe_load(generate_platform_yaml(self._custom(sr_buckets=64)))
        assert with_val["sr_buckets"] == 64

    def test_sr_buckets_zero_is_written(self):
        # `is not None` check, not truthiness — explicit 0 should land.
        config = yaml.safe_load(generate_platform_yaml(self._custom(sr_buckets=0)))
        assert config["sr_buckets"] == 0


class TestUnpromptedCarryOver:
    """Classifier-table indexes have no prompt, so `source configure` can only
    preserve them via carry_over_unprompted(). generate_platform_yaml() rebuilds
    platform.yaml from scratch — anything missing from `settings` is deleted."""

    def test_carries_ml_index_keys(self):
        existing = {
            "custom_ml_indexes": {"events_lingua": ["lang"]},
            "custom_sr_ml_indexes": {"events_lingua": ["lang", "lang2"]},
        }
        settings = carry_over_unprompted(existing, {})
        assert settings["custom_ml_indexes"] == {"events_lingua": ["lang"]}
        assert settings["custom_sr_ml_indexes"] == {"events_lingua": ["lang", "lang2"]}

    def test_absent_and_empty_keys_are_not_invented(self):
        settings = carry_over_unprompted({"custom_ml_indexes": {}}, {})
        assert settings == {}

    def test_does_not_clobber_existing_settings(self):
        settings = carry_over_unprompted({}, {"source_name": "mydata"})
        assert settings == {"source_name": "mydata"}

    def test_survives_round_trip_when_source_has_no_sr_ingest(self):
        # The interactive index block is gated on
        # `has_postgres or has_mongo or has_starrocks`, and
        # has_starrocks == ("sr_ingest" in profiles). A source running sr_ml
        # without sr_ingest never reaches it — the carry-over is the only thing
        # keeping sr_ml_indexes alive across `source configure`.
        existing = {
            "custom_ml_indexes": {"events_lingua": ["lang"]},
            "custom_sr_ml_indexes": {"events_lingua": ["lang"]},
        }
        settings = carry_over_unprompted(existing, _base_settings(custom_fields={"events": ["a"]}))
        config = yaml.safe_load(generate_platform_yaml(settings))
        assert config["ml_indexes"] == {"events_lingua": ["lang"]}
        assert config["sr_ml_indexes"] == {"events_lingua": ["lang"]}
