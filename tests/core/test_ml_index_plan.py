"""Tests for build_ml_index_plan — the classifier-table index plan builder.

Bug class:
  - postgres_ml / sr_ml index creation fails silently: a wrong lookup key or an
    accidental fallback to the base `indexes` map produces either no indexes at
    all or CREATE INDEX on columns the classifier table doesn't have. Neither
    shows up without a live database, so the lookup logic is pinned here.
  - The no-fallback rule is a deliberate design decision (base index fields name
    author/subreddit/domain, absent from classifier tables). A future "helpful"
    fallback would be caught by test_no_fallback_to_base_indexes.
"""

import pytest

from social_data_pipeline.core.config import ConfigurationError, build_ml_index_plan


class TestBuildMLIndexPlan:
    """Lookup order, no-fallback rule, and filtering."""

    def test_reads_platform_config(self):
        plan = build_ml_index_plan(
            {"comments_lingua"},
            {},
            {"ml_indexes": {"comments_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert plan == {"comments_lingua": ["lang"]}

    def test_profile_config_wins_over_platform(self):
        plan = build_ml_index_plan(
            {"comments_lingua"},
            {"ml_indexes": {"comments_lingua": ["lang_prob"]}},
            {"ml_indexes": {"comments_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert plan == {"comments_lingua": ["lang_prob"]}

    def test_empty_profile_map_falls_through_to_platform(self):
        # Profile pipeline.yaml ships `ml_indexes: {}` — falsy, so the platform
        # config must still be consulted.
        plan = build_ml_index_plan(
            {"comments_lingua"},
            {"ml_indexes": {}},
            {"ml_indexes": {"comments_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert plan == {"comments_lingua": ["lang"]}

    def test_no_fallback_to_base_indexes(self):
        # `indexes` / `sr_indexes` name base-table columns. A classifier table
        # must never inherit them.
        plan = build_ml_index_plan(
            {"comments_lingua"},
            {},
            {
                "indexes": {"comments": ["author", "subreddit"]},
                "sr_indexes": {"comments": ["author"]},
            },
            "ml_indexes",
        )
        assert plan == {}

    def test_table_without_configured_fields_is_omitted(self):
        plan = build_ml_index_plan(
            {"comments_lingua", "submissions_lingua"},
            {},
            {"ml_indexes": {"comments_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert plan == {"comments_lingua": ["lang"]}

    def test_empty_field_list_is_omitted(self):
        plan = build_ml_index_plan(
            {"comments_lingua"},
            {},
            {"ml_indexes": {"comments_lingua": []}},
            "ml_indexes",
        )
        assert plan == {}

    def test_no_indexed_tables(self):
        plan = build_ml_index_plan(
            set(),
            {},
            {"ml_indexes": {"comments_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert plan == {}

    def test_no_config_at_all(self):
        assert build_ml_index_plan({"comments_lingua"}, {}, {}, "ml_indexes") == {}

    def test_sr_key_is_independent_of_pg_key(self):
        platform = {
            "ml_indexes": {"comments_lingua": ["lang"]},
            "sr_ml_indexes": {"comments_lingua": ["lang", "lang2"]},
        }
        assert build_ml_index_plan({"comments_lingua"}, {}, platform, "sr_ml_indexes") == {
            "comments_lingua": ["lang", "lang2"]
        }

    @pytest.mark.parametrize(
        "indexed_tables",
        [
            {"comments_lingua", "submissions_lingua"},                 # sr_ml passes a set
            {"comments_lingua": "comments", "submissions_lingua": "submissions"},  # postgres_ml a dict
        ],
    )
    def test_accepts_any_iterable_of_table_names(self, indexed_tables):
        plan = build_ml_index_plan(
            indexed_tables,
            {},
            {"ml_indexes": {"comments_lingua": ["lang"], "submissions_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert plan == {"comments_lingua": ["lang"], "submissions_lingua": ["lang"]}

    def test_tables_returned_in_sorted_order(self):
        plan = build_ml_index_plan(
            {"submissions_lingua", "comments_lingua"},
            {},
            {"ml_indexes": {"comments_lingua": ["lang"], "submissions_lingua": ["lang"]}},
            "ml_indexes",
        )
        assert list(plan) == ["comments_lingua", "submissions_lingua"]

    def test_nested_sr_shape_raises_for_postgres(self):
        # PostgreSQL has one index mechanism. Without the guard, list(dict)
        # yields ['bitmap', 'bloomfilter'] and postgres_ml would CREATE INDEX on
        # columns with those literal names, silently.
        with pytest.raises(ConfigurationError, match="must be a plain list"):
            build_ml_index_plan(
                {"comments_lingua"},
                {},
                {"ml_indexes": {"comments_lingua": {"bitmap": ["lang"]}}},
                "ml_indexes",
            )

    def test_error_message_names_table_and_key(self):
        with pytest.raises(ConfigurationError, match=r"ml_indexes\['comments_lingua'\]"):
            build_ml_index_plan(
                {"comments_lingua"},
                {},
                {"ml_indexes": {"comments_lingua": {"bloomfilter": ["lang"]}}},
                "ml_indexes",
            )

    def test_does_not_alias_config_field_lists(self):
        # The plan is handed to CREATE INDEX loops; mutating it must not write
        # back into the loaded config.
        platform = {"ml_indexes": {"comments_lingua": ["lang"]}}
        plan = build_ml_index_plan({"comments_lingua"}, {}, platform, "ml_indexes")
        plan["comments_lingua"].append("injected")
        assert platform["ml_indexes"]["comments_lingua"] == ["lang"]
