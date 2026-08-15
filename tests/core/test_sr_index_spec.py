"""Tests for the StarRocks per-index-type config shape.

Bug class:
  - `normalize_index_spec` decides whether a configured column becomes a BITMAP
    index or a bloom filter. Both failures are silent against a live server: a
    plain list reinterpreted as bloom would build the wrong mechanism on tables
    that already have bitmaps, and a typo'd sub-key (`bloom_filter`) would index
    nothing at all while the run reports success.
  - The legacy contract is load-bearing: config/sources/*/platform.yaml holds
    plain lists written before per-type support existed, and sr_ingest's
    fallback chain reads PostgreSQL's `indexes` key for sources with no
    `sr_indexes`. A plain list must always mean BITMAP.
  - `build_sr_index_plan` serves two callers with different fallback depths
    (sr_ingest: sr_indexes -> indexes; sr_ml: sr_ml_indexes only). Collapsing
    them would either drop a production fallback or invent one.
"""

import pytest

from social_data_pipeline.core.config import (
    ConfigurationError,
    build_sr_index_plan,
    normalize_index_spec,
    normalize_sr_column_type,
    sr_index_types_for_column,
)


class TestColumnTypeEligibility:
    """Which mechanisms a column type can carry.

    FLOAT/DOUBLE is the case that matters: StarRocks excludes it from BOTH
    bitmap and bloom filter, so such a column must be dropped from an index
    plan, not rerouted to the other type — every classifier table is full of
    float score columns, and lingua tables carry `lang_prob`.
    """

    @pytest.mark.parametrize("data_type", ["float", "double", "FLOAT", "Double"])
    def test_float_supports_neither(self, data_type):
        assert sr_index_types_for_column(data_type) == {"bitmap": False, "bloomfilter": False}

    @pytest.mark.parametrize("data_type", ["tinyint", "boolean", "decimal64(18,2)", "decimal128"])
    def test_bitmap_only_types(self, data_type):
        assert sr_index_types_for_column(data_type) == {"bitmap": True, "bloomfilter": False}

    @pytest.mark.parametrize(
        "data_type", ["varchar(1048576)", "char(7)", "int(11)", "bigint", "date", "datetime", "string"]
    )
    def test_both_mechanisms(self, data_type):
        assert sr_index_types_for_column(data_type) == {"bitmap": True, "bloomfilter": True}

    @pytest.mark.parametrize("data_type", ["", None, "json", "array<int>"])
    def test_unknown_types_are_permissive(self, data_type):
        # The list guards against known-bad builds; it is not an allowlist.
        assert sr_index_types_for_column(data_type) == {"bitmap": True, "bloomfilter": True}

    @pytest.mark.parametrize(
        "raw,expected",
        [("varchar(65533)", "varchar"), ("CHAR(7)", "char"), ("decimal64(18,2)", "decimal"),
         ("decimal128", "decimal"), ("  INT ", "int"), (None, "")],
    )
    def test_normalize_column_type(self, raw, expected):
        assert normalize_sr_column_type(raw) == expected


class TestNormalizeIndexSpec:
    """Shape normalization. A plain list is BITMAP, always."""

    def test_plain_list_is_bitmap(self):
        assert normalize_index_spec(["author", "subreddit"]) == {
            "bitmap": ["author", "subreddit"],
            "bloomfilter": [],
        }

    def test_empty_list(self):
        assert normalize_index_spec([]) == {"bitmap": [], "bloomfilter": []}

    def test_none(self):
        assert normalize_index_spec(None) == {"bitmap": [], "bloomfilter": []}

    def test_empty_dict(self):
        # `{}` as a per-table spec — distinct from `{}` at the map level, which
        # is what config/templates/reddit.yaml ships.
        assert normalize_index_spec({}) == {"bitmap": [], "bloomfilter": []}

    def test_both_types(self):
        assert normalize_index_spec({"bitmap": ["subreddit"], "bloomfilter": ["author"]}) == {
            "bitmap": ["subreddit"],
            "bloomfilter": ["author"],
        }

    def test_only_bloomfilter(self):
        assert normalize_index_spec({"bloomfilter": ["author"]}) == {
            "bitmap": [],
            "bloomfilter": ["author"],
        }

    def test_only_bitmap(self):
        assert normalize_index_spec({"bitmap": ["subreddit"]}) == {
            "bitmap": ["subreddit"],
            "bloomfilter": [],
        }

    def test_null_sub_key_is_empty(self):
        # `bloomfilter:` with nothing after it parses as None in YAML.
        assert normalize_index_spec({"bitmap": ["a"], "bloomfilter": None}) == {
            "bitmap": ["a"],
            "bloomfilter": [],
        }

    def test_dedupes_within_a_type(self):
        assert normalize_index_spec({"bitmap": ["a", "a", "b"]})["bitmap"] == ["a", "b"]

    def test_same_column_allowed_under_both_types(self):
        # Bitmap and bloom filter are independent mechanisms; a column may carry
        # both, so no cross-type dedupe.
        spec = normalize_index_spec({"bitmap": ["author"], "bloomfilter": ["author"]})
        assert spec == {"bitmap": ["author"], "bloomfilter": ["author"]}

    @pytest.mark.parametrize("bad_key", ["bloom_filter", "bloomFilter", "bloom", "ngram"])
    def test_unknown_sub_key_raises(self, bad_key):
        with pytest.raises(ConfigurationError, match="unknown index type"):
            normalize_index_spec({bad_key: ["author"]}, where="sr_indexes['comments']")

    def test_error_message_names_the_table(self):
        with pytest.raises(ConfigurationError, match=r"sr_indexes\['comments'\]"):
            normalize_index_spec({"bloom_filter": ["a"]}, where="sr_indexes['comments']")

    def test_non_list_sub_key_raises(self):
        with pytest.raises(ConfigurationError, match="must be a list"):
            normalize_index_spec({"bitmap": "author"})

    def test_scalar_spec_raises(self):
        with pytest.raises(ConfigurationError, match="expected a list"):
            normalize_index_spec("author")

    def test_does_not_alias_input(self):
        original = {"bitmap": ["a"], "bloomfilter": ["b"]}
        out = normalize_index_spec(original)
        out["bitmap"].append("injected")
        assert original["bitmap"] == ["a"]


class TestBuildSRIndexPlan:
    """Lookup order, fallback depth, and the list-only rule for `indexes`."""

    def test_sr_ml_has_no_fallback(self):
        # A classifier table must not inherit base-table index fields.
        plan = build_sr_index_plan(
            {"comments_lingua"},
            {},
            {"indexes": {"comments_lingua": ["author"]},
             "sr_indexes": {"comments_lingua": ["author"]}},
            ("sr_ml_indexes",),
        )
        assert plan == {}

    def test_sr_ml_reads_its_own_key(self):
        plan = build_sr_index_plan(
            {"comments_lingua"},
            {},
            {"sr_ml_indexes": {"comments_lingua": {"bloomfilter": ["lang"]}}},
            ("sr_ml_indexes",),
        )
        assert plan == {"comments_lingua": {"bitmap": [], "bloomfilter": ["lang"]}}

    def test_sr_ingest_prefers_sr_indexes(self):
        plan = build_sr_index_plan(
            {"comments"},
            {},
            {"sr_indexes": {"comments": ["subreddit"]},
             "indexes": {"comments": ["author"]}},
            ("sr_indexes", "indexes"),
        )
        assert plan == {"comments": {"bitmap": ["subreddit"], "bloomfilter": []}}

    def test_sr_ingest_falls_back_to_indexes_as_bitmap(self):
        # reddit's live config: `indexes` and no `sr_indexes`. This fallback is
        # what builds its StarRocks bitmaps today.
        plan = build_sr_index_plan(
            {"comments"},
            {},
            {"indexes": {"comments": ["dataset", "author", "subreddit", "link_id"]}},
            ("sr_indexes", "indexes"),
        )
        assert plan == {
            "comments": {
                "bitmap": ["dataset", "author", "subreddit", "link_id"],
                "bloomfilter": [],
            }
        }

    def test_nested_spec_under_indexes_raises(self):
        # `indexes` is shared with postgres_ingest, which has one index
        # mechanism; a per-type dict there must not be silently accepted.
        with pytest.raises(ConfigurationError, match="must be a plain list"):
            build_sr_index_plan(
                {"comments"},
                {},
                {"indexes": {"comments": {"bloomfilter": ["author"]}}},
                ("sr_indexes", "indexes"),
            )

    def test_profile_config_wins_over_platform(self):
        plan = build_sr_index_plan(
            {"comments"},
            {"sr_indexes": {"comments": ["from_profile"]}},
            {"sr_indexes": {"comments": ["from_platform"]}},
            ("sr_indexes", "indexes"),
        )
        assert plan["comments"]["bitmap"] == ["from_profile"]

    def test_table_without_config_omitted(self):
        plan = build_sr_index_plan(
            {"comments", "submissions"},
            {},
            {"sr_indexes": {"comments": ["author"]}},
            ("sr_indexes", "indexes"),
        )
        assert list(plan) == ["comments"]

    def test_spec_with_only_empty_lists_omitted(self):
        plan = build_sr_index_plan(
            {"comments"},
            {},
            {"sr_indexes": {"comments": {"bitmap": [], "bloomfilter": []}}},
            ("sr_indexes", "indexes"),
        )
        assert plan == {}

    def test_empty_map_at_map_level(self):
        assert build_sr_index_plan({"comments"}, {}, {"sr_indexes": {}}, ("sr_indexes",)) == {}

    def test_no_config_at_all(self):
        assert build_sr_index_plan({"comments"}, {}, {}, ("sr_indexes", "indexes")) == {}

    def test_accepts_any_iterable_of_table_names(self):
        # sr_ingest passes data types, sr_ml passes classifier table names.
        platform = {"sr_indexes": {"a": ["x"], "b": ["y"]}}
        as_set = build_sr_index_plan({"a", "b"}, {}, platform, ("sr_indexes",))
        as_list = build_sr_index_plan(["a", "b"], {}, platform, ("sr_indexes",))
        assert as_set == as_list
        assert list(as_set) == ["a", "b"]
