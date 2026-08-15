"""Tests for setup utility functions."""



from social_data_pipeline.setup.utils import (
    ask,
    ask_int,
    ask_bool,
    ask_choice,
    ask_multi_select,
    ask_list,
    detect_cpu_cores,
    glob_to_regex,
    derive_file_patterns,
)


# ============================================================================
# ask
# ============================================================================


class TestAsk:
    def test_returns_input(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "hello")
        assert ask("prompt") == "hello"

    def test_returns_default_on_empty(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert ask("prompt", default="world") == "world"

    def test_returns_empty_without_default(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert ask("prompt") == ""

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "  spaced  ")
        assert ask("prompt") == "spaced"

    def test_default_converted_to_str(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = ask("prompt", default=42)
        assert result == "42"


# ============================================================================
# ask_int
# ============================================================================


class TestAskInt:
    def test_valid_int(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "42")
        assert ask_int("prompt") == 42

    def test_default_on_empty(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert ask_int("prompt", default=10) == 10

    def test_retries_on_invalid(self, monkeypatch):
        responses = iter(["abc", "def", "7"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        assert ask_int("prompt") == 7


# ============================================================================
# ask_bool
# ============================================================================


class TestAskBool:
    def test_default_true(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert ask_bool("prompt", default=True) is True

    def test_default_false(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert ask_bool("prompt", default=False) is False

    def test_yes_variants(self, monkeypatch):
        for answer in ["y", "yes", "true", "1", "Y", "YES"]:
            monkeypatch.setattr("builtins.input", lambda _, a=answer: a)
            assert ask_bool("prompt", default=False) is True

    def test_no_variants(self, monkeypatch):
        for answer in ["n", "no", "false", "0"]:
            monkeypatch.setattr("builtins.input", lambda _, a=answer: a)
            assert ask_bool("prompt", default=True) is False


# ============================================================================
# ask_choice
# ============================================================================


class TestAskChoice:
    def test_select_by_number(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "2")
        result = ask_choice("Pick one", ["a", "b", "c"], default="a")
        assert result == "b"

    def test_select_by_name(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "c")
        result = ask_choice("Pick one", ["a", "b", "c"], default="a")
        assert result == "c"

    def test_default_on_empty(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = ask_choice("Pick one", ["a", "b", "c"], default="b")
        assert result == "b"

    def test_retries_on_invalid(self, monkeypatch):
        responses = iter(["99", "invalid", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(responses))
        result = ask_choice("Pick one", ["x", "y"], default="x")
        assert result == "x"


# ============================================================================
# ask_multi_select
# ============================================================================


class TestAskMultiSelect:
    def test_default_on_empty(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = ask_multi_select("Select", ["a", "b", "c"], defaults=["a", "c"])
        assert result == ["a", "c"]

    def test_select_by_numbers(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "1,3")
        result = ask_multi_select("Select", ["a", "b", "c"], defaults=["a"])
        assert result == ["a", "c"]

    def test_select_single(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "2")
        result = ask_multi_select("Select", ["a", "b", "c"], defaults=["a"])
        assert result == ["b"]

    def test_defaults_all_when_none(self, monkeypatch):
        """When defaults=None, all options are default."""
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = ask_multi_select("Select", ["a", "b", "c"])
        assert result == ["a", "b", "c"]

    def test_invalid_input_returns_defaults(self, monkeypatch):
        """If all parts are invalid, returns defaults."""
        monkeypatch.setattr("builtins.input", lambda _: "invalid,stuff")
        result = ask_multi_select("Select", ["a", "b"], defaults=["b"])
        assert result == ["b"]

    def test_empty_defaults_selects_nothing(self, monkeypatch):
        """defaults=[] must survive the None check and mean 'nothing selected'.

        `sdp db create-indexes` relies on this for its BITMAP question: every
        index defaults to a bloom filter and the user opts specific columns in.
        Passing None (or omitting the argument) would silently make EVERY column
        a BITMAP — an expensive, hard-to-notice inversion on billion-row tables.
        """
        monkeypatch.setattr("builtins.input", lambda _: "")
        assert ask_multi_select("Select", ["a", "b", "c"], defaults=[]) == []

    def test_empty_defaults_still_accepts_explicit_selection(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "2")
        assert ask_multi_select("Select", ["a", "b", "c"], defaults=[]) == ["b"]

    def test_empty_defaults_with_unparseable_input_selects_nothing(self, monkeypatch):
        """Fails safe toward the cheaper index type rather than toward BITMAP."""
        monkeypatch.setattr("builtins.input", lambda _: "nope")
        assert ask_multi_select("Select", ["a", "b"], defaults=[]) == []


# ============================================================================
# ask_list
# ============================================================================


class TestAskList:
    def test_comma_separated(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "one, two, three")
        result = ask_list("Items")
        assert result == ["one", "two", "three"]

    def test_default(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = ask_list("Items", default=["x", "y"])
        assert result == ["x", "y"]

    def test_strips_empty_items(self, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "a,,b,  ,c")
        result = ask_list("Items")
        assert result == ["a", "b", "c"]


# ============================================================================
# detect_cpu_cores
# ============================================================================


class TestDetectCpuCores:
    def test_returns_int(self):
        result = detect_cpu_cores()
        assert isinstance(result, int)
        assert result >= 1


# ============================================================================
# glob_to_regex
# ============================================================================


class TestGlobToRegex:
    def test_simple_wildcard(self):
        import re
        regex = glob_to_regex("RC_*.zst")
        assert re.match(regex, "RC_2024-01.zst")
        assert not re.match(regex, "RS_2024-01.zst")

    def test_question_mark(self):
        import re
        regex = glob_to_regex("data_?.csv")
        assert re.match(regex, "data_1.csv")
        assert not re.match(regex, "data_12.csv")

    def test_dot_escaped(self):
        import re
        regex = glob_to_regex("file.csv")
        # The dot should be escaped, not match any character
        assert re.match(regex, "file.csv")
        assert not re.match(regex, "fileXcsv")


# ============================================================================
# derive_file_patterns
# ============================================================================


class TestDeriveFilePatterns:
    def test_zst_compression(self):
        import re
        result = derive_file_patterns("RC_*.zst", "zst")
        assert "dump" in result
        assert "json" in result
        assert "csv" in result
        assert "parquet" in result
        assert "prefix" in result
        assert result["prefix"] == "RC_"
        assert result["compression"] == "zst"
        # Dump regex matches .zst files
        assert re.match(result["dump"], "RC_2024-01.zst")
        # JSON regex matches extensionless files
        assert re.match(result["json"], "RC_2024-01")
        # CSV regex matches .csv files
        assert re.match(result["csv"], "RC_2024-01.csv")
        # Parquet regex matches .parquet files
        assert re.match(result["parquet"], "RC_2024-01.parquet")

    def test_json_gz_compression(self):
        import re
        result = derive_file_patterns("data_*.json.gz", "gz")
        assert re.match(result["dump"], "data_2024.json.gz")
        # JSON: stem is "data_*" (after stripping .json.gz -> .json -> strip .json)
        assert re.match(result["json"], "data_2024")
        assert re.match(result["csv"], "data_2024.csv")

    def test_csv_input_format(self):
        import re
        result = derive_file_patterns("tweets_*.csv.gz", "gz", input_format="csv")
        assert re.match(result["dump"], "tweets_2024.csv.gz")
        # For csv input: json regex is built BEFORE stripping .csv
        assert re.match(result["json"], "tweets_2024.csv")
        assert re.match(result["csv"], "tweets_2024.csv")

    def test_tar_gz_compression(self):
        import re
        result = derive_file_patterns("archive_*.tar.gz", "tar.gz")
        assert re.match(result["dump"], "archive_2024.tar.gz")
        assert re.match(result["json"], "archive_2024")

    def test_prefix_extraction(self):
        result = derive_file_patterns("tweets_*.json.gz", "gz")
        assert result["prefix"] == "tweets_"

    def test_no_wildcard_prefix(self):
        result = derive_file_patterns("data.zst", "zst")
        assert result["prefix"] == "data.zst"

    def test_dump_glob_preserved(self):
        result = derive_file_patterns("RC_*.zst", "zst")
        assert result["dump_glob"] == "RC_*.zst"
