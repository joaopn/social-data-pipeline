"""Tests for the source-name helpers of the control boundary: the line-break test, the one-path-component check and the lookup of an existing source folder.

A name from a request is joined to config/sources/ to find the source's folder, so a name that is not exactly one path component must never address a folder. The helpers are reached through the module (`names.<name>`), so each test fails on its own when one is missing; sources live in the temp tree of the `source_tree` fixture. Line and paragraph separators are written as escapes: an editor can turn a raw U+2028 into a space, and the test would keep passing.
"""

from __future__ import annotations

import pytest

from social_data_pipeline.control import names

# Not one path component, although each of them addresses an existing folder in the test tree (see `_with_odd_folders`).
MALFORMED = ["", ".", "..", "../x", "a/b", "a\\b", "ok\n"]


def _with_odd_folders(root):
    """Add to the fixture tree a folder whose name holds a backslash and one whose name ends in a line break, so a refusal of those names cannot come from "no such folder"."""
    for name in ("a\\b", "ok\n"):
        (root / "config" / "sources" / name).mkdir()


class TestHasLineBreakOrControl:
    @pytest.mark.parametrize("value", ["a\nb", "a\rb", "a\x85b", "a\u2028b", "a\u2029b", "a\x00b", "ok\n", "\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "a\tb", "\x7f"])
    def test_finds_every_line_break_and_control_character(self, value):
        """Bug class: a check for `\\n` alone. `str.splitlines()` also breaks on `\\r`, `\\x0b`, `\\x0c`, `\\x1c` to `\\x1e`, `\\x85`, U+2028 and U+2029, so a value with one of those would still become two lines in a file that is read line by line."""
        assert names.has_line_break_or_control(value) is True

    @pytest.mark.parametrize("value", ["", "plain text 123", "caf\u00e9", "a\u00a0b", "RS_2024-*", "p@ss w0rd!"])
    def test_leaves_ordinary_text_alone(self, value):
        """Bug class: a check so wide it refuses a space, a non-ASCII letter or punctuation, which would lock out ordinary passwords and names."""
        assert names.has_line_break_or_control(value) is False

    @pytest.mark.parametrize("value", ["a\nb", "a\rb", "a\x0bb", "a\x0cb", "a\x1cb", "a\x1db", "a\x1eb", "a\x85b", "a\u2028b", "a\u2029b"])
    def test_covers_everything_splitlines_breaks_on(self, value):
        """Bug class: the list of categories drifting from what the line reader splits on. Each value here really is two lines for `str.splitlines()`, and each must be found."""
        assert len(value.splitlines()) == 2

        assert names.has_line_break_or_control(value) is True


class TestIsSourceNameComponent:
    @pytest.mark.parametrize("name", ["ok", "reddit", "has space", "my-data.v2", "2024data", ".hidden", "-x", "$x", "caf\u00e9", "a" * 255, "\u00e9" * 127])
    def test_accepts_one_path_component(self, name):
        """Bug class: a check narrowed to a pattern, which would refuse a folder name that exists today (a space, a dash, a leading dot or digit, a non-ASCII letter), or an off-by-one that refuses a name of exactly 255 bytes."""
        assert names.is_source_name_component(name) is True

    @pytest.mark.parametrize("name", MALFORMED + ["/abs/path", "a/", "ok\r", "a\x00b", "a\x85b", "a\u2028b", "a\u2029b", "a" * 256, "a" * 300, "\u00e9" * 128, None, 3, b"ok", ["ok"]])
    def test_refuses_anything_else(self, name):
        """Bug class: a single-component check with a gap: the empty name, `.` and `..`, either separator, an absolute path, a line break or control character, a name longer than 255 bytes (counted in bytes, so 128 two-byte letters are too long), or a value that is not a string."""
        assert names.is_source_name_component(name) is False


class TestExistingSourceDir:
    def test_returns_the_folder_of_an_existing_source(self, source_tree):
        """Bug class: a guard that refuses the legitimate case, or one that looks somewhere other than the config/sources/ folder in use (the folder is read when the lookup runs, not when the module is imported)."""
        sources = source_tree / "config" / "sources"

        assert names.existing_source_dir("ok") == sources / "ok"
        assert names.existing_source_dir("bare") == sources / "bare"

    def test_accepts_a_symlinked_source_folder(self, source_tree):
        """Bug class: a containment check built on resolved paths, which would turn "is a link" into "not found". The lookup accepts a symlinked folder and returns the link itself; the type that must not follow a link refuses it on its own."""
        link = source_tree / "config" / "sources" / "linked"

        found = names.existing_source_dir("linked")

        assert found == link
        assert found.is_symlink()

    @pytest.mark.parametrize("name", MALFORMED)
    def test_refuses_a_name_that_is_not_one_component_even_though_a_folder_is_there(self, source_tree, name):
        """Bug class: path traversal through the join. Each of these names addresses a real folder when joined to config/sources/ (the folder itself, its parent, a sibling of sources/ that holds a platform.yaml, a nested folder, a folder named with a backslash or a trailing line break), so only the component check can refuse it."""
        _with_odd_folders(source_tree)
        assert (source_tree / "config" / "sources" / name).is_dir()

        with pytest.raises(ValueError, match="single folder name"):
            names.existing_source_dir(name)

    @pytest.mark.parametrize("name", ["nosuch", "a" * 300, None, 3])
    def test_refuses_a_name_whose_folder_does_not_exist(self, source_tree, name):
        """Bug class: a lookup that accepts a well-formed name without checking the folder, or that crashes with something other than ValueError on a name no file system can hold or on a value that is not a string."""
        with pytest.raises(ValueError):
            names.existing_source_dir(name)

    def test_refuses_a_file_in_place_of_a_folder(self, source_tree):
        """Bug class: `exists()` in place of `is_dir()`, handing a plain file to code that lists or deletes a folder."""
        assert (source_tree / "config" / "sources" / "plainfile").is_file()

        with pytest.raises(ValueError, match="not found") as excinfo:
            names.existing_source_dir("plainfile")

        assert "plainfile" not in str(excinfo.value)
