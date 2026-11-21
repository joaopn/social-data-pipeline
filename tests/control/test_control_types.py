"""Tests for the closed set of input types a verb may accept over the socket.

Each type refuses instead of coercing, checks a string with `re.fullmatch` and with the shared line-break check, and gives a reason that never holds the refused value. The types are reached through the module (`types.<Name>`), so each test fails on its own when one is missing.
"""

from __future__ import annotations

import dataclasses

import pytest

from social_data_pipeline.control import names, types

# One value each string type accepts ("ok" is a source in the `source_tree` fixture).
VALID_STRING = [
    (types.Enum(("a", "b")), "a"),
    (types.Name(), "toxic_roberta"),
    (types.FileFilter(), "RS_2024-*"),
    (types.ConfiguredSource(), "ok"),
    (types.ConfiguredSource(require_config=False), "ok"),
    (types.Secret(), "correct horse"),
]

ACCEPTED = [
    (types.Bool(), True),
    (types.Bool(), False),
    (types.IntRange(1, 8), 1),
    (types.IntRange(1, 8), 8),
    (types.IntRange(-2, 2), 0),
    (types.Enum(("a", "b")), "b"),
    (types.Enum(["postgres-mcp"]), "postgres-mcp"),
    (types.Name(), "a"),
    (types.Name(), "toxic_roberta"),
    (types.Name(), "v1.2-x"),
    (types.Name(), "a" * 64),
    (types.FileFilter(), "RS_2024-*"),
    (types.FileFilter(), "*2024*"),
    (types.FileFilter(), "[!a]*"),
    (types.FileFilter(), "RC_2024-0?"),
    (types.FileFilter(), "a.b"),
    (types.FileFilter(), "a" * 128),
    (types.Secret(), "p"),
    (types.Secret(), "correct horse"),
    (types.Secret(), "../etc/passwd"),
    (types.Secret(), "x" * 1024),
    (types.Secret(max_len=4), "abcd"),
]

REFUSED = [
    (types.Bool(), 1),
    (types.Bool(), 0),
    (types.Bool(), "true"),
    (types.Bool(), None),
    (types.IntRange(1, 8), True),
    (types.IntRange(0, 8), False),
    (types.IntRange(1, 8), 0),
    (types.IntRange(1, 8), 9),
    (types.IntRange(1, 8), 1.0),
    (types.IntRange(1, 8), "3"),
    (types.IntRange(1, 8), None),
    (types.Enum(("a", "b")), "c"),
    (types.Enum(("a", "b")), "A"),
    (types.Enum(("a", "b")), "a\n"),
    (types.Enum(("a", "b")), ""),
    (types.Enum(("a", "b")), ["a"]),
    (types.Enum(("1", "2")), 1),
    (types.Enum(("a", "b")), None),
    (types.Name(), "."),
    (types.Name(), ".."),
    (types.Name(), ""),
    (types.Name(), ".hidden"),
    (types.Name(), "-x"),
    (types.Name(), "a b"),
    (types.Name(), "a/b"),
    (types.Name(), "a\\b"),
    (types.Name(), "café"),
    (types.Name(), "a" * 65),
    (types.Name(), "ok\n"),
    (types.Name(), "ok\ud800"),
    (types.Name(), 7),
    (types.Name(), None),
    (types.FileFilter(), ".."),
    (types.FileFilter(), "."),
    (types.FileFilter(), ""),
    (types.FileFilter(), "a" * 129),
    (types.FileFilter(), "a b"),
    (types.FileFilter(), "a/b"),
    (types.FileFilter(), "{a,b}"),
    (types.FileFilter(), "RS_*\n"),
    (types.FileFilter(), None),
    (types.Secret(), ""),
    (types.Secret(), "a\nb"),
    (types.Secret(), "a\rb"),
    (types.Secret(), "a\x00b"),
    (types.Secret(), "a\u2028b"),
    (types.Secret(), "a\x85b"),
    (types.Secret(), "a\ud800b"),
    (types.Secret(), "x" * 1025),
    (types.Secret(max_len=4), "abcde"),
    (types.Secret(), b"pw"),
    (types.Secret(), 5),
    (types.Secret(), None),
]


def _case_id(case):
    kind, value = case
    shown = repr(value)
    return f"{kind!r}:{shown if len(shown) <= 24 else shown[:21] + '...'}"


class TestAcceptAndRefuse:
    @pytest.mark.parametrize("case", ACCEPTED, ids=_case_id)
    def test_accepts_and_returns_the_value_unchanged(self, case):
        """Bug class: a rule so strict it refuses what the CLI help itself suggests (`RS_2024-*`, `*2024*`), or a type that hands back something other than what was sent."""
        kind, value = case

        result = kind.validate(value)

        assert result == value
        assert type(result) is type(value)

    @pytest.mark.parametrize("case", REFUSED, ids=_case_id)
    def test_refuses_instead_of_coercing(self, case):
        """Bug class: coercion and loose matching at a trust boundary: 1 or "true" passing as a flag, True passing as the number 1, a name that is only dots or holds a separator, a length or character-set gap, a trailing newline, a non-string turned into text."""
        kind, value = case

        with pytest.raises(ValueError):
            kind.validate(value)


class TestConfiguredSource:
    @pytest.mark.parametrize("require_config", [True, False])
    def test_an_unknown_well_formed_name_is_not_echoed(self, source_tree, require_config):
        """Bug class: the folder lookup's own error reaching the caller. A well-formed name that names no source passes every shape check and is refused only by the lookup, so this is the one path on which a message could carry the name that was sent."""
        with pytest.raises(ValueError) as excinfo:
            types.ConfiguredSource(require_config=require_config).validate("ZQX9marker")

        assert "ZQX9marker" not in str(excinfo.value)
        assert "ZQX9marker" not in repr(excinfo.value)

    def test_accepts_a_real_source_folder(self, source_tree):
        """Bug class: a guard that refuses the legitimate case, here a plain configured source."""
        assert types.ConfiguredSource().validate("ok") == "ok"

    @pytest.mark.parametrize("name", ["nosuch", "../x", "..", ".", "", "a/b", "a\\b", "ok\n", "ok\ud800", "a" * 300, "plainfile", "linked", None, 3])
    def test_refuses_anything_that_is_not_one_real_source_folder(self, source_tree, name):
        """Bug class: path traversal and link following from a request: `../x` (which holds a platform.yaml), a nested path, a plain file, an unknown name, or a symlinked folder that leads outside config/sources/; and a name the file system cannot look up (longer than a file name may be, or not encodable) surfacing as a crash that quotes it instead of a refusal."""
        with pytest.raises(ValueError):
            types.ConfiguredSource().validate(name)

    @pytest.mark.parametrize("kind", [types.ConfiguredSource(), types.ConfiguredSource(require_config=False)], ids=repr)
    @pytest.mark.parametrize("name", ["has space", "$x", "-x", "café", ".hidden", "a" * 65])
    def test_refuses_an_existing_source_whose_name_is_not_a_plain_name(self, source_tree, monkeypatch, kind, name):
        """Bug class: the socket accepting whatever folder name exists, so a source named with a space, `$` or a leading `-` reaches a command line, a log or a page as an option or shell syntax. The folder is real and holds a platform.yaml, and the folder lookup accepts it; over the socket the name is refused for its shape, before the file system is asked."""
        folder = source_tree / "config" / "sources" / name
        folder.mkdir()
        (folder / "platform.yaml").write_text("data_types: [posts]\n")
        assert names.existing_source_dir(name) == folder
        lookups = []
        real_lookup = names.existing_source_dir
        monkeypatch.setattr(names, "existing_source_dir", lambda value: lookups.append(value) or real_lookup(value))

        with pytest.raises(ValueError, match="must be the name of a configured source"):
            kind.validate(name)

        assert lookups == []

    def test_accepts_an_existing_source_with_dots_and_dashes_in_its_name(self, source_tree):
        """Bug class: the name-shape rule narrowed to a lower-case identifier, which would lock out over the socket an existing source whose name holds `-` or `.` or starts with a digit (allowed for the Name type, and the CLI sets no rule for the name of a source)."""
        for name in ("my-data.v2", "2024data", "a" * 64):
            folder = source_tree / "config" / "sources" / name
            folder.mkdir()
            (folder / "platform.yaml").write_text("data_types: [posts]\n")

            assert types.ConfiguredSource().validate(name) == name
            assert types.ConfiguredSource(require_config=False).validate(name) == name

    def test_shares_its_name_shape_with_the_name_type(self, source_tree, monkeypatch):
        """Bug class: a second copy of the name pattern drifting from the first, so the two types disagree about what a plain name is. With the shared pattern replaced, both types must follow it."""
        monkeypatch.setattr(types, "NAME_PATTERN", r"[a-z]{3}")

        with pytest.raises(ValueError):
            types.Name().validate("ok")
        with pytest.raises(ValueError, match="must be the name of a configured source"):
            types.ConfiguredSource().validate("ok")

    def test_symlinked_folder_is_refused_with_or_without_the_config_rule(self, source_tree):
        """Bug class: the symlink check living only on the require_config path, so `source.remove` (which passes require_config=False) follows a link the folder lookup accepts."""
        assert names.existing_source_dir("linked").is_dir()

        with pytest.raises(ValueError, match="symlink"):
            types.ConfiguredSource(require_config=False).validate("linked")

    def test_folder_without_platform_yaml_depends_on_require_config(self, source_tree):
        """Bug class: the two modes swapped or merged: a run started on a folder that has no config, or a half-written source that can no longer be removed."""
        with pytest.raises(ValueError, match="platform.yaml"):
            types.ConfiguredSource().validate("bare")

        assert types.ConfiguredSource(require_config=False).validate("bare") == "bare"


class TestStringLayers:
    @pytest.mark.parametrize("case", VALID_STRING, ids=_case_id)
    def test_every_string_type_runs_the_shared_line_break_check(self, source_tree, monkeypatch, case):
        """Bug class: a string type that relies on its pattern alone and skips the shared check, so the day the pattern is loosened a line break gets through. With the check forced to say yes, a value the type otherwise accepts must be refused for that reason."""
        kind, value = case
        assert kind.validate(value) == value

        monkeypatch.setattr(names, "has_line_break_or_control", lambda text: True)

        with pytest.raises(ValueError, match="line break or control character"):
            kind.validate(value)

    @pytest.mark.parametrize("case", [
        (types.Enum(("a", "b")), "a\n"),
        (types.Name(), "ok\n"),
        (types.FileFilter(), "RS_*\n"),
    ], ids=_case_id)
    def test_patterns_refuse_a_trailing_newline_on_their_own(self, monkeypatch, case):
        """Bug class: `re.match` with a trailing `$`, which accepts a value that ends in a newline. With the shared check forced to say no, the pattern alone must still refuse it."""
        kind, value = case
        monkeypatch.setattr(names, "has_line_break_or_control", lambda text: False)

        with pytest.raises(ValueError):
            kind.validate(value)

    def test_string_and_secret_flags(self):
        """Bug class: a wrong flag that silently narrows a safety net: a secret not marked secret is returned unwrapped and shows in a repr, a string type not marked string is skipped by the boundary sweep over string inputs."""
        flags = {kind.__name__: (kind.is_string, kind.is_secret) for kind in types.INPUT_TYPES}

        assert flags == {
            "Bool": (False, False),
            "IntRange": (False, False),
            "Enum": (True, False),
            "Name": (True, False),
            "FileFilter": (True, False),
            "ConfiguredSource": (True, False),
            "Secret": (True, True),
        }


class TestNoValueInReason:
    @pytest.mark.parametrize("kind", [
        types.Enum(("a", "b")),
        types.Name(),
        types.FileFilter(),
        types.ConfiguredSource(),
        types.ConfiguredSource(require_config=False),
        types.Secret(),
        types.Secret(max_len=8),
    ], ids=repr)
    @pytest.mark.parametrize("value", ["ZQX9marker\n", "ZQX9marker/..", "ZQX9marker x", "ZQX9marker\ud800", "ZQX9marker" * 200])
    def test_reason_never_quotes_the_refused_value(self, source_tree, kind, value):
        """Bug class: a refusal that echoes what was sent ("'<value>' not found"), which puts a mistyped password or attacker-written text into a log or a page."""
        try:
            kind.validate(value)
        except ValueError as exc:
            assert "ZQX9marker" not in str(exc)
            assert "ZQX9marker" not in repr(exc)
        else:
            # Only a secret may accept one of these values (it may look like a path or hold a space).
            assert kind.is_secret


class TestImmutability:
    @pytest.mark.parametrize("kind, attribute", [
        (types.IntRange(1, 8), "hi"),
        (types.Enum(("a", "b")), "choices"),
        (types.ConfiguredSource(), "require_config"),
        (types.Secret(), "max_len"),
    ], ids=repr)
    def test_a_type_cannot_be_widened_after_it_is_built(self, kind, attribute):
        """Bug class: one type object shared by several verbs being changed at run time, which widens every verb that uses it."""
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(kind, attribute, None)

    def test_enum_keeps_its_own_tuple_of_choices(self):
        """Bug class: an Enum built from a list that the caller later appends to, adding a choice nobody reviewed."""
        source = ["a", "b"]
        kind = types.Enum(source)
        source.append("c")

        assert kind.choices == ("a", "b")
        with pytest.raises(ValueError):
            kind.validate("c")

    @pytest.mark.parametrize("build", [
        lambda: types.Enum("ab"),
        lambda: types.Enum(()),
        lambda: types.Enum(("a", "a")),
        lambda: types.Enum(("a", "b\n")),
        lambda: types.Enum(("a", 1)),
        lambda: types.IntRange(8, 1),
        lambda: types.IntRange(True, 8),
        lambda: types.Secret(max_len=0),
    ])
    def test_a_malformed_type_is_refused_when_it_is_built(self, build):
        """Bug class: a registry typo that produces a type which accepts the wrong set: a bare string read as single-character choices, an empty or inverted range, a choice that itself holds a line break."""
        with pytest.raises((TypeError, ValueError)):
            build()


class TestSecretValue:
    def test_never_shows_its_value(self):
        """Bug class: a secret printed by accident through repr(), str(), an f-string or a container's repr."""
        wrapped = types.SecretValue("hunter2-ZQX")

        for shown in (repr(wrapped), str(wrapped), f"{wrapped}", f"{wrapped!r}", repr({"admin_password": wrapped}), repr([wrapped])):
            assert "hunter2-ZQX" not in shown
        assert repr(wrapped) == "<secret>"
        assert wrapped.reveal() == "hunter2-ZQX"
