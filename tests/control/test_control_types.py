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
    (types.RunId(), "r20261121T101500-0a1b2c"),
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
    (types.RunId(), "r20261121T101500-0a1b2c"),
    (types.RunId(), "r00000000T000000-000000"),
    (types.RunId(), "live-0123456789ab"),
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
    (types.RunId(), ""),
    (types.RunId(), ".."),
    (types.RunId(), "r20261121T101500-0a1b2c/.."),
    (types.RunId(), "../r20261121T101500-0a1b2c"),
    (types.RunId(), "r20261121T101500-0a1b2c\n"),
    (types.RunId(), "live-0123456789ab\n"),
    (types.RunId(), "r20261121T101500-0A1B2C"),
    (types.RunId(), "live-0123456789AB"),
    (types.RunId(), "R20261121T101500-0a1b2c"),
    (types.RunId(), "r20261121t101500-0a1b2c"),
    (types.RunId(), "r2026112T101500-0a1b2c"),
    (types.RunId(), "r20261121T101500-0a1b2"),
    (types.RunId(), "r20261121T101500-0a1b2c0"),
    (types.RunId(), "r20261121T1015000a1b2c"),
    (types.RunId(), "r\u0662\u0660261121T101500-0a1b2c"),
    (types.RunId(), "live-0123456789a"),
    (types.RunId(), "live-0123456789abc"),
    (types.RunId(), "live-0123456789ag"),
    (types.RunId(), "live-"),
    (types.RunId(), "live-0123456789ab r20261121T101500-0a1b2c"),
    (types.RunId(), b"live-0123456789ab"),
    (types.RunId(), ["live-0123456789ab"]),
    (types.RunId(), 20261121),
    (types.RunId(), None),
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
        (types.RunId(), "r20261121T101500-0a1b2c\n"),
        (types.RunId(), "live-0123456789ab\n"),
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
            "RunId": (True, False),
            "Secret": (True, True),
            "HfDatasetId": (True, False),
            "Path": (True, False),
            "Port": (False, False),
            "Text": (False, False),
            "ListOf": (False, False),
            "MapOf": (False, False),
            "Record": (False, False),
        }


class TestNoValueInReason:
    @pytest.mark.parametrize("kind", [
        types.Enum(("a", "b")),
        types.Name(),
        types.FileFilter(),
        types.ConfiguredSource(),
        types.ConfiguredSource(require_config=False),
        types.RunId(),
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


# ── the wide types and the containers ───────────────────────────────────────

# A marker that must never show in a refusal: the refused values below carry it.
MARK = "ZQX9marker"
TARGET = types.Record({"name": types.Field(types.Name(), required=True), "database": types.Field(types.Enum(("postgres", "starrocks")), required=True), "timeout_hours": types.Field(types.IntRange(1, 72))})
WITH_SECRET = types.Record({"user": types.Field(types.Name(), required=True), "password": types.Field(types.Secret(max_len=16))})


class TestPath:
    @pytest.mark.parametrize("value, expected", [
        ("/mnt/bulk/dumps", "/mnt/bulk/dumps"),
        ("/mnt/bulk/dumps/", "/mnt/bulk/dumps"),
        ("data/./dumps//reddit/../hf", "data/dumps/hf"),
        ("data", "data"),
        ("../elsewhere", "../elsewhere"),
        ("/mnt/caf\u00e9 space", "/mnt/caf\u00e9 space"),
    ])
    def test_accepts_and_normalizes_a_path_without_looking_at_the_disk(self, value, expected):
        """Bug class: two spellings of one folder taken as two (a duplicate mount, a folder check that a `..` walks around), or a relative path refused when the deployment's relative paths are what setup writes."""
        assert types.Path().validate(value) == expected

    @pytest.mark.parametrize("value", ["", None, 5, ["/x"], "/x\n", "/x\ty", "/x\x00y", "/x\ry", "/x\u2028y", "/x\x85y", "/x\ud800"])
    def test_refuses_what_no_path_setting_can_hold(self, value):
        """Bug class: a path with a line break that writes a second setting into .env or docker-compose.override.yml, a NUL that cuts it at the system call, an empty path read as the working directory."""
        with pytest.raises(ValueError):
            types.Path().validate(value)

    def test_the_byte_bound_counts_utf8_bytes(self):
        """Bug class: a bound counted in characters, which lets a path of multi-byte characters past the size the type promises."""
        assert types.Path(max_bytes=4).validate("\u00e9\u00e9") == "\u00e9\u00e9"
        with pytest.raises(ValueError, match="at most 4 bytes"):
            types.Path(max_bytes=4).validate("\u00e9\u00e9a")
        assert types.Path().validate("/" + "a" * 4095) == "/" + "a" * 4095
        with pytest.raises(ValueError):
            types.Path().validate("/" + "a" * 4096)


class TestPort:
    @pytest.mark.parametrize("value", [1024, 8080, 65535])
    def test_accepts_an_unprivileged_port(self, value):
        assert types.Port().validate(value) == value

    @pytest.mark.parametrize("value", [1023, 0, -1, 65536, True, "8080", 8080.0, None])
    def test_refuses_a_privileged_or_impossible_port_and_every_other_json_type(self, value):
        """Bug class: a port below 1024 (needs root on the host), past 65535, or a coerced string or float that the compose file would read differently."""
        with pytest.raises(ValueError):
            types.Port().validate(value)


class TestText:
    def test_accepts_lines_and_tabs_when_multiline(self):
        """Bug class: PGTune's output (several lines, maybe tabs) refused, or an empty paste refused although it means "none"."""
        text = "# PGTune\nshared_buffers = 4GB\n\twork_mem = 16MB\n"
        assert types.Text(1024).validate(text) == text
        assert types.Text(1024).validate("") == ""

    @pytest.mark.parametrize("value", ["a\rb", "a\r\nb", "a\x00b", "a\x1bb", "a\u2028b", "a\x85b", "a\x0bb", "a\ud800", 5, None, ["a"]])
    def test_refuses_other_control_characters(self, value):
        """Bug class: a carriage return, an escape sequence or a separator inside text that is later written into a configuration file or shown in a terminal."""
        with pytest.raises(ValueError):
            types.Text(1024).validate(value)

    def test_one_line_refuses_a_line_break_and_the_bound_counts_bytes(self):
        """Bug class: a one-line text that writes a second line, or a bound counted in characters."""
        with pytest.raises(ValueError, match="line break"):
            types.Text(64, multiline=False).validate("a\nb")
        assert types.Text(64, multiline=False).validate("a\tb") == "a\tb"
        assert types.Text(4).validate("\u00e9\u00e9") == "\u00e9\u00e9"
        with pytest.raises(ValueError, match="at most 4 bytes"):
            types.Text(4).validate("\u00e9\u00e9a")


class TestContainers:
    def test_a_list_keeps_its_order_and_checks_each_item(self):
        """Bug class: a list whose items skip validation (a container that trusts what it holds), or whose order is lost (the scheduler's targets are ordered)."""
        kind = types.ListOf(types.Name(), 4)
        assert kind.validate(["b", "a", "c"]) == ["b", "a", "c"]
        assert kind.validate([]) == []
        with pytest.raises(ValueError, match="item 2 must start"):
            kind.validate(["a", "../x"])

    def test_a_unique_list_compares_values_after_validation(self):
        """Bug class: two spellings of one folder accepted as two items of a list that must not repeat."""
        kind = types.ListOf(types.Path(), 4, unique=True)
        assert kind.validate(["/a", "/b"]) == ["/a", "/b"]
        with pytest.raises(ValueError, match="item 2 repeats"):
            kind.validate(["/a/b", "/a/./b"])

    def test_a_list_of_records(self):
        """Bug class: a per-item object that takes a key it does not list, misses a required one, or reports its error without saying where."""
        kind = types.ListOf(TARGET, 8)
        assert kind.validate([{"name": "warehouse", "database": "starrocks", "timeout_hours": 4}, {"database": "postgres", "name": "pg"}]) == [{"name": "warehouse", "database": "starrocks", "timeout_hours": 4}, {"name": "pg", "database": "postgres"}]
        for value, reason in [
            ([{"name": "w", "database": "starrocks", "extra": 1}], "item 1 key 'extra' is not accepted"),
            ([{"name": "w", "database": "starrocks", "Extra Key": 1}], "item 1 has a key that is not accepted"),
            ([{"name": "w"}], "item 1 key 'database' is required"),
            ([{"name": "w", "database": "starrocks", "timeout_hours": None}], "item 1 key 'timeout_hours' must be a whole number"),
            ([{"name": "w", "database": "starrocks"}, {"name": "x", "database": "mongo"}], "item 2 key 'database' must be one of"),
            ([["name", "w"]], "item 1 must be an object"),
        ]:
            with pytest.raises(ValueError) as exc:
                kind.validate(value)
            assert str(exc.value).startswith(reason), value

    def test_a_map_checks_every_key_before_any_value(self):
        """Bug class: a map key that is a path or holds a line break (a data type becomes a folder name), or a value checked before its key so that a refusal quotes a hostile key."""
        kind = types.MapOf(types.Name(), types.ListOf(types.Name(), 8), 4)
        assert kind.validate({"submissions": ["id", "title"], "comments": []}) == {"submissions": ["id", "title"], "comments": []}
        with pytest.raises(ValueError) as exc:
            kind.validate({"submissions": [MARK + "\n"], "../" + MARK: []})
        assert str(exc.value).startswith("has a key that") and MARK not in str(exc.value)
        with pytest.raises(ValueError, match="key 'submissions' item 1"):
            kind.validate({"submissions": ["a b"]})
        with pytest.raises(ValueError, match="must be an object"):
            kind.validate([["submissions", []]])

    @pytest.mark.parametrize("kind, value", [
        (types.ListOf(types.Name(), 3), ["a", "b", "c", "d"]),
        (types.MapOf(types.Name(), types.Bool(), 2), {"a": True, "b": True, "c": True}),
        (types.ListOf(types.ListOf(types.Name(), 2), 2), [["a"], ["a", "b", "c"]]),
    ])
    def test_too_many_items_are_refused_before_any_is_looked_at(self, kind, value):
        """Bug class: a container that validates (and copies) every item of a huge list before it counts them."""
        with pytest.raises(ValueError, match="at most"):
            kind.validate(value)

    def test_the_size_is_counted_before_the_items(self, monkeypatch):
        """Bug class: as above, counted by steps: with one item too many, no item is validated."""
        seen = []
        kind = types.ListOf(types.Name(), 2)
        monkeypatch.setattr(types.Name, "validate", lambda self, value: seen.append(value) or value)
        with pytest.raises(ValueError):
            kind.validate(["a", "b", "c"])
        assert seen == []

    def test_nested_secrets_are_wrapped_and_never_echoed(self):
        """Bug class: a secret inside a list or a record returned as plain text (it then shows in a repr, a log or an audit line), or a refusal of one that quotes it."""
        kind = types.ListOf(WITH_SECRET, 4)
        clean = kind.validate([{"user": "ro", "password": "s3cret-" + MARK[:6]}])
        assert isinstance(clean[0]["password"], types.SecretValue) and MARK[:6] not in repr(clean)
        assert types.reveal(clean) == [{"user": "ro", "password": "s3cret-" + MARK[:6]}]
        assert [secret.reveal() for secret in types.secrets_in({"a": clean, "b": types.SecretValue("x")})] == ["s3cret-" + MARK[:6], "x"]
        mapped = types.MapOf(types.Name(), types.Secret(), 2).validate({"hf": "tok"})
        assert isinstance(mapped["hf"], types.SecretValue)
        for refused in ([{"user": "ro", "password": MARK * 3}], [{"user": "ro", "password": MARK + "\n"}], [{"user": MARK + " x", "password": MARK}]):
            with pytest.raises(ValueError) as exc:
                kind.validate(refused)
            assert MARK not in str(exc.value) and MARK not in repr(exc.value)

    @pytest.mark.parametrize("build", [
        lambda: types.ListOf(types.Name(), 0),
        lambda: types.ListOf(types.Name(), 1025),
        lambda: types.ListOf(types.Name(), True),
        lambda: types.ListOf("name", 4),
        lambda: types.ListOf(types.Secret(), 4, unique=True),
        lambda: types.ListOf(types.ListOf(types.Name(), 2), 2, unique=True),
        lambda: types.ListOf(types.ListOf(types.Name(), 1000), 1000),
        lambda: types.ListOf(types.ListOf(types.ListOf(types.ListOf(types.ListOf(types.Name(), 1), 1), 1), 1), 1),
        lambda: types.MapOf(types.Path(), types.Name(), 4),
        lambda: types.MapOf(types.Secret(), types.Name(), 4),
        lambda: types.MapOf(types.Name(), object(), 4),
        lambda: types.Record({}),
        lambda: types.Record({"Name": types.Field(types.Name())}),
        lambda: types.Record({"name": types.Name()}),
        lambda: types.Path(max_bytes=0),
        lambda: types.Path(max_bytes=4097),
        lambda: types.Text(0),
        lambda: types.Text(65537),
        lambda: types.Text(10, multiline=1),
    ])
    def test_a_malformed_container_or_wide_type_is_refused_when_it_is_built(self, build):
        """Bug class: a registry typo that declares an unbounded shape (a container of a million items, nesting deeper than any form), a map keyed by a path or a secret (a key is quoted in errors), or a record whose keys cannot be quoted safely."""
        with pytest.raises((TypeError, ValueError)):
            build()

    def test_a_record_cannot_be_widened_after_it_is_built(self):
        """Bug class: a Record shared by several verbs whose list of keys is changed at run time."""
        fields = {"name": types.Field(types.Name())}
        kind = types.Record(fields)
        fields["extra"] = types.Field(types.Name())
        with pytest.raises(ValueError, match="'extra' is not accepted"):
            kind.validate({"extra": "x"})
        with pytest.raises(TypeError):
            kind.fields["extra"] = types.Field(types.Name())


class TestNestedRequests:
    """`validate_request` with container fields, through a verb made for the test (no registered verb uses a container yet)."""

    @pytest.fixture
    def nested_verb(self, monkeypatch):
        from social_data_pipeline.control import verbs

        verb = verbs.Verb(name="test.nested", cls="step_up", cli=("test",), socket_enabled=True, socket_inputs={
            "targets": verbs.Field(types.ListOf(TARGET, 4), required=True),
            "users": verbs.Field(types.ListOf(WITH_SECRET, 2)),
            "folder": verbs.Field(types.Path()),
        })
        monkeypatch.setattr(verbs, "VERBS", {**verbs.VERBS, "test.nested": verb})
        return verbs

    def test_a_nested_refusal_names_the_field_and_the_place(self, nested_verb):
        """Bug class: an error inside a container that names no field (the page cannot say which input was refused) or that quotes the value."""
        from social_data_pipeline.control import errors

        with pytest.raises(errors.InputError) as exc:
            nested_verb.validate_request("test.nested", {"targets": [{"name": "w", "database": "starrocks"}, {"name": "w", "database": "starrocks", "zz": MARK}]})
        assert exc.value.field == "targets"
        assert str(exc.value) == "test.nested: field 'targets' item 2 key 'zz' is not accepted."
        with pytest.raises(errors.InputError) as exc:
            nested_verb.validate_request("test.nested", {"targets": [], "users": [{"user": "ro", "password": MARK * 2}]})
        assert exc.value.field == "users" and MARK not in str(exc.value)

    def test_nested_secrets_come_back_wrapped(self, nested_verb):
        """Bug class: a secret in a container returned unwrapped by the boundary, so the broker's redaction and the audit's masking (both keyed on SecretValue) miss it."""
        from social_data_pipeline.control import audit

        validated = nested_verb.validate_request("test.nested", {"targets": [], "users": [{"user": "ro", "password": "pw-" + MARK[:4]}], "folder": "data/./x"})
        assert validated["folder"] == "data/x"
        assert isinstance(validated["users"][0]["password"], types.SecretValue)
        shown = audit.fields_for_audit(nested_verb.VERBS["test.nested"], validated)
        assert MARK[:4] not in repr(shown) and shown["users"] == [{"user": "ro", "password": audit.SECRET_MARK}]
