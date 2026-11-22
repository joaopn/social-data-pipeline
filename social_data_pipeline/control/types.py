"""The closed set of input types a verb may accept over the socket.

Each type is a small immutable object whose `validate(value)` returns the accepted value or raises ValueError with a reason that never contains the value. A value is refused, never coerced (two exceptions, both normalizations of text: `Path` returns its value through `os.path.normpath`, and nothing else changes a value).

The scalar types are `Bool`, `IntRange`, `Enum`, `Name`, `FileFilter`, `ConfiguredSource`, `RunId`, `Secret`, and the three wide ones the setup verbs need (decided by the maintainer: any path and any port with the management password): `Path` (a host path as text, never looked up here), `Port` (1024 to 65535; whether another service uses it is the action's check) and `Text` (free text, such as PGTune's output). A wide type is reachable only through the inputs listed in the registry test's allow-list of wide inputs.

The containers are `ListOf`, `MapOf` and `Record`. They refuse unknown keys at every level, check their size before they look at any item, and wrap every nested secret in `SecretValue` themselves; `reveal` and `secrets_in` walk a validated value for the broker. Each type has a `capacity` (the most values it can hold, counted leaf by leaf) and a `depth`; a container whose capacity is over ITEMS_MAX or whose depth is over DEPTH_MAX is refused when it is built, so no declared shape can cost more than that, whatever a request holds (a request frame is at most 64 KiB, `control.REQUEST_MAX_BYTES`).
"""

import os
import re
import unicodedata
from dataclasses import dataclass
from types import MappingProxyType

from social_data_pipeline.control import names


def _checked_string(value):
    """Return value when it is a string without a line break or control character. Raises ValueError otherwise."""
    if not isinstance(value, str):
        raise ValueError("must be a string")
    if names.has_line_break_or_control(value):
        raise ValueError("must not contain a line break or control character")
    # JSON can carry a lone surrogate, which no file, environment variable or log can hold.
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("must be well-formed text") from None
    return value


def _is_int(value):
    """True for an int that is not a bool (bool is a subclass of int, and True must not pass as 1)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _only_dots(value):
    return value.strip(".") == ""


# The most values a declared type may hold in all (leaves counted, a map's keys included), and the most containers nested in one another.
ITEMS_MAX = 10000
DEPTH_MAX = 4
# The bounds of a container's own size and of a text's bytes, so that a typo in the registry cannot declare a huge one.
CONTAINER_ITEMS_MAX = 1024
PATH_MAX_BYTES = 4096
TEXT_MAX_BYTES = 65536
PORT_MIN = 1024
PORT_MAX = 65535
# The names a Record's keys may have: lower-case identifiers, which are also safe to quote in an error.
RECORD_KEY_PATTERN = r"[a-z][a-z0-9_]{0,63}"


@dataclass(frozen=True)
class Bool:
    """A JSON boolean. 0, 1 and "true" are refused: a coerced flag is how a stray string switches something on."""

    is_string = False
    is_secret = False
    capacity = 1
    depth = 0

    def validate(self, value):
        if not isinstance(value, bool):
            raise ValueError("must be true or false")
        return value


@dataclass(frozen=True)
class IntRange:
    """A whole number from lo to hi, both included."""

    lo: int
    hi: int

    is_string = False
    is_secret = False
    capacity = 1
    depth = 0

    def __post_init__(self):
        if not _is_int(self.lo) or not _is_int(self.hi):
            raise TypeError("IntRange bounds must be whole numbers")
        if self.lo > self.hi:
            raise ValueError("IntRange needs lo <= hi")

    def validate(self, value):
        if not _is_int(value):
            raise ValueError("must be a whole number")
        if not self.lo <= value <= self.hi:
            raise ValueError(f"must be between {self.lo} and {self.hi}")
        return value


@dataclass(frozen=True)
class Enum:
    """A string equal to one of a fixed list of choices."""

    choices: tuple

    is_string = True
    is_secret = False
    capacity = 1
    depth = 0

    def __post_init__(self):
        # A bare string would be read as a list of its characters.
        if isinstance(self.choices, str):
            raise TypeError("Enum choices must be a list of strings, not one string")
        choices = tuple(self.choices)
        if not choices:
            raise ValueError("Enum needs at least one choice")
        for choice in choices:
            if not isinstance(choice, str) or not choice or names.has_line_break_or_control(choice):
                raise ValueError("every Enum choice must be a non-empty string without a line break or control character")
        if len(set(choices)) != len(choices):
            raise ValueError("Enum choices must be distinct")
        object.__setattr__(self, "choices", choices)

    def validate(self, value):
        value = _checked_string(value)
        if value not in self.choices:
            raise ValueError(f"must be one of: {', '.join(self.choices)}")
        return value


# Both patterns are checked with re.fullmatch: re.match with a trailing "$" would accept a value that ends in a newline.
NAME_PATTERN = r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}"
FILE_FILTER_PATTERN = r"[A-Za-z0-9_.*?\[\]!-]{1,128}"


def _is_plain_name(value):
    """True when value fully matches NAME_PATTERN and is not only dots. The dots check holds even if the pattern is later loosened to allow a leading dot."""
    return re.fullmatch(NAME_PATTERN, value) is not None and not _only_dots(value)


@dataclass(frozen=True)
class Name:
    """An identifier such as a classifier name: a letter or digit, then letters, digits, '_', '.' and '-', at most 64 characters."""

    is_string = True
    is_secret = False
    capacity = 1
    depth = 0

    def validate(self, value):
        value = _checked_string(value)
        if not _is_plain_name(value):
            raise ValueError("must start with a letter or digit and hold only letters, digits, '_', '.' and '-' (at most 64 characters)")
        return value


@dataclass(frozen=True)
class FileFilter:
    """An fnmatch glob on a file id, for example `RS_2024-*`. It has no '/' and is never only dots, so it cannot name a folder."""

    is_string = True
    is_secret = False
    capacity = 1
    depth = 0

    def validate(self, value):
        value = _checked_string(value)
        if not re.fullmatch(FILE_FILTER_PATTERN, value) or _only_dots(value):
            raise ValueError("must be a file-id pattern of 1 to 128 characters from letters, digits and _ . * ? [ ] ! -")
        return value


@dataclass(frozen=True)
class ConfiguredSource:
    """The name of a source that exists under config/sources/.

    Narrower than the CLI on purpose, in two ways. The name must have the shape the Name type accepts, checked before the file system is touched, so an existing source folder named with a space, '$' or a leading '-' stays usable in the terminal and is refused here. And the CLI follows a symlinked source folder, which over the socket would let a request act on a folder outside config/sources/. With require_config the folder must also hold platform.yaml (removal passes require_config=False, because the CLI removes a source folder with or without one).
    """

    require_config: bool = True

    is_string = True
    is_secret = False
    capacity = 1
    depth = 0

    def validate(self, value):
        value = _checked_string(value)
        if not _is_plain_name(value):
            raise ValueError("must be the name of a configured source")
        try:
            source_dir = names.existing_source_dir(value)
            is_link = source_dir.is_symlink()
            has_config = (source_dir / "platform.yaml").is_file()
        except (ValueError, OSError):
            # The helper's error is replaced so every refusal of the lookup reads the same; OSError is caught so that any file-system failure during the lookup reads as "not a configured source" too.
            raise ValueError("must be the name of a configured source") from None
        if is_link:
            raise ValueError("must be a source whose folder is not a symlink")
        if self.require_config and not has_config:
            raise ValueError("must be a source that has a platform.yaml")
        return value


# The id of a pipeline run: a recorded run (`r`, the UTC date, `T`, the UTC time, `-`, six hex digits) or a run started in a terminal (`live-` and the first twelve hex digits of its container id). A literal copy of the two patterns of the run store, which this package does not import; a test compares them.
RUN_ID_PATTERN = r"r[0-9]{8}T[0-9]{6}-[0-9a-f]{6}|live-[0-9a-f]{12}"


@dataclass(frozen=True)
class RunId:
    """The id of a pipeline run, in one of its two forms. Only the form is checked, nothing on disk and nothing in Docker: whether the run exists is the action's answer. The form has no '/', no '.' and no upper-case letter, so an id can be joined to the run state folder and handed to Docker as it is."""

    is_string = True
    is_secret = False
    capacity = 1
    depth = 0

    def validate(self, value):
        value = _checked_string(value)
        if not re.fullmatch(RUN_ID_PATTERN, value):
            raise ValueError("must be the id of a run")
        return value


@dataclass(frozen=True)
class Secret:
    """A password or token: 1 to max_len characters on one line. It may look like a path, since it never reaches one."""

    max_len: int = 1024

    is_string = True
    is_secret = True
    capacity = 1
    depth = 0

    def __post_init__(self):
        if not _is_int(self.max_len) or self.max_len < 1:
            raise ValueError("Secret needs max_len >= 1")

    def validate(self, value):
        value = _checked_string(value)
        if not 1 <= len(value) <= self.max_len:
            raise ValueError(f"must be 1 to {self.max_len} characters long")
        return value


class SecretValue:
    """Holds a validated secret so that it never shows in repr() or str() of a validated request. Read it with reveal()."""

    __slots__ = ("_value",)

    def __init__(self, value):
        self._value = value

    def reveal(self):
        return self._value

    def __repr__(self):
        return "<secret>"

    __str__ = __repr__


def _bounded_int(value, lo, hi, what):
    if not _is_int(value) or not lo <= value <= hi:
        raise ValueError(f"{what} must be a whole number from {lo} to {hi}")


def _utf8_length(value):
    """The length of a text in UTF-8 bytes. Raises ValueError for a lone surrogate, which no file, environment variable or log can hold."""
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise ValueError("must be well-formed text") from None


@dataclass(frozen=True)
class Path:
    """A host path as text: 1 to max_bytes bytes of UTF-8, no control character (a tab or a line break included) and no line or paragraph separator, absolute or relative (a relative path is to the deployment). Returned through `os.path.normpath`; nothing on disk is looked at, so whether it exists, where a link leads and whether it belongs to someone else are the action's checks, made when it acts."""

    max_bytes: int = PATH_MAX_BYTES

    is_string = True
    is_secret = False
    capacity = 1
    depth = 0

    def __post_init__(self):
        _bounded_int(self.max_bytes, 1, PATH_MAX_BYTES, "Path max_bytes")

    def validate(self, value):
        value = _checked_string(value)
        if not value:
            raise ValueError("must not be empty")
        if _utf8_length(value) > self.max_bytes:
            raise ValueError(f"must be at most {self.max_bytes} bytes long")
        return os.path.normpath(value)


@dataclass(frozen=True)
class Port:
    """A TCP port from 1024 to 65535 (below 1024 needs root on the host). Whether another service of the deployment already uses it is not a property of the value: the verb's check or its action decides that."""

    is_string = False
    is_secret = False
    capacity = 1
    depth = 0

    def validate(self, value):
        return IntRange(PORT_MIN, PORT_MAX).validate(value)


@dataclass(frozen=True)
class Text:
    """Free text of 0 to max_bytes bytes of UTF-8, such as a configuration pasted whole. A tab is allowed; with multiline the line break `\\n` is too (and only it: a `\\r` is refused, so a caller that takes text from a browser turns `\\r\\n` into `\\n` first). Every other control character and the line and paragraph separators are refused. Not a one-line string (`is_string` is false): a value may hold line breaks, so it must never be written into .env, a command line or a log line as it is."""

    max_bytes: int
    multiline: bool = True

    is_string = False
    is_secret = False
    capacity = 1
    depth = 0

    def __post_init__(self):
        _bounded_int(self.max_bytes, 1, TEXT_MAX_BYTES, "Text max_bytes")
        if not isinstance(self.multiline, bool):
            raise TypeError("Text multiline must be true or false")

    def validate(self, value):
        if not isinstance(value, str):
            raise ValueError("must be a string")
        allowed = ("\t", "\n") if self.multiline else ("\t",)
        for character in value:
            if character not in allowed and unicodedata.category(character) in ("Cc", "Zl", "Zp"):
                raise ValueError("must not contain a control character" + ("" if self.multiline else " or a line break"))
        if _utf8_length(value) > self.max_bytes:
            raise ValueError(f"must be at most {self.max_bytes} bytes long")
        return value


def _wrapped(kind, value):
    """The clean value of `value` under `kind`, a secret wrapped in SecretValue."""
    clean = kind.validate(value)
    return SecretValue(clean) if kind.is_secret else clean


def _check_inner(kind, what):
    if type(kind) not in INPUT_TYPES:
        raise TypeError(f"{what} needs an instance of one of the types in control.types")


def _check_shape(capacity, depth):
    if capacity > ITEMS_MAX:
        raise ValueError(f"a container may hold at most {ITEMS_MAX} values in all")
    if depth > DEPTH_MAX:
        raise ValueError(f"containers may be nested at most {DEPTH_MAX} deep")


@dataclass(frozen=True)
class ListOf:
    """A JSON list of 0 to max_items items, each validated by item_type, returned as a new list in the same order. With unique, an item equal to an earlier one (after validation, so two spellings of one path are equal) is refused; unique is allowed for scalar items that are not secrets only."""

    item_type: object
    max_items: int
    unique: bool = False

    is_string = False
    is_secret = False

    def __post_init__(self):
        _check_inner(self.item_type, "ListOf")
        _bounded_int(self.max_items, 1, CONTAINER_ITEMS_MAX, "ListOf max_items")
        if not isinstance(self.unique, bool):
            raise TypeError("ListOf unique must be true or false")
        if self.unique and (self.item_type.depth > 0 or self.item_type.is_secret):
            raise ValueError("ListOf unique needs items that are scalars and not secrets")
        _check_shape(self.capacity, self.depth)

    @property
    def capacity(self):
        return self.max_items * self.item_type.capacity

    @property
    def depth(self):
        return 1 + self.item_type.depth

    def validate(self, value):
        if not isinstance(value, list):
            raise ValueError("must be a list")
        if len(value) > self.max_items:
            raise ValueError(f"must hold at most {self.max_items} items")
        clean = []
        seen = set()
        for number, item in enumerate(value, start=1):
            try:
                checked = _wrapped(self.item_type, item)
            except ValueError as exc:
                raise ValueError(f"item {number} {exc}") from None
            if self.unique:
                if checked in seen:
                    raise ValueError(f"item {number} repeats an earlier item")
                seen.add(checked)
            clean.append(checked)
        return clean


@dataclass(frozen=True)
class MapOf:
    """A JSON object of 0 to max_items entries whose keys pass key_type (a Name or an Enum, so a key is safe to quote in an error) and whose values pass value_type, returned as a new dict. Every key is checked before any value."""

    key_type: object
    value_type: object
    max_items: int

    is_string = False
    is_secret = False

    def __post_init__(self):
        if type(self.key_type) not in (Name, Enum):
            raise TypeError("MapOf keys must be a Name or an Enum")
        _check_inner(self.value_type, "MapOf")
        _bounded_int(self.max_items, 1, CONTAINER_ITEMS_MAX, "MapOf max_items")
        _check_shape(self.capacity, self.depth)

    @property
    def capacity(self):
        return self.max_items * (1 + self.value_type.capacity)

    @property
    def depth(self):
        return 1 + self.value_type.depth

    def validate(self, value):
        if not isinstance(value, dict):
            raise ValueError("must be an object")
        if len(value) > self.max_items:
            raise ValueError(f"must hold at most {self.max_items} entries")
        for key in value:
            try:
                self.key_type.validate(key)
            except ValueError as exc:
                raise ValueError(f"has a key that {exc}") from None
        clean = {}
        for key, item in value.items():
            try:
                clean[key] = _wrapped(self.value_type, item)
            except ValueError as exc:
                raise ValueError(f"key '{key}' {exc}") from None
        return clean


@dataclass(frozen=True)
class Field:
    """One input: its type and whether a request must carry it. A verb's socket inputs are Fields, and so are the keys of a Record."""

    type: object
    required: bool = False

    def __post_init__(self):
        if type(self.type) not in INPUT_TYPES:
            raise TypeError("a socket input needs an instance of one of the types in control.types")


@dataclass(frozen=True)
class Record:
    """A JSON object with fixed keys, each a Field (its type, and whether the object must carry it), such as one target of the scheduler {name, database, timeout_hours}. A key that is not listed is refused before any value is looked at; an optional key that is absent stays absent, and None is a refused value. Returned as a new dict in the order of `fields`."""

    fields: object

    is_string = False
    is_secret = False

    def __post_init__(self):
        if not isinstance(self.fields, dict) or not self.fields:
            raise TypeError("Record needs a dict of at least one Field")
        for key, spec in self.fields.items():
            if not isinstance(key, str) or re.fullmatch(RECORD_KEY_PATTERN, key) is None:
                raise ValueError("Record keys must be lower-case identifiers of at most 64 characters")
            if not isinstance(spec, Field):
                raise TypeError("each key of a Record needs a Field")
        object.__setattr__(self, "fields", MappingProxyType(dict(self.fields)))
        _check_shape(self.capacity, self.depth)

    @property
    def capacity(self):
        return sum(spec.type.capacity for spec in self.fields.values())

    @property
    def depth(self):
        return 1 + max(spec.type.depth for spec in self.fields.values())

    def validate(self, value):
        if not isinstance(value, dict):
            raise ValueError("must be an object")
        for key in value:
            if key not in self.fields:
                quoted = key if isinstance(key, str) and re.fullmatch(RECORD_KEY_PATTERN, key) else None
                raise ValueError(f"key '{quoted}' is not accepted" if quoted else "has a key that is not accepted")
        clean = {}
        for key, spec in self.fields.items():
            if key not in value:
                if spec.required:
                    raise ValueError(f"key '{key}' is required")
                continue
            try:
                clean[key] = _wrapped(spec.type, value[key])
            except ValueError as exc:
                raise ValueError(f"key '{key}' {exc}") from None
        return clean


def reveal(value):
    """A copy of a validated value with every SecretValue in it, at any depth, replaced by its text: what an action is called with."""
    if isinstance(value, SecretValue):
        return value.reveal()
    if isinstance(value, list):
        return [reveal(item) for item in value]
    if isinstance(value, dict):
        return {key: reveal(item) for key, item in value.items()}
    return value


def secrets_in(value):
    """Every SecretValue in a validated value, at any depth, in order: what the broker redacts from a reply."""
    if isinstance(value, SecretValue):
        return [value]
    if isinstance(value, list):
        return [secret for item in value for secret in secrets_in(item)]
    if isinstance(value, dict):
        return [secret for item in value.values() for secret in secrets_in(item)]
    return []


# The whole set. A socket input whose type is not one of these classes is refused when the registry is built.
INPUT_TYPES = (Bool, IntRange, Enum, Name, FileFilter, ConfiguredSource, RunId, Secret, Path, Port, Text, ListOf, MapOf, Record)
# The types that accept a host path, any port or free text: each input of one of them (also nested in a container) is a decision, listed in the registry test.
WIDE_TYPES = (Path, Port, Text)
