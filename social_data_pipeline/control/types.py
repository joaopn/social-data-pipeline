"""The closed set of input types a verb may accept over the socket.

Each type is a small immutable object whose `validate(value)` returns the accepted value or raises ValueError with a reason that never contains the value. A value is refused, never coerced. There is on purpose no type for a host path, a port or free text: an input that needs one of those stays terminal-only until it has a narrower type.
"""

import re
from dataclasses import dataclass

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


@dataclass(frozen=True)
class Bool:
    """A JSON boolean. 0, 1 and "true" are refused: a coerced flag is how a stray string switches something on."""

    is_string = False
    is_secret = False

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


@dataclass(frozen=True)
class Secret:
    """A password or token: 1 to max_len characters on one line. It may look like a path, since it never reaches one."""

    max_len: int = 1024

    is_string = True
    is_secret = True

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


# The whole set. A socket input whose type is not one of these classes is refused when the registry is built.
INPUT_TYPES = (Bool, IntRange, Enum, Name, FileFilter, ConfiguredSource, Secret)
