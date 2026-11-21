"""Checks on a source name before it is joined to config/sources/, and the line-break test every string input runs.

These belong to the control boundary and are not used by the CLI, which keeps its own handling of source names. `SOURCES_DIR` is read through the `utils` module at call time, so a test can point it at a temp folder.
"""

import unicodedata

from social_data_pipeline.setup import utils


def has_line_break_or_control(value):
    """Return True when value holds a control character or a line or paragraph separator.

    Every reader of .env splits with str.splitlines(), which breaks on more than "\\n": also on "\\r", "\\x0b", "\\x0c", "\\x1c" to "\\x1e", "\\x85", U+2028 and U+2029. Unicode categories Cc, Zl and Zp cover all of them.
    """
    return any(unicodedata.category(ch) in ("Cc", "Zl", "Zp") for ch in value)


def is_source_name_component(name):
    """Return True when name is exactly one path component, so SOURCES_DIR / name stays inside config/sources/."""
    if not isinstance(name, str) or not name:
        return False
    if "/" in name or "\\" in name or name in (".", ".."):
        return False
    # A longer name cannot be a file name on common filesystems, and stat() on it raises OSError instead of answering "no such folder".
    if len(name.encode("utf-8", "surrogatepass")) > 255:
        return False
    return not has_line_break_or_control(name)


def existing_source_dir(name):
    """Return the folder of an existing source, config/sources/<name>.

    Accepts only a name that is one path component and whose folder exists (a symlinked folder counts; a caller that must not follow a link checks for one itself). Raises ValueError otherwise, so a name taken from a request cannot address anything outside config/sources/.
    """
    if not is_source_name_component(name):
        raise ValueError("A source name must be a single folder name under config/sources/.")
    source_dir = utils.SOURCES_DIR / name
    if not source_dir.is_dir():
        # No name in the message: it is written by whoever sent the request.
        raise ValueError("Source not found in config/sources/")
    return source_dir
