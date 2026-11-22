"""How the Queue window shows a query's statement: every character that would not be seen as itself made visible, and the hints, a list of words worth a look.

`visible(text)` cuts a text into runs of characters shown as they are and runs shown as escapes (`\\u202e`, `\\U000e0041`): every character of the Unicode categories Cc (but the line break and the tab), Cf, Zl, Zp, Cs and Co, which a browser would interpret (a direction change, an invisible joiner, a line separator) or could not encode (a lone surrogate). The statement is otherwise shown exactly as stored.

`hints(text, backend)` lists the words of a short list per backend that are in the statement, case-insensitively, as a reminder of what deserves a look before an approval. They are hints, not a check: the list is short, a word can be written so that it is not found, and an empty list proves nothing. The text is scanned once, in steps proportional to its length (`steps` counts them for a test). Standard library only.
"""

import functools
import re
import sys
import unicodedata

VISIBLE_CATEGORIES = frozenset({"Cc", "Cf", "Zl", "Zp", "Cs", "Co"})
KEPT = frozenset("\n\t")

# Per backend: (the word as shown, the lower-case text looked for, whether it must end at a word's end). Every word must begin at a word's start.
_WORDS = {
    "postgres": (
        ("PROGRAM", "program", True), ("COPY", "copy", True), ("pg_read_", "pg_read_", False), ("pg_ls_", "pg_ls_", False), ("pg_write_", "pg_write_", False),
        ("lo_import", "lo_import", False), ("lo_export", "lo_export", False), ("dblink", "dblink", False),
    ),
    "starrocks": (
        ("INTO OUTFILE", "into outfile", True), ("FILES(", "files(", False), ("FILES(", "files (", False), ("INSERT", "insert", True), ("LOAD", "load", True), ("SET", "set", True),
    ),
    "mongodb": (
        ("$out", "$out", True), ("$merge", "$merge", True), ("$function", "$function", True), ("$accumulator", "$accumulator", True), ("$where", "$where", True),
    ),
}
SEMICOLON_HINT = "a ; that is not the last character"
LEADING_HINT = "a leading ANALYZE or ( option list (EXPLAIN then runs the statement)"
_WORD_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789_$")
# PostgreSQL: `$` ends a dollar quote (`$q$COPY ...$q$`), so it does not join a word.
_WORD_CHARACTERS_BY_BACKEND = {"postgres": _WORD_CHARACTERS - {"$"}}
# MongoDB: a pipeline's JSON can write the `$` of an operator as the escape `\u0024` (any case of the `u`, after lower-casing one form).
_DOLLAR_ESCAPE = "\\u0024"
_WHITESPACE = re.compile(r"\s+")


@functools.lru_cache(maxsize=1)
def _visible_pattern():
    """A pattern of every character to be made visible, built once from this Python's Unicode tables."""
    ranges = []
    start = None
    for code in range(sys.maxunicode + 1):
        character = chr(code)
        hit = character not in KEPT and unicodedata.category(character) in VISIBLE_CATEGORIES
        if hit and start is None:
            start = code
        elif not hit and start is not None:
            ranges.append((start, code - 1))
            start = None
    if start is not None:
        ranges.append((start, sys.maxunicode))
    parts = "".join(re.escape(chr(low)) if low == high else f"{re.escape(chr(low))}-{re.escape(chr(high))}" for low, high in ranges)
    return re.compile(f"[{parts}]+")


def escape_of(character):
    code = ord(character)
    return f"\\u{code:04x}" if code <= 0xFFFF else f"\\U{code:08x}"


def visible(text):
    """`(parts, count)`: `parts` a list of `(is_escape, text)` runs, which joined (escapes as written) show `text`; `count` the number of characters made visible."""
    if not isinstance(text, str):
        return [], 0
    parts = []
    count = 0
    at = 0
    for match in _visible_pattern().finditer(text):
        if match.start() > at:
            parts.append((False, text[at:match.start()]))
        run = match.group()
        count += len(run)
        parts.append((True, "".join(escape_of(character) for character in run)))
        at = match.end()
    if at < len(text):
        parts.append((False, text[at:]))
    return parts, count


def shown_text(text):
    """A text of the scheduler as one string fit to put into a page: the characters `visible` makes visible written as their escapes."""
    parts, _ = visible(text)
    return "".join(part for _, part in parts)


def _skip_block_comment(text, at):
    """The index after the block comment that starts at `at`, PostgreSQL's way: block comments nest (`/* /* */ */` is one), counted in one pass; an unclosed one runs to the end."""
    depth = 0
    end = len(text)
    while at < end:
        if text.startswith("/*", at):
            depth += 1
            at += 2
        elif text.startswith("*/", at):
            depth -= 1
            at += 2
            if depth == 0:
                return at
        else:
            at += 1
    return end


def _skip_leading_comments(text):
    """`text` without the comments and spaces at its start (`--` to the line's end, `/* */` nested as PostgreSQL nests them), for the check of its first word. One pass: every character is looked at a bounded number of times."""
    at = 0
    end = len(text)
    while at < end:
        if text[at].isspace():
            at += 1
        elif text.startswith("--", at):
            newline = text.find("\n", at)
            at = end if newline < 0 else newline + 1
        elif text.startswith("/*", at):
            at = _skip_block_comment(text, at)
        else:
            break
    return text[at:]


def hints(text, backend, steps=None):
    """The hints for a statement of `backend` (`postgres`, `starrocks`, `mongodb`): the words of its list found in `text`, each once, in the list's order. `steps`, when a list, gets the number of steps taken."""
    if not isinstance(text, str):
        return []
    words = _WORDS.get(backend, ())
    raw = text.lower()
    if backend == "mongodb":
        raw = raw.replace(_DOLLAR_ESCAPE, "$")
    lowered = _WHITESPACE.sub(" ", raw)
    word_characters = _WORD_CHARACTERS_BY_BACKEND.get(backend, _WORD_CHARACTERS)
    by_first = {}
    for word in words:
        by_first.setdefault(word[1][0], []).append(word)
    found = set()
    count = 0
    end = len(lowered)
    previous = " "
    for at in range(end):
        count += 1
        character = lowered[at]
        candidates = by_first.get(character)
        if candidates is not None and previous not in word_characters:
            for shown, looked_for, whole in candidates:
                count += 1
                if shown in found or not lowered.startswith(looked_for, at):
                    continue
                after = at + len(looked_for)
                if whole and after < end and lowered[after] in word_characters:
                    continue
                found.add(shown)
        previous = character
    names = []
    for shown, _looked_for, _whole in words:
        if shown in found and shown not in names:
            names.append(shown)
    if backend in ("postgres", "starrocks"):
        stripped = lowered.rstrip()
        count += len(lowered)
        if ";" in stripped[:-1]:
            names.append(SEMICOLON_HINT)
    if backend == "postgres":
        # Comments end at a line break: they are skipped in the text as written, not in the one whose spaces were joined.
        head = _skip_leading_comments(raw)
        count += len(raw) - len(head)
        if head.startswith(("analyze", "analyse", "(")):
            names.append(LEADING_HINT)
    if steps is not None:
        steps.append(count)
    return names
