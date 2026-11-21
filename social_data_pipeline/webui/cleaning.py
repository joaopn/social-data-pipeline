"""Text a program wrote (a run's log, Docker's output, the end of a command's error output) made fit to show as text in a page.

`clean_log_text` makes one pass over the text and keeps, of each line, what a reader would see:

- Escape sequences are removed whole: CSI (`ESC [`, or the one-character form U+009B) with its parameters, intermediates and final character; OSC, DCS, SOS, PM and APC strings (`ESC ]`, `ESC P`, `ESC X`, `ESC ^`, `ESC _`, or their one-character forms) up to their end (BEL, `ESC \\` or U+009C), or up to the next line break when they do not end there, so that an unended one does not swallow the rest of the log; any other `ESC` with its intermediates and final character.
- A carriage return that is not followed by a line feed means "the line is written again from its start": the characters before it on that line are dropped once a character follows it on the same line (a progress bar shows its last state). One that ends a line or the text leaves the line as it was. `\\r\\n` is a line break.
- Control characters (Unicode category Cc) other than the line feed and the tab are removed, as are the characters that change the direction of text (U+202A to U+202E, U+2066 to U+2069), which could make a line read as something else than it holds.

The cost is linear in the length of the text whatever it holds: every character is looked at a bounded number of times. The result still has to be escaped by whoever puts it into HTML. Standard library only.
"""

import unicodedata

ESC = "\x1b"
BEL = "\x07"
STRING_TERMINATOR = "\x9c"
C1_CSI = "\x9b"
# The introducers of a string that runs to its terminator: after ESC, and in their one-character forms.
STRING_AFTER_ESC = frozenset("]PX^_")
C1_STRINGS = frozenset("\x9d\x90\x98\x9e\x9f")
DIRECTION_CHARACTERS = frozenset(chr(code) for code in [*range(0x202A, 0x202F), *range(0x2066, 0x206A)])


def _in(character, low, high):
    return low <= ord(character) <= high


def _skip_csi(text, at):
    """The index after a CSI sequence whose parameters start at `at`."""
    end = len(text)
    while at < end and _in(text[at], 0x30, 0x3F):
        at += 1
    while at < end and _in(text[at], 0x20, 0x2F):
        at += 1
    if at < end and _in(text[at], 0x40, 0x7E):
        at += 1
    return at


def _skip_string(text, at):
    """The index after a string whose content starts at `at`: past its terminator, or at the line break or the escape that cuts it short."""
    end = len(text)
    while at < end:
        character = text[at]
        if character in (BEL, STRING_TERMINATOR):
            return at + 1
        if character == ESC:
            return at + 2 if at + 1 < end and text[at + 1] == "\\" else at
        if character == "\n":
            return at
        at += 1
    return at


def _skip_escape(text, at):
    """The index after the escape sequence that starts at `at` (an ESC, U+009B or a one-character string introducer)."""
    character = text[at]
    if character == C1_CSI:
        return _skip_csi(text, at + 1)
    if character in C1_STRINGS:
        return _skip_string(text, at + 1)
    after = at + 1
    if after >= len(text):
        return after
    following = text[after]
    if following == "[":
        return _skip_csi(text, after + 1)
    if following in STRING_AFTER_ESC:
        return _skip_string(text, after + 1)
    if _in(following, 0x20, 0x2F):
        while after < len(text) and _in(text[after], 0x20, 0x2F):
            after += 1
        return after + 1 if after < len(text) and _in(text[after], 0x30, 0x7E) else after
    if _in(following, 0x30, 0x7E):
        return after + 1
    # ESC before anything else: the ESC alone goes, the character after it is looked at as usual.
    return after


def clean_log_text(text, steps=None):
    """`text` with escape sequences, overwritten line starts, control characters other than line feed and tab, and direction-changing characters removed (see the module). `steps`, when a list, gets the number of character visits made, for a test of the cost."""
    lines = []
    line = []
    # A lone carriage return was seen on this line and nothing has followed it yet.
    rewind = False
    visits = 0
    at = 0
    end = len(text)
    while at < end:
        visits += 1
        character = text[at]
        if character == "\n":
            lines.append("".join(line))
            line = []
            rewind = False
            at += 1
        elif character == "\r":
            if not (at + 1 < end and text[at + 1] == "\n"):
                rewind = True
            at += 1
        elif character == ESC or character == C1_CSI or character in C1_STRINGS:
            after = _skip_escape(text, at)
            visits += after - at
            at = after
        else:
            at += 1
            if character != "\t" and (character in DIRECTION_CHARACTERS or unicodedata.category(character) == "Cc"):
                continue
            if rewind:
                visits += len(line)
                line = []
                rewind = False
            line.append(character)
    lines.append("".join(line))
    if steps is not None:
        steps.append(visits)
    return "\n".join(lines)
