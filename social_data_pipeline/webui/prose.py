"""The assistant's text as blocks the Chat page draws: a small renderer of the web UI's own, a pure function from text to data, never to markup.

`blocks(text)` returns a list of blocks, each a dict with `kind`: `p` (a paragraph, its line breaks kept), `h` (a heading, `level` 1 to 3: `#`, `##`, `###`, and deeper ones as 3), `ul` and `ol` (a bullet and a numbered list, `items` each a list of runs, one level: an indented item is an item of the same list; an `ol` has `start`, the number of its first item, so a list split by blank lines keeps its numbers), `code` (a fenced block between lines starting with three backticks; one not closed runs to the end), `quote` (lines starting with `>`), `table` and `hr` (a divider: a line, indented at most three spaces, of three or more of one of `-`, `*` and `_`, spaces allowed between them; it is read before a list item, so `- - -` is a divider); a paragraph, a heading and a quote hold `runs`. A run is plain, bold (`**`), italic (`*`), both, or code (between single backticks; an empty pair is text), as `{"bold", "italic", "code", "parts"}`, its `parts` the runs of `statements.visible`, so every character that would not show as itself is an escape. Nothing else is read: a link, an image or raw HTML is its literal text, which the template escapes. A marker that is not closed is text; `*` opens only before a character that is not a space and closes only after one.

A `table` is a pipe table: a line holding `|` followed by a delimiter line of as many cells, each one or more `-` with an optional `:` at either end, starts it (the line is its header, never the last line of a paragraph above it), and the lines after them up to a blank line or a line without `|` are its rows. A line is split at every `|` not after a backslash (`\\|` is a `|` in its cell), a `|` at its start and one at its end are dropped, and every cell is trimmed. The block is `{"kind": "table", "align", "head", "rows"}`: `align` per column `"left"`, `"center"`, `"right"` or None (`:-`, `:-:`, `-:`, `-`), `head` the header's cells and `rows` a list of rows of cells, each cell its runs. A row longer than the header is cut to its width; a shorter one is not padded (the model stays linear in the text: a row of one cell under a header of many columns is one cell), and the page draws only the cells it has.

The input is cut at 64 KiB (`cut` is then true). Every step is linear in the text: lines are split once (a table's lines into cells once each, a line at most twice when it is tried as a header and as a delimiter), the backticks and asterisks are found in one pass each, and no regular expression is used, and no text is copied with what came before it (a list item's lines are joined once); `steps`, when a list, gets the number of steps taken, counted per character handled (for a test). Standard library and this package only.
"""

from social_data_pipeline.webui import statements

TEXT_MAX_BYTES = 64 * 1024
HEADING_MAX = 3
FENCE = "```"


def cut(text):
    """(the text at most TEXT_MAX_BYTES of UTF-8, whether it was cut)."""
    data = text.encode("utf-8", "surrogatepass")
    if len(data) <= TEXT_MAX_BYTES:
        return text, False
    return data[:TEXT_MAX_BYTES].decode("utf-8", "ignore"), True


def _tokens(text, counter):
    """The text as tokens `[kind, text, can_open, can_close]`: `text`, `code` (a backtick pair's content), `**` and `*`."""
    ticks = [at for at, character in enumerate(text) if character == "`"]
    counter[0] += len(text)
    pieces, position = [], 0
    for index in range(0, len(ticks) - 1, 2):
        start, end = ticks[index], ticks[index + 1]
        if end == start + 1:
            continue
        pieces.append((False, text[position:start]))
        pieces.append((True, text[start + 1:end]))
        position = end + 1
    pieces.append((False, text[position:]))
    tokens = []
    for is_code, piece in pieces:
        if is_code:
            tokens.append(["code", piece, False, False])
            continue
        at, plain = 0, 0
        while at < len(piece):
            counter[0] += 1
            if piece[at] != "*":
                at += 1
                continue
            if at > plain:
                tokens.append(["text", piece[plain:at], False, False])
            run = at
            while at < len(piece) and piece[at] == "*":
                at += 1
            before, after = piece[run - 1] if run else " ", piece[at] if at < len(piece) else " "
            opens, closes = not after.isspace(), not before.isspace()
            for marker in ["**"] * ((at - run) // 2) + ["*"] * ((at - run) % 2):
                tokens.append([marker, marker, opens, closes])
            plain = at
        if plain < len(piece):
            tokens.append(["text", piece[plain:], False, False])
    return tokens


def runs(text, counter=None):
    """The runs of one paragraph, heading, quote or item."""
    counter = counter if counter is not None else [0]
    tokens = _tokens(text, counter)
    paired = set()
    for marker in ("**", "*"):
        opened = None
        for index, token in enumerate(tokens):
            counter[0] += 1
            if token[0] != marker:
                continue
            if opened is None:
                if token[2]:
                    opened = index
            elif token[3]:
                paired.update((opened, index))
                opened = None
    found, bold, italic = [], False, False
    for index, (kind, piece, _opens, _closes) in enumerate(tokens):
        counter[0] += 1
        if index in paired:
            bold, italic = (not bold, italic) if kind == "**" else (bold, not italic)
            continue
        flags = (bold, italic, kind == "code")
        if found and found[-1][0] == flags and kind != "code":
            found[-1][1].append(piece)
        else:
            found.append((flags, [piece]))
    return [{"bold": flags[0], "italic": flags[1], "code": flags[2], "parts": statements.visible("".join(pieces))[0]} for flags, pieces in found if "".join(pieces) or flags[2]]


def _item(stripped):
    """(`ul` or `ol`, the item's text, its number or None) when the line is a list item, else None."""
    if stripped[:2] in ("- ", "* ", "+ "):
        return "ul", stripped[2:], None
    digits = 0
    while digits < len(stripped) and digits < 9 and stripped[digits].isdigit():
        digits += 1
    if digits and stripped[digits:digits + 2] in (". ", ") "):
        return "ol", stripped[digits + 2:], int(stripped[:digits])
    return None


def _rule(stripped):
    """Whether the line (its indent removed) is a divider: three or more of one of `-`, `*` and `_`, spaces allowed between them."""
    if stripped[:1] not in ("-", "*", "_"):
        return False
    compact = stripped.replace(" ", "")
    return len(compact) >= 3 and not compact.strip(compact[0])


def _cells(line, counter):
    """The cells of a table's line, as texts: split at every `|` not after a backslash (`\\|` a literal `|`), a `|` at the start and one at the end dropped, every cell trimmed."""
    line = line.strip()
    counter[0] += len(line)
    cells, start = [], 0
    for at, character in enumerate(line):
        if character == "|" and (not at or line[at - 1] != "\\"):
            cells.append(line[start:at])
            start = at + 1
    cells.append(line[start:])
    if line.startswith("|"):
        cells.pop(0)
    if cells and line.endswith("|") and not line.endswith("\\|"):
        cells.pop()
    return [cell.replace("\\|", "|").strip() for cell in cells]


def _delimiter(line, counter):
    """The alignment of each column when the line is a table's delimiter line, else None."""
    if "-" not in line:
        return None
    align = []
    for cell in _cells(line, counter):
        left, right = cell.startswith(":"), cell.endswith(":")
        dashes = cell[left:len(cell) - right]
        if not dashes or dashes.strip("-"):
            return None
        align.append("center" if left and right else "right" if right else "left" if left else None)
    return align or None


def blocks(text, steps=None):
    """The blocks of the assistant's text (see the module), and whether it was cut: `(blocks, cut)`."""
    text, was_cut = cut(text if isinstance(text, str) else "")
    counter = [0]
    found = []
    open_kind, lines, start = None, [], None

    def close():
        nonlocal open_kind, lines
        if open_kind == "p":
            found.append({"kind": "p", "runs": runs("\n".join(lines), counter)})
        elif open_kind == "quote":
            found.append({"kind": "quote", "runs": runs("\n".join(lines), counter)})
        elif open_kind == "ul":
            found.append({"kind": "ul", "items": [runs("\n".join(item), counter) for item in lines]})
        elif open_kind == "ol":
            found.append({"kind": "ol", "start": start, "items": [runs("\n".join(item), counter) for item in lines]})
        elif open_kind == "code":
            found.append({"kind": "code", "parts": statements.visible("\n".join(lines))[0]})
        open_kind, lines = None, []

    source = text.split("\n")
    skip = False
    for index, line in enumerate(source):
        counter[0] += 1 + len(line)
        if skip:
            # The delimiter line of the table that the line before it started.
            skip = False
            continue
        line = line[:-1] if line.endswith("\r") else line
        stripped = line.lstrip(" ")
        indent = len(line) - len(stripped)
        fence = indent <= 3 and stripped.startswith(FENCE)
        if open_kind == "code":
            if fence:
                close()
            else:
                lines.append(line)
            continue
        if fence:
            close()
            open_kind = "code"
            continue
        if open_kind == "table":
            if "|" in line and stripped.strip():
                # A table, the last block found while it is open, takes its rows as they come: cut to the header's width, never padded.
                found[-1]["rows"].append([runs(cell, counter) for cell in _cells(line, counter)[:len(found[-1]["head"])]])
                continue
            close()
        if not stripped.strip():
            close()
            continue
        level = 0
        while level < len(stripped) and stripped[level] == "#":
            level += 1
        if indent <= 3 and level and stripped[level:level + 1] == " ":
            close()
            found.append({"kind": "h", "level": min(level, HEADING_MAX), "runs": runs(stripped[level + 1:].strip(), counter)})
            continue
        if "|" in line and index + 1 < len(source):
            following = source[index + 1]
            align = _delimiter(following[:-1] if following.endswith("\r") else following, counter)
            head = _cells(line, counter) if align is not None else None
            if head is not None and len(head) == len(align):
                close()
                found.append({"kind": "table", "align": align, "head": [runs(cell, counter) for cell in head], "rows": []})
                open_kind, skip = "table", True
                continue
        if indent <= 3 and _rule(stripped):
            close()
            found.append({"kind": "hr"})
            continue
        item = _item(stripped)
        if item is not None:
            if open_kind != item[0]:
                close()
                open_kind, start = item[0], item[2]
            # An item is the list of its lines, joined once when the list closes: a continuation line is never copied with what came before it.
            lines.append([item[1]])
            continue
        if stripped.startswith(">"):
            if open_kind != "quote":
                close()
                open_kind = "quote"
            lines.append(stripped[2:] if stripped.startswith("> ") else stripped[1:])
            continue
        if open_kind in ("ul", "ol") and indent:
            lines[-1].append(stripped)
            continue
        if open_kind != "p":
            close()
            open_kind = "p"
        lines.append(line)
    close()
    if steps is not None:
        steps.append(counter[0])
    return found, was_cut
