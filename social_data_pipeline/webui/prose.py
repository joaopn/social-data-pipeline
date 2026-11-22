"""The assistant's text as blocks the Chat page draws: a small renderer of the web UI's own, a pure function from text to data, never to markup.

`blocks(text)` returns a list of blocks, each a dict with `kind`: `p` (a paragraph, its line breaks kept), `h` (a heading, `level` 1 to 3: `#`, `##`, `###`, and deeper ones as 3), `ul` and `ol` (a bullet and a numbered list, `items` each a list of runs, one level: an indented item is an item of the same list), `code` (a fenced block between lines starting with three backticks; one not closed runs to the end) and `quote` (lines starting with `>`); a paragraph, a heading and a quote hold `runs`. A run is plain, bold (`**`), italic (`*`), both, or code (between single backticks; an empty pair is text), as `{"bold", "italic", "code", "parts"}`, its `parts` the runs of `statements.visible`, so every character that would not show as itself is an escape. Nothing else is read: a link, an image or raw HTML is its literal text, which the template escapes. A marker that is not closed is text; `*` opens only before a character that is not a space and closes only after one.

The input is cut at 64 KiB (`cut` is then true). Every step is linear in the text: lines are split once, the backticks and asterisks are found in one pass each, and no regular expression is used, and no text is copied with what came before it (a list item's lines are joined once); `steps`, when a list, gets the number of steps taken, counted per character handled (for a test). Standard library and this package only.
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
    """(`ul` or `ol`, the item's text) when the line is a list item, else None."""
    if stripped[:2] in ("- ", "* ", "+ "):
        return "ul", stripped[2:]
    digits = 0
    while digits < len(stripped) and digits < 9 and stripped[digits].isdigit():
        digits += 1
    if digits and stripped[digits:digits + 2] in (". ", ") "):
        return "ol", stripped[digits + 2:]
    return None


def blocks(text, steps=None):
    """The blocks of the assistant's text (see the module), and whether it was cut: `(blocks, cut)`."""
    text, was_cut = cut(text if isinstance(text, str) else "")
    counter = [0]
    found = []
    open_kind, lines = None, []

    def close():
        nonlocal open_kind, lines
        if open_kind == "p":
            found.append({"kind": "p", "runs": runs("\n".join(lines), counter)})
        elif open_kind == "quote":
            found.append({"kind": "quote", "runs": runs("\n".join(lines), counter)})
        elif open_kind in ("ul", "ol"):
            found.append({"kind": open_kind, "items": [runs("\n".join(item), counter) for item in lines]})
        elif open_kind == "code":
            found.append({"kind": "code", "parts": statements.visible("\n".join(lines))[0]})
        open_kind, lines = None, []

    for line in text.split("\n"):
        counter[0] += 1 + len(line)
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
        item = _item(stripped)
        if item is not None:
            if open_kind != item[0]:
                close()
                open_kind = item[0]
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
