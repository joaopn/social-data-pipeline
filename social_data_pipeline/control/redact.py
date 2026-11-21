"""Best-effort overwriting of secrets in what leaves the broker: the text of a log, and the data of a reply.

What is overwritten, always with `*` and always byte for byte (one `*` for every byte of UTF-8 that is covered, so no offset and no length of the text changes):

- every occurrence of each secret that is known at that moment, in its plain form and in the forms `urllib.parse.quote` and `urllib.parse.quote_plus` give it, occurrences that overlap each other included; a secret of fewer than 4 characters is ignored (it would overwrite half of the text);
- on every line that starts, after optional spaces or tabs, with `$ docker compose ` (the line with which the CLI shows the command it runs): everything after that prefix up to the end of the line. Only the first 64 KiB of such a line are covered, counted from the line's start; the cut is moved to the end of a character it would split.

Which bytes are covered is decided on the text as it was, not on text that was already partly overwritten, so the answer for one byte depends only on the bytes near it: at most the length of the longest form of a secret, less one, on either side, and the reach of the `$ docker compose ` rule (plus three bytes) to the left. That is what makes a log readable in pieces: `window` gives the stretch of the log to read for a piece, `cut` takes the piece out of the overwritten stretch, and the pieces are byte for byte what overwriting the whole log and cutting it afterwards gives, for any offsets and sizes. The bound on the `$ docker compose ` rule exists for that reason: without it a piece that begins inside a very long line could not know how the line began.

Which bytes are covered is marked in a mask of the text's size, so memory stays a small multiple of the text's size whatever the secrets match (a secret of four spaces in a log full of them included). Time grows with the number of separate stretches overwritten: a run of occurrences that repeat a secret is one stretch and costs a few comparisons, but occurrences that stand apart cost a step each.

One call takes on at most 4 MiB of text (for `redact_piece`, the stretch it reads) and at most 256 secrets; `redact_data` counts work instead, against the same figure: every string costs its bytes and one unit per form of a secret, counted while walking so that the walk stops early, and a string too short to hold anything is not searched; beyond that it raises DataTooLargeToRedact (code `data_too_large_to_redact`) and overwrites nothing. The caller then answers that the data is too large (a log is read in smaller pieces), and never sends the text as it was.

`window` and `cut` count in bytes of the log file. A caller that gets a log as decoded text has other offsets wherever the log holds bytes that are not UTF-8, and must read the bytes themselves for the stretch `window` names.

What this does NOT do. It knows only the secrets it is handed: the database passwords of read-only users, tokens of any kind and passwords of sessions that have ended are not among them. It finds only the three forms above: a secret that was base64- or hex-encoded, escaped for JSON or a shell, written with other percent-encoding rules (lower-case hex digits, `%20` together with an encoded `/`), cut in two by a line break or a colour code, or shown only in part is not found. A secret that is a number in a reply's data is not found, only text is looked at. It is a second line of defence behind not writing secrets in the first place, never the first.

The module uses the standard library only and keeps no state.
"""

import re
from urllib.parse import quote, quote_plus

from social_data_pipeline.control.errors import ControlError

# The smallest piece of a log a caller should serve: a smaller request is raised to it, so that the margins `window` adds do not outweigh what is read for.
MIN_PIECE_BYTES = 4096
MIN_SECRET_CHARS = 4
# How far from the start of a `$ docker compose ` line its overwriting reaches.
COMPOSE_LINE_BYTES = 65536
DATA_MAX_DEPTH = 64
# What one call takes on: the bytes of text (for `redact_data`, of all its strings together, keys included; for `redact_piece`, of the stretch it reads), and the number of secrets.
MAX_REDACT_BYTES = 4 * 1024 * 1024
MAX_SECRETS = 256
# What `redact_data` takes on, counted as work: every string costs its bytes of UTF-8 and one unit for each form of a secret (each is looked for in it), so that many small strings, empty ones included, have a bound too.
MAX_REDACT_COST = MAX_REDACT_BYTES
# A string shorter than this and than the shortest form holds nothing to overwrite: no form fits in it, and a `$ docker compose ` line needs at least this many bytes.
_COMPOSE_PREFIX_BYTES = len(b"$ docker compose ")
# One search for a form in a text, the first of each: a name of its own, so that a test can count them.
_find_first = bytes.find

_COMPOSE_AT_START = re.compile(rb"[ \t]*\$ docker compose ")
_COMPOSE_AFTER_BREAK = re.compile(rb"(?<=[\r\n])[ \t]*\$ docker compose ")
_LINE_END = re.compile(rb"[\r\n]")
# A character of UTF-8 has at most three bytes after its first.
_CHARACTER_TAIL_BYTES = 3
# The mask holds 1 for a byte that is covered; as a whole number it needs all eight bits set.
_MASK_TO_BYTE = bytes.maketrans(b"\x00\x01", b"\x00\xff")


class DataTooDeep(ControlError):
    """The data of a reply is nested deeper than the bound."""

    code = "data_too_deep"

    def __init__(self):
        super().__init__("the data is nested too deep")


class DataTooLargeToRedact(ControlError):
    """One call was handed more text, or more secrets, than it takes on. Nothing was overwritten and nothing is returned: the caller answers "too large" and never sends the text as it was."""

    code = "data_too_large_to_redact"

    def __init__(self):
        super().__init__("the data is too large to redact")


class DataNotJson(ControlError):
    """The data of a reply holds something that decoded JSON cannot hold."""

    code = "data_not_json"

    def __init__(self):
        super().__init__("the data is not what decoded JSON holds")


def _forms(secrets):
    """The byte strings to look for, as a dict from each form to the unit by which its occurrences repeat (None until the form is found; the unit is worked out then, once for the whole call). The forms are each secret of at least 4 characters as it is, and as `quote` and `quote_plus` write it. A secret is text or an object with `reveal()`. Raises TypeError for anything else, and for one string in the place of a list of secrets, which would be read as its characters; DataTooLargeToRedact for more than 256 secrets."""
    if isinstance(secrets, (str, bytes)) or hasattr(secrets, "reveal"):
        raise TypeError("the secrets are a list, not one secret")
    forms = {}
    for count, secret in enumerate(secrets, start=1):
        if count > MAX_SECRETS:
            raise DataTooLargeToRedact()
        value = secret.reveal() if hasattr(secret, "reveal") else secret
        if not isinstance(value, str):
            raise TypeError("a secret is text")
        if len(value) < MIN_SECRET_CHARS:
            continue
        # With "surrogatepass" on both sides, text that JSON carried with a lone surrogate is matched like any other.
        forms[value.encode("utf-8", "surrogatepass")] = None
        try:
            forms[quote(value).encode("ascii")] = None
            forms[quote_plus(value).encode("ascii")] = None
        except UnicodeEncodeError:
            # A lone surrogate has no percent-encoded form.
            pass
    return forms


def _smallest_period(form):
    """The smallest p > 0 for which form[i] == form[i + p] wherever both exist (the prefix function of Knuth, Morris and Pratt)."""
    border = [0] * len(form)
    for index in range(1, len(form)):
        length = border[index - 1]
        while length and form[index] != form[length]:
            length = border[length - 1]
        if form[index] == form[length]:
            length += 1
        border[index] = length
    return len(form) - border[-1]


def _repeated_to(data, position, unit):
    """Where the copies of `unit` that follow each other from `position` on end. Found by doubling the stretch compared and then halving it, so it takes a few dozen comparisons, done by `startswith`, however long the run is."""
    block = unit
    while data.startswith(block, position):
        position += len(block)
        block += block
    while len(block) > len(unit):
        block = block[:len(block) // 2]
        if data.startswith(block, position):
            position += len(block)
    return position


def _mark(mask, start, end):
    mask[start:end] = b"\x01" * (end - start)


def _mark_secrets(data, forms, mask):
    """Mark in the mask every byte that an occurrence of a form covers, occurrences that overlap included. `forms` is what `_forms` returned; the unit of a form is worked out when it is first found, and kept in it for the rest of the call. Occurrences that follow each other at the form's own period (a run of the same character, `abab...` for `abab`), or as whole copies of the form, are taken as one stretch, found in a few comparisons: nothing is kept per occurrence, and the time is one step per stretch of that kind. Occurrences spaced otherwise cost one step each."""
    size = len(data)
    for form, unit in forms.items():
        # A form longer than the text cannot be in it.
        if len(form) > size:
            continue
        start = _find_first(data, form)
        if start < 0:
            continue
        if unit is None:
            unit = forms[form] = form[len(form) - _smallest_period(form):]
        length = len(form)
        while start >= 0:
            # The form continued by whole periods: an occurrence at every period, all of them inside [start, end). Then copies of the whole form right after it, a run of occurrences whose period is not the form's own (`aabaa` repeated).
            end = start + length
            if data.startswith(unit, end):
                end = _repeated_to(data, end, unit)
            if len(unit) != length and data.startswith(form, end):
                end = _repeated_to(data, end, form)
            _mark(mask, start, end)
            # Every occurrence that starts before the last one found ends inside what is marked; the next one to look for starts after it.
            start = data.find(form, end - length + 1)


def _mark_compose_lines(data, at_start, reach, mask):
    """Mark what the `$ docker compose ` rule covers. The first byte of the data is the start of a line only with `at_start`."""
    first = _COMPOSE_AT_START.match(data) if at_start else None
    matches = _COMPOSE_AFTER_BREAK.finditer(data)
    for match in ([first] if first is not None else []) + list(matches):
        line_end = _LINE_END.search(data, match.end())
        line_end = len(data) if line_end is None else line_end.start()
        end = min(line_end, match.start() + reach)
        if end < line_end:
            # Cut by the reach: a character the cut would split is covered to its end.
            for _ in range(_CHARACTER_TAIL_BYTES):
                if end < line_end and data[end] & 0xC0 == 0x80:
                    end += 1
        if match.end() < end:
            _mark(mask, match.end(), end)


def _overwritten(data, mask):
    """The data with every byte the mask marks overwritten by `*`, and how many separate stretches that were. Done on whole numbers of the data's size, so the cost does not depend on how many stretches there are."""
    stretches = mask.count(b"\x00\x01") + (1 if mask[:1] == b"\x01" else 0)
    if not stretches:
        return bytes(data), 0
    size = len(data)
    marked = int.from_bytes(mask.translate(_MASK_TO_BYTE), "big")
    stars = int.from_bytes(b"*" * size, "big")
    plain = int.from_bytes(data, "big")
    return ((plain & ~marked) | (stars & marked)).to_bytes(size, "big"), stretches


def _checked_reach(compose_reach):
    if isinstance(compose_reach, bool) or not isinstance(compose_reach, int) or compose_reach < 1:
        raise ValueError("the reach is a whole number of bytes, at least 1")
    return compose_reach


def _redact(data, forms, at_start, reach):
    """Which bytes are covered is marked in a mask of the data's size, on the data as it was; memory stays a small multiple of the data's size whatever the secrets match."""
    mask = bytearray(len(data))
    _mark_secrets(data, forms, mask)
    _mark_compose_lines(data, at_start, reach, mask)
    return _overwritten(data, mask)


def _checked_size(size):
    if size > MAX_REDACT_BYTES:
        raise DataTooLargeToRedact()


def redact_bytes(data, secrets, *, at_start=True, compose_reach=COMPOSE_LINE_BYTES):
    """Overwrite the secrets and the `$ docker compose ` lines in bytes of UTF-8 text: (the new bytes, of the same length; the number of separate stretches that were overwritten). `at_start` says whether the first byte is the start of a line: true for a whole text and for a stretch of a log that begins at offset 0, false for any other stretch. Raises DataTooLargeToRedact, before anything is done, for more than 4 MiB or more than 256 secrets."""
    _checked_size(len(data))
    return _redact(bytes(data), _forms(secrets), at_start, _checked_reach(compose_reach))


def redact_text(text, secrets, *, compose_reach=COMPOSE_LINE_BYTES):
    """Overwrite the secrets and the `$ docker compose ` lines in a whole text: (the new text, the number of separate stretches that were overwritten). The UTF-8 form of the text keeps its length: a covered character of several bytes becomes as many `*`. Not for a piece of a log, whose first byte need not start a line and whose ends can cut a secret: see `window`. Raises DataTooLargeToRedact, before anything is done, for more than 4 MiB of UTF-8 or more than 256 secrets."""
    if not isinstance(text, str):
        raise TypeError("the text is a string")
    # A character is at least one byte: more characters than the limit are more bytes, and are refused before anything is encoded.
    _checked_size(len(text))
    forms, reach = _forms(secrets), _checked_reach(compose_reach)
    data = text.encode("utf-8", "surrogatepass")
    _checked_size(len(data))
    new, count = _redact(data, forms, True, reach)
    return new.decode("utf-8", "surrogatepass"), count


def redact_data(value, secrets, *, max_depth=DATA_MAX_DEPTH):
    """Overwrite the secrets in every string of decoded JSON, the keys of its objects included: (the new value, the number of stretches that were overwritten). Numbers, booleans and null are kept as they are. Two keys that become the same text are one key afterwards, with the value of the later one. Raises DataTooDeep for lists and objects nested more than `max_depth` deep, DataNotJson for a value or a key of any other type, and DataTooLargeToRedact for more than 256 secrets or as soon as the strings walked so far cost more than MAX_REDACT_COST: each string costs its bytes of UTF-8 and one unit per form of a secret (three at most per secret), charged before it is looked at. A refusal returns nothing, so no data leaves partly overwritten. A string too short to hold a form or a `$ docker compose ` line is charged and returned as it is, without any search."""
    forms = _forms(secrets)
    per_string = len(forms)
    shortest = min([len(form) for form in forms] + [_COMPOSE_PREFIX_BYTES])
    total, cost = [0], [0]

    def charge(amount):
        if cost[0] + amount > MAX_REDACT_COST:
            raise DataTooLargeToRedact()
        cost[0] += amount

    def walk(item, depth):
        if isinstance(item, str):
            # A character is at least one byte: checked before anything is encoded, and again with the bytes.
            if cost[0] + len(item) + per_string > MAX_REDACT_COST:
                raise DataTooLargeToRedact()
            data = item.encode("utf-8", "surrogatepass")
            charge(len(data) + per_string)
            if len(data) < shortest:
                return item
            new, count = _redact(data, forms, True, COMPOSE_LINE_BYTES)
            total[0] += count
            return new.decode("utf-8", "surrogatepass")
        if item is None or isinstance(item, (bool, int, float)):
            return item
        if not isinstance(item, (dict, list)):
            raise DataNotJson()
        if depth >= max_depth:
            raise DataTooDeep()
        if isinstance(item, list):
            return [walk(entry, depth + 1) for entry in item]
        if not all(isinstance(key, str) for key in item):
            raise DataNotJson()
        return {walk(key, depth + 1): walk(entry, depth + 1) for key, entry in item.items()}

    return walk(value, 0), total[0]


def _window(offset, limit, forms, reach):
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise ValueError("the offset is a whole number, at least 0")
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
        raise ValueError("the limit is a whole number, at least 1")
    after = max((len(form) for form in forms), default=1) - 1
    before = max(after, reach + _CHARACTER_TAIL_BYTES)
    read_offset = max(0, offset - before)
    return read_offset, offset - read_offset + limit + after, offset - read_offset


def window(offset, limit, secrets, *, compose_reach=COMPOSE_LINE_BYTES):
    """The stretch of a log to read so that the piece of `limit` bytes at `offset` can be overwritten exactly: (the offset to read from, the number of bytes to read, where the piece begins in what was read). The stretch reaches past the piece's end by the length of the longest form of a secret, less one (nothing without secrets), and before its start by the larger of that and the reach of the `$ docker compose ` rule plus three bytes, never before offset 0. Overwrite what was read with `redact_bytes(..., at_start=<the offset to read from is 0>)` and take the piece with `cut`; `redact_piece` does the three steps. Raises ValueError for an offset or limit that is not a whole number, a negative offset and a limit below 1."""
    return _window(offset, limit, _forms(secrets), _checked_reach(compose_reach))


def cut(data, cut_start, limit):
    """The piece out of a stretch that was read as `window` said and then overwritten: `limit` bytes from `cut_start`, fewer where the log ended."""
    return bytes(data[cut_start:cut_start + limit])


def redact_piece(read_at, offset, limit, secrets, *, compose_reach=COMPOSE_LINE_BYTES):
    """The piece of `limit` bytes at `offset` of a log, overwritten as the whole log would be: (the bytes of the piece, the number of stretches overwritten in what was read for it). `read_at(offset, count)` returns up to `count` bytes of the log from `offset`, fewer at its end. Raises DataTooLargeToRedact, before anything is read, when the stretch to read (the piece and its margins, see `window`) is more than 4 MiB, and for more than 256 secrets."""
    forms, reach = _forms(secrets), _checked_reach(compose_reach)
    read_offset, read_limit, cut_start = _window(offset, limit, forms, reach)
    _checked_size(read_limit)
    data, count = _redact(bytes(read_at(read_offset, read_limit))[:read_limit], forms, read_offset == 0, reach)
    return cut(data, cut_start, limit), count
