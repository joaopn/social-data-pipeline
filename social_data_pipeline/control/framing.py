"""The frames of the broker's socket: one request and one reply per connection, each a 4-byte big-endian length followed by that many bytes of UTF-8 JSON, whose top level is an object.

`encode` makes a frame and `read_frame` reads one through a function that returns bytes, so nothing here knows about sockets. A frame is refused, with a typed error that has its own code and one constant message, before anything looks at its content: a length of 0 or above the limit is refused before a single payload byte is read, and no message ever holds a byte of the payload (it can be a password, or text written to end up in a log). A document in which an object repeats a key is refused, so that two parsers can never disagree about a field; so are the literals `NaN` and `Infinity` and a number too large for a float, which are not JSON.

The module uses the standard library only and keeps no state.
"""

import json
import math
import re
import struct
import time

from social_data_pipeline import control
from social_data_pipeline.control.errors import ControlError

# The largest request is the package's own limit; a reply can carry a piece of a log.
REQUEST_MAX_BYTES = control.REQUEST_MAX_BYTES
REPLY_MAX_BYTES = 16 * 1024 * 1024
HEADER_BYTES = 4
# The deepest nesting of objects and lists a frame may have; the top-level object is the first level.
DEPTH_MAX = 32
_HEADER = struct.Struct(">I")
_LENGTH_MAX = 0xFFFFFFFF


class FrameError(ControlError):
    """A frame was refused. Each subclass has its own `code` and one constant message."""

    code = "frame_error"
    text = "the frame was refused"

    def __init__(self):
        super().__init__(self.text)


class FrameNotSerialisable(FrameError):
    code = "frame_not_serialisable"
    text = "the document cannot be written as JSON"


class FrameTooLarge(FrameError):
    code = "frame_too_large"
    text = "the frame is larger than the limit"


class FrameEmpty(FrameError):
    code = "frame_empty"
    text = "the frame has a length of 0"


class FrameHeaderIncomplete(FrameError):
    code = "frame_header_incomplete"
    text = "the connection ended before the length of the frame was complete"


class FrameIncomplete(FrameError):
    code = "frame_incomplete"
    text = "the connection ended before the frame was complete"


class FrameTimeout(FrameError):
    code = "frame_timeout"
    text = "the frame did not arrive in time"


class FrameReadFailed(FrameError):
    code = "frame_read_failed"
    text = "the frame could not be read"


class FrameNotUtf8(FrameError):
    code = "frame_not_utf8"
    text = "the frame is not UTF-8"


class FrameNotJson(FrameError):
    code = "frame_not_json"
    text = "the frame is not JSON"


class FrameTooDeep(FrameError):
    code = "frame_too_deep"
    text = "the frame is nested deeper than the limit"


class FrameDuplicateKey(FrameError):
    code = "frame_duplicate_key"
    text = "an object of the frame repeats a key"


class FrameNotObject(FrameError):
    code = "frame_not_object"
    text = "the frame is not a JSON object"


class _Repeated(Exception):
    """Raised inside the JSON decoder for an object that repeats a key. Not a ValueError, so it is not taken for "not JSON"."""


def _object(pairs):
    found = dict(pairs)
    if len(found) != len(pairs):
        raise _Repeated()
    return found


# The characters that matter to the depth of a JSON text: a quote and a backslash (where strings begin and end) and the brackets. A class of single characters, which the regular expression engine finds without going back.
_STRUCTURE = re.compile(r'["\\\[\]{}]')


def _scan_depth(text, depth):
    """One pass over a JSON text, by one `finditer` over a class of single characters (which never goes back): (whether its objects and lists nest deeper than `depth`, outside strings; the number of steps taken). The steps are the characters that matter, at most one per character of the text, so the time is linear in the text whatever it holds (a string that never ends, quotes or backslashes only included). Brackets that do not match are not judged: only the parser can tell them apart from a document that is merely deep."""
    level, inside, escaped_at, steps = 0, False, -1, 0
    for found in _STRUCTURE.finditer(text):
        steps += 1
        position, character = found.start(), found.group()
        if inside:
            if position == escaped_at:
                continue
            if character == "\\":
                escaped_at = position + 1
            elif character == '"':
                inside = False
            continue
        if character == '"':
            inside = True
        elif character in "[{":
            level += 1
            if level > depth:
                return True, steps
        elif character in "]}":
            level -= 1
    return False, steps


def _deeper_than(text, depth):
    """Whether the objects and lists of a JSON text nest deeper than `depth`, found in one pass and without recursion."""
    return _scan_depth(text, depth)[0]


def _no_constant(_name):
    raise ValueError("not a JSON number")


def _finite(text):
    value = float(text)
    if not math.isfinite(value):
        raise ValueError("not a JSON number")
    return value


def _checked_limit(max_bytes):
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or not 1 <= max_bytes <= _LENGTH_MAX:
        raise ValueError("the limit of a frame is a whole number of bytes that fits its 4-byte length")
    return max_bytes


def _keys_are_text(document):
    """Whether every key of every object in the document is a string. The JSON encoder writes the keys 1 and "1" as the same key."""
    todo = [document]
    while todo:
        value = todo.pop()
        if isinstance(value, dict):
            if not all(isinstance(key, str) for key in value):
                return False
            todo.extend(value.values())
        elif isinstance(value, (list, tuple)):
            todo.extend(value)
    return True


def encode(document, *, max_bytes):
    """The frame of a document: its length and its JSON, as bytes. The document is a dict whose values the JSON encoder can write. Raises FrameNotObject for anything but a dict; FrameNotSerialisable for a value JSON has no form for (a number that is not finite, an object of another type, a key that is not a string, text that is not well-formed, a document that holds itself); FrameTooLarge for JSON of more than `max_bytes` bytes."""
    _checked_limit(max_bytes)
    if not isinstance(document, dict):
        raise FrameNotObject()
    try:
        # The encoder comes first: it is the one that finds a document that holds itself.
        payload = json.dumps(document, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError, RecursionError):
        raise FrameNotSerialisable() from None
    if not _keys_are_text(document):
        raise FrameNotSerialisable()
    if len(payload) > max_bytes:
        raise FrameTooLarge()
    return _HEADER.pack(len(payload)) + payload


def decode(payload):
    """The JSON object that the bytes `payload` hold. Raises FrameNotUtf8, FrameTooDeep (objects and lists nested more than 32 levels deep, the top level counted), FrameNotJson (also for `NaN`, `Infinity` and a number too large for a float), FrameDuplicateKey or FrameNotObject; no message holds a byte of the payload.

    The depth is found before the parser runs, in one pass whose work is linear in the text, without recursion. A text found too deep is still handed to the parser, which tells a deep document (FrameTooDeep, also when the parser itself runs out of recursion) from one whose brackets do not match (FrameNotJson)."""
    # Each error is raised after its `except` block has ended: raised inside it, the error would carry the decoder's own exception along, and that one holds the whole payload.
    text = None
    try:
        text = bytes(payload).decode("utf-8")
    except UnicodeDecodeError:
        pass
    if text is None:
        raise FrameNotUtf8()
    refusal = None
    deep = _deeper_than(text, DEPTH_MAX)
    try:
        document = json.loads(text, object_pairs_hook=_object, parse_constant=_no_constant, parse_float=_finite)
    except _Repeated:
        refusal = FrameTooDeep if deep else FrameDuplicateKey
    except RecursionError:
        refusal = FrameTooDeep
    except ValueError:
        # Not JSON, a literal that is not a JSON number, a whole number too long to convert.
        refusal = FrameNotJson
    if refusal is None and deep:
        refusal = FrameTooDeep
    if refusal is not None:
        raise refusal()
    if not isinstance(document, dict):
        raise FrameNotObject()
    return document


def _read_exactly(read, count, deadline, now, short):
    """Read `count` bytes through `read`, which returns at most as many bytes as it is asked for and b"" at the end. The clock is looked at before every read. Raises `short` when the end comes first."""
    data = bytearray()
    while len(data) < count:
        if now() >= deadline:
            raise FrameTimeout()
        try:
            chunk = read(count - len(data))
        except TimeoutError:
            raise FrameTimeout() from None
        except OSError:
            raise FrameReadFailed() from None
        if not chunk:
            raise short()
        if len(chunk) > count - len(data):
            # More than was asked for: the bytes after the frame are not this function's to take.
            raise FrameReadFailed()
        data += chunk
    return bytes(data)


def read_frame(read, *, max_bytes, deadline, now=time.monotonic):
    """Read one frame and return the JSON object it holds.

    `read(n)` returns up to n bytes and b"" at the end of the connection. `deadline` is a time of the clock `now` (seconds) by which the whole frame, its length included, must have arrived; the clock is looked at before every read and once more when the frame is complete. A `read` that waits on a socket should be given the time that is left as its timeout: a `TimeoutError` it raises is the deadline passing, any other `OSError` is "could not be read".

    Raises FrameHeaderIncomplete (the end before four bytes), FrameEmpty and FrameTooLarge (both before any byte of the payload is read), FrameIncomplete (the end before the payload was complete), FrameTimeout, FrameReadFailed, and what `decode` raises.
    """
    _checked_limit(max_bytes)
    header = _read_exactly(read, HEADER_BYTES, deadline, now, FrameHeaderIncomplete)
    (length,) = _HEADER.unpack(header)
    if length == 0:
        raise FrameEmpty()
    if length > max_bytes:
        raise FrameTooLarge()
    payload = _read_exactly(read, length, deadline, now, FrameIncomplete)
    if now() >= deadline:
        raise FrameTimeout()
    return decode(payload)
