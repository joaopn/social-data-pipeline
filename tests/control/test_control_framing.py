"""Tests for `control.framing`, the frames of the broker's socket: a 4-byte big-endian length and that many bytes of UTF-8 JSON whose top level is an object.

The contracts pinned here:

- A frame that was encoded is read back as the same document, however the bytes arrive, and the bytes after it are left alone.
- Every refusal has its own code and one constant message, and nothing of the payload is in the error: not in its message, not in its arguments, not in an exception chained to it.
- A length of 0 or above the limit is refused before a single byte of the payload is asked for.
- The deadline is one total for the whole frame, checked between reads, whatever the pace of the single reads.
- A repeated key, `NaN`, `Infinity`, a number too large for a float and a top level that is not an object are refused.
- The largest document that is allowed passes, and one byte more does not, on both sides.

The connection is a function that hands out prepared bytes and records every read; the clock is a counter.
"""

from __future__ import annotations

import json
import struct

import pytest

from social_data_pipeline import control
from social_data_pipeline.control import framing

MARKER = "made-up-payload-marker-41c7"
LIMIT = 1024


class Connection:
    """Prepared bytes, handed out at most `piece` at a time, with the size of every read that was asked for."""

    def __init__(self, data, piece=None, on_read=None):
        self.data = data
        self.position = 0
        self.piece = piece
        self.on_read = on_read
        self.reads = []

    def __call__(self, count):
        self.reads.append(count)
        if self.on_read is not None:
            self.on_read(len(self.reads))
        size = count if self.piece is None else min(count, self.piece)
        chunk = self.data[self.position:self.position + size]
        self.position += len(chunk)
        return chunk


def framed(payload):
    return struct.pack(">I", len(payload)) + payload


def read(data, *, piece=None, max_bytes=LIMIT):
    """Read one frame from prepared bytes with a clock that stands still; returns (document, connection)."""
    connection = Connection(data, piece)
    return framing.read_frame(connection, max_bytes=max_bytes, deadline=1, now=lambda: 0), connection


def refusal_of(call):
    """The FrameError a call raises."""
    with pytest.raises(framing.FrameError) as caught:
        call()
    return caught.value


def marked(text):
    return text.replace("MARKER", MARKER).encode("utf-8")


# Every way a frame is refused when it is read, as (the bytes on the connection, the error class, its code). The payloads hold a marker that must appear nowhere in the error.
READ_REFUSALS = [
    ("nothing at all", b"", framing.FrameHeaderIncomplete, "frame_header_incomplete"),
    ("three bytes of length", b"\x00\x00\x00", framing.FrameHeaderIncomplete, "frame_header_incomplete"),
    ("length 0", framed(b""), framing.FrameEmpty, "frame_empty"),
    ("length above the limit", struct.pack(">I", LIMIT + 1) + marked("MARKER"), framing.FrameTooLarge, "frame_too_large"),
    ("the largest length", b"\xff\xff\xff\xff" + marked("MARKER"), framing.FrameTooLarge, "frame_too_large"),
    ("end inside the payload", struct.pack(">I", 200) + marked('{"a": "MARKER'), framing.FrameIncomplete, "frame_incomplete"),
    ("end right after the length", struct.pack(">I", 5), framing.FrameIncomplete, "frame_incomplete"),
    ("not UTF-8", framed(marked('{"a": "MARKER') + b"\xff\xfe\"}"), framing.FrameNotUtf8, "frame_not_utf8"),
    ("a cut-off character", framed(marked('{"a": "MARKER') + b"\xe2\x82"), framing.FrameNotUtf8, "frame_not_utf8"),
    ("not JSON", framed(marked('{"a": "MARKER"')), framing.FrameNotJson, "frame_not_json"),
    ("text after the document", framed(marked('{"a": "MARKER"} x')), framing.FrameNotJson, "frame_not_json"),
    ("a byte order mark", framed(b"\xef\xbb\xbf" + marked('{"a": "MARKER"}')), framing.FrameNotJson, "frame_not_json"),
    ("NaN", framed(marked('{"a": "MARKER", "b": NaN}')), framing.FrameNotJson, "frame_not_json"),
    ("Infinity", framed(marked('{"a": "MARKER", "b": Infinity}')), framing.FrameNotJson, "frame_not_json"),
    ("-Infinity", framed(marked('{"a": "MARKER", "b": -Infinity}')), framing.FrameNotJson, "frame_not_json"),
    ("a number too large for a float", framed(marked('{"a": "MARKER", "b": 1e999}')), framing.FrameNotJson, "frame_not_json"),
    ("a repeated key", framed(marked('{"a": "MARKER", "a": 2}')), framing.FrameDuplicateKey, "frame_duplicate_key"),
    ("a repeated key in an inner object", framed(marked('{"a": "MARKER", "b": [{"c": 1, "c": 1}]}')), framing.FrameDuplicateKey, "frame_duplicate_key"),
    ("a top-level list", framed(marked('["MARKER"]')), framing.FrameNotObject, "frame_not_object"),
    ("a top-level string", framed(marked('"MARKER"')), framing.FrameNotObject, "frame_not_object"),
    ("a top-level null", framed(b"null"), framing.FrameNotObject, "frame_not_object"),
    ("a top-level number", framed(b"12"), framing.FrameNotObject, "frame_not_object"),
]


def assert_holds_nothing_of_the_payload(error):
    """The error and everything chained to it is free of the marker; the decoder's own exception, which holds the whole document, is not chained at all."""
    assert error.message == type(error).text == str(error)
    assert "\n" not in error.message
    assert MARKER not in error.message and MARKER not in repr(error) and MARKER not in repr(error.args)
    assert error.__cause__ is None and error.__context__ is None


class TestRoundTrip:
    @pytest.mark.parametrize("piece", [None, 1, 3, 7])
    def test_a_document_comes_back_however_the_bytes_arrive(self, piece):
        """Bug class: a reader that takes one `read` for the whole frame (a socket hands a frame over in parts), a length written little-endian or counted in characters instead of bytes, non-ASCII text escaped on one side and not on the other."""
        document = {"verb": "run.start", "fields": {"profile": "parse", "build": True, "limit": 3, "ratio": 0.5, "none": None}, "text": "gr\u00fc\u00dfe \u20ac", "list": [1, [2, {"k": "v"}]]}

        frame = framing.encode(document, max_bytes=LIMIT)
        back, connection = read(frame + b"the next frame", piece=piece)

        payload = json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        assert frame == len(payload).to_bytes(4, "big") + payload
        assert back == document
        # The bytes after the frame are not this reader's.
        assert connection.position == len(frame)

    @pytest.mark.parametrize("limit", [framing.REQUEST_MAX_BYTES, framing.REPLY_MAX_BYTES], ids=["request", "reply"])
    def test_the_largest_document_passes_and_one_byte_more_does_not(self, limit):
        """Bug class: a limit that is off by one or is applied to the characters, so that the largest reply the broker may send cannot be read by its own client, or a frame one byte over the limit is accepted."""
        room = limit - len('{"text":""}')
        largest = {"text": "a" * room}

        frame = framing.encode(largest, max_bytes=limit)
        back, _ = read(frame, max_bytes=limit)

        assert len(frame) == 4 + limit
        assert back == largest
        assert refusal_of(lambda: framing.encode({"text": "a" * (room + 1)}, max_bytes=limit)).code == "frame_too_large"
        assert refusal_of(lambda: read(framed(b"{" + b" " * (limit - 1) + b"}"), max_bytes=limit)).code == "frame_too_large"

    def test_the_limits_are_the_documented_ones(self):
        """Bug class: a second request limit that drifts from the one the other processes of the package use, or a reply limit changed without anyone noticing."""
        assert framing.REQUEST_MAX_BYTES == control.REQUEST_MAX_BYTES == 65536
        assert framing.REPLY_MAX_BYTES == 16 * 1024 * 1024


class TestRefusals:
    @pytest.mark.parametrize("data, error_class, code", [case[1:] for case in READ_REFUSALS], ids=[case[0] for case in READ_REFUSALS])
    def test_every_refusal_has_its_own_code_and_holds_nothing_of_the_payload(self, data, error_class, code):
        """Bug class: one catch-all error for everything (a client cannot tell "too large" from "not JSON"), a refusal that slips through as a document, and an error text built from the payload, which puts a password or a line someone crafted into a log."""
        error = refusal_of(lambda: read(data))

        assert type(error) is error_class
        assert error.code == code
        assert_holds_nothing_of_the_payload(error)

    @pytest.mark.parametrize("levels", [33, 40, 900, 100000])
    def test_a_document_nested_deeper_than_thirty_two_levels_is_refused_with_its_own_code(self, levels):
        """Bug class: no bound on nesting below the parser's own recursion limit, so that every later walk over a request (validation, redaction, the audit line) is handed hundreds of levels; and the parser's RecursionError, at the deep end, leaving the reader as itself with the whole payload in its traceback. Both a depth the parser takes (33 to 900) and one it does not (100000) are refused as too deep."""
        data = framed(marked('{"a": "MARKER", "b":') + b"[" * (levels - 1) + b"]" * (levels - 1) + b"}")

        error = refusal_of(lambda: read(data, max_bytes=framing.REPLY_MAX_BYTES))

        assert error.code == "frame_too_deep"
        assert_holds_nothing_of_the_payload(error)

    def test_thirty_two_levels_pass_and_brackets_inside_text_do_not_count(self):
        """Bug class: a bound that is off by one, that counts the top level or not as documented, or that counts brackets inside strings (a log line with `[[[[` in it would make a reply unreadable)."""
        lists = b'{"a":' + b"[" * 31 + b"]" * 31 + b"}"
        mixed = b"{" + b'"k":{' * 15 + b'"l":' + b"[" * 16 + b"]" * 16 + b"}" * 15 + b"}"
        deeper = b"{" + b'"k":{' * 15 + b'"l":' + b"[" * 17 + b"]" * 17 + b"}" * 15 + b"}"
        text = json.dumps({"a": "[{" * 5000 + "\\", "b": '"[[[[]]', "c": ["]]]]", "{{{{"]}).encode()
        # Shallow and wide: many pairs at each level, which a check that takes one pair at a time would read as deep.
        wide = json.dumps({"a": [[[]] for _ in range(500)], "b": [{"c": {}} for _ in range(500)]}).encode()
        # An escaped quote does not end a string: the brackets after it are still text.
        escaped = json.dumps({"a": '\\"' + "[" * 40 + '\\\\"' + "{" * 40, "b": "\\"}).encode()

        innermost = framing.decode(lists)["a"]
        for _ in range(30):
            innermost = innermost[0]
        assert innermost == []
        assert framing.decode(mixed)["k"]["k"]["k"]
        assert refusal_of(lambda: framing.decode(deeper)).code == "frame_too_deep"
        assert framing.decode(text) == json.loads(text)
        assert framing.decode(wide) == json.loads(wide)
        assert framing.decode(escaped) == json.loads(escaped)
        assert framing.DEPTH_MAX == 32

    @pytest.mark.parametrize("payload", [b'{"a": [1}', b'{"a": ]', b'{"a": {"b": [}]}', b'{"a":' + b"[" * 40 + b"}", b'{"a":' + b"]" * 40 + b"}", b'{"a": "unterminated [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[}'])
    def test_brackets_that_do_not_match_are_not_json_whatever_their_number(self, payload):
        """Bug class: the depth check reading a document whose brackets do not match as "too deep", which tells a client the wrong thing about a typing mistake."""
        assert refusal_of(lambda: framing.decode(payload)).code == "frame_not_json"

    @pytest.mark.parametrize("shape", ["unclosed-escaped-quotes", "quotes", "backslashes", "quote-backslash", "closers", "pairs", "string-of-brackets"])
    def test_the_depth_check_takes_linear_work_on_hostile_text(self, shape):
        """Bug class: a check before the parser that does more than linear work on some text. A pattern for strings that needs a closing quote tries a match at every quote of `"` followed by `\\"` repeated, runs each to the end and gives up: seconds of work for one frame of 64 KiB, before any login. Counted, not timed: the steps of the one pass are at most one per character, and grow with the text in proportion, at 1, 2 and 4 times the size."""
        def text(size):
            unit = {
                "unclosed-escaped-quotes": '\\"', "quotes": '"', "backslashes": "\\", "quote-backslash": '"\\',
                "closers": "]", "pairs": "[]", "string-of-brackets": "[{",
            }[shape]
            # The first two shapes are inside a string that never ends; the others are where structure is.
            start = '{"a":"' if shape in ("unclosed-escaped-quotes", "string-of-brackets") else '{"a":'
            return start + unit * (size // len(unit))

        steps = [framing._scan_depth(text(size), framing.DEPTH_MAX)[1] for size in (16384, 32768, 65536)]

        for size, counted in zip((16384, 32768, 65536), steps):
            assert counted <= len(text(size))
        assert steps[1] <= 2 * steps[0] + 8 and steps[2] <= 4 * steps[0] + 16
        # None of these is a JSON document, and each is answered so.
        assert refusal_of(lambda: framing.decode(text(65536).encode())).code == "frame_not_json"

    def test_the_codes_are_all_different(self):
        """Bug class: two refusals sharing a code after a copy and paste."""
        classes = [framing.FrameNotSerialisable, framing.FrameTooLarge, framing.FrameEmpty, framing.FrameHeaderIncomplete, framing.FrameIncomplete, framing.FrameTimeout, framing.FrameReadFailed, framing.FrameNotUtf8, framing.FrameNotJson, framing.FrameTooDeep, framing.FrameDuplicateKey, framing.FrameNotObject]

        assert len({cls.code for cls in classes}) == len(classes)
        assert len({cls.text for cls in classes}) == len(classes)
        assert all(issubclass(cls, framing.FrameError) for cls in classes)

    @pytest.mark.parametrize("length", [0, LIMIT + 1, 2**31, 2**32 - 1])
    @pytest.mark.parametrize("piece", [None, 1])
    def test_a_refused_length_costs_no_read_of_the_payload(self, length, piece):
        """Bug class: reading (or allocating) what the length announces before checking it, so that four bytes make the broker take in four gigabytes."""
        connection = Connection(struct.pack(">I", length) + b"x" * 64, piece)

        refusal_of(lambda: framing.read_frame(connection, max_bytes=LIMIT, deadline=1, now=lambda: 0))

        # Only the four bytes of the length were asked for and taken.
        assert connection.position == 4
        assert connection.reads == ([4] if piece is None else [4, 3, 2, 1])

    def test_a_length_at_the_limit_is_read(self):
        """Bug class: the check written as "at least the limit", which refuses the largest frame that is allowed."""
        payload = b"{" + b" " * (LIMIT - 2) + b"}"

        document, connection = read(framed(payload))

        assert document == {}
        assert connection.reads == [4, LIMIT]

    def test_a_read_that_returns_too_much_is_refused(self):
        """Bug class: trusting the read function to return at most what was asked for, and so swallowing the start of whatever follows the frame."""
        frame = framed(b'{"a":1}')

        error = refusal_of(lambda: framing.read_frame(lambda count: frame, max_bytes=LIMIT, deadline=1, now=lambda: 0))

        assert error.code == "frame_read_failed"

    @pytest.mark.parametrize("failure, code", [(TimeoutError("timed out"), "frame_timeout"), (ConnectionResetError(104, MARKER), "frame_read_failed"), (OSError(5, MARKER), "frame_read_failed")])
    def test_an_error_of_the_connection_is_a_refusal_with_a_constant_message(self, failure, code):
        """Bug class: a socket timeout or a reset connection leaving the reader as a raw OSError, which a server that handles FrameError does not catch."""
        def failing(count):
            raise failure

        error = refusal_of(lambda: framing.read_frame(failing, max_bytes=LIMIT, deadline=1, now=lambda: 0))

        assert error.code == code
        assert MARKER not in error.message and error.message == type(error).text


class TestDeadline:
    def test_the_deadline_is_a_total_for_the_whole_frame(self):
        """Bug class: a timeout per read instead of one for the frame. A client that sends one byte just inside every timeout would hold a connection, and the thread that serves it, for as long as it likes."""
        frame = framed(b'{"a":"' + b"x" * 40 + b'"}')
        clock = {"now": 0}

        def tick(_read_number):
            clock["now"] += 1

        connection = Connection(frame, piece=1, on_read=tick)

        error = refusal_of(lambda: framing.read_frame(connection, max_bytes=LIMIT, deadline=10, now=lambda: clock["now"]))

        assert error.code == "frame_timeout"
        # Every read took one second: ten of them fit before the deadline, the eleventh is not made.
        assert len(connection.reads) == 10

    def test_a_frame_that_is_complete_before_the_deadline_is_read(self):
        """Bug class: a deadline that fires early (compared with the wrong sign, or counted from the wrong start) and refuses frames that arrived in time."""
        frame = framed(b'{"a":1}')
        clock = {"now": 0}

        def tick(_read_number):
            clock["now"] += 1

        connection = Connection(frame, piece=4, on_read=tick)

        assert framing.read_frame(connection, max_bytes=LIMIT, deadline=4, now=lambda: clock["now"]) == {"a": 1}
        assert len(connection.reads) == 3

    def test_a_frame_that_is_complete_only_after_the_deadline_is_refused(self):
        """Bug class: the clock looked at only before a read, so that a last read which itself took too long still delivers its frame."""
        frame = framed(b'{"a":1}')
        clock = {"now": 0}

        def slow_last_read(read_number):
            if read_number == 2:
                clock["now"] = 99

        connection = Connection(frame, on_read=slow_last_read)

        error = refusal_of(lambda: framing.read_frame(connection, max_bytes=LIMIT, deadline=10, now=lambda: clock["now"]))

        assert error.code == "frame_timeout"

    def test_a_deadline_that_has_passed_costs_no_read(self):
        """Bug class: one read is always made before the clock is looked at, so a connection that is already out of time still gets to wait on the socket once."""
        connection = Connection(framed(b'{"a":1}'))

        error = refusal_of(lambda: framing.read_frame(connection, max_bytes=LIMIT, deadline=5, now=lambda: 5))

        assert error.code == "frame_timeout"
        assert connection.reads == []


class Unknown:
    """An object JSON has no form for; its repr carries the marker."""

    def __repr__(self):
        return MARKER


class TestEncode:
    @pytest.mark.parametrize("document", [[1, 2], "text", None, 12, ("a",)], ids=["list", "string", "null", "number", "tuple"])
    def test_only_an_object_is_encoded(self, document):
        """Bug class: the broker sending a reply its own reader refuses (a top-level list or string)."""
        assert refusal_of(lambda: framing.encode(document, max_bytes=LIMIT)).code == "frame_not_object"

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), Unknown(), {1, 2}, b"bytes", "\ud800 lone surrogate", {1: "a number as a key"}, {"inner": {None: 1}}], ids=["nan", "inf", "-inf", "object", "set", "bytes", "surrogate", "number-key", "null-key"])
    def test_what_json_has_no_form_for_is_refused(self, value):
        """Bug class: writing `NaN`, a key that is not text (the encoder turns 1 and "1" into the same key) or text no parser accepts, each of which the other side then refuses or, worse, reads differently."""
        error = refusal_of(lambda: framing.encode({"value": value}, max_bytes=LIMIT))

        assert error.code == "frame_not_serialisable"
        assert MARKER not in error.message and error.message == type(error).text

    def test_a_document_that_holds_itself_is_refused(self):
        """Bug class: an endless walk, or a RecursionError that leaves the encoder as itself."""
        document = {"a": []}
        document["a"].append(document)

        assert refusal_of(lambda: framing.encode(document, max_bytes=LIMIT)).code == "frame_not_serialisable"

    def test_a_document_over_the_limit_is_refused_by_its_bytes(self):
        """Bug class: the limit applied to the number of characters, which lets through three times the bytes for text outside ASCII."""
        fits = {"text": "\u20ac" * 300}
        too_large = {"text": "\u20ac" * 340}

        assert len(framing.encode(fits, max_bytes=LIMIT)) == 4 + len('{"text":""}') + 900
        assert refusal_of(lambda: framing.encode(too_large, max_bytes=LIMIT)).code == "frame_too_large"

    @pytest.mark.parametrize("limit", [0, -1, 2**32, True, 1.5, None, "1024"])
    def test_a_limit_the_length_cannot_hold_is_a_programming_error(self, limit):
        """Bug class: a limit above what four bytes can announce, with which a frame would be written whose length field has wrapped around."""
        with pytest.raises(ValueError):
            framing.encode({}, max_bytes=limit)
        with pytest.raises(ValueError):
            framing.read_frame(Connection(b""), max_bytes=limit, deadline=1, now=lambda: 0)
