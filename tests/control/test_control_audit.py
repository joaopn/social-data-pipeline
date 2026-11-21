"""Tests for `control.audit`, the audit log of the broker: `audit.jsonl` in the broker folder, one JSON object per line, each naming the hash of the line before it.

The contracts pinned here:

- The bytes of a line follow from its content: keys in a fixed order, no spaces, ASCII only, the hash of the previous line's bytes without its newline. Two lines are written out literally.
- The chain goes on across restarts of the writer; a cut-off last line is closed and marked `repaired`; an end that is not what the writer wrote last (appended to, cut, edited, replaced, removed) is marked `tail_changed` before the next line.
- A line is one `write` call. A line that cannot be written returns False, raises nothing and is counted; `writable()` says so before the next state-changing call, and the next `counts` line says how many were lost.
- Secrets never reach a line: a field is replaced by its type, not its name, and a `SecretValue` anywhere in a value is replaced.
- What is counted is written at most once a minute, as numbers only.
- `verify` finds a removed, edited, moved, inserted and hand-appended line and a cut-off end, and never raises, whatever the file holds; `read_tail` leaves out nothing without a trace.
- The file is 0600 in a folder 0700, never reached through a link, and has one writer.

The clock is a number the test sets; everything else is the real file system under a temp folder.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import signal
import stat
import tracemalloc

import pytest

from social_data_pipeline.control import audit, verbs
from social_data_pipeline.control.types import Name, Secret, SecretValue

MARKER = "made-up-secret-marker-b41d"
ZEROS = "0" * 64
# 2026-11-21T10:15:00Z
START = 1795256100

# The first two lines of a log, written out: a change of the key order, the separators, the time format or what is hashed is a change here.
LINE_1 = b'{"n":1,"t":"2026-11-21T10:15:00Z","session":null,"event":"broker_start","prev":"0000000000000000000000000000000000000000000000000000000000000000"}'
LINE_2 = b'{"n":2,"t":"2026-11-21T10:15:07Z","session":1,"event":"login","prev":"23c8b2e118c0a7f70c7cb5a80f29b364d1edcb5aaa70aff396f4aa7be55667c7"}'

# A verb of the test's own: a secret whose name sounds harmless, and a harmless field whose name sounds secret.
VERB = verbs.Verb(name="test.verb", cls="write", cli=("test",), socket_inputs={"note": verbs.Field(Secret()), "password_hint": verbs.Field(Name()), "source": verbs.Field(Name())})


class Clock:
    def __init__(self):
        self.now = START

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def folder(tmp_path):
    return tmp_path / "broker"


@pytest.fixture
def path(folder):
    return folder / "audit.jsonl"


@pytest.fixture
def log(folder, clock):
    """An open writer on an empty log; closed at the end of the test."""
    writer = audit.AuditLog(folder, now=clock)
    yield writer
    writer.close()


def lines_of(path):
    """The lines of the file as bytes, without their newlines; the file must end with a newline."""
    data = path.read_bytes()
    assert data == b"" or data.endswith(b"\n")
    return data.split(b"\n")[:-1]


def records_of(path):
    """The objects of the lines that are JSON objects; anything else in the file is passed over."""
    found = []
    for line in lines_of(path):
        try:
            record = json.loads(line)
        except ValueError:
            continue
        if isinstance(record, dict):
            found.append(record)
    return found


def events_of(path):
    return [record["event"] for record in records_of(path)]


def digest(line):
    return hashlib.sha256(line).hexdigest()


def write_some(log, count=6):
    """A few lines of several kinds."""
    kinds = [log.broker_start, lambda: log.login(1), lambda: log.call_refused(1, "run.start", "bad_input", "profile"), lambda: log.session_end(1, "expired"), lambda: log.login(2), lambda: log.logout(2)]
    for index in range(count):
        assert kinds[index % len(kinds)]() is True


def mode(path):
    return stat.S_IMODE(os.lstat(path).st_mode)


@contextlib.contextmanager
def failing_instead_of_hanging(seconds=5):
    def too_long(signum, frame):
        raise AssertionError("the call did not return: it waits on the named pipe")

    previous = signal.signal(signal.SIGALRM, too_long)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


# ── 1. the bytes of a line ──────────────────────────────────────────────────


class TestLines:
    def test_the_bytes_of_two_lines_are_exactly_these(self, log, path, clock):
        """Bug class: a line whose bytes do not follow from its content (keys in dict order of the day, spaces, local time, the hash taken with the newline or of the parsed object). Anyone who checks the chain with another program would then find every line broken."""
        log.broker_start()
        clock.now += 7.9
        log.login(1)

        assert path.read_bytes() == LINE_1 + b"\n" + LINE_2 + b"\n"
        assert digest(LINE_1) == json.loads(LINE_2)["prev"]

    def test_every_event_has_its_fields_in_a_fixed_order(self, log, path):
        """Bug class: an event written with other fields than documented, or in an order that changes with how the caller passed them."""
        log.broker_start()
        log.login(3)
        log.session_end(3, "password_changed")
        log.call_accepted(4, VERB, {"source": "demo"}, "session")
        log.call_ended(4, VERB, {"source": "demo"}, "session", "finished", 0)
        log.call_ended(4, VERB, {"source": "demo"}, "none", "failed", None)
        log.call_refused(None, "run.start", "unauthorized")
        log.call_refused(4, "run.start", "bad_input", "profile")
        log.step_up_failed(4)
        log.logout(4)
        log.count_read(4)
        log.broker_stop()

        records = records_of(path)
        assert [list(record) for record in records] == [
            ["n", "t", "session", "event", "prev"],
            ["n", "t", "session", "event", "prev"],
            ["n", "t", "session", "event", "reason", "prev"],
            ["n", "t", "session", "event", "verb", "fields", "db_password_from", "prev"],
            ["n", "t", "session", "event", "verb", "fields", "db_password_from", "outcome", "exit_code", "prev"],
            ["n", "t", "session", "event", "verb", "fields", "db_password_from", "outcome", "exit_code", "prev"],
            ["n", "t", "session", "event", "verb", "code", "field", "prev"],
            ["n", "t", "session", "event", "verb", "code", "field", "prev"],
            ["n", "t", "session", "event", "prev"],
            ["n", "t", "session", "event", "prev"],
            ["n", "t", "session", "event", "failed_logins", "unauthenticated", "reads", "reads_other", "lost", "prev"],
            ["n", "t", "session", "event", "prev"],
        ]
        assert [record["event"] for record in records] == ["broker_start", "login", "session_end", "call_accepted", "call_ended", "call_ended", "call_refused", "call_refused", "step_up_failed", "logout", "counts", "broker_stop"]
        assert [record["n"] for record in records] == list(range(1, 13))
        assert [record["session"] for record in records] == [None, 3, 3, 4, 4, 4, None, 4, 4, 4, None, None]
        assert records[4] == {**records[4], "verb": "test.verb", "fields": {"source": "demo"}, "db_password_from": "session", "outcome": "finished", "exit_code": 0}
        assert records[5]["exit_code"] is None and records[6]["field"] is None and records[7]["field"] == "profile"
        assert set(audit.EVENTS) == set(audit.EVENT_FIELDS) and len(audit.EVENTS) == 12

    def test_each_line_names_the_hash_of_the_line_before_it(self, log, path):
        """Bug class: a chain in name only (a constant, the hash of something else), which no later check can hold a line against."""
        write_some(log, 6)

        lines = lines_of(path)
        assert [json.loads(line)["prev"] for line in lines] == [ZEROS] + [digest(line) for line in lines[:-1]]
        assert all(line.isascii() and b" " not in line for line in lines)

    def test_text_outside_ascii_is_written_as_escapes(self, log, path):
        """Bug class: raw text of a request in the file, with which a line separator or a right-to-left mark inside a value changes what someone reading the log sees."""
        log.call_accepted(1, VERB, {"source": "caf\u00e9\u2028\u202e"}, "none")

        line = lines_of(path)[0]
        assert line.isascii() and b"caf\\u00e9\\u2028\\u202e" in line
        assert json.loads(line)["fields"] == {"source": "caf\u00e9\u2028\u202e"}

    @pytest.mark.parametrize("write", [
        lambda log: log.session_end(1, "bored"),
        lambda log: log.session_end(1, None),
        lambda log: log.login("1"),
        lambda log: log.login(0),
        lambda log: log.login(True),
        lambda log: log.login(1.0),
        lambda log: log.call_accepted(1, VERB, {"source": "demo"}, "somewhere"),
        lambda log: log.call_accepted(1, "test.verb", {"source": "demo"}, "none"),
        lambda log: log.call_accepted(1, verbs.Verb(name="a verb\nwith a line break", cls="write", cli=("test",)), {}, "none"),
        lambda log: log.call_accepted(1, VERB, ["source"], "none"),
        lambda log: log.call_ended(1, VERB, {}, "none", "it went\nwrong", 1),
        lambda log: log.call_ended(1, VERB, {}, "none", "finished", "0"),
        lambda log: log.call_ended(1, VERB, {}, "none", "finished", 256),
        lambda log: log.call_ended(1, VERB, {}, "none", "finished", True),
        lambda log: log.call_refused(1, "run.start", "a code with spaces"),
        lambda log: log.call_refused(1, "run.start", SecretValue(MARKER)),
        lambda log: log.call_accepted(1, VERB, {"source": "x" * 70000}, "none"),
    ], ids=["reason", "no-reason", "session-text", "session-zero", "session-bool", "session-float", "password-from", "verb-as-text", "verb-name", "fields-list", "outcome", "exit-code-text", "exit-code-range", "exit-code-bool", "code", "code-secret", "too-long"])
    def test_what_an_event_cannot_hold_is_not_written_and_does_not_raise(self, log, path, write):
        """Bug class: free text in a field that is a closed list or a number (the log would hold whatever a caller, or through it a client, put there), or an exception out of the audit call after the action it records has already run."""
        log.broker_start()

        assert write(log) is False

        assert events_of(path) == ["broker_start"]
        assert log.lost == 1
        assert MARKER.encode() not in path.read_bytes()

    def test_a_malformed_verb_or_field_name_of_a_refused_call_becomes_null(self, log, path):
        """Bug class: the text a client sent as the verb's name written into the log as it came: thousands of characters, or a line made to look like another event when the log is shown."""
        assert log.call_refused(None, "run.start\n{\"event\":\"login\"}", "unknown_verb", "a field\u2028name") is True
        assert log.call_refused(None, "x" * 65, "unknown_verb", 12) is True
        assert log.call_refused(None, None, "frame_not_json") is True
        assert log.call_refused(None, ["run.start"], "unknown_verb", SecretValue(MARKER)) is True

        records = records_of(path)
        assert [(record["verb"], record["field"]) for record in records] == [(None, None)] * 4
        assert MARKER.encode() not in path.read_bytes()


# ── 2. secrets ──────────────────────────────────────────────────────────────


class TestSecrets:
    def test_a_field_is_replaced_by_its_type_and_not_by_its_name(self):
        """Bug class: redaction by a list of names ("password", "token"). A secret field that is named otherwise is then written in clear, and a new secret field is forgotten the day it is added."""
        validated = {"note": SecretValue(MARKER), "password_hint": "blue", "source": "demo"}

        shown = audit.fields_for_audit(VERB, validated)

        assert shown == {"note": "<secret>", "password_hint": "blue", "source": "demo"}
        assert list(shown) == ["note", "password_hint", "source"]
        assert MARKER not in repr(shown)

    def test_a_secret_field_is_replaced_even_as_plain_text(self):
        """Bug class: relying on the value being wrapped. A caller that has already unwrapped the secret for the action, and passes that dict on, would write it."""
        shown = audit.fields_for_audit(VERB, {"note": MARKER, "source": "demo"})

        assert shown == {"note": "<secret>", "source": "demo"}

    def test_a_field_the_verb_does_not_list_is_treated_as_a_secret(self):
        """Bug class: a field whose type is not known written as it is. What is not known to be harmless is not written."""
        shown = audit.fields_for_audit(VERB, {"extra": MARKER, "source": "demo"})

        assert shown == {"extra": "<secret>", "source": "demo"}

    def test_the_registry_s_own_secret_fields_are_replaced(self, source_tree):
        """Bug class: the audit module and the registry disagreeing about what a secret type is, so that the real password fields of the real verbs pass."""
        for verb_name, fields in (("run.start", {"profile": "mongo_ingest", "source": "ok", "admin_password": MARKER}), ("db.start", {"service": "mongo", "admin_password": MARKER})):
            verb = verbs.VERBS[verb_name]

            validated = verbs.validate_request(verb_name, fields)
            # As the registry returns them (the secret wrapped), and as a caller holds them once it has unwrapped the secret for the action.
            for given in (validated, {**validated, "admin_password": validated["admin_password"].reveal()}):
                shown = audit.fields_for_audit(verb, given)

                assert shown["admin_password"] == "<secret>"
                assert MARKER not in json.dumps(shown)
                assert {name: value for name, value in shown.items() if name != "admin_password"} == {name: value for name, value in fields.items() if name != "admin_password"}

    def test_a_wrapped_secret_anywhere_in_a_value_never_reaches_the_file(self, log, path):
        """Bug class: only the top level of the fields looked at, so that a secret one level down (in a list, in an inner object) is written; or the wrapper written through its repr."""
        nested = {"source": {"inner": [1, "two", SecretValue(MARKER), {"deeper": SecretValue(MARKER)}], "tuple": (SecretValue(MARKER), "kept")}, "password_hint": "blue", "note": SecretValue(MARKER)}

        assert log.call_accepted(1, VERB, nested, "fields") is True
        assert log.call_ended(1, VERB, nested, "fields", "finished", 0) is True

        data = path.read_bytes()
        assert MARKER.encode() not in data
        assert records_of(path)[0]["fields"] == {"note": "<secret>", "password_hint": "blue", "source": {"inner": [1, "two", "<secret>", {"deeper": "<secret>"}], "tuple": ["<secret>", "kept"]}}

    def test_what_json_has_no_form_for_is_replaced_too(self):
        """Bug class: an object of an unknown type written through its repr, which is where a secret hides when someone wraps it in a class of their own."""
        class Unknown:
            def __repr__(self):
                return MARKER

        deep = [MARKER]
        for _ in range(40):
            deep = [deep]

        shown = audit.fields_for_audit(VERB, {"source": Unknown(), "password_hint": {"a": Unknown(), 1: "a key that is not text"}})
        too_deep = audit.fields_for_audit(VERB, {"source": deep})

        assert shown == {"source": "<secret>", "password_hint": "<secret>"}
        assert MARKER not in json.dumps(too_deep)

    def test_fields_that_are_not_a_dict_are_a_type_error_without_their_content(self):
        """Bug class: a traceback or message that quotes the fields it could not handle."""
        for fields in ([MARKER], MARKER, None, {1: MARKER}):
            with pytest.raises(TypeError) as caught:
                audit.fields_for_audit(VERB, fields)
            assert MARKER not in str(caught.value)


# ── 3. restarts, repairs, a changed end ─────────────────────────────────────


class TestTail:
    def test_the_chain_goes_on_across_restarts_of_the_writer(self, folder, path, clock):
        """Bug class: a writer that starts every time at number 1 with 64 zeros, so that the log is as many unconnected chains as the broker had starts, and lines can be removed at every seam."""
        for _ in range(3):
            with audit.AuditLog(folder, now=clock) as log:
                write_some(log, 3)
            clock.now += 3600

        report = audit.verify(path)
        assert [record["n"] for record in records_of(path)] == list(range(1, 10))
        assert (report.ok, report.lines, report.breaks, report.repaired, report.tail_changed) == (True, 9, 0, 0, 0)

    def test_a_cut_off_last_line_is_closed_and_marked_when_the_writer_opens(self, folder, path, clock):
        """Bug class: the next line glued onto the half-written one after a crash. Both are then lost: neither is a line any more, and the chain breaks for good."""
        with audit.AuditLog(folder, now=clock) as log:
            write_some(log, 3)
        whole = lines_of(path)
        path.write_bytes(b"\n".join(whole[:2]) + b"\n" + whole[2][:40])

        with audit.AuditLog(folder, now=clock) as log:
            log.login(5)

        lines = lines_of(path)
        records = [json.loads(line) for line in lines[3:]]
        assert lines[:3] == [whole[0], whole[1], whole[2][:40]]
        assert [(record["n"], record["event"]) for record in records] == [(3, "repaired"), (4, "login")]
        assert records[0]["prev"] == digest(whole[2][:40])
        report = audit.verify(path)
        assert (report.ok, report.lines, report.breaks, report.repaired, report.cut_off) == (True, 5, 0, 1, False)

    def test_a_last_line_that_only_lacks_its_newline_is_kept_as_a_line(self, folder, path, clock):
        """Bug class: a complete line thrown away, or counted as damage, because the crash came between its last byte and its newline."""
        with audit.AuditLog(folder, now=clock) as log:
            write_some(log, 3)
        path.write_bytes(path.read_bytes()[:-1])

        with audit.AuditLog(folder, now=clock) as log:
            log.login(5)

        assert [(record["n"], record["event"]) for record in records_of(path)][2:] == [(3, "call_refused"), (4, "repaired"), (5, "login")]
        assert audit.verify(path).ok

    @pytest.mark.parametrize("change", ["appended", "appended-without-newline", "truncated", "truncated-inside-a-line", "truncated-to-nothing", "edited", "removed", "replaced"])
    def test_an_end_that_is_not_what_the_writer_wrote_is_marked_before_the_next_line(self, log, folder, path, change):
        """Bug class: a writer that goes on as if nothing happened after someone appended to, cut or rewrote the log behind its back. The next line would chain onto a line that is no longer there, and nothing in the log would say when that happened."""
        write_some(log, 4)
        before = lines_of(path)
        if change == "appended":
            path.write_bytes(path.read_bytes() + b'{"n":5,"t":"2026-11-21T10:15:00Z","session":9,"event":"login","prev":"' + digest(before[3]).encode() + b'"}\n')
        elif change == "appended-without-newline":
            path.write_bytes(path.read_bytes() + b"scribble")
        elif change == "truncated":
            path.write_bytes(b"\n".join(before[:2]) + b"\n")
        elif change == "truncated-inside-a-line":
            path.write_bytes(b"\n".join(before[:2]) + b"\n" + before[2][:25])
        elif change == "truncated-to-nothing":
            path.write_bytes(b"")
        elif change == "edited":
            path.write_bytes(b"\n".join(before[:3]) + b"\n" + before[3].replace(b'"session":1', b'"session":7') + b"\n")
        elif change == "removed":
            os.unlink(path)
        elif change == "replaced":
            (folder / "other").write_bytes(b"\n".join(before[:3]) + b"\n")
            os.replace(folder / "other", path)

        assert log.login(8) is True

        lines = lines_of(path)
        records = records_of(path)
        events = [record["event"] for record in records]
        marked = events.index("tail_changed")
        assert events[marked:] == ["tail_changed", "login"] and events.count("tail_changed") == 1
        # The writer's own numbering goes on: 5 for the mark (6 when a repair came first), whatever the file now holds.
        expected_repair = change in ("appended-without-newline", "truncated-inside-a-line")
        assert ("repaired" in events) == expected_repair
        assert [record["n"] for record in records[marked:]] == ([6, 7] if expected_repair else [5, 6])
        # The mark is chained onto whatever line is the last one now.
        position = lines.index(json.dumps(records[marked], separators=(",", ":")).encode())
        assert records[marked]["prev"] == (digest(lines[position - 1]) if position else ZEROS)
        assert records[-1]["session"] == 8 and records[-1]["prev"] == digest(lines[position])
        if change == "removed":
            assert mode(path) == 0o600
        assert log.writable() is True

    def test_an_unchanged_end_is_not_marked(self, log, path):
        """Bug class: a check that always finds a difference (comparing with the newline on one side only, a size that is off by one) and fills the log with false alarms."""
        write_some(log, 6)
        log.count_read(1)
        log.flush_counts(force=True)
        write_some(log, 6)

        assert "tail_changed" not in events_of(path) and "repaired" not in events_of(path)
        assert audit.verify(path).ok

    def test_a_new_writer_marks_a_last_line_that_no_writer_wrote(self, folder, path, clock):
        """Bug class: a writer that starts on a log whose end someone scribbled on between two runs of the broker, and chains onto the scribble without a word."""
        with audit.AuditLog(folder, now=clock) as log:
            write_some(log, 3)
        path.write_bytes(path.read_bytes() + b"scribbled while the broker was down\n")

        with audit.AuditLog(folder, now=clock) as log:
            log.login(5)

        records = records_of(path)[3:]
        assert [(record["n"], record["event"]) for record in records] == [(4, "tail_changed"), (5, "login")]
        assert records[0]["prev"] == digest(b"scribbled while the broker was down")
        report = audit.verify(path)
        assert (report.breaks, report.first_break, report.tail_changed) == (1, 4, 1)


# ── 4. a line that cannot be written ────────────────────────────────────────


class TestFailures:
    def test_a_write_that_fails_returns_false_raises_nothing_and_is_counted(self, log, path, monkeypatch):
        """Bug class: an exception out of the audit call. It is made after the action ran: the caller's error handling would then report, or roll back, an action that happened. And a failure nobody can see: the caller must be able to read how many lines are missing."""
        log.broker_start()
        calls = []

        def no_space(fd, data):
            calls.append(len(data))
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(os, "write", no_space)
        results = [log.login(1), log.call_accepted(1, VERB, {"source": "demo"}, "none"), log.call_ended(1, VERB, {"source": "demo"}, "none", "finished", 0), log.logout(1)]
        not_writable = log.writable()
        monkeypatch.undo()

        assert results == [False, False, False, False]
        assert log.lost == 4 and not_writable is False
        # Four lines were tried, and the probe of writable() tried its one: nothing else.
        assert len(calls) == 5
        assert events_of(path) == ["broker_start"]

    def test_writable_says_no_until_a_line_can_be_written_again_and_the_log_then_says_what_was_lost(self, log, path, monkeypatch):
        """Bug class: a state-changing call that runs while its record cannot be written (the one thing the audit log is for), a log that stays "not writable" for ever after one bad moment, and lost lines that leave no trace in the log itself."""
        log.broker_start()
        assert log.writable() is True
        monkeypatch.setattr(os, "write", lambda fd, data: (_ for _ in ()).throw(OSError(28, "No space left on device")))
        log.login(1)
        log.logout(1)
        assert log.writable() is False and log.writable() is False
        monkeypatch.undo()

        assert log.writable() is True
        assert log.login(2) is True

        records = records_of(path)
        assert [record["event"] for record in records] == ["broker_start", "counts", "login"]
        assert records[1] == {**records[1], "failed_logins": 0, "unauthenticated": 0, "reads": {}, "lost": 2}
        # The probes that failed are not themselves counted as lost lines.
        assert log.lost == 2
        assert audit.verify(path).ok
        log.count_failed_login()
        log.flush_counts(force=True)
        assert records_of(path)[-1]["lost"] == 0

    def test_a_write_that_stops_inside_a_line_is_repaired_before_the_next_line(self, log, path, monkeypatch):
        """Bug class: the next line glued onto the half of a line that a full disk let through; or that half taken for someone else's change of the log."""
        log.broker_start()
        real_write = os.write
        monkeypatch.setattr(os, "write", lambda fd, data: real_write(fd, data[:30]))
        cut = log.login(1)
        monkeypatch.undo()

        assert cut is False and log.lost == 1
        assert log.login(2) is True

        lines = lines_of(path)
        records = [json.loads(line) for line in lines[2:]]
        assert len(lines[1]) == 30 and lines[1].startswith(b'{"n":2,')
        assert [(record["n"], record["event"]) for record in records] == [(2, "repaired"), (3, "login")]
        assert records[0]["prev"] == digest(lines[1])
        assert audit.verify(path).ok and audit.verify(path).repaired == 1

    @pytest.mark.parametrize("cut_writes", [2, 3])
    def test_short_writes_in_a_row_leave_one_run_that_one_repaired_line_closes(self, log, path, monkeypatch, cut_writes):
        """Bug class: a full disk that cuts the `repaired` line itself as well. The file then holds two (or more) fragments before one `repaired` line, and a check that excuses only one fragment reports breaks nobody caused."""
        log.broker_start()
        real_write, cut = os.write, []

        def cutting(fd, data):
            if data.startswith(b'{"n"') and len(cut) < cut_writes:
                cut.append(len(data))
                return real_write(fd, data[:30])
            return real_write(fd, data)

        monkeypatch.setattr(os, "write", cutting)
        results = [log.login(number) for number in range(1, cut_writes + 1)]
        monkeypatch.undo()
        assert log.login(9) is True

        lines = lines_of(path)
        records = records_of(path)
        assert results == [False] * cut_writes and len(cut) == cut_writes
        assert len(lines) == 1 + cut_writes + 2 and all(len(line) == 30 for line in lines[1:1 + cut_writes])
        assert [(record["n"], record["event"]) for record in records] == [(1, "broker_start"), (2, "repaired"), (3, "login")]
        assert records[1]["prev"] == digest(lines[cut_writes]) and records[1]["after"] == digest(lines[0])
        report = audit.verify(path)
        assert (report.ok, report.lines, report.breaks, report.repaired, report.tail_changed) == (True, 1 + cut_writes + 2, 0, 1, 0)

    @pytest.mark.parametrize("scribble", ["a whole line", "a line without its newline"])
    def test_a_short_write_of_a_tail_changed_line_is_repaired_before_it_is_written_again(self, log, path, monkeypatch, scribble):
        """Bug class: the repair put after a `tail_changed` line that is still owed. When a full disk cuts the `tail_changed` line itself (after a hand-made change of the log, or after the `repaired` line that a scribble without a newline needed), the file then reads fragment, `tail_changed`, `repaired`, and the writer's own log fails its check."""
        log.broker_start()
        path.write_bytes(path.read_bytes() + (b"scribbled\n" if scribble == "a whole line" else b"scribbled"))
        real_write, lines_seen = os.write, []

        def cutting(fd, data):
            if data.startswith(b'{"n"'):
                lines_seen.append(data)
                if b'"event":"tail_changed"' in data and len(lines_seen) <= 2:
                    return real_write(fd, data[:30])
            return real_write(fd, data)

        monkeypatch.setattr(os, "write", cutting)
        cut = log.login(1)
        monkeypatch.undo()
        assert log.login(2) is True

        events = [record["event"] for record in records_of(path)]
        assert cut is False
        assert events == (["broker_start", "repaired", "tail_changed", "login"] if scribble == "a whole line" else ["broker_start", "repaired", "repaired", "tail_changed", "login"])
        report = audit.verify(path)
        assert report.ok and report.breaks == 0 and report.tail_changed == 1

    def test_a_fragment_in_a_file_that_was_replaced_is_not_the_writer_s_own(self, log, folder, path, monkeypatch):
        """Bug class: a writer that remembers having cut a line in the old file and, once the log was replaced under its name, takes the end of the new file for its own: no `tail_changed` line, and the new file's last number taken over as if it were the line that was cut."""
        log.broker_start()
        log.login(1)
        real_write = os.write
        monkeypatch.setattr(os, "write", lambda fd, data: real_write(fd, data[:30]))
        cut = log.login(2)
        monkeypatch.undo()
        whole = path.read_bytes().split(b"\n")[:2]
        planted = audit._line(3, START, 9, "login", {}, digest(whole[1]))
        (folder / "other").write_bytes(b"\n".join(whole + [planted]) + b"\n")
        os.replace(folder / "other", path)

        assert cut is False and log.login(4) is True

        records = records_of(path)
        assert [(record["n"], record["event"], record["session"]) for record in records[3:]] == [(3, "tail_changed", None), (4, "login", 4)]

    def test_a_changed_line_next_to_a_repaired_run_is_still_found(self, log, path, monkeypatch):
        """Bug class: a run of fragments that hides the line before it. Without a successor that names that line's hash, it could be edited at will; the `repaired` line names it in `after`. The lines after the repair are chained as usual."""
        log.broker_start()
        log.login(1)
        real_write, cut = os.write, []

        def cutting(fd, data):
            if data.startswith(b'{"n"') and len(cut) < 2:
                cut.append(1)
                return real_write(fd, data[:30])
            return real_write(fd, data)

        monkeypatch.setattr(os, "write", cutting)
        log.logout(1)
        log.logout(1)
        monkeypatch.undo()
        log.login(2)
        log.login(3)
        lines = lines_of(path)
        assert [json.loads(line)["event"] for line in lines if not len(line) == 30] == ["broker_start", "login", "repaired", "login", "login"]
        assert audit.verify(path).ok

        before_the_run = lines[1].replace(b'"session":1', b'"session":5')
        rewrite(path, [lines[0], before_the_run] + lines[2:])
        changed_before = audit.verify(path)
        after_the_run = lines[5].replace(b'"session":2', b'"session":6')
        rewrite(path, lines[:5] + [after_the_run] + lines[6:])
        changed_after = audit.verify(path)
        fragment = lines[2][:-1] + b"X"
        rewrite(path, lines[:2] + [fragment] + lines[3:])
        changed_fragment = audit.verify(path)

        # Before the run: the repair does not close it, so its two lines are breaks at the number expected there.
        assert (changed_before.breaks, changed_before.first_break, changed_before.repaired) == (2, 3, 0)
        assert (changed_after.breaks, changed_after.first_break) == (1, 5)
        # The first fragment is only in the run: changing it is not visible. The last one is named by the repair's `prev`.
        assert changed_fragment.ok
        rewrite(path, lines[:3] + [lines[3][:-1] + b"X"] + lines[4:])
        assert (audit.verify(path).breaks, audit.verify(path).first_break) == (2, 3)

    def test_a_new_writer_closes_a_run_of_fragments_left_by_a_crash(self, folder, path, clock):
        """Bug class: a writer that, opening after a crash, names in its `repaired` line the fragment before the cut-off one instead of the last real line, so that its own repair reads as a break."""
        with audit.AuditLog(folder, now=clock) as log:
            write_some(log, 3)
        whole = lines_of(path)
        path.write_bytes(b"\n".join(whole) + b"\n" + whole[2][:20] + b"\n" + whole[2][:40])

        with audit.AuditLog(folder, now=clock) as log:
            log.login(5)

        records = records_of(path)
        assert [(record["n"], record["event"]) for record in records][3:] == [(4, "repaired"), (5, "login")]
        assert records[3]["after"] == digest(whole[2])
        report = audit.verify(path)
        assert (report.ok, report.repaired, report.breaks) == (True, 1, 0)

    def test_a_write_that_lacks_only_its_newline_leaves_a_whole_line_with_its_number(self, log, path, monkeypatch):
        """Bug class: the number of a line that did reach the file handed out a second time, because the write was one byte short and counted as failed. The chain would then show a break that nobody caused."""
        log.broker_start()
        real_write = os.write
        monkeypatch.setattr(os, "write", lambda fd, data: real_write(fd, data[:-1]))
        cut = log.login(1)
        monkeypatch.undo()

        assert cut is False and log.lost == 1
        assert log.login(2) is True

        assert [(record["n"], record["event"], record["session"]) for record in records_of(path)] == [(1, "broker_start", None), (2, "login", 1), (3, "repaired", None), (4, "login", 2)]
        report = audit.verify(path)
        assert (report.ok, report.lines, report.repaired, report.tail_changed) == (True, 4, 0, 0)

    def test_a_line_that_was_written_and_not_synced_is_reported_as_not_written(self, log, path, monkeypatch):
        """Bug class: "written" answered for a line that may not survive a power cut, on which the caller then lets a call run."""
        calls = []

        def no_sync(fd):
            calls.append(fd)
            raise OSError(5, "Input/output error")

        monkeypatch.setattr(os, "fsync", no_sync)
        result = log.broker_start()
        monkeypatch.undo()

        assert result is False and len(calls) == 1 and log.lost == 1
        # The line is in the file, and the chain goes on from it.
        assert log.login(1) is True
        assert events_of(path)[:2] == ["broker_start", "login"] and audit.verify(path).breaks == 0

    def test_every_line_is_synced(self, log, monkeypatch):
        """Bug class: lines left in the page cache, where a crash right after a destructive call takes the record of that call with it."""
        synced = []
        real_fsync = os.fsync
        monkeypatch.setattr(os, "fsync", lambda fd: (synced.append(fd), real_fsync(fd))[1])

        write_some(log, 4)

        assert len(synced) == 4 and len(set(synced)) == 1

    def test_a_closed_writer_writes_nothing_and_says_so(self, folder, path, clock):
        """Bug class: a write to a descriptor that was closed (and whose number another file may have got since), or a closed log that still reports itself writable."""
        log = audit.AuditLog(folder, now=clock)
        log.broker_start()
        log.close()
        log.close()

        assert log.writable() is False
        assert log.login(1) is False and log.writable() is False and log.flush_counts(force=True) is False
        assert log.lost == 1 and events_of(path) == ["broker_start"]

    def test_one_write_call_per_line(self, log, path, monkeypatch):
        """Bug class: a line written in parts (the object, then the newline). Another process's append, or a crash, can land between the parts, and a reader sees half a line."""
        written = []
        real_write = os.write

        def recording_write(fd, data):
            written.append(bytes(data))
            return real_write(fd, data)

        monkeypatch.setattr(os, "write", recording_write)
        write_some(log, 6)
        log.count_unauthenticated()
        log.flush_counts(force=True)
        monkeypatch.undo()

        assert written == [line + b"\n" for line in lines_of(path)]
        assert len(written) == 7 and all(data.count(b"\n") == 1 for data in written)


# ── 5. counts ───────────────────────────────────────────────────────────────


class TestCounts:
    def test_counts_are_written_at_most_once_a_minute(self, log, path, clock):
        """Bug class: a line per failed login or per unauthenticated request. Whoever floods the socket would then fill the disk through the audit log, and the lines that matter drown."""
        for _ in range(1000):
            log.count_failed_login()
            log.count_unauthenticated()
            log.count_read(2)
            assert log.flush_counts() is False
        clock.now += 59.999
        assert log.flush_counts() is False
        clock.now += 0.001
        assert log.flush_counts() is True
        log.count_read(3)
        log.count_read(11)
        log.count_read(2)
        clock.now += 59.999
        assert log.flush_counts() is False
        clock.now += 0.001
        assert log.flush_counts() is True
        clock.now += 600
        assert log.flush_counts() is False and log.flush_counts(force=True) is False

        records = records_of(path)
        assert [(record["event"], record["failed_logins"], record["unauthenticated"], record["reads"], record["lost"]) for record in records] == [("counts", 1000, 1000, {"2": 1000}, 0), ("counts", 0, 0, {"2": 1, "3": 1, "11": 1}, 0)]
        assert list(records[1]["reads"]) == ["2", "3", "11"]
        assert [record["session"] for record in records] == [None, None]

    def test_a_counts_line_holds_only_numbers(self, log, path):
        """Bug class: anything a client sent (a user name that was tried, a token, a verb) in the one line that is written for requests nobody authenticated."""
        log.count_failed_login()
        log.count_unauthenticated()
        log.count_read(1)
        log.flush_counts(force=True)

        record = records_of(path)[0]
        values = [record["failed_logins"], record["unauthenticated"], record["reads_other"], record["lost"], *record["reads"].values()]
        assert set(record) == {"n", "t", "session", "event", "failed_logins", "unauthenticated", "reads", "reads_other", "lost", "prev"}
        assert all(type(value) is int for value in values) and all(key.isdigit() for key in record["reads"])

    @pytest.mark.parametrize("number", [None, "1", 0, -1, True, 1.5, MARKER])
    def test_a_read_is_counted_for_a_session_number_only(self, log, number):
        """Bug class: the count keyed by whatever the caller passed (a token, a user's text), which would then be written as a key of the counts line."""
        with pytest.raises(TypeError) as caught:
            log.count_read(number)

        assert MARKER not in str(caught.value)
        assert log.flush_counts(force=True) is False

    def test_the_stop_writes_what_was_counted_first(self, log, path):
        """Bug class: the counts since the last minute lost at every stop, which is exactly when a broker that was attacked is restarted."""
        log.broker_start()
        log.count_failed_login()

        assert log.broker_stop() is True
        assert events_of(path) == ["broker_start", "counts", "broker_stop"]

    def test_a_counts_line_names_at_most_sixty_four_sessions_and_totals_the_rest(self, log, path):
        """Bug class: a map of reads per session with no bound. With enough sessions the line is longer than a line may be and fails on every try, and after one lost line `writable()` stays false until the broker restarts: every state-changing call is refused for good."""
        for number in range(1, 101):
            for _ in range(number % 7 + 1):
                log.count_read(number)
        log.count_read(5)

        assert log.flush_counts(force=True) is True

        record = records_of(path)[0]
        counted = {number: number % 7 + 1 + (number == 5) for number in range(1, 101)}
        named = sorted(sorted(counted, key=lambda number: (-counted[number], number))[:64])
        assert record["reads"] == {str(number): counted[number] for number in named}
        assert list(record["reads"]) == [str(number) for number in named]
        assert record["reads_other"] == sum(counted.values()) - sum(record["reads"].values())
        assert audit.COUNTS_SESSIONS_MAX == 64

    def test_the_counts_line_always_fits_so_writable_comes_back(self, log, path, monkeypatch):
        """Bug class: the scenario the bound is for. Ten thousand sessions with the largest numbers, one lost line, and then `writable()` must be true again, because its probe, the counts line, fits."""
        for number in range(2**53 - 10000, 2**53):
            log.count_read(number)
        monkeypatch.setattr(os, "write", lambda fd, data: (_ for _ in ()).throw(OSError(28, "No space left on device")))
        lost = log.login(1)
        monkeypatch.undo()

        assert lost is False and log.writable() is True
        record = records_of(path)[0]
        assert len(record["reads"]) == 64 and record["reads_other"] == 10000 - 64 and record["lost"] == 1
        assert len(lines_of(path)[0]) < audit.LINE_MAX_BYTES

    def test_a_counts_line_with_more_than_sixty_four_sessions_cannot_be_made(self):
        """Bug class: the bound held by the writer only, so that any other way of making a counts line (a later change of the writer) can make one that never fits again."""
        reads = {str(number): 1 for number in range(1, 66)}

        with pytest.raises(ValueError):
            audit._line(1, START, None, "counts", {"failed_logins": 0, "unauthenticated": 0, "reads": reads, "reads_other": 0, "lost": 0}, ZEROS)
        assert audit._line(1, START, None, "counts", {"failed_logins": 0, "unauthenticated": 0, "reads": dict(list(reads.items())[:64]), "reads_other": 1, "lost": 0}, ZEROS)

    @pytest.mark.parametrize("number", [2**53 + 1, 10**100])
    def test_a_session_number_beyond_what_json_keeps_exactly_is_refused(self, log, number):
        """Bug class: a session number so large that one key of the counts line is longer than a line."""
        with pytest.raises(TypeError):
            log.count_read(number)

    def test_counts_that_could_not_be_written_are_kept_for_the_next_try(self, log, path, clock, monkeypatch):
        """Bug class: counters set back to zero before the line is safely written, so that a failed write loses them."""
        log.count_failed_login()
        log.count_read(4)
        monkeypatch.setattr(os, "write", lambda fd, data: (_ for _ in ()).throw(OSError(28, "No space left on device")))
        failed = log.flush_counts(force=True)
        monkeypatch.undo()
        log.count_failed_login()

        assert failed is False
        assert log.flush_counts(force=True) is True
        record = records_of(path)[0]
        assert (record["failed_logins"], record["reads"], record["lost"]) == (2, {"4": 1}, 0)


# ── 6. the file ─────────────────────────────────────────────────────────────


class TestFile:
    @pytest.mark.parametrize("mask", [0o000, 0o077])
    def test_the_folder_and_the_file_are_private_whatever_the_umask(self, folder, path, clock, mask):
        """Bug class: an audit log every user of the host can read (it names verbs, sources and times) or, with a permissive umask, write."""
        previous = os.umask(mask)
        try:
            with audit.AuditLog(folder, now=clock) as log:
                log.broker_start()
        finally:
            os.umask(previous)

        assert mode(folder) == 0o700 and mode(path) == 0o600

    def test_the_log_is_appended_to_and_never_rewritten(self, folder, path, clock, monkeypatch):
        """Bug class: a file opened so that its earlier lines can be overwritten (no append mode, a truncating open), which turns one bug into the loss of the whole history."""
        with audit.AuditLog(folder, now=clock) as log:
            write_some(log, 3)
        before = path.read_bytes()
        flags = []
        real_open = os.open

        def recording_open(name, how, *args, **kwargs):
            if name == "audit.jsonl":
                flags.append(how)
            return real_open(name, how, *args, **kwargs)

        monkeypatch.setattr(os, "open", recording_open)
        with audit.AuditLog(folder, now=clock) as log:
            write_some(log, 3)
        monkeypatch.undo()

        assert path.read_bytes().startswith(before) and len(lines_of(path)) == 6
        assert flags and all(how & os.O_APPEND and how & os.O_NOFOLLOW and how & os.O_NONBLOCK and not how & os.O_TRUNC for how in flags)

    def test_a_second_writer_is_refused_while_the_first_lives(self, folder, path, clock):
        """Bug class: two brokers (or a broker and a forgotten one) both appending. Each chains onto its own idea of the last line, and the log is two interleaved chains that verify as neither."""
        first = audit.AuditLog(folder, now=clock)
        first.broker_start()

        with pytest.raises(audit.AuditInUse) as caught:
            audit.AuditLog(folder, now=clock)
        first.close()
        with audit.AuditLog(folder, now=clock) as second:
            second.broker_start()

        assert caught.value.code == "audit_in_use" and caught.value.message == "the audit log is held by another writer"
        assert events_of(path) == ["broker_start", "broker_start"] and audit.verify(path).ok

    @pytest.mark.parametrize("planted", ["link", "dangling-link", "pipe", "subfolder", "linked-folder", "file-as-folder"])
    def test_a_link_or_a_special_file_in_the_log_s_place_is_refused_without_waiting(self, folder, tmp_path, clock, planted):
        """Bug class: the audit log written through a link into a file elsewhere (as the user the broker runs as), or a writer that waits for ever on a named pipe and never starts."""
        elsewhere = tmp_path / "elsewhere"
        elsewhere.write_bytes(b"")
        if planted in ("linked-folder", "file-as-folder"):
            (tmp_path / "real").mkdir()
            folder.symlink_to(tmp_path / "real") if planted == "linked-folder" else folder.write_bytes(b"")
        else:
            folder.mkdir(mode=0o700)
            target = folder / "audit.jsonl"
            {"link": lambda: target.symlink_to(elsewhere), "dangling-link": lambda: target.symlink_to(tmp_path / "nowhere"), "pipe": lambda: os.mkfifo(target), "subfolder": target.mkdir}[planted]()

        with failing_instead_of_hanging(), pytest.raises(audit.AuditNotUsable) as caught:
            audit.AuditLog(folder, now=clock)

        assert caught.value.code == "audit_not_usable" and caught.value.message == "the audit log cannot be used"
        assert str(tmp_path) not in caught.value.message
        # Nothing was written through a link: not into the file a link names, and not into the folder a linked folder names.
        assert elsewhere.read_bytes() == b"" and not (tmp_path / "nowhere").exists() and not (tmp_path / "real" / "audit.jsonl").exists()

    @pytest.mark.parametrize("foreign", ["folder", "file"])
    def test_a_log_of_another_user_is_refused(self, folder, path, clock, foreign):
        """Bug class: appending to, and trusting the end of, a log that another user of the host created, or one in a folder of another user, who can swap the file at will."""
        with audit.AuditLog(folder, now=clock) as log:
            log.broker_start()
        before = path.read_bytes()
        is_foreign = stat.S_ISDIR if foreign == "folder" else stat.S_ISREG
        real_fstat = os.fstat

        class AnotherUsers:
            """What the system says about a file, with another owner."""

            def __init__(self, info):
                self.info = info
                self.st_uid = info.st_uid + 1

            def __getattr__(self, name):
                return getattr(self.info, name)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "fstat", lambda fd: AnotherUsers(real_fstat(fd)) if is_foreign(real_fstat(fd).st_mode) else real_fstat(fd))
            with pytest.raises(audit.AuditNotUsable):
                audit.AuditLog(folder, now=clock)

        assert path.read_bytes() == before


# ── 7. verify ───────────────────────────────────────────────────────────────


@pytest.fixture
def ten_lines(log, path):
    """A log of ten lines, closed; returns the lines."""
    write_some(log, 10)
    log.close()
    return lines_of(path)


def rewrite(path, lines, end=b"\n"):
    path.write_bytes(b"\n".join(lines) + end)


class TestVerify:
    def test_an_untouched_log_verifies(self, ten_lines, path):
        """Bug class: a check that reports breaks in a log nobody touched, after which no report is believed."""
        report = audit.verify(path)

        assert report == audit.AuditReport(readable=True, lines=10, breaks=0, first_break=None, cut_off=False, repaired=0, tail_changed=0)
        assert report.ok is True

    def test_a_removed_line_is_found(self, ten_lines, path):
        """Bug class: only the hashes or only the numbers checked, or neither: the record of one call taken out of the middle of the log without a trace."""
        rewrite(path, ten_lines[:4] + ten_lines[5:])

        report = audit.verify(path)

        assert (report.ok, report.lines, report.breaks, report.first_break) == (False, 9, 1, 5)

    def test_a_removed_first_line_is_found(self, ten_lines, path):
        """Bug class: the first line of a file accepted with any number and any hash, so that the start of the log can be cut away."""
        rewrite(path, ten_lines[3:])

        report = audit.verify(path)

        assert (report.ok, report.breaks, report.first_break) == (False, 1, 1)

    def test_an_edited_line_is_found_by_the_line_after_it(self, ten_lines, path):
        """Bug class: a line's own fields checked and not its bytes: a changed session number, verb or outcome in a line that still parses."""
        edited = ten_lines[4].replace(b'"session":2', b'"session":1')
        assert edited != ten_lines[4]
        rewrite(path, ten_lines[:4] + [edited] + ten_lines[5:])

        report = audit.verify(path)

        assert (report.ok, report.lines, report.breaks, report.first_break) == (False, 10, 1, 6)

    def test_a_line_made_unreadable_is_found(self, ten_lines, path):
        """Bug class: a line that is not a line of the log skipped as noise, which is how a record is blanked out."""
        for damaged in (b"x" * len(ten_lines[4]), b"", b"{}", b'{"n":5}', b"[1]", ten_lines[4][:-1], ten_lines[4].replace(b'"n":5', b'"n":5.0'), ten_lines[4].replace(b'"n":5', b'"n":"5"'), ten_lines[4].replace(b'"n":5', b'"n":true'), ten_lines[4].replace(b'"event":"login"', b'"event":"login","n":5'), ten_lines[4] + b"\xff", ten_lines[4].replace(b'"prev":"', b'"prev":"X')):
            rewrite(path, ten_lines[:4] + [damaged] + ten_lines[5:])

            report = audit.verify(path)

            assert (report.ok, report.lines, report.breaks, report.first_break) == (False, 10, 1, 5), damaged

    def test_moved_lines_are_found(self, ten_lines, path):
        """Bug class: lines checked one by one and not against their neighbours, so that the order of what happened can be changed."""
        rewrite(path, ten_lines[:3] + [ten_lines[4], ten_lines[3]] + ten_lines[5:])

        report = audit.verify(path)

        assert (report.ok, report.lines, report.first_break) == (False, 10, 4)
        assert report.breaks == 3

    def test_an_inserted_line_is_found(self, ten_lines, path):
        """Bug class: a copy of a real line, or a made-up one with a plausible number, put between two lines."""
        made_up = ten_lines[5].replace(b'"session":2', b'"session":9')
        assert made_up != ten_lines[5]
        # A copy of an earlier line does not continue line 5, and neither does text that is no line at all. A made-up line 6 does, and the real line 6 after it does not continue the made-up one.
        for inserted, first_break in ((ten_lines[2], 6), (b"text that is not a line of the log", 6), (b"", 6), (made_up, 7)):
            rewrite(path, ten_lines[:5] + [inserted] + ten_lines[5:])

            report = audit.verify(path)

            assert (report.ok, report.lines, report.first_break) == (False, 11, first_break) and report.breaks >= 1, inserted

    def test_a_line_appended_by_hand_is_found(self, ten_lines, path):
        """Bug class: the end of the log open to anyone who can write a JSON line: a made-up "logout" or "call_ended" added after the fact."""
        without_chain = b'{"n":11,"t":"2026-11-21T10:15:00Z","session":2,"event":"logout","prev":"' + ZEROS.encode() + b'"}'
        wrong_number = b'{"n":12,"t":"2026-11-21T10:15:00Z","session":2,"event":"logout","prev":"' + digest(ten_lines[9]).encode() + b'"}'
        for appended in (without_chain, wrong_number, b"just some text", b'{"event":"logout"}'):
            rewrite(path, ten_lines + [appended])

            report = audit.verify(path)

            assert (report.ok, report.lines, report.breaks, report.first_break) == (False, 11, 1, 11), appended

    def test_a_cut_off_end_is_reported(self, ten_lines, path):
        """Bug class: a half-written last line counted as a line, or not mentioned at all: the reader would not know that the last thing the broker did is missing."""
        rewrite(path, ten_lines[:9], end=b"\n" + ten_lines[9][:50])
        inside = audit.verify(path)
        rewrite(path, ten_lines, end=b"")
        newline_only = audit.verify(path)

        assert (inside.ok, inside.lines, inside.breaks, inside.cut_off) == (False, 9, 0, True)
        assert (newline_only.ok, newline_only.lines, newline_only.breaks, newline_only.cut_off) == (False, 9, 0, True)

    def test_a_repair_that_does_not_fit_is_a_break(self, ten_lines, path):
        """Bug class: any line with the event `repaired` excusing the damage before it, which would make that one word the way to blank out a record."""
        fake_repair = b'{"n":5,"t":"2026-11-21T10:15:00Z","session":null,"event":"repaired","prev":"' + ZEROS.encode() + b'"}'
        rewrite(path, ten_lines[:4] + [b"blanked out", fake_repair])

        report = audit.verify(path)

        assert (report.ok, report.breaks, report.first_break, report.repaired) == (False, 1, 5, 0)

    @pytest.mark.parametrize("content", [b"", b"\n", b"\n\n\n", b"\x00" * 5000, bytes(range(256)) * 40, b"\xff\xfe" + b"garbage \xe2\x82 no newline at all", b"{" * 10000 + b"\n", b'{"n":1}\n' * 300, b"[" * 100000 + b"\n" + b"]" * 100000, b'{"n":1,"prev":"' + b"0" * 64 + b'","n":1}\n', b'{"n":' + b"9" * 5000 + b',"prev":"' + b"0" * 64 + b'"}\n'], ids=["empty", "newline", "newlines", "nulls", "binary", "no-newline", "braces", "many", "deep", "repeated-key", "huge-number"])
    def test_garbage_never_raises(self, folder, path, content):
        """Bug class: the check itself falling over on the damaged file it is there to describe."""
        folder.mkdir()
        path.write_bytes(content)

        report = audit.verify(path)
        tail = audit.read_tail(path, 5)

        assert report.readable is True and (report.ok is False or content == b"")
        assert report.lines == content.count(b"\n")
        assert all(entry in ({"unreadable": True}, {"n": 1}) for entry in tail) and len(tail) == min(5, content.count(b"\n") + (not content.endswith(b"\n") and content != b""))

    def test_a_huge_line_costs_no_more_memory_than_a_line_may_have(self, ten_lines, path):
        """Bug class: a reader that takes a whole line into memory. One line of gigabytes, planted or grown from a bug, would then take the process that checks the log down."""
        huge = b"x" * (8 * 1024 * 1024)
        rewrite(path, ten_lines[:5] + [huge] + ten_lines[5:], end=b"\n" + huge)

        tracemalloc.start()
        try:
            report = audit.verify(path)
            tail = audit.read_tail(path, 7)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

        assert (report.ok, report.lines, report.breaks, report.first_break, report.cut_off) == (False, 11, 1, 6, True)
        assert tail == [{"unreadable": True}] + [json.loads(line) for line in ten_lines[5:]] + [{"unreadable": True}]
        assert peak < 2 * 1024 * 1024

    @pytest.mark.parametrize("planted", ["missing", "link", "pipe", "folder"])
    def test_what_is_not_a_regular_file_is_not_readable(self, tmp_path, planted):
        """Bug class: following a link to another file and "verifying" that, or waiting on a named pipe for ever."""
        target = tmp_path / "audit.jsonl"
        elsewhere = tmp_path / "elsewhere"
        elsewhere.write_bytes(LINE_1 + b"\n")
        {"missing": lambda: None, "link": lambda: target.symlink_to(elsewhere), "pipe": lambda: os.mkfifo(target), "folder": target.mkdir}[planted]()

        with failing_instead_of_hanging():
            report = audit.verify(target)
            tail = audit.read_tail(target, 5)

        assert report == audit.AuditReport(readable=False) and report.ok is False
        assert tail == []


# ── 8. read_tail ────────────────────────────────────────────────────────────


def write_chained(path, events):
    """A log written without a writer and without a sync per line, for the tests that need many large lines: each event is (event, session, fields), each line chained onto the one before."""
    prev, data = audit.ZERO_HASH, bytearray()
    for number, (event, session, fields) in enumerate(events, start=1):
        line = audit._line(number, START, session, event, fields, prev)
        data += line + b"\n"
        prev = digest(line)
    path.parent.mkdir(exist_ok=True)
    path.write_bytes(bytes(data))


class TestReadTail:
    def test_the_bytes_held_are_bounded_and_a_cut_is_marked(self, path):
        """Bug class: a tail bounded by the number of lines only. Ten thousand lines of 64 KiB are 640 MiB, all held at once by a request for the newest lines; and a tail that is cut without saying so, which reads as the whole of what was asked for."""
        big = ("call_accepted", 1, {"verb": "test.verb", "fields": {"source": "x" * 60000}, "db_password_from": "none"})
        write_chained(path, [big] * 400)
        line_bytes = len(lines_of(path)[0])

        tracemalloc.start()
        try:
            tail = audit.read_tail(path, 400)
            peak = tracemalloc.get_traced_memory()[1]
        finally:
            tracemalloc.stop()

        fitting = audit.TAIL_MAX_BYTES // line_bytes
        assert tail[0] == {"truncated": True} == audit.truncated_marker()
        assert [entry["n"] for entry in tail[1:]] == list(range(401 - fitting, 401))
        assert peak < 16 * 1024 * 1024 and audit.TAIL_MAX_BYTES == 8 * 1024 * 1024

    def test_a_tail_that_fits_has_no_marker(self, path):
        """Bug class: the marker given every time, so that it means nothing."""
        small = ("login", 1, {})
        write_chained(path, [small] * 50)

        assert [entry.get("n") for entry in audit.read_tail(path, 50)] == list(range(1, 51))
        assert {"truncated": True} not in audit.read_tail(path, 10)


    def test_the_newest_lines_come_back_newest_last(self, ten_lines, path):
        """Bug class: the oldest lines returned instead of the newest, the order reversed, or one line more or fewer than asked for."""
        assert [entry["n"] for entry in audit.read_tail(path, 3)] == [8, 9, 10]
        assert [entry["n"] for entry in audit.read_tail(path, 10)] == list(range(1, 11))
        assert [entry["n"] for entry in audit.read_tail(path, 500)] == list(range(1, 11))
        assert audit.read_tail(path, 1) == [json.loads(ten_lines[9])]

    def test_a_line_that_cannot_be_read_is_returned_as_a_marker(self, ten_lines, path):
        """Bug class: damaged lines dropped from the listing. The operator looking at the last entries would see a tidy log exactly where it was tampered with."""
        rewrite(path, ten_lines[:7] + [b"not json", ten_lines[8], b"[1, 2]", b""], end=b"\n" + ten_lines[9][:30])

        tail = audit.read_tail(path, 6)

        assert tail == [json.loads(ten_lines[6]), {"unreadable": True}, json.loads(ten_lines[8]), {"unreadable": True}, {"unreadable": True}, {"unreadable": True}]
        assert audit.unreadable_line() == {"unreadable": True} and audit.unreadable_line() is not audit.unreadable_line()

    def test_a_last_line_without_its_newline_is_a_marker_even_when_it_reads_as_a_line(self, ten_lines, path):
        """Bug class: a line that is still being written, or was cut exactly before its newline, handed out as a finished record."""
        rewrite(path, ten_lines, end=b"")

        tail = audit.read_tail(path, 2)

        assert tail == [json.loads(ten_lines[8]), {"unreadable": True}]

    def test_a_line_of_exactly_the_longest_length_is_read_and_one_byte_more_is_not(self, folder, path, clock):
        """Bug class: an off-by-one at the longest line (the writer writes it, the readers call it too long), and a line start looked for across the seam of two reads landing one line too early, which glues two records into one."""
        with audit.AuditLog(folder, now=clock) as log:
            log.broker_start()
            room = 65536 - len(audit._line(2, clock.now, 1, "call_accepted", {"verb": "test.verb", "fields": {"source": ""}, "db_password_from": "none"}, "0" * 64))
            longest = log.call_accepted(1, VERB, {"source": "x" * room}, "none")
            # The longest line is the last one here: the newline before it is the last byte before the piece the reader takes first.
            as_last_line = [entry["n"] for entry in audit.read_tail(path, 2)]
            too_long = log.call_accepted(1, VERB, {"source": "x" * (room + 1)}, "none")
            log.login(1)

        lines = lines_of(path)
        assert (longest, too_long) == (True, False) and as_last_line == [1, 2]
        assert [len(line) for line in lines][1] == 65536 and len(lines) == 3
        assert [entry["n"] for entry in audit.read_tail(path, 3)] == [1, 2, 3]
        assert audit.read_tail(path, 3)[1]["fields"] == {"source": "x" * room}
        assert audit.verify(path).ok
        rewrite(path, [lines[0], lines[1][:-2] + b'x"}', lines[2]])
        assert audit.read_tail(path, 3)[1] == {"unreadable": True} and audit.verify(path).first_break == 2

    def test_a_log_longer_than_one_read_is_cut_at_whole_lines(self, folder, path, clock):
        """Bug class: the first line of what was read backwards returned although it began in the middle of a line, or lines lost at the seam between two reads."""
        with audit.AuditLog(folder, now=clock) as log:
            for index in range(1500):
                log.call_refused(1 + index % 7, "run.start", "bad_input", "profile")

        tail = audit.read_tail(path, 1200)

        assert path.stat().st_size > 3 * 65536
        assert [entry["n"] for entry in tail] == list(range(301, 1501))

    @pytest.mark.parametrize("limit", [0, -1, 10001, True, 2.0, "5", None])
    def test_a_limit_out_of_range_is_refused(self, ten_lines, path, limit):
        """Bug class: a limit of nothing or of everything taken from a request as it came."""
        with pytest.raises(ValueError):
            audit.read_tail(path, limit)
