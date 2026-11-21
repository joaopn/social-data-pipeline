"""What a browser session keeps besides its tokens, in this process's memory only: its pending actions, the outcomes of the actions it confirmed, its output views, and whether a database admin password was kept for it at the broker.

- A pending action is an action the operator chose and has not confirmed yet: its id (random, 16 bytes, url-safe), the action's name, its checked fields (a secret in `actions.Hidden`) and when it was made. It lives five minutes (on the injected clock); a session keeps at most sixteen, and a seventeenth drops the oldest. Taking it out (`take_pending`) is one step under the session's lock, so two confirms of one action at once get it once.
- An outcome is what one confirmed action came to, written by the thread that runs the call and read by the pages: under way, a result (the exit code and the cleaned end of the output and the errors, at most 16 KiB of each; for `run.start` the run's id), or a refusal (its code, its sentence and, for `failed`, the cleaned end of the errors). A session keeps the newest 32; finished ones are evicted before running ones. An outcome holds no secret: neither the fields of its action nor the management password.
- An output view is the state of one output of a run that a page shows: the offset it asks for next, the unfinished end of the last piece (at most 64 KiB, held until its line is whole), how many bytes it has shown, and whether it shows pieces or Docker's tail. At most eight per session; the oldest is dropped.

No `repr` here shows a field, a secret or a text. Standard library and this package only.
"""

import secrets
import threading
from collections import OrderedDict
from dataclasses import dataclass, field

from social_data_pipeline.webui import actions, words
from social_data_pipeline.webui.cleaning import clean_log_text

PENDING_SECONDS = 5 * 60
PENDING_MAX = 16
OUTCOMES_MAX = 32
TEXT_MAX_BYTES = 16 * 1024
VIEWS_MAX = 8
ID_PATTERN = r"[A-Za-z0-9_-]{22}"
VIEW_ID_PATTERN = r"[0-9a-f]{16}"
SHAPE_SENTENCE = "The broker's answer did not have the expected shape."


def new_id():
    return secrets.token_urlsafe(16)


def new_view_id():
    return secrets.token_hex(8)


def text_end(text, max_bytes=TEXT_MAX_BYTES):
    """`text` cleaned (`cleaning.clean_log_text`) and cut to its last `max_bytes` bytes of UTF-8; a text that was cut begins with a whole line, or with a whole character when its last line alone is longer."""
    if not isinstance(text, str) or not text:
        return ""
    cleaned = clean_log_text(text)
    data = cleaned.encode("utf-8", "surrogatepass")
    if len(data) <= max_bytes:
        return cleaned
    kept = data[-max_bytes:]
    newline = kept.find(b"\n")
    if 0 <= newline < len(kept) - 1:
        kept = kept[newline + 1:]
    return kept.decode("utf-8", "ignore")


class Pending:
    """An action chosen and not confirmed yet."""

    __slots__ = ("id", "action", "fields", "created")

    def __init__(self, pending_id, action, fields, created):
        self.id = pending_id
        self.action = action
        self.fields = fields
        self.created = created

    def __repr__(self):
        return f"<pending {self.action}>"


class Outcome:
    """What one confirmed action came to. `state` is `running` until `finish` is called, then `done`; `kind` is then `result` or `refusal`."""

    def __init__(self, outcome_id, action, command, created):
        self.id = outcome_id
        self.action = action
        self.command = command
        self.created = created
        self._done = threading.Event()
        self.kind = None
        self.exit_code = None
        self.output = ""
        self.errors = ""
        self.run_id = None
        self.code = None
        self.sentence = None
        self.field = None

    def __repr__(self):
        return f"<outcome {self.action} {'done' if self.done else 'running'}>"

    @property
    def done(self):
        return self._done.is_set()

    @property
    def state(self):
        return "done" if self.done else "running"

    def wait(self, seconds):
        return self._done.wait(seconds)

    def finish(self, kind, value, text_max_bytes=TEXT_MAX_BYTES):
        """Write the answer of the call: `kind` "ok" with the result, or "error" with the `BrokerError` (as `app._call_broker` gives them)."""
        try:
            if kind == "ok":
                self._result(value, text_max_bytes)
            else:
                self._refusal(value, text_max_bytes)
        except Exception:
            self.kind, self.code, self.sentence = "refusal", "bad_reply", SHAPE_SENTENCE
        self._done.set()

    def _result(self, result, text_max_bytes):
        if not isinstance(result, dict):
            raise ValueError("not a result")
        if self.action == "run.start":
            run_id = result.get("run_id")
            if not actions.is_run_id(run_id):
                raise ValueError("no run id")
            self.run_id = run_id
        elif not actions.TABLE[self.action].op:
            exit_code = result.get("exit_code")
            if exit_code is not None and (not isinstance(exit_code, int) or isinstance(exit_code, bool)):
                raise ValueError("no exit code")
            output, errors = result.get("output", ""), result.get("errors", "")
            if not isinstance(output, str) or not isinstance(errors, str):
                raise ValueError("no text")
            self.exit_code = exit_code
            self.output = text_end(output, text_max_bytes)
            self.errors = text_end(errors, text_max_bytes)
        self.kind = "result"

    def _refusal(self, error, text_max_bytes):
        self.kind = "refusal"
        code = getattr(error, "code", None)
        self.code = code if isinstance(code, str) else None
        self.sentence = words.sentence(error)
        field_name = getattr(error, "field", None)
        if isinstance(field_name, str) and field_name in actions.TABLE[self.action].inputs:
            self.field = actions.TABLE[self.action].inputs[field_name].label
        details = getattr(error, "details", None) or {}
        if code == "failed":
            if isinstance(details.get("errors"), str):
                self.errors = text_end(details["errors"], text_max_bytes)
            exit_code = details.get("exit_code")
            if isinstance(exit_code, int) and not isinstance(exit_code, bool):
                self.exit_code = exit_code


@dataclass
class View:
    """The state of one output view."""

    run_id: str
    offset: int
    held: bytes = field(default=b"", repr=False)
    shown: int = 0
    mode: str = "start"


class SessionWork:
    """The pending actions, outcomes and output views of one session, under one lock."""

    def __init__(self, *, pending_max=PENDING_MAX, pending_seconds=PENDING_SECONDS, outcomes_max=OUTCOMES_MAX, views_max=VIEWS_MAX):
        self.lock = threading.Lock()
        self.pending_max = pending_max
        self.pending_seconds = pending_seconds
        self.outcomes_max = outcomes_max
        self.views_max = views_max
        self._pending = OrderedDict()
        self._outcomes = OrderedDict()
        self._views = OrderedDict()
        self.db_password_kept = False

    def __repr__(self):
        return f"<session work: {len(self._pending)} pending, {len(self._outcomes)} outcomes, {len(self._views)} views>"

    # ── pending actions ──

    def _prune(self, now):
        for key in [key for key, pending in self._pending.items() if now - pending.created >= self.pending_seconds]:
            self._pending.pop(key).fields = None

    def add_pending(self, action, fields, now):
        """Keep a pending action and return it; the oldest is dropped when sixteen are kept."""
        pending = Pending(new_id(), action, fields, now)
        with self.lock:
            self._prune(now)
            while len(self._pending) >= self.pending_max:
                _, dropped = self._pending.popitem(last=False)
                dropped.fields = None
            self._pending[pending.id] = pending
        return pending

    def pending(self, pending_id, now):
        """The pending action of an id that has not expired, or None. Changes nothing but dropping expired ones."""
        with self.lock:
            self._prune(now)
            return self._pending.get(pending_id)

    def take_pending(self, pending_id, now):
        """Take a pending action out, in one step: the action, or None when there is none of this id (taken already, expired, never made)."""
        with self.lock:
            self._prune(now)
            return self._pending.pop(pending_id, None)

    def put_back(self, pending, now):
        """Keep a pending action that was taken out again, under its id, unless it has expired since."""
        with self.lock:
            if now - pending.created >= self.pending_seconds:
                pending.fields = None
                return
            while len(self._pending) >= self.pending_max:
                _, dropped = self._pending.popitem(last=False)
                dropped.fields = None
            self._pending[pending.id] = pending

    def pending_count(self):
        with self.lock:
            return len(self._pending)

    # ── outcomes ──

    def add_outcome(self, outcome):
        """Keep an outcome; beyond 32 the oldest finished one is evicted, or the oldest running one when none is finished."""
        with self.lock:
            while len(self._outcomes) >= self.outcomes_max:
                finished = next((key for key, kept in self._outcomes.items() if kept.done), None)
                if finished is not None:
                    del self._outcomes[finished]
                else:
                    self._outcomes.popitem(last=False)
            self._outcomes[outcome.id] = outcome

    def outcome(self, outcome_id):
        with self.lock:
            return self._outcomes.get(outcome_id)

    def remove_outcome(self, outcome_id):
        with self.lock:
            self._outcomes.pop(outcome_id, None)

    def outcome_ids(self):
        with self.lock:
            return list(self._outcomes)

    # ── output views ──

    def add_view(self, view):
        """Keep a new output view and return its id; the oldest is dropped when eight are kept."""
        view_id = new_view_id()
        with self.lock:
            while len(self._views) >= self.views_max:
                self._views.popitem(last=False)
            self._views[view_id] = view
        return view_id

    def view(self, view_id, run_id):
        """The output view of an id for this run, or None."""
        with self.lock:
            view = self._views.get(view_id)
            return view if view is not None and view.run_id == run_id else None

    def view_count(self):
        with self.lock:
            return len(self._views)
