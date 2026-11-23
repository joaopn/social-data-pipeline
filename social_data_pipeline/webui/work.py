"""What a browser session keeps besides its tokens, in this process's memory only: its pending actions, the outcomes of the actions it confirmed, its output views, and whether a database admin password was kept for it at the broker.

- A pending action is an action the operator chose and has not confirmed yet: its id (random, 16 bytes, url-safe), the action's name, its checked fields (a secret in `actions.Hidden`), when it was made, whether its confirmation needs the management password again (by the entry's class, or for a change of the scheduler's auto-accept by what it changes), and for the Queue what its page shows (`shown`: for an approval the whole record of the job as the scheduler sent it). It lives five minutes (on the injected clock); a session keeps at most sixteen, and a seventeenth drops the oldest; the oldest are also dropped while the records they hold are more than 4 MiB together. Taking it out (`take_pending`) is one step under the session's lock, so two confirms of one action at once get it once.
- An outcome is what one confirmed action came to, written by the thread that runs the call and read by the pages: under way, a result (the exit code and the cleaned end of the output and the errors, at most 16 KiB of each; for `run.start` the run's id; for the scheduler's entries what the scheduler answered: whether a killed job was running, the start of an EXPLAIN's plan, at most 16 KiB, the target's new auto-accept settings), or a refusal (its code, its sentence and, for `failed`, the cleaned end of the errors; for the scheduler's `explain_failed` the start of the database's error text, for `not_pending` the job's phase). Whether a refusal came from the scheduler is kept (`from_door`): only the broker's codes end a session or put an action back. A session keeps the newest 32; finished ones are evicted before running ones. An outcome holds no secret: neither the fields of its action nor the management password.
- An output view is the state of one output of a run that a page shows: the offset it asks for next, the unfinished end of the last piece (at most 64 KiB, held until its line is whole), how many bytes it has shown, and whether it shows pieces or Docker's tail. At most eight per session; the oldest is dropped.

No `repr` here shows a field, a secret or a text. Standard library and this package only.
"""

import secrets
import threading
from collections import OrderedDict
from dataclasses import dataclass, field

from social_data_pipeline.control import client
from social_data_pipeline.webui import actions, words
from social_data_pipeline.webui.cleaning import clean_log_text

PENDING_SECONDS = 5 * 60
PENDING_MAX = 16
# The most bytes of records the pending actions of one session hold together (an approval keeps the whole record of its job, up to 1 MiB).
PENDING_WEIGHT_MAX = 4 * 1024 * 1024
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


def text_start(text, max_bytes=TEXT_MAX_BYTES):
    """(the start of `text`, cleaned and cut to `max_bytes` bytes of UTF-8 at a whole character; whether it was cut)."""
    cleaned = clean_log_text(text).encode("utf-8", "replace").decode("utf-8")
    data = cleaned.encode("utf-8")
    if len(data) <= max_bytes:
        return cleaned, False
    return data[:max_bytes].decode("utf-8", "ignore"), True


class Pending:
    """An action chosen and not confirmed yet. `step_up` says whether its confirmation needs the management password again; `shown` is what a Queue action's page shows (None for the others), `weight` the bytes of record it holds."""

    __slots__ = ("id", "action", "fields", "created", "step_up", "shown", "weight", "then")

    def __init__(self, pending_id, action, fields, created, step_up=None, shown=None, weight=0, then=None):
        self.id = pending_id
        self.action = action
        self.fields = fields
        self.created = created
        self.step_up = actions.TABLE[action].cls == "step_up" if step_up is None else step_up
        self.shown = shown
        self.weight = weight
        # The steps run after this one, each only when the one before ended with exit code 0: a list of (action, fields), or None (a restart: db.stop then db.start).
        self.then = then

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
        self.from_door = False
        self.sent = False
        # A Queue action's refusal by the broker's check of the session, before anything else was asked.
        self.at_session_check = False
        # Where the outcome page of an action of the Queue links back to: (address, label), or None.
        self.back = None
        # What the scheduler answered (its entries only): `was_running`, `plan`, `plan_cut`, `auto_accept`, `phase`, `detail`.
        self.door = {}

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

    def finish(self, kind, value, text_max_bytes=TEXT_MAX_BYTES, from_door=False, sent=False):
        """Write the answer of the call: `kind` "ok" with the result, or "error" with the `BrokerError` (as `calls.call_broker` gives them); `from_door` when the scheduler's control socket gave it (or could not be asked, or the web UI refused before it); `sent` when it is the answer of the changing request itself, the only one whose lost reply may mean the change was made."""
        self.from_door = from_door
        self.sent = sent
        try:
            if kind == "ok" and actions.TABLE[self.action].door is not None:
                self._door_result(value, text_max_bytes)
            elif kind == "ok":
                self._result(value, text_max_bytes)
            elif from_door:
                self._door_refusal(value, text_max_bytes)
            else:
                self._refusal(value, text_max_bytes)
        except Exception:
            self.kind, self.code = "refusal", "bad_reply"
            self.sentence = words.DOOR_CODE_SENTENCES["bad_reply"] if from_door else SHAPE_SENTENCE
        self._done.set()

    def _door_result(self, result, text_max_bytes):
        if not isinstance(result, dict):
            raise ValueError("not a result")
        if self.action == "queue.kill":
            if not isinstance(result.get("was_running"), bool):
                raise ValueError("no answer")
            self.door = {"was_running": result["was_running"]}
        elif self.action == "queue.explain":
            plan = result.get("plan")
            if not isinstance(plan, str):
                raise ValueError("no plan")
            shown, cut = text_start(plan, text_max_bytes)
            self.door = {"plan": shown, "plan_cut": cut}
        elif self.action == "queue.auto_accept":
            self.door = {"auto_accept": result}
        self.kind = "result"

    def _door_refusal(self, error, text_max_bytes):
        self.kind = "refusal"
        code = getattr(error, "code", None)
        self.code = code if isinstance(code, str) else None
        self.sentence = words.door_sentence(error)
        # A change sent and then no reply: it may have been made; the sentence says so instead of "refused".
        if self.sent and getattr(error, "reason", None) == client.NO_REPLY and actions.TABLE[self.action].state_changing:
            self.sentence = words.door_no_answer_sentence(self.action)
        details = getattr(error, "details", None) or {}
        if code == "not_pending" and details.get("phase") in (*actions.PHASES, "gone"):
            self.door = {"phase": details["phase"]}
        elif code == "explain_failed" and isinstance(details.get("detail"), str):
            shown, cut = text_start(details["detail"], text_max_bytes)
            self.door = {"detail": shown, "detail_cut": cut}

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
        # When the broker last said that this session still lives, for the Queue's reads (on the application's clock), or None.
        self.broker_checked_at = None

    def __repr__(self):
        return f"<session work: {len(self._pending)} pending, {len(self._outcomes)} outcomes, {len(self._views)} views>"

    # ── pending actions ──

    def _prune(self, now):
        for key in [key for key, pending in self._pending.items() if now - pending.created >= self.pending_seconds]:
            dropped = self._pending.pop(key)
            dropped.fields = None
            dropped.shown = None
            dropped.then = None

    def _drop_oldest(self):
        _, dropped = self._pending.popitem(last=False)
        dropped.fields = None
        dropped.shown = None
        dropped.then = None

    def _weight(self):
        return sum(kept.weight for kept in self._pending.values())

    def add_pending(self, action, fields, now, step_up=None, shown=None, weight=0, then=None):
        """Keep a pending action and return it, or None (nothing kept, nothing dropped) when it alone weighs more than PENDING_WEIGHT_MAX; the oldest is dropped when sixteen are kept, and while the records kept weigh more than PENDING_WEIGHT_MAX with it."""
        if weight > PENDING_WEIGHT_MAX:
            return None
        pending = Pending(new_id(), action, fields, now, step_up=step_up, shown=shown, weight=weight, then=then)
        with self.lock:
            self._prune(now)
            while len(self._pending) >= self.pending_max or (self._pending and self._weight() + weight > PENDING_WEIGHT_MAX):
                self._drop_oldest()
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
                pending.shown = None
                pending.then = None
                return
            while len(self._pending) >= self.pending_max or (self._pending and self._weight() + pending.weight > PENDING_WEIGHT_MAX):
                self._drop_oldest()
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
