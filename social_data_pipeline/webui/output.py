"""The output view of a run: what one answer of `run.logs` adds to what a view shows, and how the view goes on.

Each answer says how to go on. One WITH `next_offset` is a piece of the run's log file: its text is appended. One WITHOUT is Docker's tail (a run started in a terminal, a recorded run whose supervisor is gone): it replaces what is shown, and so does the first piece after a tail. Whole lines only are appended: the unfinished end of a piece is held in the view's state (at most `held_max` bytes; a longer unfinished line is shown as it is) and joined with the next piece before it is cleaned, so that an escape sequence or a carriage-return progress line cut by a piece boundary is cleaned as one. The held end is shown apart, in the element that makes the next request, which the next answer replaces; it is appended for good when the view stops.

The next request comes every 3 seconds for a recorded run that is alive, at once for the rest of an ended run (and while a piece came back full), every 10 seconds for a tail. The view stops when the run has ended (`finished`, `cancelled`) and the offset did not move, after one empty answer for a `lost` or `unreadable` run, and once it has shown `shown_max` bytes (4 MiB), when it offers to continue from there in a fresh view. Standard library and this package only.
"""

from dataclasses import dataclass

from social_data_pipeline.webui import actions
from social_data_pipeline.webui.cleaning import clean_log_text

ENDED_STATES = ("finished", "cancelled")
GONE_STATES = ("lost", "unreadable")
LIVE = "load delay:3s"
NOW = "load"
TAIL = "load delay:10s"
BUSY_TRIGGER = "load delay:10s"
SHOWN_MAX_BYTES = 4 * 1024 * 1024
HELD_MAX_BYTES = 64 * 1024
# A piece that came back within this many bytes of the limit was cut by the limit (a piece never ends inside a character): more is waiting.
FULL_PIECE_SLACK = 3

ENDED_LINE = "The run has ended: this is all of its output."
GONE_LINES = {
    "lost": "The run is lost (its supervisor and its container are gone): this is all of its output there is.",
    "unreadable": "The run's record cannot be read: this is all of its output that could be read.",
}
LIMIT_LINE = "This view has shown 4 MiB of output and stops here."


class ShapeError(Exception):
    """An answer of `run.logs` of a shape this module does not read."""


@dataclass
class Step:
    """What one answer adds: `replace` (the view's content is replaced instead of appended to), `text` (the cleaned whole lines), `preview` (the cleaned unfinished line, shown until the next answer), `trigger` (the htmx trigger of the next request, or None when the view stops), `offset` (the offset of the next request), `end` (the line shown when the view stops) and `continue_offset` (where a fresh view would go on, after the size limit)."""

    replace: bool
    text: str
    preview: str
    trigger: str | None
    offset: int
    end: str | None = None
    continue_offset: int | None = None


def _clean(text):
    return clean_log_text(text) if text else ""


def advance(view, asked_offset, data, *, limit, held_max=HELD_MAX_BYTES, shown_max=SHOWN_MAX_BYTES):
    """Apply one answer (`data`, the `data` of a `run.logs` reply for the offset `asked_offset`) to `view` (a `work.View`) and return the Step. Raises ShapeError for an answer it cannot read, leaving the view as it was."""
    if not isinstance(data, dict):
        raise ShapeError()
    text, next_offset, state = data.get("text"), data.get("next_offset"), data.get("state")
    if not isinstance(text, str) or not isinstance(state, str):
        raise ShapeError()
    if next_offset is not None and (not isinstance(next_offset, int) or isinstance(next_offset, bool) or not 0 <= next_offset <= actions.LOG_OFFSET_MAX):
        raise ShapeError()

    if next_offset is None:
        view.mode, view.held = "tail", b""
        return Step(replace=True, text=_clean(text), preview="", trigger=TAIL, offset=asked_offset)

    replace = view.mode == "tail"
    held = b"" if replace or asked_offset != view.offset else view.held
    joined = held.decode("utf-8", "surrogatepass") + text
    cut = joined.rfind("\n") + 1
    committed, rest = joined[:cut], joined[cut:]
    if len(rest.encode("utf-8", "surrogatepass")) > held_max:
        committed, rest = joined, ""
    moved = max(0, next_offset - asked_offset)
    view.offset = max(next_offset, asked_offset)
    view.shown += moved
    view.mode = "append"

    ended, gone = state in ENDED_STATES, state in GONE_STATES
    end = continue_offset = trigger = None
    if moved == 0 and (ended or gone):
        end = ENDED_LINE if ended else GONE_LINES[state]
    elif view.shown >= shown_max:
        end, continue_offset = LIMIT_LINE, view.offset
    elif ended or gone or moved >= limit - FULL_PIECE_SLACK:
        trigger = NOW
    else:
        trigger = LIVE
    if end is not None:
        committed, rest = committed + rest, ""
    view.held = rest.encode("utf-8", "surrogatepass")
    return Step(replace=replace, text=_clean(committed), preview=_clean(rest), trigger=trigger, offset=view.offset, end=end, continue_offset=continue_offset)


def flush(view):
    """The view's unfinished line, cleaned, for a view that stops on an error (the element that showed it is replaced); the view holds nothing afterwards."""
    held, view.held = view.held, b""
    return _clean(held.decode("utf-8", "surrogatepass"))
