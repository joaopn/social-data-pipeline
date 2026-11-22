"""The live indicators of the top bar: the broker, scheduler and sandbox pills and the number on the Queue window's tab, for `GET /status`.

The frame draws the pills grey and the tab without a number, and asks `/status` once the page has loaded (and every 30 seconds after), so that a slow or dead service never slows or breaks a page. `read(...)` runs in a worker of its own bound (`Limits.status_workers`, apart from the pages' so that a hung service never takes a page's place), never on the event loop. It asks the broker first whether it still knows the session (`queue.broker_session_error`, `audit.list` through the table, with a short time limit): a session it has ended ends the web session as on every other route; a broker that does not answer is a red broker pill, and then nothing else is asked, so that a session the broker has not confirmed learns nothing of the Queue. Then the scheduler's `queue.counts` (its pending number is the tab's) and the chat runner's `hello`, each with a short time limit. A service the web UI was not given (`no_settings`) is a grey pill that says so. Standard library and this package only.
"""

from social_data_pipeline.webui import words
from social_data_pipeline.webui.assistant import AssistantUnavailable
from social_data_pipeline.webui.calls import SESSION_ENDING_CODES, call_broker
from social_data_pipeline.webui.scheduler import SchedulerUnavailable

# The time limit of each call of the indicators: they are asked again within 30 seconds anyway.
STATUS_SECONDS = 5
NOT_ASKED = "Not asked: the broker did not confirm the session."


class SessionEnded:
    """What `read` returns when the broker has ended the session: the broker's error."""

    def __init__(self, error):
        self.error = error


def pill(dot, title):
    return {"dot": dot, "title": title}


def _counts(value):
    """The scheduler's numbers when the reply has the expected shape, else None."""
    if isinstance(value, dict) and all(isinstance(value.get(key), int) and not isinstance(value.get(key), bool) and value.get(key) >= 0 for key in ("pending", "approved", "running")):
        return value
    return None


def read(session_error, scheduler, assistant):
    """The indicators' model: {"broker", "scheduler", "sandbox": a pill, "pending": the number of queries that wait for approval or None}, or `SessionEnded`. `session_error()` is the broker's check of the session (None when it knows it)."""
    error = session_error()
    if error is not None:
        if getattr(error, "code", None) in SESSION_ENDING_CODES:
            return SessionEnded(error)
        not_asked = pill("", NOT_ASKED)
        return {"broker": pill("bad", f"The broker did not answer: {words.sentence(error)}"), "scheduler": not_asked, "sandbox": not_asked, "pending": None}
    model = {"broker": pill("ok", "The broker answered."), "pending": None}

    kind, value = call_broker(lambda: scheduler, lambda made: made.call("queue.counts", {}, timeout=STATUS_SECONDS))
    if kind == "error" and isinstance(value, SchedulerUnavailable) and value.reason == "no_settings":
        model["scheduler"] = pill("", "The query queue is not set up for the web UI.")
    elif kind == "error":
        model["scheduler"] = pill("bad", f"The scheduler did not answer: {words.door_sentence(value)}")
    elif _counts(value) is None:
        model["scheduler"] = pill("warn", "The scheduler's answer did not have the expected shape.")
    else:
        model["scheduler"] = pill("ok", "The scheduler answered.")
        model["pending"] = value["pending"]

    kind, value = call_broker(lambda: assistant, lambda made: made.hello(timeout=STATUS_SECONDS))
    if kind == "error" and isinstance(value, AssistantUnavailable) and value.reason == "no_settings":
        model["sandbox"] = pill("", "No chat is set up for the web UI.")
    elif kind == "error":
        model["sandbox"] = pill("bad", f"The sandbox did not answer: {words.chat_sentence(value)}")
    else:
        model["sandbox"] = pill("ok", "The sandbox's runner answered.")
    return model
