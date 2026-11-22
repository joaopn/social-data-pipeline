"""How the web UI's routes call the broker: never on the event loop, always under a bound.

`Gate` is a bound on the worker threads of one kind of call; `call_broker` runs one call in such a worker and turns every exception into a `BrokerError`, so that no traceback with a password or a token in its frames leaves the thread; `session_ends` and `why` say whether an error of a read ends the browser's session and with which line of the login page. Standard library, anyio and the broker's client only.
"""

import logging
import threading
import time

import anyio

from social_data_pipeline.control import client as broker_client

# What ends the web UI's session when the broker answers it.
SESSION_ENDING_CODES = frozenset({"unauthorized", "session_ended"})

log = logging.getLogger("social_data_pipeline.webui")


class _Busy:
    """What `Gate.run` returns when its bound is full."""


BUSY_GATE = _Busy()


class Gate:
    """A bound on the worker threads that call the broker for one kind of request. `run(function, wait_seconds=0)` runs it in a worker thread and returns its result, or `BUSY_GATE` when `workers` calls of this gate already run and no place came free within `wait_seconds` (looked for every 50 ms, without blocking the event loop). The place is given back only when the thread has ended, also when the client went away."""

    def __init__(self, workers):
        self._places = threading.BoundedSemaphore(workers)

    async def run(self, function, wait_seconds=0):
        deadline = time.monotonic() + wait_seconds
        while not self._places.acquire(blocking=False):
            if time.monotonic() >= deadline:
                return BUSY_GATE
            await anyio.sleep(0.05)
        try:
            return await anyio.to_thread.run_sync(function)
        finally:
            self._places.release()

    def take(self):
        """Take a place without waiting, for a call that runs in a thread of its own: True when one was free. The caller gives it back with `give_back` when that thread ends."""
        return self._places.acquire(blocking=False)

    def give_back(self):
        self._places.release()


def call_broker(make_client, function):
    """Run `function(client)` and return ("ok", its result) or ("error", the BrokerError). Any other exception is turned into ("error", an `internal` BrokerError) here, in the worker, so that no traceback with a password or a token in its frames leaves the thread."""
    try:
        return "ok", function(make_client())
    except broker_client.BrokerError as error:
        return "error", error
    except Exception as error:
        log.error("a broker call failed (%s)", type(error).__name__)
        return "error", broker_client.BrokerError("internal", "the web UI could not use the answer")


def session_ends(error):
    """Whether an error of a read ends the browser's session: the broker no longer knows it, or cannot be reached."""
    return isinstance(error, broker_client.BrokerUnreachable) or getattr(error, "code", None) in SESSION_ENDING_CODES


def why(error):
    """The `why` of the login page for an error that ended a session."""
    if isinstance(error, broker_client.BrokerUnreachable) and error.reason in broker_client.REASONS:
        return f"unreachable_{error.reason}"
    return "ended"
