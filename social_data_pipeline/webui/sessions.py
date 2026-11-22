"""The browser sessions of the web UI, kept in this process's memory only.

A login that the broker accepted gets a new random id (32 bytes, url-safe), which goes to the browser in the session cookie; the broker's token stays here, under the SHA-256 of that id, and leaves the process only towards the broker. Each session also has a form token, one for its whole life: a request that changes something must carry it, and it is compared in constant time. Each session also holds `work`, what `new_work()` made for it (the web UI gives it a `work.SessionWork`: its pending actions, outcomes and output views), which is forgotten with the session. At most 64 sessions are kept; a login beyond that forgets the oldest. Nothing here is written to a file, and no repr shows an id, a token or the work. Standard library only.
"""

import hashlib
import hmac
import secrets
import threading
from collections import OrderedDict
from dataclasses import dataclass, field

SESSIONS_MAX = 64


def _new_id():
    return secrets.token_urlsafe(32)


def _key(session_id):
    if not isinstance(session_id, str) or not session_id or not session_id.isascii():
        return None
    return hashlib.sha256(session_id.encode("ascii")).digest()


@dataclass
class WebSession:
    """One session: the broker's token, its form token, when it began (on the clock of `WebSessions`), its work, and its number at the broker (from the login's reply; None when the reply had none), which names the session to the scheduler as `web-<number>`."""

    token: str = field(repr=False)
    form_token: str = field(repr=False)
    started: float = 0.0
    work: object = field(default=None, repr=False)
    number: int | None = None


class WebSessions:
    def __init__(self, clock, *, sessions_max=SESSIONS_MAX, new_id=_new_id, new_work=lambda: None):
        self._clock = clock
        self._max = sessions_max
        self._new_id = new_id
        self._new_work = new_work
        self._lock = threading.Lock()
        self._table = OrderedDict()

    def create(self, token, number=None):
        """Keep a new session for a broker token (and the broker's number of the session) and return (its id, the session). The oldest session is forgotten when the table is full."""
        session_id = self._new_id()
        key = _key(session_id)
        if key is None:
            raise RuntimeError("the id source did not return an id")
        session = WebSession(token=token, form_token=self._new_id(), started=self._clock(), work=self._new_work(), number=number)
        with self._lock:
            if key in self._table:
                raise RuntimeError("the id source did not return a new id")
            while len(self._table) >= self._max:
                self._table.popitem(last=False)
            self._table[key] = session
        return session_id, session

    def get(self, session_id):
        """The session of an id, or None."""
        key = _key(session_id)
        if key is None:
            return None
        with self._lock:
            return self._table.get(key)

    def forget(self, session_id):
        """Forget a session; returns it, or None when the id named none."""
        key = _key(session_id)
        if key is None:
            return None
        with self._lock:
            return self._table.pop(key, None)

    def form_token_matches(self, session_id, presented):
        """Whether `presented` is the form token of the session of `session_id`, compared in constant time."""
        key = _key(session_id)
        if key is None or not isinstance(presented, str) or not presented.isascii():
            return False
        with self._lock:
            session = self._table.get(key)
            return session is not None and hmac.compare_digest(session.form_token.encode("ascii"), presented.encode("ascii"))

    def __len__(self):
        with self._lock:
            return len(self._table)
