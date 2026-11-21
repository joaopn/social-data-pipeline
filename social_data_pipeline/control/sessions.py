"""Who may ask the broker for something: the login budget, the sessions and the second check of the management password before a dangerous call.

One object, `Sessions`, holds all of it behind one lock, so that a decision and the change it leads to are one step and at most one password derivation runs at a time, whoever asks for it.

- **Login budget.** Five units, one refilled every 30 seconds. A login reads the password file, refuses with `throttled` when the budget is empty WITHOUT verifying anything (the right password is refused too), verifies, and takes one unit for a wrong password. A right password takes nothing and gives nothing back.
- **Sessions.** A login that verified returns a random token once. The table is keyed by the SHA-256 of the token and the token itself is not kept, so nothing read out of this object is a usable token. A session has a public number (a counter, unrelated to the token), its start time, the digest of the password file its login was verified against, an optional database password (wrapped, never shown by repr) and its count of failed second checks. At most 64 sessions live at a time: a login beyond that ends the expired ones and then, when there is still no room, the oldest (reason `superseded`).
- **Every authenticated request** goes through `session_for(token, current_file_digest)`, and the three calls about a session's database password make the same checks: an unknown token is refused; a session eight hours old is ended; and when the digest of the password file as it is now differs from the session's (the password was changed, or the file is gone), every session with another digest is ended. A login that verified against the old file while it was being replaced therefore ends at its first request.
- **Step-up.** `step_up` verifies the management password again for one session. It does not draw on the login budget, and a success changes nothing: the fifth failure in the session's life ends it.
- **Secrets for redaction.** `known_secrets()` returns the database passwords the live sessions hold, the newest 64 that sessions held before (ended, cleared or replaced), so that what a run printed with one is still overwritten after its session is gone, until the broker stops, and the newest 64 that `remember_secret` was given (a database password a request carried in its fields). The two lists are kept apart: a request that carries a password needs no second check of the management password, so whoever could send many could push only other such passwords out, never a session's.

A refusal is a `SessionRefused` with a `code`, one constant message, the number of the session it is about (when there is one) and the sessions the call ended, as (number, reason), for the audit log. No message, result or repr holds a token, a password or any input value. A traceback's frames do hold them: never log a traceback that passes through this module.

The clock, the token source and the verification function are parameters; the default clock is the boot-time clock of Linux, which goes on while the machine is suspended, so a session does not outlive its eight hours by a night with the lid closed; where that clock does not exist, the monotonic one. Besides the standard library the module uses this package's errors, `types` (for `SecretValue`, which loads `names` and with it `social_data_pipeline.setup.utils` and PyYAML) and `passwords`.
"""

import hashlib
import hmac
import secrets
import threading
import time
from dataclasses import dataclass, field

from social_data_pipeline.control import passwords
from social_data_pipeline.control.errors import ControlError
from social_data_pipeline.control.types import SecretValue

BUDGET_CAPACITY = 5
BUDGET_REFILL_SECONDS = 30
SESSION_SECONDS = 8 * 60 * 60
STEP_UP_FAILURES = 5
SESSIONS_MAX = 64
RETIRED_SECRETS_MAX = 64
REMEMBERED_SECRETS_MAX = 64
# The shortest password `remember_secret` keeps: a shorter one would overwrite common words in every later reply.
REMEMBERED_SECRET_MIN_CHARS = 8

UNAUTHORIZED = "unauthorized"
THROTTLED = "throttled"
NO_PASSWORD_SET = "no_password_set"
PASSWORD_FILE_NOT_USABLE = "password_file_not_usable"
STEP_UP_FAILED = "step_up_failed"
SESSION_ENDED = "session_ended"

# Why a session was ended by this module (a logout is the caller's own doing).
END_EXPIRED = "expired"
END_PASSWORD_CHANGED = "password_changed"
END_STEP_UP_FAILURES = "step_up_failures"
END_SUPERSEDED = "superseded"

_MESSAGES = {
    UNAUTHORIZED: "not logged in, or the password is wrong",
    THROTTLED: "too many failed logins: wait before the next attempt",
    NO_PASSWORD_SET: "no management password is set",
    PASSWORD_FILE_NOT_USABLE: "the password file is not usable",
    STEP_UP_FAILED: "the password is wrong",
    SESSION_ENDED: "the password was wrong too often: the session has ended",
}


class SessionRefused(ControlError):
    """A login, a request or a step-up was refused. `code` is one of the six codes above, `message` the constant line of that code, `number` the public number of the session the refusal is about (None when there is none) and `ended` the sessions this call ended, as a tuple of (number, reason)."""

    def __init__(self, code, *, number=None, ended=()):
        super().__init__(_MESSAGES[code])
        self.code = code
        self.number = number
        self.ended = tuple(ended)


@dataclass(frozen=True)
class Login:
    """A login that succeeded: the token, handed out this once, the public number of the new session, and the sessions the login ended to make room, as a tuple of (number, reason) for the audit log. repr() does not show the token."""

    token: str = field(repr=False)
    number: int
    ended: tuple = ()


@dataclass(frozen=True)
class SessionInfo:
    """What a caller may know of a live session: its public number, its start time on the clock of `Sessions`, and whether it holds a database password."""

    number: int
    started: float
    has_db_password: bool


class _Session:
    __slots__ = ("key", "number", "started", "digest", "db_password", "failed_step_ups")

    def __init__(self, key, number, started, digest):
        self.key = key
        self.number = number
        self.started = started
        self.digest = digest
        self.db_password = None
        self.failed_step_ups = 0

    def __repr__(self):
        return f"<session {self.number}>"


def _token_key(token):
    """The table key of a token, or None for something that cannot be a token."""
    if not isinstance(token, str) or not token:
        return None
    try:
        return hashlib.sha256(token.encode("utf-8")).digest()
    except UnicodeEncodeError:
        return None


def _same_digest(one, other):
    if not isinstance(one, bytes) or not isinstance(other, bytes):
        return False
    return hmac.compare_digest(one, other)


def _new_token():
    return secrets.token_urlsafe(32)


def _default_clock():
    """The clock a `Sessions` object uses when it is given none, chosen once: seconds since the machine started, the time it was suspended included, where the system has that clock (Linux); `time.monotonic` where it has not (a system without `CLOCK_BOOTTIME`, or one that refuses it)."""
    boot = getattr(time, "CLOCK_BOOTTIME", None)
    if boot is not None:
        try:
            time.clock_gettime(boot)
        except OSError:
            pass
        else:
            return lambda: time.clock_gettime(boot)
    return time.monotonic


class Sessions:
    """The login budget and the table of sessions. `now()` returns seconds and never goes back (the default is the boot-time clock, or the monotonic one where there is none), `new_token()` returns a new random token, `verify(password_file, candidate)` answers whether a candidate is the password of a file. Every method may be called from any thread."""

    def __init__(self, *, now=None, new_token=_new_token, verify=passwords.verify):
        self._now = _default_clock() if now is None else now
        self._new_token = new_token
        self._verify = verify
        self._lock = threading.Lock()
        self._sessions = {}
        self._last_number = 0
        self._budget = BUDGET_CAPACITY
        # The time from which the next unit is counted; None while the budget is full.
        self._refill_from = None
        # The database passwords sessions held before, oldest first, each once.
        self._retired = []
        # The database passwords requests carried in their fields, oldest first, each once.
        self._remembered = []

    # ── the budget ──────────────────────────────────────────────────────────

    def _refill(self, now):
        if self._refill_from is None:
            return
        units = int(max(0, now - self._refill_from) // BUDGET_REFILL_SECONDS)
        if units:
            self._budget = min(BUDGET_CAPACITY, self._budget + units)
            self._refill_from += units * BUDGET_REFILL_SECONDS
        if self._budget >= BUDGET_CAPACITY:
            self._refill_from = None

    def _take_unit(self, now):
        if self._refill_from is None:
            self._refill_from = now
        self._budget -= 1

    def reset_budget(self):
        """Fill the login budget. For the terminal: after the password was set there, and at a restart."""
        with self._lock:
            self._budget = BUDGET_CAPACITY
            self._refill_from = None

    def budget(self):
        """How many failed logins the budget has room for right now."""
        with self._lock:
            self._refill(self._now())
            return self._budget

    # ── helpers, all called with the lock held ──────────────────────────────

    def _read(self, read_file):
        """The password file through the caller's function: a file object, or None when no password is set."""
        try:
            password_file = read_file()
        except passwords.PasswordFileNotUsable:
            raise SessionRefused(PASSWORD_FILE_NOT_USABLE) from None
        return password_file if password_file else None

    def _verified(self, password_file, candidate):
        try:
            return self._verify(password_file, candidate) is True
        except (passwords.PasswordFileNotUsable, passwords.PasswordNotDerivable):
            raise SessionRefused(PASSWORD_FILE_NOT_USABLE) from None

    def _retire(self, secret):
        """Keep a database password that a session no longer holds, as the newest of the retired ones."""
        if secret is None:
            return
        value = secret.reveal()
        self._retired = [kept for kept in self._retired if kept.reveal() != value]
        self._retired.append(secret)
        del self._retired[:-RETIRED_SECRETS_MAX]

    def _end(self, session):
        self._sessions.pop(session.key, None)
        self._retire(session.db_password)
        session.db_password = None

    def _make_room(self, now):
        """End sessions until there is room for one more: first those that have reached their eight hours, then the oldest. Returns what was ended, as (number, reason)."""
        ended = []
        if len(self._sessions) < SESSIONS_MAX:
            return ended
        for session in sorted(self._sessions.values(), key=lambda entry: entry.number):
            if now - session.started >= SESSION_SECONDS:
                self._end(session)
                ended.append((session.number, END_EXPIRED))
        oldest = sorted(self._sessions.values(), key=lambda entry: entry.number)
        while len(oldest) >= SESSIONS_MAX:
            session = oldest.pop(0)
            self._end(session)
            ended.append((session.number, END_SUPERSEDED))
        return ended

    def _live(self, token, now):
        """The session of a token. Raises SessionRefused `unauthorized` for a token that names none and for a session that has reached its eight hours, which is ended."""
        key = _token_key(token)
        session = self._sessions.get(key) if key is not None else None
        # The table already compared the key; this comparison takes the same time whatever the two hold.
        if session is None or not hmac.compare_digest(session.key, key):
            raise SessionRefused(UNAUTHORIZED)
        if now - session.started >= SESSION_SECONDS:
            self._end(session)
            raise SessionRefused(UNAUTHORIZED, number=session.number, ended=((session.number, END_EXPIRED),))
        return session

    def _current(self, token, digest, now):
        """The session of a token when the password file it was verified against is still the one in force. Otherwise every session with another digest is ended and the request is refused."""
        session = self._live(token, now)
        if not _same_digest(session.digest, digest):
            ended = []
            for other in sorted(self._sessions.values(), key=lambda entry: entry.number):
                if not _same_digest(other.digest, digest):
                    self._end(other)
                    ended.append((other.number, END_PASSWORD_CHANGED))
            raise SessionRefused(UNAUTHORIZED, number=session.number, ended=ended)
        return session

    @staticmethod
    def _info(session):
        return SessionInfo(number=session.number, started=session.started, has_db_password=session.db_password is not None)

    # ── login, requests, step-up ────────────────────────────────────────────

    def login(self, candidate, read_file):
        """Log in with the management password and return a `Login` (the token, the session's number, and the sessions ended to make room when 64 were live). `read_file()` returns the password file as `passwords.read_password_file` does. One login runs at a time, start to end. Raises SessionRefused: `no_password_set`, `password_file_not_usable`, `throttled` (the budget is empty: nothing was verified), `unauthorized` (the password is wrong: one unit of the budget is gone)."""
        with self._lock:
            password_file = self._read(read_file)
            if password_file is None:
                raise SessionRefused(NO_PASSWORD_SET)
            now = self._now()
            self._refill(now)
            if self._budget < 1:
                raise SessionRefused(THROTTLED)
            if not self._verified(password_file, candidate):
                self._take_unit(now)
                raise SessionRefused(UNAUTHORIZED)
            token = self._new_token()
            key = _token_key(token)
            if key is None or key in self._sessions:
                raise RuntimeError("the token source did not return a new token")
            ended = self._make_room(now)
            self._last_number += 1
            session = _Session(key, self._last_number, now, password_file.digest)
            self._sessions[key] = session
            return Login(token=token, number=session.number, ended=tuple(ended))

    def session_for(self, token, current_file_digest):
        """The `SessionInfo` of the session a token belongs to. Every authenticated request passes here first. `current_file_digest` is the digest of the password file as it is now, None when there is none. Raises SessionRefused `unauthorized` for an unknown token, for a session eight hours old (it is ended) and for a session that was verified against another password file (every such session is ended); `ended` lists what was ended."""
        with self._lock:
            return self._info(self._current(token, current_file_digest, self._now()))

    def step_up(self, token, candidate, read_file):
        """Verify the management password again for a session, before a call that needs it, and return the session's `SessionInfo`. It waits for a login that is running and does not draw on the login budget. Raises SessionRefused: `unauthorized` as `session_for` does (the file read here is the current one); `password_file_not_usable`; `step_up_failed` for a wrong password; `session_ended` for the fifth wrong password in the session's life, which ends the session (it is in `ended`, with the reason `step_up_failures`). A right password changes nothing: it does not set the count of failures back."""
        with self._lock:
            now = self._now()
            self._live(token, now)
            password_file = self._read(read_file)
            session = self._current(token, password_file.digest if password_file is not None else None, now)
            if not self._verified(password_file, candidate):
                session.failed_step_ups += 1
                if session.failed_step_ups >= STEP_UP_FAILURES:
                    self._end(session)
                    raise SessionRefused(SESSION_ENDED, number=session.number, ended=((session.number, END_STEP_UP_FAILURES),))
                raise SessionRefused(STEP_UP_FAILED, number=session.number)
            return self._info(session)

    def logout(self, token):
        """End the session of a token. Returns its number, or None when the token names no session."""
        with self._lock:
            key = _token_key(token)
            session = self._sessions.get(key) if key is not None else None
            if session is None:
                return None
            self._end(session)
            return session.number

    def end_expired(self):
        """End every session that has reached its eight hours and return their numbers, for the audit log."""
        with self._lock:
            now = self._now()
            expired = sorted((session for session in self._sessions.values() if now - session.started >= SESSION_SECONDS), key=lambda entry: entry.number)
            for session in expired:
                self._end(session)
            return tuple(session.number for session in expired)

    # ── the database password of a session ──────────────────────────────────

    def set_db_password(self, token, current_file_digest, value):
        """Keep a database password for a session, until it is cleared or replaced or the session ends. `value` is text or a `SecretValue`. A password it replaces is kept for redaction. The caller must have verified the management password again for this very request (`step_up`; the broker does): only the newest 64 retired passwords are kept, so whoever could replace a session's password at will could push older ones out of the redaction list by replacing it 64 times. Raises TypeError for a value that is not text, before anything else, and SessionRefused `unauthorized` as `session_for` does, with the same ends."""
        secret = value.reveal() if isinstance(value, SecretValue) else value
        if not isinstance(secret, str) or not secret:
            raise TypeError("a database password is text")
        with self._lock:
            session = self._current(token, current_file_digest, self._now())
            if session.db_password is not None and session.db_password.reveal() != secret:
                self._retire(session.db_password)
            session.db_password = SecretValue(secret)

    def clear_db_password(self, token, current_file_digest):
        """Forget the database password of a session; it is kept for redaction. Raises SessionRefused `unauthorized` as `session_for` does."""
        with self._lock:
            session = self._current(token, current_file_digest, self._now())
            self._retire(session.db_password)
            session.db_password = None

    def db_password(self, token, current_file_digest):
        """The database password of a session as a `SecretValue`, or None when it holds none. Raises SessionRefused `unauthorized` as `session_for` does."""
        with self._lock:
            return self._current(token, current_file_digest, self._now()).db_password

    def remember_secret(self, value):
        """Keep a database password that a request carried in its fields (text or a `SecretValue`), for redaction only, among the newest 64 such passwords. A password of fewer than 8 characters is not kept: it is covered in the reply of the request that carried it, and in no later one. Raises TypeError for a value that is not text."""
        secret = value.reveal() if isinstance(value, SecretValue) else value
        if not isinstance(secret, str) or not secret:
            raise TypeError("a database password is text")
        if len(secret) < REMEMBERED_SECRET_MIN_CHARS:
            return
        with self._lock:
            self._remembered = [kept for kept in self._remembered if kept.reveal() != secret]
            self._remembered.append(SecretValue(secret))
            del self._remembered[:-REMEMBERED_SECRETS_MAX]

    def session_secrets(self):
        """The database passwords that live sessions hold and the newest 64 that sessions held before, each once, as `SecretValue` objects: `known_secrets` without the ones requests carried."""
        with self._lock:
            found = {}
            for secret in [session.db_password for session in self._sessions.values()] + self._retired:
                if secret is not None:
                    found.setdefault(secret.reveal(), secret)
            return tuple(found.values())

    def known_secrets(self):
        """Every database password that a live session holds, the newest 64 that sessions held before and the newest 64 that requests carried, each once, as `SecretValue` objects: what has to be overwritten in text that leaves the broker."""
        with self._lock:
            found = {}
            for secret in [session.db_password for session in self._sessions.values()] + self._retired + self._remembered:
                if secret is not None:
                    found.setdefault(secret.reveal(), secret)
            return tuple(found.values())
