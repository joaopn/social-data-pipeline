"""Tests for `control.sessions`: the login budget, the table of sessions and the second check of the management password.

The contracts pinned here:

- The login budget holds five failures and gets one back every 30 seconds. With the budget empty a login is refused without verifying anything, the right password included. A success takes nothing and gives nothing back; `reset_budget` fills it.
- Logins run one at a time, start to end, so that logins from several threads never get more verifications than the budget has units. A step-up waits for a login that is running.
- The token is handed out once and is not kept: nothing reachable from the object holds it. A session's public number is a counter.
- A session ends at exactly eight hours, and at its first request after the password file changed or went away, together with every other session that was verified against another file, the one whose login raced the change included.
- A step-up does not draw on the login budget; its fifth failure in a row ends the session and says so; a success sets the session's count back and leaves the login budget alone.
- A database password a session holds is in no repr, and is forgotten when the session ends.
- No refusal holds a token, a password or any other input.

The clock, the token source and the verification are stand-ins: no test waits for time to pass and none derives anything.
"""

from __future__ import annotations

import gc
import hashlib
import threading
from types import SimpleNamespace

import pytest

from social_data_pipeline.control import passwords, sessions
from social_data_pipeline.control.types import SecretValue

RIGHT = "made-up-right-password-77aa"
WRONG = "made-up-wrong-password-19f0"
DB_PASSWORD = "made-up-database-password-5c2d"
OLD = SimpleNamespace(digest=b"o" * 32)
NEW = SimpleNamespace(digest=b"n" * 32)
EIGHT_HOURS = 8 * 60 * 60


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class Tokens:
    """A token source whose tokens are known to the test: made-up text that looks like nothing else in the object."""

    def __init__(self, prefix="made-up-token"):
        self.prefix = prefix
        self.handed_out = []

    def __call__(self):
        token = f"{self.prefix}-{len(self.handed_out) + 1:04d}-Qz8"
        self.handed_out.append(token)
        return token


class Verify:
    """Stands in for the derivation: answers whether the candidate is RIGHT and records every call."""

    def __init__(self):
        self.calls = []

    def __call__(self, password_file, candidate):
        self.calls.append((password_file, candidate))
        return candidate == RIGHT


class Broker:
    """A `Sessions` object with its stand-ins, and the password file the broker would read."""

    def __init__(self, verify=None):
        self.clock = Clock()
        self.tokens = Tokens()
        self.verify = verify or Verify()
        self.sessions = sessions.Sessions(now=self.clock, new_token=self.tokens, verify=self.verify)
        self.file = OLD
        self.reads = 0

    def read_file(self):
        self.reads += 1
        return self.file

    def login(self, candidate=RIGHT):
        return self.sessions.login(candidate, self.read_file)

    def fail_logins(self, count):
        for _ in range(count):
            assert refused(lambda: self.login(WRONG)).code == "unauthorized"


@pytest.fixture
def broker():
    return Broker()


def refused(call):
    """The SessionRefused a call raises."""
    with pytest.raises(sessions.SessionRefused) as caught:
        call()
    return caught.value


def reachable_from(root):
    """Every object that can be reached from `root` by following references. Classes and the test's own stand-ins (they are callable) are not entered. The keys of a dict are added by hand: the garbage collector does not report keys that are text."""
    seen, todo, found = set(), [root], []
    while todo:
        item = todo.pop()
        if id(item) in seen or isinstance(item, type) or callable(item):
            continue
        seen.add(id(item))
        found.append(item)
        todo.extend(gc.get_referents(item))
        if isinstance(item, dict):
            todo.extend(item)
        todo.extend(getattr(item, "__dict__", {}))
    return found


class Recorder:
    """What the threads of a test did, in order, as (what, thread name): `asks` when a thread asks for the lock of the `Sessions` object (before it waits for it), `enter` and `leave` around a verification. Every note wakes up whoever waits for one."""

    def __init__(self):
        self.order = []
        self.changed = threading.Condition()

    def note(self, what):
        """Note what this thread did; returns its place in the order."""
        with self.changed:
            self.order.append((what, threading.current_thread().name))
            self.changed.notify_all()
            return len(self.order) - 1

    def wait_for(self, condition):
        """Wait until `condition(order)` holds. A test that gets here with a broken lock does not wait: its condition also holds as soon as another verification has begun. The time limit only stops a test that is wrong in itself, and is then a failure."""
        with self.changed:
            if not self.changed.wait_for(lambda: condition(self.order), timeout=20):
                self.order.append(("gave up", threading.current_thread().name))

    def began_after(self, place):
        """Whether a verification began after the one noted at `place`."""
        return any(what == "enter" for what, _ in self.order[place + 1:])

    def most_running(self):
        count = most = 0
        for what, _ in self.order:
            count += 1 if what == "enter" else -1 if what == "leave" else 0
            most = max(most, count)
        return most

    def lock(self):
        """A lock that notes every thread that asks for it, then takes a real one."""
        recorder, real = self, threading.Lock()

        class Announcing:
            def __enter__(self):
                recorder.note("asks")
                real.acquire()
                return self

            def __exit__(self, *exc):
                real.release()

        return Announcing()


def test_the_search_finds_text_wherever_it_is_kept():
    """Bug class: a search of the object graph that is blind to one kind of place (the keys of a dict, a slot, a nested list), so that the test that relies on it passes whatever the object keeps."""
    class Slots:
        __slots__ = ("held",)

        def __init__(self, held):
            self.held = held

    class Plain:
        def __init__(self):
            self.attribute = "as an attribute"
            self.table = {"as a key": Slots(["in a slot", ("in a tuple", {"in a set"})]), b"bytes key": "as a value"}

    found = [item for item in reachable_from(Plain()) if isinstance(item, (str, bytes))]

    assert {"as an attribute", "as a key", "in a slot", "in a tuple", "in a set", b"bytes key", "as a value", "attribute", "table"} <= set(found)


# ── 1. the login budget ─────────────────────────────────────────────────────


class TestBudget:
    def test_five_failures_empty_it_and_the_sixth_attempt_is_not_even_verified(self, broker):
        """Bug class: a throttle that still verifies and only then refuses. Guessing would cost the attacker nothing but the answer's text, and every guess would still cost the host a derivation; and a throttle that lets the right password through would tell the attacker when a guess was right."""
        broker.fail_logins(5)

        with_wrong = refused(lambda: broker.login(WRONG))
        with_right = refused(lambda: broker.login(RIGHT))

        assert with_wrong.code == with_right.code == "throttled"
        assert len(broker.verify.calls) == 5
        assert broker.sessions.budget() == 0

    def test_the_thirty_seconds_run_from_the_first_failure_and_not_from_the_last(self, broker):
        """Bug class: every failure pushing the refill further away. One wrong login every 29 seconds, from anyone who can reach the socket, would then keep the budget from ever refilling, and the operator out for as long as it goes on."""
        broker.fail_logins(1)
        broker.clock.now += 20
        broker.fail_logins(1)
        assert broker.sessions.budget() == 3
        broker.clock.now += 10

        assert broker.sessions.budget() == 4
        broker.clock.now += 29.999
        assert broker.sessions.budget() == 4
        broker.clock.now += 0.001
        assert broker.sessions.budget() == 5

    def test_one_unit_comes_back_every_thirty_seconds_and_never_more_than_five(self, broker):
        """Bug class: a budget that refills all at once, per failed attempt or without a cap (after a quiet day an attacker would have thousands of guesses), or one that never refills and locks the operator out for good."""
        broker.fail_logins(5)

        broker.clock.now += 29.999
        assert refused(lambda: broker.login(WRONG)).code == "throttled"
        broker.clock.now += 0.001
        assert refused(lambda: broker.login(WRONG)).code == "unauthorized"
        assert refused(lambda: broker.login(WRONG)).code == "throttled"
        broker.clock.now += 59.999
        assert broker.sessions.budget() == 1
        broker.clock.now += 0.001
        assert broker.sessions.budget() == 2
        broker.clock.now += 10 * EIGHT_HOURS
        assert broker.sessions.budget() == 5
        broker.fail_logins(5)
        assert refused(lambda: broker.login(WRONG)).code == "throttled"
        assert len(broker.verify.calls) == 11

    def test_a_success_takes_nothing_and_gives_nothing_back(self, broker):
        """Bug class: a success that refills the budget (an attacker who also holds a low-value right guess, or the operator logging in, would reset the throttle for everyone) or one that uses a unit up (the operator's own logins would lock the operator out)."""
        broker.fail_logins(4)

        for _ in range(10):
            broker.login(RIGHT)

        assert broker.sessions.budget() == 1
        broker.fail_logins(1)
        assert refused(lambda: broker.login(RIGHT)).code == "throttled"

    def test_reset_budget_fills_it(self, broker):
        """Bug class: no way back for the operator at the terminal after the budget ran empty, or a reset that leaves the refill clock running from the old failures."""
        broker.fail_logins(5)

        broker.sessions.reset_budget()

        assert broker.sessions.budget() == 5
        assert broker.login(RIGHT).number == 1
        broker.fail_logins(5)
        broker.clock.now += 29.999
        assert refused(lambda: broker.login(RIGHT)).code == "throttled"

    @pytest.mark.parametrize("candidate", [None, 12, b"bytes", ["list"], ""])
    def test_a_candidate_that_is_not_a_password_costs_a_unit_like_any_wrong_one(self, candidate):
        """Bug class: junk input skipping the budget, so that a flood of malformed logins is never throttled."""
        broker = Broker(verify=passwords.verify)
        broker.file = passwords.PasswordFile(n=2**15, r=8, p=1, salt=b"s" * 16, hash=b"h" * 32, digest=b"d" * 32)

        assert refused(lambda: broker.login(candidate)).code == "unauthorized"
        assert broker.sessions.budget() == 4

    def test_no_password_set_and_a_file_that_is_not_usable_have_their_own_codes(self, broker):
        """Bug class: "no password was set" or "the file is damaged" answered as a wrong password: the operator is told to retype a password that cannot work, and the budget is used up by it."""
        def not_usable():
            raise passwords.PasswordFileNotUsable()

        missing = refused(lambda: broker.sessions.login(RIGHT, lambda: passwords.NO_PASSWORD_FILE))
        none = refused(lambda: broker.sessions.login(RIGHT, lambda: None))
        damaged = refused(lambda: broker.sessions.login(RIGHT, not_usable))

        assert (missing.code, none.code, damaged.code) == ("no_password_set", "no_password_set", "password_file_not_usable")
        assert broker.verify.calls == [] and broker.sessions.budget() == 5

    def test_a_derivation_that_fails_is_not_a_wrong_password(self):
        """Bug class: scrypt running out of memory counted as a failed guess, or leaving the login as a raw error."""
        def no_memory(password_file, candidate):
            raise passwords.PasswordNotDerivable()

        broker = Broker(verify=no_memory)

        assert refused(lambda: broker.login(RIGHT)).code == "password_file_not_usable"
        assert broker.sessions.budget() == 5

    def test_logins_from_many_threads_get_no_more_verifications_than_the_budget_has(self):
        """Bug class: the budget read and written in separate steps. Twenty logins that arrive together all see "one unit left" and all verify: the throttle then limits nothing against exactly the attacker it is for. The first verification waits until all twenty threads have asked for the lock (they have arrived) or until a second verification has begun (there is no lock): no timing decides the outcome."""
        recorder = Recorder()

        def verify(password_file, candidate):
            place = recorder.note("enter")
            # All twenty have arrived; or another verification began meanwhile; or, with no lock at all, every thread has verified or is done.
            recorder.wait_for(lambda order: len({name for what, name in order if what == "asks"}) == 20 or recorder.began_after(place) or len({name for what, name in order if what in ("enter", "done")}) == 20)
            recorder.note("leave")
            return False

        broker = Broker(verify=verify)
        broker.sessions._lock = recorder.lock()
        results = []

        def attempt():
            results.append(refused(lambda: broker.login(WRONG)).code)
            recorder.note("done")

        threads = [threading.Thread(target=attempt, name=f"login-{number}") for number in range(20)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not any(thread.is_alive() for thread in threads)
        assert recorder.most_running() == 1 and ("gave up" not in {what for what, _ in recorder.order})
        assert sum(1 for what, _ in recorder.order if what == "enter") == 5
        assert sorted(results) == ["throttled"] * 15 + ["unauthorized"] * 5


# ── 2. tokens and numbers ───────────────────────────────────────────────────


class TestTokens:
    def test_the_token_is_handed_out_once_and_is_nowhere_in_the_object(self, broker):
        """Bug class: the table keyed by the token itself. Anything that can read the broker's memory or a dump of the object (a debugger, a core file, a repr in a log) would then hold tokens it can log in with; with only the SHA-256 kept it holds nothing usable."""
        first = broker.login()
        second = broker.login()
        broker.sessions.set_db_password(first.token, OLD.digest, DB_PASSWORD)

        strings = [item for item in reachable_from(broker.sessions) if isinstance(item, (str, bytes, bytearray))]

        assert first.token == broker.tokens.handed_out[0] and second.token == broker.tokens.handed_out[1]
        for token in (first.token, second.token):
            assert not any(token in item for item in strings if isinstance(item, str))
            assert not any(token.encode() in bytes(item) for item in strings if not isinstance(item, str))
        # The search does find what the object really keeps: the hash of each token, and the database password.
        assert all(hashlib.sha256(token.encode()).digest() in strings for token in (first.token, second.token))
        assert any(item == DB_PASSWORD for item in strings)
        assert broker.sessions.session_for(first.token, OLD.digest).number == 1

    def test_the_default_token_is_long_and_random(self):
        """Bug class: a default token source that is short, predictable or constant (a counter, the time), which makes a session guessable."""
        made = sessions.Sessions(verify=Verify())

        tokens = [made.login(RIGHT, lambda: OLD).token for _ in range(20)]

        assert len(set(tokens)) == 20
        assert all(isinstance(token, str) and len(token) >= 43 for token in tokens)

    def test_a_session_number_is_a_counter_and_tells_nothing_of_the_token(self):
        """Bug class: a public number derived from the token (its first characters, a hash of it), which hands a part of the secret to everything the number is written into: the audit log, error replies."""
        one, other = Broker(), Broker()
        other.tokens.prefix = "another-made-up-token"

        numbers = [[broker.login().number for _ in range(3)] for broker in (one, other)]

        assert numbers == [[1, 2, 3], [1, 2, 3]]
        assert one.sessions.session_for(one.tokens.handed_out[2], OLD.digest).number == 3

    def test_the_result_of_a_login_does_not_show_the_token(self, broker):
        """Bug class: the token in a log line through the repr of the login result."""
        login = broker.login()

        assert login.token not in repr(login) and "number=1" in repr(login)

    @pytest.mark.parametrize("token", [None, 0, 1, True, b"made-up-token-0001-Qz8", ["made-up-token-0001-Qz8"], "", "made-up-token-0001-Qz", "made-up-token-0001-Qz8 ", "MADE-UP-TOKEN-0001-QZ8", "made-up\ud800token"])
    def test_what_is_not_a_token_of_a_session_is_unauthorized(self, broker, token):
        """Bug class: a lookup that raises on a token that is not text, or that accepts a prefix, another case or a padded form of a real token."""
        broker.login()

        error = refused(lambda: broker.sessions.session_for(token, OLD.digest))

        assert error.code == "unauthorized" and error.ended == () and error.number is None
        assert broker.sessions.logout(token) is None

    def test_a_token_source_that_repeats_itself_is_not_trusted(self, broker):
        """Bug class: a broken token source handing two logins the same token, so that one login silently takes over the other's session."""
        broker.sessions = sessions.Sessions(now=broker.clock, new_token=lambda: "made-up-constant-token", verify=broker.verify)
        broker.login()

        with pytest.raises(RuntimeError):
            broker.login()


# ── 3. when a session ends ──────────────────────────────────────────────────


class TestEnds:
    def test_a_session_ends_at_exactly_eight_hours(self, broker):
        """Bug class: a lifetime measured in the wrong unit, counted from the last request instead of the login (a session that never ends while it is used), or an off-by-one that lets a session live past its time."""
        token = broker.login().token

        broker.clock.now += EIGHT_HOURS - 0.001
        info = broker.sessions.session_for(token, OLD.digest)
        broker.clock.now += 0.001
        at_eight_hours = refused(lambda: broker.sessions.session_for(token, OLD.digest))
        afterwards = refused(lambda: broker.sessions.session_for(token, OLD.digest))

        assert info.number == 1 and info.started == 1000.0
        assert at_eight_hours.code == "unauthorized" and at_eight_hours.ended == ((1, "expired"),) and at_eight_hours.number == 1
        assert afterwards.code == "unauthorized" and afterwards.ended == ()

    def test_a_changed_password_ends_every_session_of_the_old_one(self, broker):
        """Bug class: sessions that outlive a password change. The change is what the operator does when the password leaked; a session logged in with the old one must not keep working. That includes the login that read the old file a moment before it was replaced and got its session a moment after."""
        first = broker.login().token
        second = broker.login().token
        broker.file = NEW
        with_new = broker.login().token
        # A login that raced the change: it read the old bytes, and its session is created when the new file is already in place.
        raced = broker.sessions.login(RIGHT, lambda: OLD).token

        error = refused(lambda: broker.sessions.session_for(raced, NEW.digest))

        assert error.code == "unauthorized" and error.number == 4
        assert error.ended == ((1, "password_changed"), (2, "password_changed"), (4, "password_changed"))
        for token in (first, second, raced):
            assert refused(lambda: broker.sessions.session_for(token, NEW.digest)).ended == ()
        assert broker.sessions.session_for(with_new, NEW.digest).number == 3

    def test_a_session_of_the_current_password_does_not_end_the_others_by_asking(self, broker):
        """Bug class: the sweep running the wrong way round, a request of a good session ending it or the others."""
        first = broker.login().token
        second = broker.login().token

        assert broker.sessions.session_for(first, OLD.digest).number == 1
        assert broker.sessions.session_for(second, OLD.digest).number == 2

    @pytest.mark.parametrize("digest", [None, b"", "o" * 32, bytearray(b"o" * 32), 0])
    def test_a_password_file_that_is_gone_ends_the_sessions(self, broker, digest):
        """Bug class: "no file" compared as equal to anything, or skipped: removing the password file is how an operator shuts the web access, and must end what is logged in. A digest of another type is never taken as equal."""
        first = broker.login().token
        broker.login()

        error = refused(lambda: broker.sessions.session_for(first, digest))

        assert error.code == "unauthorized" and error.ended == ((1, "password_changed"), (2, "password_changed"))

    def test_end_expired_ends_the_old_sessions_and_names_them(self, broker):
        """Bug class: expired sessions that stay in the table until someone uses their token (their database passwords stay in memory with them), or a sweep that ends live ones."""
        old_one = broker.login().token
        broker.sessions.set_db_password(old_one, OLD.digest, DB_PASSWORD)
        broker.login()
        broker.clock.now += EIGHT_HOURS - 1
        young = broker.login().token
        assert broker.sessions.end_expired() == ()
        broker.clock.now += 1

        ended = broker.sessions.end_expired()

        assert ended == (1, 2)
        assert broker.sessions.end_expired() == ()
        assert broker.sessions.session_for(young, OLD.digest).number == 3
        # Its password is still known, for redaction.
        assert [secret.reveal() for secret in broker.sessions.known_secrets()] == [DB_PASSWORD]
        assert refused(lambda: broker.sessions.session_for(old_one, OLD.digest)).ended == ()

    def test_logout_ends_one_session(self, broker):
        """Bug class: a logout that leaves the token valid, or that ends another session than its own."""
        first = broker.login().token
        second = broker.login().token

        assert broker.sessions.logout(first) == 1
        assert broker.sessions.logout(first) is None
        assert refused(lambda: broker.sessions.session_for(first, OLD.digest)).code == "unauthorized"
        assert broker.sessions.session_for(second, OLD.digest).number == 2

    def test_at_most_sixty_four_sessions_live_and_the_oldest_makes_room(self, broker):
        """Bug class: no bound on the table, so that whoever knows the password (or a script that logs in in a loop) fills the broker's memory with sessions; or room made by ending a session at random, or the newest, or without saying so, so that the audit log cannot tell why an operator was logged out."""
        tokens = [broker.login() for _ in range(64)]
        assert all(login.ended == () for login in tokens)
        failed = refused(lambda: broker.login(WRONG))

        newest = broker.login()
        after = broker.login()

        assert failed.ended == ()
        assert newest.number == 65 and newest.ended == ((1, "superseded"),)
        assert after.number == 66 and after.ended == ((2, "superseded"),)
        assert refused(lambda: broker.sessions.session_for(tokens[0].token, OLD.digest)).code == "unauthorized"
        assert [broker.sessions.session_for(login.token, OLD.digest).number for login in tokens[2:]] == list(range(3, 65))
        assert broker.sessions.session_for(newest.token, OLD.digest).number == 65

    def test_room_is_made_from_expired_sessions_first(self, broker):
        """Bug class: a live session ended to make room while sessions that have reached their eight hours are still in the table."""
        old = [broker.login() for _ in range(3)]
        broker.clock.now += EIGHT_HOURS - 1
        young = [broker.login() for _ in range(61)]
        broker.clock.now += 1

        login = broker.login()

        assert login.ended == ((1, "expired"), (2, "expired"), (3, "expired"))
        assert all(broker.sessions.session_for(entry.token, OLD.digest) for entry in young)
        assert all(refused(lambda: broker.sessions.session_for(entry.token, OLD.digest)).ended == () for entry in old)

    @pytest.mark.parametrize("missing", ["no such clock", "the system refuses it"])
    def test_without_a_boot_time_clock_the_default_is_the_monotonic_one(self, monkeypatch, missing):
        """Bug class: a broker that cannot start where the boot-time clock does not exist (an AttributeError at the first login), or that falls back to the wall clock, which jumps when it is set."""
        if missing == "no such clock":
            monkeypatch.delattr(sessions.time, "CLOCK_BOOTTIME")
        else:
            monkeypatch.setattr(sessions.time, "clock_gettime", lambda clock: (_ for _ in ()).throw(OSError(22, "Invalid argument")))
        monkeypatch.setattr(sessions.time, "monotonic", lambda: 4321.0)
        made = sessions.Sessions(verify=Verify())

        login = made.login(RIGHT, lambda: OLD)

        assert made.session_for(login.token, OLD.digest).started == 4321.0

    def test_the_default_clock_counts_the_time_the_machine_was_suspended(self, monkeypatch):
        """Bug class: a monotonic clock that stops while the machine sleeps, so that a session started before a night with the lid closed is still within its eight hours the next morning."""
        asked = []
        monkeypatch.setattr(sessions.time, "clock_gettime", lambda clock: asked.append(clock) or 12345.0)
        made = sessions.Sessions(verify=Verify())

        login = made.login(RIGHT, lambda: OLD)

        assert made.session_for(login.token, OLD.digest).started == 12345.0
        assert asked and set(asked) == {sessions.time.CLOCK_BOOTTIME}


# ── 4. step-up ──────────────────────────────────────────────────────────────


class TestStepUp:
    def test_failures_do_not_draw_on_the_login_budget_and_the_fifth_ends_the_session(self, broker):
        """Bug class: step-up failures taken from the login budget (someone with a stolen token could lock every login out), or no bound at all on them (the stolen token would be a way to guess the management password without the throttle)."""
        token = broker.login().token
        other = broker.login().token

        failures = [refused(lambda: broker.sessions.step_up(token, WRONG, broker.read_file)) for _ in range(5)]
        afterwards = refused(lambda: broker.sessions.step_up(token, RIGHT, broker.read_file))

        assert [error.code for error in failures] == ["step_up_failed"] * 4 + ["session_ended"]
        assert [error.ended for error in failures] == [()] * 4 + [((1, "step_up_failures"),)]
        assert all(error.number == 1 for error in failures)
        assert afterwards.code == "unauthorized" and afterwards.number is None
        assert broker.sessions.budget() == 5
        # The other session is untouched.
        assert broker.sessions.step_up(other, RIGHT, broker.read_file).number == 2

    def test_a_success_does_not_set_the_count_back_nor_touch_the_login_budget(self, broker):
        """Bug class: a successful step-up setting the failures back (whoever holds a stolen token and knows one wrong-but-close guess pattern gets unlimited guesses, four at a time, by typing the right password in between, if it ever learns it; and a session that has guessed wrong four times is as good as new), or refilling the login budget (whoever is logged in could reset the throttle for a guesser)."""
        broker.fail_logins(3)
        token = broker.login().token

        codes = []
        for _ in range(2):
            codes.append(refused(lambda: broker.sessions.step_up(token, WRONG, broker.read_file)).code)
            codes.append(broker.sessions.step_up(token, RIGHT, broker.read_file).number)
        codes.extend(refused(lambda: broker.sessions.step_up(token, WRONG, broker.read_file)).code for _ in range(3))

        assert codes == ["step_up_failed", 1, "step_up_failed", 1, "step_up_failed", "step_up_failed", "session_ended"]
        assert broker.sessions.budget() == 2

    def test_a_step_up_for_no_session_is_refused_before_anything_is_read_or_verified(self, broker):
        """Bug class: verifying the management password for whoever asks, without a session: an unthrottled way to guess it."""
        broker.login()
        verified, read = len(broker.verify.calls), broker.reads

        error = refused(lambda: broker.sessions.step_up("made-up-token-of-nobody", RIGHT, broker.read_file))

        assert error.code == "unauthorized"
        assert len(broker.verify.calls) == verified and broker.reads == read

    def test_a_step_up_after_the_password_changed_ends_the_session_without_verifying(self, broker):
        """Bug class: a step-up verifying against the new file for a session of the old password, which would let that session go on by typing the new password once."""
        token = broker.login().token
        verified = len(broker.verify.calls)
        broker.file = NEW

        changed = refused(lambda: broker.sessions.step_up(token, RIGHT, broker.read_file))
        token = broker.login().token
        broker.file = passwords.NO_PASSWORD_FILE
        gone = refused(lambda: broker.sessions.step_up(token, RIGHT, broker.read_file))

        assert changed.code == "unauthorized" and changed.ended == ((1, "password_changed"),)
        assert gone.code == "unauthorized" and gone.ended == ((2, "password_changed"),)
        assert len(broker.verify.calls) == verified + 1

    def test_a_step_up_with_a_file_that_is_not_usable_is_not_a_failure(self, broker):
        """Bug class: a damaged password file counted as five wrong passwords, ending the session of an operator who typed the right one."""
        token = broker.login().token

        def not_usable():
            raise passwords.PasswordFileNotUsable()

        codes = [refused(lambda: broker.sessions.step_up(token, RIGHT, not_usable)).code for _ in range(6)]

        assert codes == ["password_file_not_usable"] * 6
        assert broker.sessions.step_up(token, RIGHT, broker.read_file).number == 1

    def test_a_step_up_of_an_expired_session_is_refused(self, broker):
        """Bug class: the lifetime checked on ordinary requests only, so that the most dangerous calls are the ones an expired session can still make."""
        token = broker.login().token
        verified = len(broker.verify.calls)
        broker.clock.now += EIGHT_HOURS

        error = refused(lambda: broker.sessions.step_up(token, RIGHT, broker.read_file))

        assert error.code == "unauthorized" and error.ended == ((1, "expired"),)
        assert len(broker.verify.calls) == verified

    def test_a_step_up_waits_for_a_login_that_is_running(self):
        """Bug class: a second lock for step-ups, with which a login and a step-up derive at the same time: twice the memory, and with a few sessions as many derivations in parallel as an attacker likes. The login's verification goes on only once the step-up has asked for the lock, or once a second verification has begun: the order of what happened decides, not a pause."""
        recorder = Recorder()

        def verify(password_file, candidate):
            place = recorder.note("enter")
            if candidate == WRONG:
                recorder.wait_for(lambda order: ("asks", "step-up") in order or recorder.began_after(place))
            recorder.note("leave")
            return candidate == RIGHT

        broker = Broker(verify=verify)
        token = broker.login().token
        recorder.order.clear()
        broker.sessions._lock = recorder.lock()
        outcome = {}
        login = threading.Thread(target=lambda: outcome.setdefault("login", refused(lambda: broker.login(WRONG)).code), name="login")
        step_up = threading.Thread(target=lambda: outcome.setdefault("step_up", broker.sessions.step_up(token, RIGHT, broker.read_file).number), name="step-up")
        login.start()
        recorder.wait_for(lambda order: ("enter", "login") in order)
        step_up.start()
        login.join(timeout=30)
        step_up.join(timeout=30)

        assert not login.is_alive() and not step_up.is_alive()
        assert outcome == {"login": "unauthorized", "step_up": 1}
        assert recorder.order == [("asks", "login"), ("enter", "login"), ("asks", "step-up"), ("leave", "login"), ("enter", "step-up"), ("leave", "step-up")]


# ── 5. the database password of a session ───────────────────────────────────


class TestDbPassword:
    def test_it_is_kept_wrapped_and_shows_in_no_repr(self, broker):
        """Bug class: the database password in a log line or an error page through the repr of the session table, of a session, of the retired passwords, of what a request gets back or of the value itself."""
        token = broker.login().token
        other = broker.login().token
        broker.sessions.set_db_password(token, OLD.digest, DB_PASSWORD)
        broker.sessions.set_db_password(other, OLD.digest, DB_PASSWORD + "-retired")
        broker.sessions.logout(other)

        held = broker.sessions.db_password(token, OLD.digest)
        info = broker.sessions.session_for(token, OLD.digest)
        shown = [repr(broker.sessions), str(broker.sessions), repr(vars(broker.sessions)), repr(list(broker.sessions._sessions.values())), repr(info), str(info), repr(held), str(held), f"{held}", repr(broker.sessions.known_secrets()), repr([held])]

        assert isinstance(held, SecretValue) and held.reveal() == DB_PASSWORD
        assert info.has_db_password is True
        assert not any(DB_PASSWORD in text for text in shown)

    def test_it_is_set_replaced_and_cleared(self, broker):
        """Bug class: a replaced or cleared password still handed to a later call of the session, which then runs with credentials the operator took back."""
        first = broker.login().token
        assert broker.sessions.db_password(first, OLD.digest) is None and broker.sessions.session_for(first, OLD.digest).has_db_password is False

        broker.sessions.set_db_password(first, OLD.digest, DB_PASSWORD)
        broker.sessions.set_db_password(first, OLD.digest, SecretValue(DB_PASSWORD + "-replaced"))
        replaced = broker.sessions.db_password(first, OLD.digest).reveal()
        broker.sessions.clear_db_password(first, OLD.digest)
        cleared = broker.sessions.db_password(first, OLD.digest)

        assert replaced == DB_PASSWORD + "-replaced" and cleared is None
        assert broker.sessions.session_for(first, OLD.digest).has_db_password is False

    @pytest.mark.parametrize("ending", ["logout", "expired", "password_changed", "step_up_failures", "superseded", "cleared", "replaced"])
    def test_a_password_a_session_no_longer_holds_is_still_known_for_redaction(self, broker, ending):
        """Bug class: redaction that forgets a password the moment its session ends. A run the session started goes on printing, and its log is read by another session (or the same operator after a new login) with the password in clear."""
        token = broker.login().token
        broker.sessions.set_db_password(token, OLD.digest, DB_PASSWORD)
        if ending == "logout":
            broker.sessions.logout(token)
        elif ending == "expired":
            broker.clock.now += EIGHT_HOURS
            broker.sessions.end_expired()
        elif ending == "password_changed":
            refused(lambda: broker.sessions.session_for(token, NEW.digest))
        elif ending == "step_up_failures":
            for _ in range(5):
                refused(lambda: broker.sessions.step_up(token, WRONG, broker.read_file))
        elif ending == "superseded":
            for _ in range(64):
                broker.login()
        elif ending == "cleared":
            broker.sessions.clear_db_password(token, OLD.digest)
        elif ending == "replaced":
            broker.sessions.set_db_password(token, OLD.digest, DB_PASSWORD + "-new")

        known = sorted(secret.reveal() for secret in broker.sessions.known_secrets())

        assert known == ([DB_PASSWORD, DB_PASSWORD + "-new"] if ending == "replaced" else [DB_PASSWORD])

    def test_the_newest_sixty_four_retired_passwords_are_kept_each_once(self, broker):
        """Bug class: a list of retired passwords that grows for as long as the broker runs, or one that keeps the oldest and drops the ones the runs that are still printing were started with; and one value kept many times, which crowds the others out."""
        def retire(value):
            token = broker.login().token
            broker.sessions.set_db_password(token, OLD.digest, value)
            broker.sessions.logout(token)

        for number in range(70):
            retire(f"{DB_PASSWORD}-{number:02d}")
        retire(f"{DB_PASSWORD}-03")
        for _ in range(10):
            retire(f"{DB_PASSWORD}-69")
        live = broker.login().token
        broker.sessions.set_db_password(live, OLD.digest, f"{DB_PASSWORD}-00")
        # 00 is held again, and dropped from the retired ones long ago: listed once, as a live one.

        known = [secret.reveal() for secret in broker.sessions.known_secrets()]

        # The live one first, then the retired ones oldest first. Of 00 to 69 the newest 64 are 06 to 69; 03 retired again pushes 06 out and goes to the newest end, and 69 retired again only moves there.
        expected_retired = [f"{DB_PASSWORD}-{number:02d}" for number in range(7, 69)] + [f"{DB_PASSWORD}-03", f"{DB_PASSWORD}-69"]
        assert len(expected_retired) == 64
        assert known == [f"{DB_PASSWORD}-00"] + expected_retired
        assert len(set(known)) == len(known)

    def test_known_secrets_hold_every_live_session_s_password_each_once(self, broker):
        """Bug class: redaction working from an incomplete list (only the asking session's password, so that one operator reads another's password in a log)."""
        tokens = [broker.login().token for _ in range(4)]
        broker.sessions.set_db_password(tokens[0], OLD.digest, DB_PASSWORD)
        broker.sessions.set_db_password(tokens[1], OLD.digest, DB_PASSWORD + "-other")
        broker.sessions.set_db_password(tokens[2], OLD.digest, DB_PASSWORD)

        everything = broker.sessions.known_secrets()

        assert sorted(secret.reveal() for secret in everything) == [DB_PASSWORD, DB_PASSWORD + "-other"]
        assert all(isinstance(secret, SecretValue) for secret in everything)

    @pytest.mark.parametrize("call", ["set", "clear", "get"])
    def test_it_needs_a_current_session(self, broker, call):
        """Bug class: a database password stored for, or read back with, a token that is unknown or expired, or whose session was verified against a password file that is no longer the one in force: these three calls are requests like any other and get the same checks."""
        token = broker.login().token
        other = broker.login().token
        calls = {"set": lambda value, digest: broker.sessions.set_db_password(value, digest, DB_PASSWORD), "clear": broker.sessions.clear_db_password, "get": broker.sessions.db_password}

        unknown = refused(lambda: calls[call]("made-up-token-of-nobody", OLD.digest))
        changed = refused(lambda: calls[call](token, NEW.digest))
        token = broker.sessions.login(RIGHT, lambda: NEW).token
        broker.clock.now += EIGHT_HOURS
        expired = refused(lambda: calls[call](token, NEW.digest))

        assert unknown.code == "unauthorized" and unknown.ended == ()
        assert changed.code == "unauthorized" and changed.ended == ((1, "password_changed"), (2, "password_changed"))
        assert refused(lambda: broker.sessions.session_for(other, OLD.digest)).ended == ()
        assert expired.code == "unauthorized" and expired.ended == ((3, "expired"),)

    @pytest.mark.parametrize("value", [None, 12, b"bytes", "", ["list"], SecretValue(None)])
    def test_only_text_is_kept(self, broker, value):
        """Bug class: a value that is not text stored as the password and handed to redaction or to a child process later, where it fails far from its cause."""
        token = broker.login().token

        with pytest.raises(TypeError) as caught:
            broker.sessions.set_db_password(token, OLD.digest, value)

        assert str(caught.value) == "a database password is text"
        assert broker.sessions.db_password(token, OLD.digest) is None


# ── 6. what a refusal holds ─────────────────────────────────────────────────


class TestRefusals:
    def test_no_refusal_holds_a_token_a_password_or_any_input(self, broker):
        """Bug class: an error text that quotes what was sent ("wrong password: ...", "unknown token ..."), which writes secrets into whatever shows or logs the error."""
        token = broker.login().token
        broker.sessions.set_db_password(token, OLD.digest, DB_PASSWORD)
        errors = [
            refused(lambda: broker.login(WRONG)),
            refused(lambda: broker.sessions.session_for(token + "x", OLD.digest)),
            refused(lambda: broker.sessions.step_up(token, WRONG, broker.read_file)),
            refused(lambda: broker.sessions.login(RIGHT, lambda: None)),
            refused(lambda: broker.sessions.session_for(token, NEW.digest)),
        ]
        broker.fail_logins(4)
        errors.append(refused(lambda: broker.login(RIGHT)))
        broker.sessions.reset_budget()
        token = broker.login().token
        errors.extend(refused(lambda: broker.sessions.step_up(token, WRONG, broker.read_file)) for _ in range(5))

        assert {error.code for error in errors} == {"unauthorized", "throttled", "no_password_set", "step_up_failed", "session_ended"}
        for error in errors:
            text = error.message + repr(error) + str(error) + repr(error.args) + repr(error.ended) + repr(error.number)
            assert not any(secret in text for secret in (RIGHT, WRONG, DB_PASSWORD, token, "made-up"))
            assert "\n" not in error.message and error.__cause__ is None

    def test_each_code_has_one_constant_message(self):
        """Bug class: a code without a message (a KeyError at the moment of a refusal), or two codes sharing one text."""
        codes = ("unauthorized", "throttled", "no_password_set", "password_file_not_usable", "step_up_failed", "session_ended")

        messages = [sessions.SessionRefused(code).message for code in codes]

        assert len(set(messages)) == len(codes)
        assert all(sessions.SessionRefused(code).code == code for code in codes)
        assert (sessions.UNAUTHORIZED, sessions.THROTTLED, sessions.NO_PASSWORD_SET, sessions.PASSWORD_FILE_NOT_USABLE, sessions.STEP_UP_FAILED, sessions.SESSION_ENDED) == codes

    def test_every_reason_for_an_end_is_one_the_audit_log_can_write(self):
        """Bug class: a reason added here and not to the audit log's closed list, so that the line which says why a session ended is refused and lost exactly when it is written."""
        from social_data_pipeline.control import audit

        reasons = {sessions.END_EXPIRED, sessions.END_PASSWORD_CHANGED, sessions.END_STEP_UP_FAILURES, sessions.END_SUPERSEDED}

        assert reasons == set(audit.SESSION_END_REASONS)
