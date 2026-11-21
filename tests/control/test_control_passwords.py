"""Tests for `control.passwords`, the management password file of the broker: `password.json` in the broker folder, a salted scrypt hash with its parameters.

The contracts pinned here:

- A new password is text of 12 to 1024 characters without a line break or control character; anything else is refused with its own code before anything is derived or written, and the password is in no error.
- The file is written in one step through a temporary file in the same folder, with folder 0700 and file 0600 whatever the umask; a write that fails leaves the old file.
- A link, a named pipe or a folder in the file's place, a linked folder and a file of another user are refused, without waiting on the pipe, on reading and on writing alike. Reading creates nothing.
- The parameters of a file are held against fixed bounds before anything is derived: a planted or edited file is "not usable", and scrypt is never called with its numbers.
- `digest` is the SHA-256 of exactly the bytes that were read, with one open and one read.
- `verify` answers False, without any derivation, for a candidate that could not be a password, and compares in constant time.

The derivation is replaced by a recording stand-in everywhere but in the one test of the real scrypt, which calls it three times.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import signal
import stat
from types import SimpleNamespace

import pytest

from social_data_pipeline.control import files, passwords

MARKER = "made-up-management-password-3e8b"
NOT_USABLE = "the password file is not usable"
SALT = bytes(range(16))


class FakeScrypt:
    """Stands in for `hashlib.scrypt`: records how it was called and returns 32 bytes that depend on the password and the salt."""

    def __init__(self):
        self.calls = []

    def __call__(self, password, *, salt, n, r, p, maxmem, dklen):
        self.calls.append({"salt": salt, "n": n, "r": r, "p": p, "maxmem": maxmem, "dklen": dklen})
        return hashlib.sha256(b"fake" + password + salt).digest()


@pytest.fixture
def folder(tmp_path):
    """The path of a broker folder that does not exist yet."""
    return tmp_path / "broker"


@pytest.fixture
def scrypt_calls(monkeypatch):
    """Replace `hashlib.scrypt` itself by the stand-in, for the tests in which nothing may be derived at all."""
    fake = FakeScrypt()
    monkeypatch.setattr(hashlib, "scrypt", fake)
    return fake.calls


def file_content(**changes):
    """The content of a well-formed password file, with changes; a value of `...` removes the key."""
    content = {"scheme": "scrypt", "n": 32768, "r": 8, "p": 1, "salt": base64.b64encode(SALT).decode(), "hash": base64.b64encode(b"h" * 32).decode()}
    content.update(changes)
    return {key: value for key, value in content.items() if value is not ...}


def plant(folder, raw):
    """Write bytes as the password file, the way someone with access to the folder would."""
    folder.mkdir(mode=0o700, exist_ok=True)
    (folder / "password.json").write_bytes(raw)
    os.chmod(folder / "password.json", 0o600)


def mode(path):
    return stat.S_IMODE(os.lstat(path).st_mode)


@contextlib.contextmanager
def umask(value):
    previous = os.umask(value)
    try:
        yield
    finally:
        os.umask(previous)


@contextlib.contextmanager
def failing_instead_of_hanging(seconds=5):
    """Fail the test when the block takes longer than a few seconds: an open that waits on a named pipe would otherwise hold the whole run."""
    def too_long(signum, frame):
        raise AssertionError("the call did not return: it waits on the named pipe")

    previous = signal.signal(signal.SIGALRM, too_long)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def refusal_of(call, error_class=Exception):
    with pytest.raises(error_class) as caught:
        call()
    return caught.value


# ── 1. a new password ───────────────────────────────────────────────────────


REFUSED_PASSWORDS = [
    ("nothing", None, "password_not_text"),
    ("a number", 123456789012345, "password_not_text"),
    ("bytes", b"made-up-bytes-password", "password_not_text"),
    ("a list", ["made-up-pass", "word"], "password_not_text"),
    ("a lone surrogate", "made-up-pass\ud800word", "password_not_text"),
    ("empty", "", "password_too_short"),
    ("eleven characters", "made-up-pas", "password_too_short"),
    ("1025 characters", "x" * 1025, "password_too_long"),
    ("a newline", "made-up-pass\nword", "password_has_control_character"),
    ("a carriage return", "made-up-pass\rword", "password_has_control_character"),
    ("a null byte", "made-up-pass\x00word", "password_has_control_character"),
    ("a tab", "made-up-pass\tword", "password_has_control_character"),
    ("an escape", "made-up-pass\x1bword", "password_has_control_character"),
    ("delete", "made-up-pass\x7fword", "password_has_control_character"),
    ("next line", "made-up-pass\x85word", "password_has_control_character"),
    ("a line separator", "made-up-pass\u2028word", "password_has_control_character"),
    ("a paragraph separator", "made-up-pass\u2029word", "password_has_control_character"),
]


class TestNewPassword:
    @pytest.mark.parametrize("password, code", [case[1:] for case in REFUSED_PASSWORDS], ids=[case[0] for case in REFUSED_PASSWORDS])
    def test_a_password_that_is_not_acceptable_is_refused_before_anything_happens(self, folder, password, code):
        """Bug class: a password too short to resist guessing, one that cannot be typed again (a control character, a line break that a terminal or a form cuts off) or one that is not text at all being hashed and stored; and the refused password ending up in the error text."""
        scrypt = FakeScrypt()

        error = refusal_of(lambda: passwords.set_password(folder, password, scrypt=scrypt), passwords.PasswordRefused)

        assert error.code == code
        assert error.message == type(error).text == str(error)
        assert "made-up" not in error.message and "made-up" not in repr(error)
        # Nothing was derived and nothing was created.
        assert scrypt.calls == []
        assert not folder.exists()

    @pytest.mark.parametrize("password", ["made-up-pass", "x" * 1024, "made up pass word with spaces", "p\u00e4ssw\u00f6rt-\u043f\u0430\u0440\u043e\u043b\u044c-\u5bc6\u7801"], ids=["12", "1024", "spaces", "non-ascii"])
    def test_the_bounds_are_included_and_any_printable_text_is_accepted(self, folder, password):
        """Bug class: an off-by-one that refuses a password of exactly 12 or 1024 characters, or a rule so narrow (ASCII only, no spaces) that it refuses a good passphrase."""
        scrypt = FakeScrypt()

        passwords.set_password(folder, password, scrypt=scrypt)

        assert passwords.verify(passwords.read_password_file(folder), password, scrypt=scrypt) is True

    def test_the_file_holds_the_parameters_the_salt_and_the_hash_and_never_the_password(self, folder):
        """Bug class: the file written with other parameters than documented, without the explicit memory limit (the default one of `hashlib.scrypt` refuses these parameters), or with the password or anything it can be read from next to the hash."""
        scrypt = FakeScrypt()

        passwords.set_password(folder, MARKER, random_bytes=lambda count: SALT[:count], scrypt=scrypt)

        raw = (folder / "password.json").read_bytes()
        expected_hash = hashlib.sha256(b"fake" + MARKER.encode() + SALT).digest()
        assert json.loads(raw) == {"scheme": "scrypt", "n": 32768, "r": 8, "p": 1, "salt": base64.b64encode(SALT).decode(), "hash": base64.b64encode(expected_hash).decode()}
        assert list(json.loads(raw)) == ["scheme", "n", "r", "p", "salt", "hash"]
        assert scrypt.calls == [{"salt": SALT, "n": 2**15, "r": 8, "p": 1, "maxmem": 64 * 1024 * 1024, "dklen": 32}]
        assert MARKER.encode() not in raw and base64.b64encode(MARKER.encode()) not in raw

    def test_every_password_gets_a_new_salt_of_sixteen_random_bytes(self, folder):
        """Bug class: a constant or reused salt, with which two brokers with the same password have the same file and one table of guesses serves all of them."""
        scrypt = FakeScrypt()

        passwords.set_password(folder, MARKER, scrypt=scrypt)
        passwords.set_password(folder, MARKER, scrypt=scrypt)

        salts = [call["salt"] for call in scrypt.calls]
        assert len(salts) == 2 and salts[0] != salts[1]
        assert all(isinstance(salt, bytes) and len(salt) == 16 for salt in salts)


# ── 2. how the file is written ──────────────────────────────────────────────


class TestWriting:
    @pytest.mark.parametrize("mask", [0o000, 0o077, 0o022])
    def test_the_folder_and_the_file_are_private_whatever_the_umask(self, folder, mask):
        """Bug class: modes left to the umask, so that under a permissive one the hash is readable by every user of the host, who can then guess the password offline."""
        with umask(mask):
            passwords.set_password(folder, MARKER, scrypt=FakeScrypt())
            passwords.set_password(folder, MARKER + "-second", scrypt=FakeScrypt())

        assert mode(folder) == 0o700
        assert mode(folder / "password.json") == 0o600
        assert os.listdir(folder) == ["password.json"]

    def test_the_file_is_replaced_in_one_step_through_a_temporary_file_in_the_same_folder(self, folder, monkeypatch):
        """Bug class: writing into the file itself (a reader in that moment sees an empty or half-written file and every session ends, or a login is verified against half a hash), or a temporary file in another folder, which a rename cannot move in one step."""
        scrypt = FakeScrypt()
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        old = (folder / "password.json").read_bytes()
        seen = []
        real_replace = os.replace

        def recording_replace(source, target, *, src_dir_fd=None, dst_dir_fd=None):
            # At the moment of the rename: the old file is whole, and so is the new content, under another name in the same folder.
            seen.append({"source": source, "target": target, "same_folder": src_dir_fd is not None and src_dir_fd == dst_dir_fd, "old": (folder / "password.json").read_bytes(), "new": (folder / source).read_bytes(), "listing": sorted(os.listdir(folder)), "mode": mode(folder / source)})
            return real_replace(source, target, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

        monkeypatch.setattr(os, "replace", recording_replace)
        passwords.set_password(folder, MARKER + "-second", scrypt=scrypt)
        monkeypatch.undo()

        new = (folder / "password.json").read_bytes()
        assert len(seen) == 1
        step = seen[0]
        assert step["target"] == "password.json" and step["same_folder"]
        assert step["source"].startswith("password.json.") and step["source"].endswith(".tmp") and "/" not in step["source"]
        assert step["old"] == old and step["new"] == new and new != old
        assert step["listing"] == sorted(["password.json", step["source"]]) and step["mode"] == 0o600
        assert os.listdir(folder) == ["password.json"]

    @pytest.mark.parametrize("failing", ["write", "fsync", "replace"])
    def test_a_write_that_fails_leaves_the_old_file_and_no_temporary_one(self, folder, monkeypatch, failing):
        """Bug class: a full disk or an interrupted write leaving half a file under the real name (nobody can log in any more, or the next reader takes it for "no password set"), or temporary files piling up."""
        scrypt = FakeScrypt()
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        old = (folder / "password.json").read_bytes()
        calls = []

        def fail(*args, **kwargs):
            calls.append(failing)
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(os, failing, fail)
        error = refusal_of(lambda: passwords.set_password(folder, MARKER + "-second", scrypt=scrypt))
        monkeypatch.undo()

        assert calls == [failing]
        assert type(error) is passwords.PasswordFileNotUsable and error.message == NOT_USABLE and error.code == "password_file_not_usable"
        assert (folder / "password.json").read_bytes() == old
        assert os.listdir(folder) == ["password.json"]
        assert passwords.verify(passwords.read_password_file(folder), MARKER, scrypt=scrypt) is True

    @pytest.mark.parametrize("failing, where", [("fchmod", "new folder"), ("fchmod", "existing folder"), ("fstat", "new folder"), ("fstat", "existing folder")])
    def test_a_file_system_that_refuses_a_mode_or_an_answer_is_a_refusal_not_an_os_error(self, folder, monkeypatch, failing, where):
        """Bug class: a mount that refuses a change of mode (some do), or a failing look at a descriptor, leaving `set_password` as a raw OSError, which the caller does not expect and whose text names the path."""
        if where == "existing folder":
            folder.mkdir(mode=0o700)
        calls = []

        def refuse(*args):
            calls.append(failing)
            raise OSError(1, f"Operation not permitted: {folder}")

        monkeypatch.setattr(os, failing, refuse)
        error = refusal_of(lambda: passwords.set_password(folder, MARKER, scrypt=FakeScrypt()))
        # Reading changes no mode; it does look at the folder.
        read = refusal_of(lambda: passwords.read_password_file(folder)) if (failing, where) == ("fstat", "existing folder") else None
        monkeypatch.undo()

        assert calls
        assert type(error) is passwords.PasswordFileNotUsable and error.message == NOT_USABLE
        assert read is None or type(read) is passwords.PasswordFileNotUsable
        assert not (folder / "password.json").exists() and [name for name in os.listdir(folder) if name.endswith(".tmp")] == []

    def test_the_helper_turns_a_failing_mode_of_a_new_folder_into_its_own_error(self, folder, monkeypatch):
        """Bug class: the one call of the file rules that let an OSError through: the change of mode of a folder it has just made."""
        calls = []
        monkeypatch.setattr(os, "fchmod", lambda *args: calls.append(args) or (_ for _ in ()).throw(OSError(1, "Operation not permitted")))

        with pytest.raises(files.BrokerFileError) as caught:
            with files.folder(folder, create=True):
                pass

        assert len(calls) == 1 and caught.value.message == files.FOLDER_NOT_USABLE

    def test_a_folder_descriptor_is_used_as_the_folder(self, folder):
        """Bug class: the descriptor form of the folder argument being closed by the call, or being joined to a path as if it were a name."""
        scrypt = FakeScrypt()
        folder.mkdir(mode=0o700)
        fd = os.open(folder, os.O_RDONLY | os.O_DIRECTORY)
        try:
            passwords.set_password(fd, MARKER, scrypt=scrypt)
            read = passwords.read_password_file(fd)
            # Still open after both calls.
            os.fstat(fd)
        finally:
            os.close(fd)

        assert read.digest == hashlib.sha256((folder / "password.json").read_bytes()).digest()


# ── 3. links, special files, other users ────────────────────────────────────


def make_link_to_a_real_file(folder, tmp_path):
    """A well-formed password file outside the broker folder, and a link to it under the file's name."""
    elsewhere = tmp_path / "elsewhere"
    passwords.set_password(elsewhere, MARKER, scrypt=FakeScrypt())
    folder.mkdir(mode=0o700)
    (folder / "password.json").symlink_to(elsewhere / "password.json")


def make_dangling_link(folder, tmp_path):
    folder.mkdir(mode=0o700)
    (folder / "password.json").symlink_to(tmp_path / "not-there")


def make_pipe(folder, tmp_path):
    folder.mkdir(mode=0o700)
    os.mkfifo(folder / "password.json")


def make_subfolder(folder, tmp_path):
    folder.mkdir(mode=0o700)
    (folder / "password.json").mkdir()


def make_linked_folder(folder, tmp_path):
    real = tmp_path / "real-folder"
    passwords.set_password(real, MARKER, scrypt=FakeScrypt())
    folder.symlink_to(real, target_is_directory=True)


def make_file_as_folder(folder, tmp_path):
    folder.write_text("a file where the folder should be")


PLANTED = [make_link_to_a_real_file, make_dangling_link, make_pipe, make_subfolder, make_linked_folder, make_file_as_folder]


def snapshot(tmp_path):
    """Every path under the temp folder with its kind and, for a regular file, its content."""
    found = []
    for path in sorted(tmp_path.rglob("*")):
        info = os.lstat(path)
        kind = "link" if stat.S_ISLNK(info.st_mode) else "dir" if stat.S_ISDIR(info.st_mode) else "pipe" if stat.S_ISFIFO(info.st_mode) else "file"
        found.append((str(path.relative_to(tmp_path)), kind, path.read_bytes() if kind == "file" else os.readlink(path) if kind == "link" else None))
    return found


class TestPlanted:
    @pytest.mark.parametrize("make", PLANTED, ids=[make.__name__[5:] for make in PLANTED])
    def test_reading_refuses_what_is_not_a_real_file_in_a_real_folder(self, folder, tmp_path, make, scrypt_calls):
        """Bug class: following a link out of the broker folder (the "password" is then whatever file someone pointed at), or opening a named pipe and waiting on it for ever, which hangs every login."""
        make(folder, tmp_path)

        with failing_instead_of_hanging():
            error = refusal_of(lambda: passwords.read_password_file(folder))

        assert type(error) is passwords.PasswordFileNotUsable and error.message == NOT_USABLE
        assert scrypt_calls == []

    @pytest.mark.parametrize("make", PLANTED, ids=[make.__name__[5:] for make in PLANTED])
    def test_writing_refuses_them_too_and_changes_nothing(self, folder, tmp_path, make):
        """Bug class: writing the hash through a link into a file elsewhere, hanging on a pipe, or quietly replacing something that someone planted (it is refused, so that it is noticed)."""
        make(folder, tmp_path)
        before = snapshot(tmp_path)

        with failing_instead_of_hanging():
            error = refusal_of(lambda: passwords.set_password(folder, MARKER + "-second", scrypt=FakeScrypt()))

        assert type(error) is passwords.PasswordFileNotUsable and error.message == NOT_USABLE
        assert snapshot(tmp_path) == before

    @pytest.mark.parametrize("what", ["read", "set"])
    def test_a_folder_or_file_of_another_user_is_refused(self, folder, monkeypatch, what):
        """Bug class: trusting a password file that another user of the host put there, or one left by a run as root, which the operator can then not replace."""
        scrypt = FakeScrypt()
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        before = (folder / "password.json").read_bytes()
        monkeypatch.setattr(os, "geteuid", lambda: os.getuid() + 1)

        error = refusal_of(lambda: passwords.read_password_file(folder) if what == "read" else passwords.set_password(folder, MARKER + "-second", scrypt=scrypt))
        monkeypatch.undo()

        assert type(error) is passwords.PasswordFileNotUsable
        assert (folder / "password.json").read_bytes() == before

    @pytest.mark.parametrize("foreign", ["folder", "file"])
    @pytest.mark.parametrize("what", ["read", "set"])
    def test_the_owner_is_checked_on_the_folder_and_on_the_file_each_by_itself(self, folder, foreign, what):
        """Bug class: the owner checked on one of the two only. A file that another user managed to put into the operator's folder is then taken as the password, or a folder of another user (who can swap its files at will) is trusted because the file in it looks right."""
        scrypt = FakeScrypt()
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        before = (folder / "password.json").read_bytes()
        is_foreign = stat.S_ISDIR if foreign == "folder" else stat.S_ISREG
        real_fstat, real_stat = os.fstat, os.stat

        class AnotherUsers:
            """What the system says about a file, with another owner."""

            def __init__(self, info):
                self.info = info
                self.st_uid = info.st_uid + 1

            def __getattr__(self, name):
                return getattr(self.info, name)

        def seen(info):
            return AnotherUsers(info) if is_foreign(info.st_mode) else info

        # Undone when the block is left, also by an error: the test runner itself asks the system about files.
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "fstat", lambda fd: seen(real_fstat(fd)))
            patch.setattr(os, "stat", lambda *args, **kwargs: seen(real_stat(*args, **kwargs)))
            error = refusal_of(lambda: passwords.read_password_file(folder) if what == "read" else passwords.set_password(folder, MARKER + "-second", scrypt=scrypt))

        assert type(error) is passwords.PasswordFileNotUsable
        assert (folder / "password.json").read_bytes() == before and os.listdir(folder) == ["password.json"]

    def test_no_password_set_is_an_answer_and_reading_creates_nothing(self, folder, tmp_path):
        """Bug class: "no password was ever set" reported as an error (the terminal could not tell the first start from a damaged file), or a read that creates the folder as a side effect."""
        missing_folder = passwords.read_password_file(folder)
        folder.mkdir(mode=0o700)
        missing_file = passwords.read_password_file(folder)

        assert missing_folder is passwords.NO_PASSWORD_FILE and missing_file is passwords.NO_PASSWORD_FILE
        assert not passwords.NO_PASSWORD_FILE and passwords.NO_PASSWORD_FILE.digest is None
        assert os.listdir(tmp_path) == ["broker"] and os.listdir(folder) == []


# ── 4. the content of a file ────────────────────────────────────────────────


def raw(**changes):
    return json.dumps(file_content(**changes)).encode()


def b64(count):
    return base64.b64encode(b"s" * count).decode()


NOT_USABLE_FILES = [
    ("another scheme", raw(scheme="pbkdf2")),
    ("the scheme in capitals", raw(scheme="SCRYPT")),
    ("the scheme as a number", raw(scheme=1)),
    ("no scheme", raw(scheme=...)),
    ("n below the bound", raw(n=2**13)),
    ("n above the bound", raw(n=2**18)),
    ("n far above the bound", raw(n=2**40)),
    ("n not a power of two", raw(n=2**15 + 1)),
    ("n three times a power of two", raw(n=3 * 2**14)),
    ("n zero", raw(n=0)),
    ("n negative", raw(n=-(2**15))),
    ("n as text", raw(n="32768")),
    ("n as a float", raw(n=32768.0)),
    ("n as a boolean", raw(n=True)),
    ("no n", raw(n=...)),
    ("r below the bound", raw(r=7)),
    ("r above the bound", raw(r=17)),
    ("r huge", raw(r=2**30)),
    ("r as text", raw(r="8")),
    ("r as a boolean", raw(r=True)),
    ("p below the bound", raw(p=0)),
    ("p above the bound", raw(p=5)),
    ("p huge", raw(p=2**20)),
    ("p as a boolean", raw(p=True)),
    ("a salt of 15 bytes", raw(salt=b64(15))),
    ("a salt of 65 bytes", raw(salt=b64(65))),
    ("n 2**14, inside the old range", raw(n=2**14)),
    ("n 2**16", raw(n=2**16)),
    ("n 2**17", raw(n=2**17)),
    ("r 9", raw(r=9)),
    ("r 16", raw(r=16)),
    ("p 2", raw(p=2)),
    ("p 4", raw(p=4)),
    ("a salt of 17 bytes", raw(salt=b64(17))),
    ("a salt of 32 bytes", raw(salt=b64(32))),
    ("a salt of 64 bytes", raw(salt=b64(64))),
    ("a salt that is not base64", raw(salt="not base64 at all!")),
    ("a salt that is a number", raw(salt=16)),
    ("no salt", raw(salt=...)),
    ("a hash of 31 bytes", raw(hash=b64(31))),
    ("a hash of 33 bytes", raw(hash=b64(33))),
    ("an empty hash", raw(hash="")),
    ("a hash that is not base64", raw(hash="*" * 44)),
    ("a hash that is null", raw(hash=None)),
    ("one field too many", raw(note="x")),
    ("a list", b"[1, 2]"),
    ("not JSON", b'{"scheme": "scrypt", '),
    ("not UTF-8", b'{"scheme": "scrypt\xff"}'),
    ("a repeated key", raw()[:-1] + b', "n": 16384}'),
    ("an empty file", b""),
    ("more than 4 KiB", raw()[:-1] + b" " * 4096 + b"}"),
]


class TestContent:
    @pytest.mark.parametrize("content", [case[1] for case in NOT_USABLE_FILES], ids=[case[0] for case in NOT_USABLE_FILES])
    def test_a_file_outside_the_bounds_is_not_usable_and_nothing_is_derived_from_it(self, folder, content, scrypt_calls):
        """Bug class: deriving with whatever cost parameters the file names. Whoever can write that one file could make every login take minutes and gigabytes, or pick a scheme or a hash length that makes guessing cheap."""
        plant(folder, content)

        error = refusal_of(lambda: passwords.read_password_file(folder))

        assert type(error) is passwords.PasswordFileNotUsable and error.message == NOT_USABLE and error.code == "password_file_not_usable"
        assert scrypt_calls == []

    def test_the_file_set_password_writes_is_read(self, folder, scrypt_calls):
        """Bug class: a check so strict that it refuses the one shape of file the module writes itself, which locks the operator out at the first login."""
        plant(folder, raw())

        read = passwords.read_password_file(folder)

        assert (read.n, read.r, read.p, read.salt, read.hash) == (2**15, 8, 1, SALT, b"h" * 32)
        # Reading derives nothing.
        assert scrypt_calls == []

    def test_the_salt_and_hash_of_the_file_are_the_ones_verified_with(self, folder):
        """Bug class: verifying against a salt or a hash other than the file's (a constant, the salt of the last `set_password`), so that a password set at another time never verifies, or every file verifies the same."""
        other_salt = bytes(range(16, 32))
        plant(folder, raw(salt=base64.b64encode(other_salt).decode()))
        scrypt = FakeScrypt()

        passwords.verify(passwords.read_password_file(folder), MARKER, scrypt=scrypt)

        assert scrypt.calls == [{"salt": other_salt, "n": 2**15, "r": 8, "p": 1, "maxmem": 64 * 1024 * 1024, "dklen": 32}]

    @pytest.mark.parametrize("changes", [{"n": 2**20}, {"n": 2**14}, {"n": 2**16}, {"r": 64}, {"r": 16}, {"p": 100}, {"p": 2}, {"salt": b"short"}, {"salt": b"s" * 32}, {"hash": b"h" * 20}, {"n": True}, {"digest": b"d"}])
    def test_an_object_with_other_parameters_cannot_even_be_made(self, changes, scrypt_calls):
        """Bug class: the check living only in the reader, so that a file object built in any other way reaches the derivation with planted numbers."""
        fields = {"n": 2**15, "r": 8, "p": 1, "salt": SALT, "hash": b"h" * 32, "digest": b"d" * 32, **changes}

        refusal_of(lambda: passwords.PasswordFile(**fields), passwords.PasswordFileNotUsable)
        # And something that only looks like a file object verifies nothing and derives nothing.
        assert passwords.verify(SimpleNamespace(**fields), MARKER) is False
        assert scrypt_calls == []

    def test_the_digest_is_of_exactly_the_bytes_read_with_one_open_and_one_read(self, folder, monkeypatch):
        """Bug class: a digest of the parsed content (two files that differ only in spacing would count as the same password file), or a second read for the digest, between which the file can be replaced: the session would then carry the digest of one file and the hash of another."""
        content = b'  {"scheme":"scrypt",\n"n": 32768, "r": 8,\t"p": 1, "salt": "' + base64.b64encode(SALT) + b'", "hash": "' + base64.b64encode(b"h" * 32) + b'"}\n\n'
        plant(folder, content)
        opened, reads = [], []
        real_open, real_read = os.open, os.read

        def recording_open(path, flags, *args, **kwargs):
            if path == "password.json":
                opened.append(flags)
            return real_open(path, flags, *args, **kwargs)

        def recording_read(fd, count):
            data = real_read(fd, count)
            reads.append(data)
            return data

        monkeypatch.setattr(os, "open", recording_open)
        monkeypatch.setattr(os, "read", recording_read)
        read = passwords.read_password_file(folder)
        monkeypatch.undo()

        assert read.digest == hashlib.sha256(content).digest()
        assert len(opened) == 1 and opened[0] & os.O_NOFOLLOW and opened[0] & os.O_NONBLOCK and opened[0] & os.O_ACCMODE == os.O_RDONLY
        assert reads == [content]

    def test_a_changed_file_has_another_digest(self, folder):
        """Bug class: a digest that does not change with the password (a constant, or one of the folder), by which no session would ever notice a new password."""
        scrypt = FakeScrypt()
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        first = passwords.read_password_file(folder)
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        second = passwords.read_password_file(folder)

        # The same password again is a new salt, so a new file and a new digest.
        assert first.digest != second.digest
        assert second.digest == passwords.read_password_file(folder).digest

    def test_repr_shows_neither_the_salt_nor_the_hash(self, folder):
        """Bug class: the hash in a log line through the repr of the file object; with the hash, the password can be guessed offline."""
        plant(folder, raw())

        text = repr(passwords.read_password_file(folder))

        assert repr(SALT) not in text and repr(b"h" * 32) not in text and "salt" not in text and "hash" not in text


# ── 5. verifying ────────────────────────────────────────────────────────────


NOT_A_CANDIDATE = [None, 0, 12345678901234, True, b"made-up-bytes-candidate", ["made-up-candidate"], "", "x" * 1025, "made-up\ncandidate", "made-up-candidate\n", "made-up\x00candidate", "made-up\x85candidate", "made-up\u2028candidate", "made-up\ud800candidate"]


class TestVerify:
    @pytest.mark.parametrize("candidate", NOT_A_CANDIDATE)
    def test_what_cannot_be_a_password_is_false_without_any_derivation(self, folder, candidate):
        """Bug class: spending a derivation (tens of megabytes, a noticeable part of a second) on input that cannot be the password, which turns a flood of junk logins into load; or an exception instead of False for input that is not text."""
        plant(folder, raw())
        scrypt = FakeScrypt()

        assert passwords.verify(passwords.read_password_file(folder), candidate, scrypt=scrypt) is False
        assert scrypt.calls == []

    def test_the_longest_and_the_shortest_candidate_are_derived(self, folder):
        """Bug class: the short cut for junk input cutting off real candidates: a password of 1024 characters, or the check for a new password's length applied to a login."""
        plant(folder, raw())
        scrypt = FakeScrypt()

        assert passwords.verify(passwords.read_password_file(folder), "x" * 1024, scrypt=scrypt) is False
        assert passwords.verify(passwords.read_password_file(folder), "x", scrypt=scrypt) is False
        assert len(scrypt.calls) == 2

    def test_the_comparison_is_the_constant_time_one(self, folder, monkeypatch):
        """Bug class: comparing the two hashes with `==`, whose running time tells how many leading bytes were right."""
        import hmac

        scrypt = FakeScrypt()
        passwords.set_password(folder, MARKER, scrypt=scrypt)
        read = passwords.read_password_file(folder)
        compared = []
        real_compare = hmac.compare_digest

        def recording_compare(one, other):
            compared.append((one, other))
            return real_compare(one, other)

        monkeypatch.setattr(hmac, "compare_digest", recording_compare)
        right = passwords.verify(read, MARKER, scrypt=scrypt)
        wrong = passwords.verify(read, MARKER + "x", scrypt=scrypt)
        monkeypatch.undo()

        assert right is True and wrong is False
        assert len(compared) == 2 and all(other == read.hash and len(one) == 32 for one, other in compared)

    def test_the_right_and_a_wrong_password_with_the_real_scrypt(self, folder):
        """Bug class: parameters the real scrypt refuses (the default memory limit does, for these), a hash that does not depend on the password or the salt, or a comparison that is always true. The only test that runs the real derivation: three times."""
        passwords.set_password(folder, MARKER)
        read = passwords.read_password_file(folder)

        assert passwords.verify(read, MARKER) is True
        assert passwords.verify(read, MARKER[:-1] + "c") is False
        assert (read.n, read.r, read.p, len(read.salt), len(read.hash)) == (2**15, 8, 1, 16, 32)
        assert read.hash == hashlib.scrypt(MARKER.encode(), salt=read.salt, n=2**15, r=8, p=1, maxmem=64 * 1024 * 1024, dklen=32)

    def test_a_derivation_that_fails_is_a_typed_error_without_the_password(self, folder):
        """Bug class: scrypt's own error (not enough memory) leaving as a ValueError, in whose traceback the candidate sits, or being read as "wrong password"."""
        plant(folder, raw())

        def no_memory(password, **kwargs):
            raise ValueError("memory limit exceeded for " + password.decode())

        error = refusal_of(lambda: passwords.verify(passwords.read_password_file(folder), MARKER, scrypt=no_memory))
        on_set = refusal_of(lambda: passwords.set_password(folder, MARKER, scrypt=no_memory))

        for caught in (error, on_set):
            assert type(caught) is passwords.PasswordNotDerivable and caught.code == "password_not_derivable"
            assert MARKER not in caught.message and MARKER not in repr(caught)


class TestFilesModule:
    def test_the_helper_refuses_with_constant_lines_that_name_no_path(self, folder, tmp_path):
        """Bug class: an error text built from the operating system's message, which names the path (and with it the deployment's folder and the user's home) in whatever shows the error."""
        make_pipe(folder, tmp_path)
        with failing_instead_of_hanging(), files.folder(folder) as folder_fd:
            pipe = refusal_of(lambda: files.read_file(folder_fd, "password.json", 4096), files.BrokerFileError)
            written = refusal_of(lambda: files.open_file(folder_fd, "password.json", os.O_WRONLY | os.O_APPEND, create=True), files.BrokerFileError)
        (tmp_path / "link").symlink_to(folder)
        link = refusal_of(lambda: files.folder(tmp_path / "link").__enter__(), files.BrokerFileError)

        for error in (pipe, written, link):
            assert error.code == "broker_file_not_usable"
            assert str(tmp_path) not in error.message and "password.json" not in error.message
        assert pipe.message == written.message == files.NOT_A_REGULAR_FILE
        assert link.message == files.FOLDER_NOT_A_FOLDER
