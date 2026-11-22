"""The management password of the broker: the file `password.json` in the broker folder, which holds a salted scrypt hash and the parameters it was made with, never the password.

`set_password` checks a new password and writes the file in one step; `read_password_file` reads it with one open and one read and returns the parameters together with the SHA-256 of exactly the bytes it read, by which a session knows whether the password it was verified against is still the one in force; `verify` compares a candidate with the hash in constant time.

The cost parameters in the file are not trusted: a file is used only when they are exactly the ones `set_password` writes (n = 2**15, r = 8, p = 1, a salt of 16 bytes, a hash of 32 bytes), so a file that someone planted or edited cannot make a login take minutes or gigabytes, nor make guessing cheap. A file with any other parameters, or of any other shape, is "not usable". The password itself is never logged, never part of an error and never returned; a candidate that could not be a password (not text, empty, too long, with a control character) is turned down without any derivation.

The folder and the file are opened by the rules of the run store (no link, no special file, owned by this user, folder 0700 and file 0600 whatever the umask). Besides the standard library, the module uses this package's errors, the strict JSON decoder of `framing`, the file rules of `files` and the line-break test of `names` (which loads `social_data_pipeline.setup.utils` and with it PyYAML). It takes the folder, the random source and the derivation function as parameters.
"""

import base64
import hashlib
import hmac
import json
import secrets
from dataclasses import dataclass, field

from social_data_pipeline.control import files, framing, names
from social_data_pipeline.control.errors import ControlError

FILE_NAME = "password.json"
SCHEME = "scrypt"
PASSWORD_MAX_CHARS = 1024

# What a file is written with, and the only parameters a file is used with. With these, scrypt needs a little over 32 MiB, which is more than the default memory limit of `hashlib.scrypt` allows: the limit is always given explicitly.
N = 2 ** 15
R = 8
P = 1
SALT_BYTES = 16
HASH_BYTES = 32
MAXMEM = 64 * 1024 * 1024
FILE_MAX_BYTES = 4096

_FIELDS = frozenset({"scheme", "n", "r", "p", "salt", "hash"})


class PasswordRefused(ControlError):
    """A new password was refused. Each subclass has its own `code` and one constant message, which never holds the password."""

    code = "password_refused"
    text = "the password was refused"

    def __init__(self):
        super().__init__(self.text)


class PasswordNotText(PasswordRefused):
    code = "password_not_text"
    text = "the password must be well-formed text"


class PasswordEmpty(PasswordRefused):
    code = "password_empty"
    text = "the password must not be empty"


class PasswordTooLong(PasswordRefused):
    code = "password_too_long"
    text = f"the password must have at most {PASSWORD_MAX_CHARS} characters"


class PasswordHasControlCharacter(PasswordRefused):
    code = "password_has_control_character"
    text = "the password must not hold a line break or a control character"


class PasswordFileNotUsable(ControlError):
    """The password file, or the folder it is in, cannot be used: a link or a special file is in its place, it belongs to another user, it is not the JSON this module writes, its parameters are not the ones this module writes, or it could not be read or written."""

    code = "password_file_not_usable"

    def __init__(self):
        super().__init__("the password file is not usable")


class PasswordNotDerivable(ControlError):
    """The hash of a password could not be computed: the memory scrypt needs is not there."""

    code = "password_not_derivable"

    def __init__(self):
        super().__init__("the hash of the password could not be computed")


class _NoPasswordFile:
    """What `read_password_file` returns when no password was set: false in a condition, and without a digest."""

    __slots__ = ()
    digest = None

    def __bool__(self):
        return False

    def __repr__(self):
        return "<no password file>"


NO_PASSWORD_FILE = _NoPasswordFile()


def _is_whole_number(value):
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class PasswordFile:
    """The content of a password file. `digest` is the SHA-256 of exactly the bytes that were read. An object with other parameters than the ones this module writes cannot be made (PasswordFileNotUsable), so nothing is ever derived with them. The salt and the hash are not shown by repr()."""

    n: int
    r: int
    p: int
    salt: bytes = field(repr=False)
    hash: bytes = field(repr=False)
    digest: bytes

    def __post_init__(self):
        checks = (
            _is_whole_number(self.n) and self.n == N,
            _is_whole_number(self.r) and self.r == R,
            _is_whole_number(self.p) and self.p == P,
            isinstance(self.salt, bytes) and len(self.salt) == SALT_BYTES,
            isinstance(self.hash, bytes) and len(self.hash) == HASH_BYTES,
            isinstance(self.digest, bytes) and len(self.digest) == hashlib.sha256().digest_size,
        )
        if not all(checks):
            raise PasswordFileNotUsable()


def _password_bytes(value):
    """The UTF-8 bytes of a value that can be a password: text of 1 to 1024 characters without a line break or control character. None for anything else."""
    if not isinstance(value, str) or not 1 <= len(value) <= PASSWORD_MAX_CHARS or names.has_line_break_or_control(value):
        return None
    try:
        return value.encode("utf-8")
    except UnicodeEncodeError:
        # A lone surrogate, which JSON can carry and no terminal can type.
        return None


def _derive(scrypt, password, salt, n, r, p):
    # The memory limit is twice what the block mixing needs with the one set of parameters there is (128 * n * r bytes).
    return (scrypt or hashlib.scrypt)(password, salt=salt, n=n, r=r, p=p, maxmem=MAXMEM, dklen=HASH_BYTES)


def set_password(folder, password, *, random_bytes=secrets.token_bytes, scrypt=None):
    """Check a new management password and write its file into the broker folder (a path, or a descriptor of the folder), which is created with mode 0700 when it is missing. The file is replaced in one step, so a reader sees the old password or the new one. Any password that is not empty is taken, whatever its length up to the maximum. Raises PasswordNotText, PasswordEmpty, PasswordTooLong or PasswordHasControlCharacter before anything is derived or written, PasswordNotDerivable when scrypt fails, and PasswordFileNotUsable when the folder or the file cannot be used (the old file is then left as it was)."""
    if not isinstance(password, str):
        raise PasswordNotText()
    if not password:
        raise PasswordEmpty()
    if len(password) > PASSWORD_MAX_CHARS:
        raise PasswordTooLong()
    if names.has_line_break_or_control(password):
        raise PasswordHasControlCharacter()
    secret = _password_bytes(password)
    if secret is None:
        raise PasswordNotText()
    salt = random_bytes(SALT_BYTES)
    if not isinstance(salt, bytes) or len(salt) != SALT_BYTES:
        raise ValueError("the random source did not return the salt's number of bytes")
    try:
        derived = _derive(scrypt, secret, salt, N, R, P)
    except (ValueError, MemoryError, OSError):
        raise PasswordNotDerivable() from None
    content = {"scheme": SCHEME, "n": N, "r": R, "p": P, "salt": base64.b64encode(salt).decode("ascii"), "hash": base64.b64encode(derived).decode("ascii")}
    payload = (json.dumps(content) + "\n").encode("ascii")
    try:
        with files.folder(folder, create=True) as folder_fd:
            files.replace_file(folder_fd, FILE_NAME, payload)
    except files.BrokerFileError:
        raise PasswordFileNotUsable() from None


def _decoded(value):
    """The bytes of a base64 string of the file; None for anything else."""
    if not isinstance(value, str):
        return None
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except ValueError:
        # Not ASCII, or not base64.
        return None


def _parsed(raw):
    """The PasswordFile of the bytes of a file. Raises PasswordFileNotUsable for anything but the six fields with the scheme this module writes and exactly the parameters it writes."""
    try:
        content = framing.decode(raw)
    except framing.FrameError:
        raise PasswordFileNotUsable() from None
    if set(content) != _FIELDS:
        raise PasswordFileNotUsable()
    scheme = content["scheme"]
    if not isinstance(scheme, str) or scheme != SCHEME:
        raise PasswordFileNotUsable()
    return PasswordFile(n=content["n"], r=content["r"], p=content["p"], salt=_decoded(content["salt"]), hash=_decoded(content["hash"]), digest=hashlib.sha256(raw).digest())


def read_password_file(folder):
    """Read the password file of the broker folder (a path, or a descriptor of the folder): a PasswordFile, or NO_PASSWORD_FILE (false in a condition) when the folder or the file does not exist. Nothing is created. The file is opened once and read once, and `digest` is of exactly those bytes. Raises PasswordFileNotUsable for a link or a special file in the place of the folder or the file (a named pipe is not waited on), a folder or file of another user, a file of more than 4 KiB, and content that is not the six fields with exactly the parameters `set_password` writes; nothing is derived from such a file."""
    try:
        with files.folder(folder) as folder_fd:
            if folder_fd is None:
                return NO_PASSWORD_FILE
            raw = files.read_file(folder_fd, FILE_NAME, FILE_MAX_BYTES)
    except files.BrokerFileError:
        raise PasswordFileNotUsable() from None
    if raw is None:
        return NO_PASSWORD_FILE
    return _parsed(raw)


def verify(password_file, candidate, *, scrypt=None):
    """Whether `candidate` is the password of a PasswordFile, compared in constant time. False, without any derivation, for a candidate that is not text, is empty, has more than 1024 characters or holds a line break or control character, and for anything that is not a PasswordFile. Raises PasswordNotDerivable when the derivation itself fails (the memory it needs is not there)."""
    if not isinstance(password_file, PasswordFile):
        return False
    secret = _password_bytes(candidate)
    if secret is None:
        return False
    try:
        derived = _derive(scrypt, secret, password_file.salt, password_file.n, password_file.r, password_file.p)
    except (ValueError, MemoryError, OSError):
        raise PasswordNotDerivable() from None
    return hmac.compare_digest(derived, password_file.hash)
