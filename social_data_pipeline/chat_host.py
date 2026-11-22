"""The host side of the Chat window: `sdp chat setup` and `sdp chat unsetup` (the `cmd_*` functions; `sdp.py` only passes the deployment folder and the questions of unsetup).

What it keeps, in `<root>/.sdp/chat/` (0700): `config.json` (0600: the back end, for an OpenAI-compatible one its address and model, the soft cap of the work folder in GiB, the Claude Code version of the sandbox image), `credential/` (0700) with `model.token` (0600: the model's secret, asked without echo, never printed, logged, put on a command line or in an environment), `work/` (0700: one folder per chat, later) and `socket/` (0700). Every folder is opened relative to the one above it without following a link, must be a real folder of the operator and is given mode 0700 whatever the umask; files are written by the broker's file rules (`control.files`: a temporary file made exclusively with mode 0600, then renamed).

`setup` builds two images, `sdp-gateway` (the sandbox's only network peer, which will hold the credential) and `sdp-sandbox` (where the assistant works); nothing is started in this version. Every Docker call goes through `_docker` (a test replaces it), with an argument list and never a shell. Standard library and this package only; the prompts come from `social_data_pipeline.setup.utils` and `getpass`, imported when `setup` runs.
"""

import contextlib
import json
import os
import re
import stat
import subprocess

from social_data_pipeline.control import files
from social_data_pipeline.gateway import table as gateway_table

GATEWAY_IMAGE = "sdp-gateway"
SANDBOX_IMAGE = "sdp-sandbox"
GATEWAY_DOCKERFILE = "Dockerfile.gateway"
SANDBOX_DOCKERFILE = "Dockerfile.sandbox"
# The version Dockerfile.sandbox pins (its build argument's default; a test holds the two equal).
CLAUDE_CODE_VERSION = "2.1.328"

STATE_FOLDER = ".sdp"
CHAT_NAME = "chat"
CHAT_FOLDER = os.path.join(STATE_FOLDER, CHAT_NAME)
CONFIG_NAME = "config.json"
CREDENTIAL_NAME = "credential"
TOKEN_NAME = "model.token"
WORK_NAME = "work"
SOCKET_NAME = "socket"
SUBFOLDERS = (CREDENTIAL_NAME, WORK_NAME, SOCKET_NAME)
TOKEN_PATH = os.path.join(CHAT_FOLDER, CREDENTIAL_NAME, TOKEN_NAME)
FOLDER_MODE = 0o700
CONFIG_MAX_BYTES = 64 * 1024

CLAUDE_CODE = "claude-code"
OPENAI_COMPATIBLE = "openai-compatible"
BACKENDS = (CLAUDE_CODE, OPENAI_COMPATIBLE)
BACKEND_TEXT = {CLAUDE_CODE: "Claude Code on your own Claude subscription login", OPENAI_COMPATIBLE: "an OpenAI-compatible endpoint"}
DEFAULT_UPSTREAM = "https://openrouter.ai/api/v1"
DEFAULT_CAP_GIB = 10
CAP_MAX_GIB = 10000
CAP_DIGITS = 5
MODEL_MAX = 200
SECRET_MAX = 4096
_VISIBLE = re.compile(r"[\x21-\x7e]+")
# The temporary files `control.files.replace_file` leaves when it is killed between its creation and the rename.
_CONFIG_LEFTOVER = re.compile(r"config\.json\.[0-9a-f]{8}\.tmp")
_TOKEN_LEFTOVER = re.compile(r"model\.token\.[0-9a-f]{8}\.tmp")
NOT_STARTED = "Set up. Nothing runs yet: starting the chat is not part of this version."


class Refused(Exception):
    """A command or an answer was refused. `message` is one line for the operator, which never holds a secret."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


# ── the answers ─────────────────────────────────────────────────────────────


def parse_upstream(text):
    """The address of an OpenAI-compatible endpoint: `https://host[:port]/path`, by the rule of the gateway's table, and also without userinfo, query, fragment, `..` or `%` anywhere. Raises Refused."""
    text = str(text).strip()
    for part, what in (("@", "a user name"), ("?", "a query"), ("#", "a fragment"), ("%", "a '%'"), ("..", "'..'")):
        if part in text:
            raise Refused(f"the address may not hold {what}")
    if not text.isascii():
        raise Refused("the address may hold ASCII characters only")
    try:
        gateway_table._upstream(text, "address", "https")
    except gateway_table.ConfigError:
        raise Refused("the address is https://host[:port]/path, the path of segments of letters, digits and '._~-' that do not start with '.'") from None
    return text


def parse_model(text):
    """The model name: 1 to 200 visible ASCII characters, no spaces. Raises Refused."""
    text = str(text).strip()
    if not 1 <= len(text) <= MODEL_MAX or not _VISIBLE.fullmatch(text):
        raise Refused(f"the model name is 1 to {MODEL_MAX} visible ASCII characters, without spaces")
    return text


def parse_cap(text):
    """The soft cap of the work folder in GiB: a whole number from 1 to 10000, typed (at most five digits, checked before the conversion, which refuses more than 4300 digits with an error of its own) or saved as a number. Raises Refused."""
    refused = Refused(f"the cap is a whole number of GiB from 1 to {CAP_MAX_GIB}")
    if isinstance(text, int) and not isinstance(text, bool):
        if not 1 <= text <= CAP_MAX_GIB:
            raise refused
        return text
    text = str(text).strip()
    if not text.isascii() or not text.isdigit() or len(text) > CAP_DIGITS or not 1 <= int(text) <= CAP_MAX_GIB:
        raise refused
    return int(text)


def check_secret(text):
    """The secret, stripped: refused when empty, longer than 4096 characters or holding anything but visible ASCII. The refusal never holds the secret nor its length."""
    text = str(text).strip()
    if not text:
        raise Refused("the secret is empty: nothing was saved")
    if len(text) > SECRET_MAX:
        raise Refused(f"the secret is longer than {SECRET_MAX} characters: nothing was saved")
    if not _VISIBLE.fullmatch(text):
        raise Refused("the secret may hold only visible ASCII characters (no spaces, no other characters): nothing was saved")
    return text


# ── Docker ──────────────────────────────────────────────────────────────────


def build_arguments(root):
    """The two `docker build` argument lists: the gateway's image, then the sandbox's."""
    return [
        ["docker", "build", "-f", os.path.join(root, GATEWAY_DOCKERFILE), "-t", GATEWAY_IMAGE, root],
        ["docker", "build", "-f", os.path.join(root, SANDBOX_DOCKERFILE), "-t", SANDBOX_IMAGE, root],
    ]


def _docker(arguments, *, capture=True, timeout=600):
    """Run a Docker command: a CompletedProcess (text). Raises Refused when Docker cannot be run at all."""
    try:
        return subprocess.run(arguments, capture_output=capture, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except (OSError, subprocess.TimeoutExpired):
        raise Refused("Docker cannot be run, or did not answer in time") from None


def _last_line(result):
    lines = [line.strip() for line in ((result.stderr or "") + "\n" + (result.stdout or "")).splitlines() if line.strip()]
    return (lines or ["(Docker printed nothing)"])[-1]


# ── the state folder ────────────────────────────────────────────────────────


def _subfolder(parent_fd, name, shown, *, create):
    """A descriptor of the folder `name` of the folder `parent_fd`, or None when it is missing and `create` is false. It is opened without following a link and must be a real folder of this user; with `create` it is made when missing, and given mode 0700 either way (the umask does not matter). Raises Refused with `shown`, its path for the operator."""
    if create:
        try:
            os.mkdir(name, FOLDER_MODE, dir_fd=parent_fd)
        except FileExistsError:
            pass
        except OSError:
            raise Refused(f"{shown} could not be made") from None
    try:
        fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent_fd)
    except FileNotFoundError:
        if create:
            raise Refused(f"{shown} could not be made") from None
        return None
    except OSError:
        raise Refused(f"{shown} is a link or not a folder: it was left as it is") from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
            raise Refused(f"{shown} is not a folder of yours (another user owns it): it was left as it is")
        if create:
            os.fchmod(fd, FOLDER_MODE)
    except OSError:
        os.close(fd)
        raise Refused(f"{shown} cannot be used") from None
    except BaseException:
        os.close(fd)
        raise
    return fd


@contextlib.contextmanager
def _chat_folder(root, *, create):
    """Yield a descriptor of `.sdp/chat`, or None when `.sdp` or it is missing and `create` is false. `.sdp` is opened by the broker's folder rule (a link or a folder of another user is refused)."""
    try:
        with files.folder(os.path.join(root, STATE_FOLDER), create=create) as state_fd:
            if state_fd is None:
                yield None
                return
            chat_fd = _subfolder(state_fd, CHAT_NAME, CHAT_FOLDER, create=create)
            try:
                yield chat_fd
            finally:
                if chat_fd is not None:
                    os.close(chat_fd)
    except files.BrokerFileError as error:
        raise Refused(f"{STATE_FOLDER}: {error.message}") from None


def _token_present(chat_fd):
    """Whether `credential/model.token` is there (looked at, never read). Raises Refused for a link or anything but a regular file of this user under either name."""
    if chat_fd is None:
        return False
    credential_fd = _subfolder(chat_fd, CREDENTIAL_NAME, os.path.join(CHAT_FOLDER, CREDENTIAL_NAME), create=False)
    if credential_fd is None:
        return False
    try:
        info = os.stat(TOKEN_NAME, dir_fd=credential_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except OSError:
        raise Refused(f"{TOKEN_PATH} cannot be looked at") from None
    finally:
        os.close(credential_fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        raise Refused(f"{TOKEN_PATH} is not a regular file of yours: it was left as it is")
    return True


def _sweep(folder_fd, pattern):
    """Unlink the regular files of the folder `folder_fd` whose names match `pattern` (leftover temporary files of a replacement), looked at without following a link and never opened; anything else under such a name is left. Returns how many were removed."""
    removed = 0
    for name in os.listdir(folder_fd):
        if pattern.fullmatch(name):
            with contextlib.suppress(FileNotFoundError):
                if stat.S_ISREG(os.stat(name, dir_fd=folder_fd, follow_symlinks=False).st_mode):
                    os.unlink(name, dir_fd=folder_fd)
                    removed += 1
    return removed


def _sweep_token_leftovers(chat_fd):
    """Remove a leftover `model.token.<8 hex>.tmp` of an interrupted write in `credential/` (it would hold a secret)."""
    if chat_fd is None:
        return
    credential_fd = _subfolder(chat_fd, CREDENTIAL_NAME, os.path.join(CHAT_FOLDER, CREDENTIAL_NAME), create=False)
    if credential_fd is None:
        return
    try:
        if _sweep(credential_fd, _TOKEN_LEFTOVER):
            print(f"  Removed a leftover temporary file of an interrupted setup in {os.path.join(CHAT_FOLDER, CREDENTIAL_NAME)}.")
    except OSError:
        raise Refused(f"{os.path.join(CHAT_FOLDER, CREDENTIAL_NAME)} cannot be cleaned of a leftover temporary file") from None
    finally:
        os.close(credential_fd)


def _previous(chat_fd):
    """The saved settings as a dict of the values that still pass the checks of the answers (for the defaults of a new setup), empty when there are none or they cannot be read."""
    if chat_fd is None:
        return {}
    try:
        saved = json.loads(files.read_file(chat_fd, CONFIG_NAME, CONFIG_MAX_BYTES) or b"{}")
    except (files.BrokerFileError, ValueError):
        return {}
    if not isinstance(saved, dict):
        return {}
    found = {}
    for key, parse in (("upstream", parse_upstream), ("model", parse_model), ("work_cap_gib", parse_cap)):
        if isinstance(saved.get(key), (str, int)) and not isinstance(saved.get(key), bool):
            with contextlib.suppress(Refused):
                found[key] = parse(saved[key])
    if saved.get("backend") in BACKENDS:
        found["backend"] = saved["backend"]
    return found


def config_payload(backend, upstream, model, cap):
    """The bytes of `config.json`."""
    saved = {"backend": backend}
    if backend == OPENAI_COMPATIBLE:
        saved.update(upstream=upstream, model=model)
    saved.update(work_cap_gib=cap, claude_code_version=CLAUDE_CODE_VERSION)
    return (json.dumps(saved, indent=2) + "\n").encode("utf-8")


# ── setup ───────────────────────────────────────────────────────────────────


def _check_root(root):
    if "," in root or "\n" in root:
        raise Refused("the deployment folder's path holds a comma or a line break, which Docker's mount option cannot take: move the deployment")


def _ask_valid(ask, parse, *arguments, **keywords):
    """Ask until the answer passes `parse`; a refusal is printed and the question asked again."""
    while True:
        answer = ask(*arguments, **keywords)
        try:
            return parse(answer)
        except Refused as refusal:
            print(f"    {refusal.message}.")


def _setup(root, ask, ask_choice, ask_bool, ask_secret):
    """`ask_secret(prompt, tag)` asks without echo. The secret is checked, written to its file and dropped; it reaches no output, no argument list and no environment."""
    _check_root(root)
    with _chat_folder(root, create=False) as chat_fd:
        _sweep_token_leftovers(chat_fd)
        previous = _previous(chat_fd)
        stored = _token_present(chat_fd)

    print()
    backend = ask_choice(f"Back end of the chat ({CLAUDE_CODE}: {BACKEND_TEXT[CLAUDE_CODE]}; {OPENAI_COMPATIBLE}: {BACKEND_TEXT[OPENAI_COMPATIBLE]} such as OpenRouter)", list(BACKENDS), default=previous.get("backend", CLAUDE_CODE), tag="chat_backend")
    upstream = model = None
    if backend == CLAUDE_CODE:
        print("  The token comes from running `claude setup-token` on a machine where you are logged in to Claude Code.")
        print("  This uses YOUR Claude subscription, under the vendor's terms for it.")
        secret_prompt = "Claude Code token (not shown as you type)"
    else:
        upstream = _ask_valid(ask, parse_upstream, "Address of the endpoint (https)", previous.get("upstream", DEFAULT_UPSTREAM), tag="chat_upstream")
        model = _ask_valid(ask, parse_model, "Model name", previous.get("model"), tag="chat_model")
        secret_prompt = "API key of the endpoint (not shown as you type)"
    secret = None
    if stored and previous.get("backend") != backend:
        if previous.get("backend") is None:
            print("  The stored credential's back end is not known (the settings are missing or cannot be read): a new secret is needed.")
        else:
            print(f"  The stored credential is for the back end {previous['backend']}, which does not work with {backend}: a new secret is needed.")
        stored = False
    if not stored or ask_bool(f"A credential is stored ({TOKEN_PATH}). Replace it?", default=False, tag="chat_replace_credential"):
        secret = check_secret(ask_secret(secret_prompt, "chat_secret"))
    cap = _ask_valid(ask, parse_cap, f"Soft cap of the chats' work folder in GiB (1-{CAP_MAX_GIB})", previous.get("work_cap_gib", DEFAULT_CAP_GIB), tag="chat_work_cap")

    with _chat_folder(root, create=True) as chat_fd:
        for name in SUBFOLDERS:
            os.close(_subfolder(chat_fd, name, os.path.join(CHAT_FOLDER, name), create=True))
        try:
            files.replace_file(chat_fd, CONFIG_NAME, config_payload(backend, upstream, model, cap))
            if secret is not None:
                credential_fd = _subfolder(chat_fd, CREDENTIAL_NAME, os.path.join(CHAT_FOLDER, CREDENTIAL_NAME), create=True)
                try:
                    files.replace_file(credential_fd, TOKEN_NAME, secret.encode("ascii"))
                finally:
                    os.close(credential_fd)
        except files.BrokerFileError as error:
            raise Refused(f"{CHAT_FOLDER}: {error.message}") from None
        finally:
            replaced, secret = secret is not None, None

    for image, arguments in zip((GATEWAY_IMAGE, SANDBOX_IMAGE), build_arguments(root)):
        print(f"\n  Building the image {image} ...", flush=True)
        built = _docker(arguments, capture=False, timeout=3600)
        if built.returncode != 0:
            raise Refused(f"the image {image} could not be built (see Docker's output above). The settings are saved: run `python sdp.py chat setup` again")

    print()
    print(f"  Back end: {backend} ({BACKEND_TEXT[backend]})")
    if backend == OPENAI_COMPATIBLE:
        print(f"  Address: {upstream}")
        print(f"  Model: {model}")
    print(f"  Soft cap of the work folder: {cap} GiB")
    print(f"  Credential: {'saved' if replaced else 'kept'} in {TOKEN_PATH}, readable by you only")
    print(f"  {NOT_STARTED}")
    return 0


def cmd_setup(root):
    """`sdp chat setup`: ask the back end, its address and model (an OpenAI-compatible one), the secret (without echo) and the work folder's soft cap; write the settings and the credential; build both images. Exit code 0, or 1 with one line."""
    from getpass import getpass

    from social_data_pipeline.setup.utils import ask, ask_bool, ask_choice

    def ask_secret(prompt, tag):
        prefix = f"[{tag}] " if tag and os.environ.get("SDP_TAGGED_MODE") else ""
        return getpass(f"  {prefix}{prompt}: ")

    try:
        return _setup(os.fspath(root), ask, ask_choice, ask_bool, ask_secret)
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1


# ── unsetup ─────────────────────────────────────────────────────────────────


_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
# How many times a folder is listed again after its names ran out, to catch entries made meanwhile; beyond that it is left as "kept changing".
RELIST_PASSES = 3


def _enter(parent_fd, name, info):
    """Open the folder `name` of `parent_fd` (whose `lstat` is `info`, a folder of ours) to empty it: it is given mode 0700 first without following a link (so a folder of mode 000 or 0500 can be listed and emptied), then opened without following a link and checked to be the same folder. None when it changed in between (a link swapped in: the change of mode can then fail with ValueError or NotImplementedError rather than OSError) or cannot be opened."""
    try:
        os.chmod(name, FOLDER_MODE, dir_fd=parent_fd, follow_symlinks=False)
        fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
    except (OSError, ValueError, NotImplementedError):
        return None
    opened = os.fstat(fd)
    if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
        os.close(fd)
        return None
    return fd


class _Frame:
    """A folder of the walk: its identity, its name in the folder above, the names still to look at, the names left in it on purpose, how often it was listed again."""

    def __init__(self, info, name, names):
        self.identity = (info.st_dev, info.st_ino)
        self.name = name
        self.names = names
        self.kept = set()
        self.passes = 0


def _remove_tree(parent_fd, name, shown):
    """Remove the folder `name` of `parent_fd` and everything in it, without recursion and without following a link: one descriptor at a time, each step relative to the folder above; the rest of each folder's listing waits on a stack of name lists, so every name is read about once; a file, a FIFO, a socket or a link is unlinked and never opened; each folder is made 0700 before it is listed; the way back up is `..` of the folder emptied, checked against the folder it came from. Not entered, and left: a folder on another file system, one of another user, one that changed between its look and its opening, one that kept changing (listed again more than `RELIST_PASSES` times). Raises Refused naming how many entries were left, or when the tree moved under the walk."""
    top = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    fd = _enter(parent_fd, name, top)
    if fd is None:
        raise Refused(f"{shown} changed while it was removed: it was left as it is")
    left = 0
    try:
        stack = [_Frame(top, name, os.listdir(fd))]
        while stack:
            frame = stack[-1]
            if frame.names:
                entry = frame.names.pop()
                try:
                    info = os.stat(entry, dir_fd=fd, follow_symlinks=False)
                    if not stat.S_ISDIR(info.st_mode):
                        os.unlink(entry, dir_fd=fd)
                        continue
                except FileNotFoundError:
                    continue
                child = _enter(fd, entry, info) if info.st_dev == top.st_dev and info.st_uid == os.geteuid() else None
                if child is None:
                    frame.kept.add(entry)
                    left += 1
                    continue
                os.close(fd)
                fd = child
                stack.append(_Frame(info, entry, os.listdir(fd)))
                continue
            again = [entry for entry in os.listdir(fd) if entry not in frame.kept]
            if again:
                if frame.passes < RELIST_PASSES:
                    frame.passes += 1
                    frame.names = again
                    continue
                frame.kept.update(again)
                left += len(again)
            stack.pop()
            if not stack:
                break
            up = os.open("..", _DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = up
            if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != stack[-1].identity:
                raise Refused(f"{shown} moved while it was removed: what is left was left as it is")
            if frame.kept:
                stack[-1].kept.add(frame.name)
                continue
            try:
                os.rmdir(frame.name, dir_fd=fd)
            except FileNotFoundError:
                pass
            except OSError:
                stack[-1].kept.add(frame.name)
                left += 1
    finally:
        os.close(fd)
    if left:
        raise Refused(f"{shown}: {left} {'entry' if left == 1 else 'entries'} could not be removed (on another file system, of another user, or changing while it was removed) and were left as they are")
    os.rmdir(name, dir_fd=parent_fd)


def _remove(parent_fd, name, shown, *, folder):
    """Remove the file or the folder `name` of the folder `parent_fd` without following a link (a folder by `_remove_tree`). Returns True when it was removed, False when there was none. Raises Refused for a link or the wrong kind in its place, or one of another user, which are left as they are."""
    try:
        info = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    kind_ok = stat.S_ISDIR(info.st_mode) if folder else stat.S_ISREG(info.st_mode)
    if not kind_ok or info.st_uid != os.geteuid():
        raise Refused(f"{shown} is a link, not a {'folder' if folder else 'file'} or not yours: it was left as it is")
    try:
        if folder:
            _remove_tree(parent_fd, name, shown)
        else:
            os.unlink(name, dir_fd=parent_fd)
    except OSError:
        raise Refused(f"{shown} could not be removed") from None
    return True


def cmd_unsetup(root, confirm, confirm_work):
    """`sdp chat unsetup`: say what is removed; after `confirm()` says yes, remove the two images, `config.json`, `credential/` and `socket/` of `.sdp/chat`; `work/` (all chats and their files) only when `confirm_work()` also says yes; leftover temporary files beside `config.json`; `.sdp/chat` itself when it is then empty (else one line says how many other entries stayed). A missing piece is not an error. Exit code 0, or 1 when something could not be removed."""
    root = os.fspath(root)
    print(f"  This removes the images {GATEWAY_IMAGE} and {SANDBOX_IMAGE}, and the chat's settings and credential ({CHAT_FOLDER}: {CONFIG_NAME}, {CREDENTIAL_NAME}/, {SOCKET_NAME}/).")
    if not confirm():
        print("  Nothing was removed.")
        return 0
    with_work = confirm_work()
    failed = False
    for image in (GATEWAY_IMAGE, SANDBOX_IMAGE):
        try:
            result = _docker(["docker", "image", "rm", image])
        except Refused as refusal:
            print(f"  Error: the image {image} could not be removed: {refusal.message}")
            failed = True
            continue
        if result.returncode == 0:
            print(f"  Removed the image {image}.")
        elif "no such image" in _last_line(result).lower():
            print(f"  There was no image {image}.")
        else:
            failed = True
            print(f"  Error: the image {image} could not be removed: {_last_line(result)}")
    names = [(CONFIG_NAME, False), (CREDENTIAL_NAME, True), (SOCKET_NAME, True)] + ([(WORK_NAME, True)] if with_work else [])
    try:
        with files.folder(os.path.join(root, STATE_FOLDER)) as state_fd:
            chat_fd = None if state_fd is None else _subfolder(state_fd, CHAT_NAME, CHAT_FOLDER, create=False)
            if chat_fd is None:
                print(f"  There was no {CHAT_FOLDER} to remove.")
                return 1 if failed else 0
            try:
                for name, folder in names:
                    shown = os.path.join(CHAT_FOLDER, name)
                    try:
                        if _remove(chat_fd, name, shown, folder=folder):
                            print(f"  Removed {shown}.")
                    except Refused as refusal:
                        print(f"  Error: {refusal.message}")
                        failed = True
                if _sweep(chat_fd, _CONFIG_LEFTOVER):
                    print(f"  Removed leftover temporary files of an interrupted setup in {CHAT_FOLDER}.")
                left = os.listdir(chat_fd)
            finally:
                os.close(chat_fd)
            if not left:
                os.rmdir(CHAT_NAME, dir_fd=state_fd)
                print(f"  Removed {CHAT_FOLDER}.")
            else:
                if WORK_NAME in left and not with_work:
                    print(f"  Kept {os.path.join(CHAT_FOLDER, WORK_NAME)} (the chats and their files).")
                others = len([entry for entry in left if entry != WORK_NAME or with_work])
                if others:
                    print(f"  {CHAT_FOLDER} was not removed: {others} other {'entry' if others == 1 else 'entries'} stayed in it.")
    except files.BrokerFileError:
        print(f"  Error: {STATE_FOLDER} is a link or not a folder of yours: {CHAT_FOLDER} was left as it is")
        failed = True
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        failed = True
    except OSError:
        print(f"  Error: {CHAT_FOLDER} could not be removed")
        failed = True
    return 1 if failed else 0
