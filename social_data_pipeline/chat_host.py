"""The host side of the Chat window: `sdp chat setup`, `start`, `stop`, `status` and `unsetup` (the `cmd_*` functions; `sdp.py` only passes the deployment folder and the questions of unsetup).

What it keeps, in `<root>/.sdp/chat/` (0700): `config.json` (0600: the back end, for an OpenAI-compatible one its address and model, the soft cap of the work folder in GiB, the Claude Code version of the sandbox image), `credential/` (0700) with `model.token` (0600: the model's secret, asked without echo, never printed, logged, put on a command line or in an environment), `work/` (0700: one folder per chat, later) and `socket/` (0700). Every folder is opened relative to the one above it without following a link, must be a real folder of the operator and is given mode 0700 whatever the umask; files are written by the broker's file rules (`control.files`: a temporary file made exclusively with mode 0600, then renamed).

`setup` builds two images, `sdp-gateway` (the sandbox's only network peer, which holds the credential) and `sdp-sandbox` (where the assistant works). `start` runs them as two containers of the same names: the sandbox on the internal network `sdp-chat` only, the gateway on it (listening on its address there only) and on the ordinary bridge `sdp-chat-up`, to which the running MCP container of each database whose MCP server is on a read-only user is attached under an alias, and the query scheduler's container when it runs with a control folder of the operator (the assistant's endpoint, behind a secret: see `_find_queue` and `_queue_secret`); `stop` removes all of it (see `_start`). Every Docker call goes through `_docker` (a test replaces it), with an argument list and never a shell. Standard library and this package only; the prompts come from `social_data_pipeline.setup.utils` and `getpass`, imported when `setup` runs.
"""

import contextlib
import ipaddress
import itertools
import json
import os
import re
import secrets
import stat
import subprocess
import time
from dataclasses import dataclass

from social_data_pipeline.chat.claude import MODELS as CLAUDE_MODELS
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
NOT_STARTED = "Set up. Start the chat with `python sdp.py chat start`."
CHAT_WINDOW = "The Chat window is in the web UI: start it, or restart it, with `python sdp.py webui start` once the chat runs."


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
    print(f"  {CHAT_WINDOW}")
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
    """`sdp chat unsetup`: say what is removed; after `confirm()` says yes, refuse while a container of the chat exists for this deployment (exit code 1, nothing removed); else remove the two images, `config.json`, `credential/`, `socket/` and `gateway/` of `.sdp/chat`; `work/` (all chats and their files) only when `confirm_work()` also says yes; leftover temporary files beside `config.json`; `.sdp/chat` itself when it is then empty (else one line says how many other entries stayed). A missing piece is not an error. Exit code 0, or 1 when something could not be removed."""
    root = os.fspath(root)
    print(f"  This removes the images {GATEWAY_IMAGE} and {SANDBOX_IMAGE}, and the chat's settings and credential ({CHAT_FOLDER}: {CONFIG_NAME}, {CREDENTIAL_NAME}/, {SOCKET_NAME}/, {GATEWAY_NAME}/).")
    if not confirm():
        print("  Nothing was removed.")
        return 0
    try:
        present = [name for name in CONTAINERS if _ours(_state(name), root)]
    except Refused as refusal:
        print(f"  Error: {refusal.message}: nothing was removed")
        return 1
    if present:
        print(f"  Error: the chat's container {' and '.join(present)} exists: stop it first: python sdp.py chat stop")
        return 1
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
    names = [(CONFIG_NAME, False), (CREDENTIAL_NAME, True), (SOCKET_NAME, True), (GATEWAY_NAME, True)] + ([(WORK_NAME, True)] if with_work else [])
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


# ── start, stop, status ─────────────────────────────────────────────────────


GATEWAY_CONTAINER = "sdp-gateway"
SANDBOX_CONTAINER = "sdp-sandbox"
CONTAINERS = (GATEWAY_CONTAINER, SANDBOX_CONTAINER)
NETWORK = "sdp-chat"
UP_NETWORK = "sdp-chat-up"
NETWORKS = (NETWORK, UP_NETWORK)
ROOT_LABEL = "sdp.chat.root"
GATEWAY_NAME = "gateway"
GATEWAY_FOLDER = os.path.join(CHAT_FOLDER, GATEWAY_NAME)
GATEWAY_MOUNT = "/run/sdp-gateway"
CREDENTIAL_MOUNT = "/run/sdp-credential"
WORK_MOUNT = "/work"
SOCKET_MOUNT = "/run/sdp-chat"
GATEWAY_ENV = "SDP_CHAT_GATEWAY"
MODEL_PORT = 8080
# How long `start` waits for the gateway to say that every listener listens.
GATEWAY_WAIT_SECONDS = 15
LOG_TAIL_LINES = 20
# The compose project of the deployment (`name:` of docker-compose.yml) and the labels Compose gives its containers.
COMPOSE_PROJECT = "sdp"
PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"
WORKING_DIR_LABEL = "com.docker.compose.project.working_dir"
ONEOFF_LABEL = "com.docker.compose.oneoff"
# One line per container: its id, its working folder label and its service label, each a JSON string, so that no label value can forge a second line or a field.
COMPOSE_FORMAT = '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}}]'
# The word `key_template` prints when the key is set.
KEY_SET = "read-only-user-set"
# The `anthropic-beta` values the gateway lets through for Claude Code, measured from the requests of Claude Code 2.1.328 (the version Dockerfile.sandbox pins); another version may need this list measured again.
BETA_ALLOWED = (
    "claude-code-20250219",
    "oauth-2025-04-20",
    "interleaved-thinking-2025-05-14",
    "context-management-2025-06-27",
    "per-turn-control-2026-07-01",
    "mid-conversation-tool-changes-2026-07-01",
    "thinking-token-count-2026-05-13",
    "effort-2025-11-24",
    "thinking-display-updates-2026-08-18",
    "extended-cache-ttl-2025-04-11",
)
# The model ids Claude Code 2.1.328 (the version Dockerfile.sandbox pins) sends for each of the models a chat may be given (`CLAUDE_MODELS`, the runner's allow-list): measured with the pinned binary against a stand-in API, with the runner's own command line plus `--model` (`opus` sent `claude-opus-5-5`, `sonnet` `claude-sonnet-5-5`, `haiku` `claude-haiku-5-5`; without `--model` it sent `claude-sonnet-5-5`, and no other model was named in any request). The gateway forwards no other model; another version needs them measured again.
CLAUDE_MODEL_IDS = {"opus": "claude-opus-5-5", "sonnet": "claude-sonnet-5-5", "haiku": "claude-haiku-5-5"}
WALK_LIMIT = 1_000_000
# The IPv4 gateway mode that leaves this machine without an address on `sdp-chat` (measured on Docker 29.5.3: the bridge then has no IPv4 address and the network's IPAM config no gateway; containers on it still reach each other).
ISOLATED_OPTION = "com.docker.network.bridge.gateway_mode_ipv4=isolated"
NEWER_DOCKER = "This Docker cannot keep the machine itself off the chat's network; a newer Docker is needed"
SETUP_FIRST = "  Set it up first: python sdp.py chat setup"
NOT_RUNNING = "its MCP server does not run"
NO_READER = "its MCP server is not on a read-only user"
# The query scheduler (compose service `jobs`): the assistant's endpoint behind a secret that `start` keeps in the scheduler's control folder and copies into the gateway's folder.
JOBS_SERVICE = "jobs"
QUEUE_ALIAS = "sdp-chat-jobs"
QUEUE_PORT = 8004
QUEUE_PATHS = ("/assistant/mcp", "/assistant/results/")
QUEUE_ENV = "SDP_CHAT_QUEUE"
QUEUE_SECRET_NAME = "assistant.secret"
JOBS_CONTROL = os.path.join("data", "jobs", "control")
QUEUE_SECRET_PATH = os.path.join(JOBS_CONTROL, QUEUE_SECRET_NAME)
QUEUE_SECRET_MAX = 256
_QUEUE_SECRET = re.compile(r"[0-9a-f]{64,256}")
DEFAULT_JOBS_PORT = 8050
ENV_MAX_BYTES = 1024 * 1024
DATABASE_FIX = "To give the chat a database: set it up with authentication and a read-only user (python sdp.py db setup), set up its MCP server (python sdp.py db setup-mcp) and start it (python sdp.py db start); then run python sdp.py chat start again."


@dataclass(frozen=True)
class Database:
    """An MCP service of the compose file: the database's name, the service, the environment key of its read-only user (set by `db setup-mcp` when the database has one), the gateway's port for it, the server's own port and the paths it serves."""

    name: str
    service: str
    key: str
    port: int
    server_port: int
    paths: tuple

    @property
    def alias(self):
        return f"sdp-chat-{self.name}-mcp"


DATABASES = (
    Database("postgres", "postgres-mcp", "POSTGRES_MCP_USER", 8001, 8000, ("/sse", "/messages/")),
    Database("mongo", "mongo-mcp", "MCP_MONGODB_USER", 8002, 3000, ("/mcp",)),
    Database("starrocks", "starrocks-mcp", "STARROCKS_MCP_USER", 8003, 9000, ("/mcp",)),
)


def key_template(key):
    """The `docker container inspect` template that prints `KEY_SET` when the container's environment has the entry `key` with a value that is not empty, and nothing else: the environment holds the database's passwords, so it is walked inside Docker and no entry is printed."""
    return '{{range .Config.Env}}{{if and (ne . "%s=") (eq (index (split . "=") 0) "%s")}}%s{{end}}{{end}}' % (key, key, KEY_SET)


def compose_listing(service):
    """The `docker ps` argument list of the running containers of the compose service `service` of the project `sdp` (one-off `compose run` containers left out)."""
    return ["docker", "ps", "--no-trunc", "--filter", f"label={PROJECT_LABEL}={COMPOSE_PROJECT}", "--filter", f"label={SERVICE_LABEL}={service}", "--filter", f"label={ONEOFF_LABEL}=False", "--format", COMPOSE_FORMAT]


def compose_containers(output, root):
    """The (container id, service) of the lines of `output` (as `COMPOSE_FORMAT` prints them) whose working folder is an absolute path that resolves to the deployment folder `root`; any other line is passed over."""
    here = os.path.realpath(root)
    found = []
    for line in output.splitlines():
        try:
            fields = json.loads(line)
        except (ValueError, RecursionError):
            continue
        if not isinstance(fields, list) or len(fields) != 3 or not all(isinstance(item, str) for item in fields):
            continue
        container, folder, service = fields
        if re.fullmatch(r"[0-9a-f]{64}", container) and os.path.isabs(folder) and os.path.realpath(folder) == here:
            found.append((container, service))
    return found


def gateway_address(subnet, network_gateway=""):
    """The gateway container's address on `sdp-chat`: the second host address of the IPv4 subnet `subnet`, or the third when the second is the network's own gateway `network_gateway` (`sdp-chat` is made without one, so it is empty; a network with one is still given an address beside it). Raises Refused for anything but an IPv4 subnet with room for it."""
    try:
        network = ipaddress.IPv4Network(subnet)
    except ValueError:
        raise Refused(f"the network {NETWORK} has no IPv4 subnet: remove it with `docker network rm {NETWORK}` and start again") from None
    for offset in (2, 3):
        if offset < network.num_addresses - 1 and str(network.network_address + offset) != network_gateway:
            return str(network.network_address + offset)
    raise Refused(f"the subnet of the network {NETWORK} has no room for the gateway: remove it with `docker network rm {NETWORK}` and start again")


def gateway_config(listen, settings, databases, queue_port=None):
    """The gateway's config as a dict: it listens on `listen`; the model on port 8080 by the back end of `settings` (the saved settings; for Claude Code only the model ids of the allow-list, for an OpenAI-compatible one only its model), one MCP listener per database of `databases`, which reaches its server by the alias it is attached under, and with `queue_port` (the scheduler's own port) one on port 8004 for the scheduler's assistant endpoint, which sends the secret of its copy in the gateway's folder."""
    if settings["backend"] == CLAUDE_CODE:
        model = {"port": MODEL_PORT, "kind": gateway_table.MODEL_ANTHROPIC, "beta_allowed": list(BETA_ALLOWED), "tool_types_allowed": [], "models": [CLAUDE_MODEL_IDS[name] for name in CLAUDE_MODELS]}
    else:
        model = {"port": MODEL_PORT, "kind": gateway_table.MODEL_OPENAI, "upstream": settings["upstream"], "model": settings["model"]}
    listeners = [model] + [{"port": db.port, "kind": gateway_table.MCP, "upstream": f"http://{db.alias}:{db.server_port}", "paths": list(db.paths)} for db in databases]
    if queue_port is not None:
        listeners.append({"port": QUEUE_PORT, "kind": gateway_table.MCP, "upstream": f"http://{QUEUE_ALIAS}:{queue_port}", "paths": list(QUEUE_PATHS), "secret_file": f"{GATEWAY_MOUNT}/{QUEUE_SECRET_NAME}"})
    return {"listen": listen, "credential_file": f"{CREDENTIAL_MOUNT}/{TOKEN_NAME}", "listeners": listeners}


def _confined(name, root, uid, gid):
    return [
        "docker", "create", "--name", name, "--label", f"{ROOT_LABEL}={root}", "--restart", "no",
        "--user", f"{uid}:{gid}", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
    ]


def gateway_arguments(root, uid, gid, address):
    """The `docker create` argument list of the gateway: on `sdp-chat` at `address` (it is connected to `sdp-chat-up` before it starts), its config folder and the credential's folder mounted read-only, no published port, no environment."""
    return [
        *_confined(GATEWAY_CONTAINER, root, uid, gid),
        "--pids-limit", "128", "--memory", "256m", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=16m",
        "--network", NETWORK, "--ip", address,
        "--mount", f"type=bind,source={os.path.join(root, GATEWAY_FOLDER)},target={GATEWAY_MOUNT},readonly",
        "--mount", f"type=bind,source={os.path.join(root, CHAT_FOLDER, CREDENTIAL_NAME)},target={CREDENTIAL_MOUNT},readonly",
        GATEWAY_IMAGE,
    ]


def sandbox_arguments(root, uid, gid, address, backend, databases, cap, model=None, queue=False):
    """The `docker create` argument list of the sandbox: on `sdp-chat` only, the work folder and the chat's socket folder mounted read-write and nothing else, no published port; its environment is the gateway's address, `HOME`, and for the runner the back end, the names of the attached databases (`databases`), the work folder's soft cap in GiB, for `openai-compatible` the model's name (`model`), the models a chat may be given (`SDP_CHAT_MODELS`: `CLAUDE_MODELS`, or the one model of an OpenAI-compatible back end) and, with `queue`, `SDP_CHAT_QUEUE=1` (the gateway's port 8004 reaches the query scheduler), none of them a secret. Docker's init runs as its first process, so that what a turn's processes leave behind is reaped."""
    return [
        *_confined(SANDBOX_CONTAINER, root, uid, gid),
        "--init", "--pids-limit", "512", "--memory", "4g", "--tmpfs", "/tmp:rw,nosuid,nodev,size=1g",
        "--network", NETWORK,
        "--mount", f"type=bind,source={os.path.join(root, CHAT_FOLDER, WORK_NAME)},target={WORK_MOUNT}",
        "--mount", f"type=bind,source={os.path.join(root, CHAT_FOLDER, SOCKET_NAME)},target={SOCKET_MOUNT}",
        "-e", f"{GATEWAY_ENV}={address}", "-e", f"HOME={WORK_MOUNT}",
        "-e", f"SDP_CHAT_BACKEND={backend}", "-e", f"SDP_CHAT_DATABASES={','.join(databases)}", "-e", f"SDP_CHAT_CAP_GIB={cap}",
        *(["-e", f"SDP_CHAT_MODEL={model}"] if backend == OPENAI_COMPATIBLE else []),
        "-e", f"SDP_CHAT_MODELS={model if backend == OPENAI_COMPATIBLE else ','.join(CLAUDE_MODELS)}",
        *(["-e", f"{QUEUE_ENV}=1"] if queue else []),
        SANDBOX_IMAGE,
    ]


def _check_docker_mapping():
    from social_data_pipeline.webui_host import user_mapping

    result = _docker(["docker", "info", "--format", "{{json .SecurityOptions}}"])
    if result.returncode != 0:
        raise Refused(f"Docker cannot be asked about itself: {_last_line(result)}")
    try:
        options = json.loads(result.stdout)
    except ValueError:
        raise Refused("Docker's answer about its security options cannot be read") from None
    mapping = user_mapping(options if isinstance(options, list) else [])
    if mapping is not None:
        raise Refused(f"Docker {'runs rootless' if mapping == 'rootless' else 'remaps user ids (userns-remap)'}: the sandbox's files would land on this machine owned by another user, and the gateway could not read your credential. The chat needs Docker without user-id mapping")


def _state(name):
    """(status, root label) of the container `name`, or None when there is none."""
    result = _docker(["docker", "container", "inspect", "--format", "{{json .State.Status}}\n{{json .Config.Labels}}", name])
    if result.returncode != 0:
        listing = _docker(["docker", "ps", "-a", "--filter", f"name=^/{name}$", "--format", "{{.ID}}"])
        if listing.returncode != 0:
            raise Refused(f"Docker cannot be asked about the container {name}: {_last_line(listing)}")
        if listing.stdout.strip():
            raise Refused(f"Docker cannot say what the container {name} is: {_last_line(result)}")
        return None
    status_text, _, labels_text = result.stdout.strip().partition("\n")
    try:
        status, labels = json.loads(status_text), json.loads(labels_text) or {}
    except ValueError:
        raise Refused(f"Docker's answer about the container {name} cannot be read") from None
    return status, labels.get(ROOT_LABEL) if isinstance(labels, dict) else None


def _ours(state, root):
    return state is not None and state[1] == root


def _network_root(name):
    """The root label of the network `name` ('' for none), or None when there is no such network."""
    listing = _docker(["docker", "network", "ls", "--format", "{{.Name}}"])
    if listing.returncode != 0:
        raise Refused(f"Docker cannot list its networks: {_last_line(listing)}")
    if name not in listing.stdout.split():
        return None
    result = _docker(["docker", "network", "inspect", "--format", "{{json .Labels}}", name])
    try:
        labels = json.loads(result.stdout) if result.returncode == 0 else None
    except ValueError:
        labels = None
    if result.returncode != 0:
        raise Refused(f"Docker cannot say what the network {name} is: {_last_line(result)}")
    return labels.get(ROOT_LABEL, "") if isinstance(labels, dict) else ""


def internal_network_arguments(root, subnet=None):
    """The `docker network create` argument list of `sdp-chat`: internal (no route out), and IPv4 only (`--ipv6=false`, against a daemon whose default network options enable IPv6), with the IPv4 gateway mode `isolated`, so that this machine has no address on it (a plain internal bridge gives the machine the network's gateway address, through which the sandbox could reach every service of the machine that listens on all addresses); with `subnet`, made with that subnet."""
    return ["docker", "network", "create", "--internal", "--ipv6=false", "-o", ISOLATED_OPTION, *(["--subnet", subnet] if subnet else []), "--label", f"{ROOT_LABEL}={root}", NETWORK]


def isolated_subnet(internal, options, ipam):
    """The IPv4 subnet of `sdp-chat` from its read-back (`docker network inspect`: `.Internal`, `.Options`, `.IPAM.Config`, decoded). Raises Refused unless it is internal, carries the isolated gateway mode, has no IPv6 entry (the isolated mode is IPv4's), and has one IPv4 subnet and no gateway in its IPAM config (an older Docker can store the option and still give the machine an address)."""
    key, _, value = ISOLATED_OPTION.partition("=")
    entries = [entry for entry in ipam if isinstance(entry, dict)] if isinstance(ipam, list) else []
    subnets = [entry["Subnet"] for entry in entries if isinstance(entry.get("Subnet"), str) and ":" not in entry["Subnet"]]
    if any(":" in str(entry.get("Subnet", "")) or ":" in str(entry.get("Gateway", "")) for entry in entries):
        raise Refused(f"the network {NETWORK} came back with IPv6, and the chat needs an IPv4-only network (the isolated gateway mode keeps this machine off it for IPv4 only). Check whether Docker's default network options enable IPv6 (`default-network-opts` in the daemon's configuration)")
    if internal is not True:
        raise Refused(f"the network {NETWORK} was not made internal. {NEWER_DOCKER}")
    if not isinstance(options, dict) or options.get(key) != value:
        raise Refused(f"the network {NETWORK} was not made with the gateway mode {value}. {NEWER_DOCKER}")
    if any(entry.get("Gateway") for entry in entries):
        raise Refused(f"the network {NETWORK} was made with a gateway address of this machine on it. {NEWER_DOCKER}")
    if len(subnets) != 1:
        raise Refused(f"the network {NETWORK} has no single IPv4 subnet")
    return subnets[0]


def _try(arguments):
    """`_docker`, with Docker that cannot be run at all turned into a failed result (for clean-up, which goes on after a failure)."""
    try:
        return _docker(arguments)
    except Refused as refusal:
        return subprocess.CompletedProcess(arguments, 1, "", refusal.message)


def _gone(result):
    text = _last_line(result).lower()
    return "no such container" in text or "is not connected" in text or "not found" in text or "no such network" in text


def _take_down(network):
    """Disconnect every container of the network `network`, in any state (a container left attached to a network that is then removed can no longer start), then remove the network. A container or a network that is gone is no error. Returns the lines of what failed."""
    listing = _try(["docker", "ps", "-a", "--no-trunc", "--filter", f"network={network}", "--format", "{{.ID}}"])
    if listing.returncode != 0:
        return [f"the containers on the network {network} cannot be listed: {_last_line(listing)}"]
    problems = []
    for container in listing.stdout.split():
        result = _try(["docker", "network", "disconnect", network, container])
        if result.returncode != 0 and not _gone(result):
            problems.append(f"a container could not be detached from the network {network}: {_last_line(result)}")
    removed = _try(["docker", "network", "rm", network])
    if removed.returncode != 0 and not _gone(removed):
        problems.append(f"the network {network} could not be removed: {_last_line(removed)}")
    return problems


def _remove_gateway_config(root):
    """Remove `.sdp/chat/gateway/config.json` and the gateway's copy of the scheduler's secret (both written at each start); a missing one is no error."""
    with _chat_folder(root, create=False) as chat_fd:
        gateway_fd = None if chat_fd is None else _subfolder(chat_fd, GATEWAY_NAME, GATEWAY_FOLDER, create=False)
        if gateway_fd is None:
            return
        try:
            for name in (CONFIG_NAME, QUEUE_SECRET_NAME):
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(name, dir_fd=gateway_fd)
        finally:
            os.close(gateway_fd)


def _write_gateway_secret(root, secret):
    """Write the gateway's copy of the scheduler's secret, `.sdp/chat/gateway/assistant.secret` (0600), which the gateway reads from its read-only mount of that folder."""
    with _chat_folder(root, create=False) as chat_fd:
        gateway_fd = _subfolder(chat_fd, GATEWAY_NAME, GATEWAY_FOLDER, create=True)
        try:
            files.replace_file(gateway_fd, QUEUE_SECRET_NAME, (secret + "\n").encode("ascii"))
        except files.BrokerFileError as error:
            raise Refused(f"{GATEWAY_FOLDER}: {error.message}") from None
        finally:
            os.close(gateway_fd)


def _write_gateway_config(root, config):
    try:
        gateway_table.parse(config)
    except gateway_table.ConfigError as error:
        raise Refused(f"the gateway's config would not load ({error})") from None
    payload = (json.dumps(config, indent=2) + "\n").encode("utf-8")
    with _chat_folder(root, create=False) as chat_fd:
        gateway_fd = _subfolder(chat_fd, GATEWAY_NAME, GATEWAY_FOLDER, create=True)
        try:
            files.replace_file(gateway_fd, CONFIG_NAME, payload)
        except files.BrokerFileError as error:
            raise Refused(f"{GATEWAY_FOLDER}: {error.message}") from None
        finally:
            os.close(gateway_fd)


def _settings_or_refuse(root):
    """The saved settings, checked as setup checks its answers. Raises Refused when they, the token, the work folder or the socket folder are missing."""
    with _chat_folder(root, create=False) as chat_fd:
        settings = _previous(chat_fd)
        if settings.get("backend") is None or "work_cap_gib" not in settings or (settings["backend"] == OPENAI_COMPATIBLE and not {"upstream", "model"} <= settings.keys()):
            raise Refused(f"the chat is not set up ({os.path.join(CHAT_FOLDER, CONFIG_NAME)} is missing or cannot be used).\n{SETUP_FIRST}")
        if not _token_present(chat_fd):
            raise Refused(f"no credential is stored ({TOKEN_PATH}).\n{SETUP_FIRST}")
        for name in (WORK_NAME, SOCKET_NAME):
            fd = _subfolder(chat_fd, name, os.path.join(CHAT_FOLDER, name), create=False)
            if fd is None:
                raise Refused(f"{os.path.join(CHAT_FOLDER, name)} is missing.\n{SETUP_FIRST}")
            os.close(fd)
    return settings


def _find_databases(root):
    """([(Database, container id)] of the databases to attach, [(Database, why)] of the others): a database is attached when its MCP service has exactly one running container of this deployment and that container's read-only user key is set (looked at with `key_template`, which prints nothing of the environment)."""
    attached, left = [], []
    for db in DATABASES:
        listing = _docker(compose_listing(db.service))
        if listing.returncode != 0:
            raise Refused(f"Docker cannot list the databases' containers: {_last_line(listing)}")
        found = [container for container, service in compose_containers(listing.stdout, root) if service == db.service]
        if len(found) != 1:
            left.append((db, NOT_RUNNING if not found else "more than one container of its MCP server runs"))
            continue
        result = _docker(["docker", "container", "inspect", "--format", key_template(db.key), found[0]])
        if result.returncode != 0:
            left.append((db, NOT_RUNNING))
        elif result.stdout.strip() != KEY_SET:
            left.append((db, NO_READER))
        else:
            attached.append((db, found[0]))
    return attached, left


def jobs_port(root):
    """The scheduler's port: `JOBS_PORT` of the deployment's `.env` as `sdp` reads it (lines `KEY=value`, spaces around stripped, `#` lines skipped, the last one wins), 8050 when there is no file, no such line or an empty value (as Compose's `${JOBS_PORT:-8050}`). The file holds passwords: only that one value is kept, and nothing of the file is printed. Raises Refused for a value that is not a port."""
    try:
        with open(os.path.join(root, ".env"), "rb") as env:
            data = env.read(ENV_MAX_BYTES + 1)
    except FileNotFoundError:
        return DEFAULT_JOBS_PORT
    except OSError:
        raise Refused("the deployment's .env cannot be read") from None
    if len(data) > ENV_MAX_BYTES:
        raise Refused("the deployment's .env is larger than 1 MiB")
    value = ""
    for line in data.decode("utf-8", "replace").splitlines():
        line = line.strip()
        key, sep, rest = line.partition("=")
        if line and not line.startswith("#") and sep and key.strip() == "JOBS_PORT":
            value = rest.strip()
    if not value:
        return DEFAULT_JOBS_PORT
    if not re.fullmatch(r"[0-9]{1,5}", value) or not 1 <= int(value) <= 65535:
        raise Refused("JOBS_PORT in the deployment's .env is not a port number")
    return int(value)


def _find_queue(root):
    """((the scheduler's container id, its port), None) when exactly one container of the compose service `jobs` of this deployment runs, else (None, why)."""
    listing = _docker(compose_listing(JOBS_SERVICE))
    if listing.returncode != 0:
        return None, f"Docker cannot list the query scheduler's container: {_last_line(listing)}"
    found = [container for container, service in compose_containers(listing.stdout, root) if service == JOBS_SERVICE]
    if not found:
        return None, "the query scheduler does not run (python sdp.py db start jobs)"
    if len(found) > 1:
        return None, "more than one container of the query scheduler runs"
    try:
        return (found[0], jobs_port(root)), None
    except Refused as refusal:
        return None, refusal.message


def _read_queue_secret(control_fd):
    """The secret in `assistant.secret` of the control folder when it is a valid one (a regular file of this user, not open to the group or others, of 64 to 256 lowercase hex digits after stripping), None when there is none or it is not valid (it is then replaced). Raises Refused for a link or anything but a regular file of this user in its place, which is left as it is."""
    try:
        info = os.stat(QUEUE_SECRET_NAME, dir_fd=control_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        raise Refused(f"{QUEUE_SECRET_PATH} cannot be looked at") from None
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid():
        raise Refused(f"{QUEUE_SECRET_PATH} is a link or not a regular file of yours: it was left as it is")
    if info.st_mode & 0o077:
        return None
    try:
        data = files.read_file(control_fd, QUEUE_SECRET_NAME, QUEUE_SECRET_MAX)
    except files.BrokerFileError:
        return None
    value = (data or b"").decode("ascii", "replace").strip()
    return value if _QUEUE_SECRET.fullmatch(value) else None


def _queue_secret(root):
    """The scheduler's secret for the assistant, made when needed: `data/jobs/control` must be a real folder of this user (not a link; the scheduler makes it) without access for the group or others; a valid `assistant.secret` in it is kept, else one of 32 random bytes as hex is written (0600, a temporary file renamed, no link followed). Raises Refused with why the queue cannot be used. The secret is never printed."""
    try:
        with files.folder(os.path.join(root, JOBS_CONTROL)) as control_fd:
            if control_fd is None:
                raise Refused(f"{JOBS_CONTROL} is missing (the scheduler makes it when the data folder is yours)")
            if os.fstat(control_fd).st_mode & 0o077:
                raise Refused(f"{JOBS_CONTROL} is open to the group or to others")
            secret = _read_queue_secret(control_fd)
            if secret is None:
                secret = secrets.token_hex(32)
                try:
                    files.replace_file(control_fd, QUEUE_SECRET_NAME, (secret + "\n").encode("ascii"))
                except files.BrokerFileError:
                    raise Refused(f"{QUEUE_SECRET_PATH} could not be written") from None
            return secret
    except files.BrokerFileError:
        raise Refused(f"{JOBS_CONTROL} is a link or not a folder of yours") from None
    except OSError:
        raise Refused(f"{JOBS_CONTROL} cannot be used") from None


def _step(arguments, what):
    result = _docker(arguments)
    if result.returncode != 0:
        raise Refused(f"{what}: {_last_line(result)}")
    return result


def _make_networks(root, made):
    """Make `sdp-chat-up` and the internal `sdp-chat`; returns the gateway's address on `sdp-chat`. `docker create --ip` needs a network whose subnet was given when it was made (on older Docker versions; 29 also takes one Docker picked), so `sdp-chat` is made once to let Docker pick a free subnet, read, removed, and made again with that subnet."""
    _step(["docker", "network", "create", "--driver", "bridge", "--label", f"{ROOT_LABEL}={root}", UP_NETWORK], f"the network {UP_NETWORK} could not be made")
    made.append(UP_NETWORK)
    made_internal = _docker(internal_network_arguments(root))
    if made_internal.returncode != 0:
        said = _last_line(made_internal)
        if "gateway_mode_ipv4" in said:
            raise Refused(f"Docker refused to make the network {NETWORK} with the machine kept off it. {NEWER_DOCKER}. Docker said: {said}")
        raise Refused(f"the chat's network could not be made: {said}")
    made.append(NETWORK)
    subnet = _subnet()
    _step(["docker", "network", "rm", NETWORK], f"the network {NETWORK} could not be made again with its subnet")
    made.remove(NETWORK)
    _step(internal_network_arguments(root, subnet), f"the network {NETWORK} could not be made again with its subnet {subnet}")
    made.append(NETWORK)
    if _subnet() != subnet:
        raise Refused(f"the network {NETWORK} was not made with the subnet {subnet}")
    return gateway_address(subnet)


def _subnet():
    """The IPv4 subnet of `sdp-chat`, read back and checked by `isolated_subnet`."""
    result = _step(["docker", "network", "inspect", "--format", "{{json .Internal}}\n{{json .Options}}\n{{json .IPAM.Config}}", NETWORK], f"the network {NETWORK} cannot be looked at")
    try:
        internal, options, ipam = (json.loads(line) for line in result.stdout.strip().split("\n"))
    except ValueError:
        raise Refused(f"Docker's answer about the network {NETWORK} cannot be read") from None
    return isolated_subnet(internal, options, ipam)


def _wait_for_gateway(listeners):
    """Wait until the gateway says that each of its `listeners` listens. Raises Refused with its last lines when it ended (it refuses a config it cannot use with its own message, which holds no secret) or did not listen in time."""
    deadline = time.monotonic() + GATEWAY_WAIT_SECONDS
    while True:
        state = _state(GATEWAY_CONTAINER)
        logs = _docker(["docker", "logs", "--tail", str(LOG_TAIL_LINES), GATEWAY_CONTAINER])
        text = (logs.stdout or "") + (logs.stderr or "")
        if state is not None and state[0] == "running" and text.count(" listening on port ") >= listeners:
            return
        if state is None or state[0] != "running" or time.monotonic() > deadline:
            lines = "".join(f"\n    {line}" for line in text.splitlines()[-LOG_TAIL_LINES:])
            raise Refused(f"the gateway {'ended right after its start' if state is None or state[0] != 'running' else f'did not listen within {GATEWAY_WAIT_SECONDS} s'}. Its last lines:{lines}")
        time.sleep(0.2)


def _undo(root, made):
    """Remove what a start made (`made`: container and network names, in the order made); the databases are detached with the network."""
    problems = []
    for name in reversed(made):
        if name in CONTAINERS:
            result = _try(["docker", "rm", "--force", name])
            if result.returncode != 0 and not _gone(result):
                problems.append(f"the container {name} could not be removed: {_last_line(result)}")
        else:
            problems += _take_down(name)
    return problems + _gateway_config_removed(root)


def _gateway_config_removed(root):
    """`_remove_gateway_config`, its failure turned into a line of what failed."""
    try:
        _remove_gateway_config(root)
    except Refused as refusal:
        return [refusal.message]
    except OSError:
        return [f"{os.path.join(GATEWAY_FOLDER, CONFIG_NAME)} could not be removed"]
    return []


def _start(root, uid, gid):
    _check_root(root)
    settings = _settings_or_refuse(root)
    for image in (GATEWAY_IMAGE, SANDBOX_IMAGE):
        if _docker(["docker", "image", "inspect", "--format", "{{.Id}}", image]).returncode != 0:
            raise Refused(f"the image {image} is missing.\n{SETUP_FIRST}")
    _check_docker_mapping()
    states = {name: _state(name) for name in CONTAINERS}
    networks = {name: _network_root(name) for name in NETWORKS}
    for name, state in states.items():
        if state is not None and not _ours(state, root):
            raise Refused(f"a container named {name} exists that this deployment did not start (another deployment's chat): it was left as it is")
        if state is not None and state[0] == "running":
            raise Refused(f"the chat already runs (its container {name} runs). To start it again: python sdp.py chat stop, then python sdp.py chat start")
    for name, owner in networks.items():
        if owner is not None and owner != root:
            raise Refused(f"a network named {name} exists that this deployment did not make (another deployment's chat): it was left as it is")
    attached, left = _find_databases(root)
    if not attached:
        lines = "".join(f"\n    {db.name}: {why}" for db, why in left)
        raise Refused(f"no database can be read by the chat:{lines}\n  {DATABASE_FIX}")
    queue, queue_why = _find_queue(root)
    secret = None
    if queue is not None:
        try:
            secret = _queue_secret(root)
        except Refused as refusal:
            queue, queue_why = None, refusal.message
    for name, state in states.items():
        if state is not None:
            _step(["docker", "rm", "--force", name], f"the leftover container {name} could not be removed")
            print(f"  Removed the leftover container {name} of an earlier start.")
    for name, owner in networks.items():
        if owner is not None:
            problems = _take_down(name)
            if problems:
                raise Refused(f"the leftover network {name} of an earlier start could not be removed:" + "".join(f"\n    {line}" for line in problems))
            print(f"  Removed the leftover network {name} of an earlier start.")

    made = []
    try:
        address = _make_networks(root, made)
        if queue is not None:
            _write_gateway_secret(root, secret)
        secret = None
        _write_gateway_config(root, gateway_config(address, settings, [db for db, _ in attached], queue[1] if queue is not None else None))
        _step(gateway_arguments(root, uid, gid, address), "the gateway's container could not be created")
        made.append(GATEWAY_CONTAINER)
        _step(["docker", "network", "connect", UP_NETWORK, GATEWAY_CONTAINER], f"the gateway could not be connected to the network {UP_NETWORK}")
        _step(["docker", "start", GATEWAY_CONTAINER], "the gateway did not start")
        _wait_for_gateway(1 + len(attached) + (queue is not None))
        for db, container in attached:
            _step(["docker", "network", "connect", "--alias", db.alias, UP_NETWORK, container], f"the MCP server of {db.name} could not be attached")
        if queue is not None:
            _step(["docker", "network", "connect", "--alias", QUEUE_ALIAS, UP_NETWORK, queue[0]], "the query scheduler could not be attached")
        _step(sandbox_arguments(root, uid, gid, address, settings["backend"], [db.name for db, _ in attached], settings["work_cap_gib"], settings.get("model"), queue=queue is not None), "the sandbox's container could not be created")
        made.append(SANDBOX_CONTAINER)
        _step(["docker", "start", SANDBOX_CONTAINER], "the sandbox did not start")
        after = _state(SANDBOX_CONTAINER)
        if after is None or after[0] != "running":
            raise Refused("the sandbox ended right after its start")
    except Refused as failure:
        problems = _undo(root, made)
        print(f"  Error: {failure.message}")
        for line in problems:
            print(f"  Error: {line}")
        print("  What this start made was removed." if not problems else "  What this start made was removed, except what the lines above name.")
        return 1

    print("  The chat is started.")
    print(f"  Back end: {settings['backend']} ({BACKEND_TEXT[settings['backend']]})")
    print(f"  The chat can read: {', '.join(db.name for db, _ in attached)}")
    for db, why in left:
        print(f"    {db.name}: left out: {why}")
    if left:
        print(f"  {DATABASE_FIX}")
    if queue is not None:
        print("  The query queue is attached: the assistant can submit queries to the scheduler (each waits for a person's approval, never auto-accepted), see its own and fetch their results.")
    else:
        print(f"  The assistant cannot use the query queue: {queue_why}.")
    print(f"  {CHAT_WINDOW}")
    return 0


def cmd_start(root):
    """`sdp chat start`: make the two networks, write the gateway's config, start the gateway, attach the databases, start the sandbox. Exit code 0 when it was started, 1 with the reason otherwise (what it made is then removed)."""
    try:
        return _start(os.fspath(root), os.getuid(), os.getgid())
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1


def cmd_stop(root):
    """`sdp chat stop`: stop and remove both containers, detach the databases, remove both networks. Exit code 0 when the chat ran, 1 when nothing ran or something could not be removed."""
    try:
        return _stop(os.fspath(root))
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1


def _stop(root):
    states = {name: _state(name) for name in CONTAINERS}
    networks = {name: _network_root(name) for name in NETWORKS}
    foreign = [name for name, state in states.items() if state is not None and not _ours(state, root)] + [name for name, owner in networks.items() if owner is not None and owner != root]
    if foreign:
        print(f"  Error: {' and '.join(foreign)} {'belongs' if len(foreign) == 1 else 'belong'} to another deployment's chat: nothing was stopped")
        return 1
    ran = any(state is not None and state[0] == "running" for state in states.values())
    problems = []
    for name in (SANDBOX_CONTAINER, GATEWAY_CONTAINER):
        if states[name] is None:
            continue
        if states[name][0] == "running":
            stopped = _try(["docker", "stop", "-t", "10", name])
            if stopped.returncode != 0 and not _gone(stopped):
                problems.append(f"the container {name} could not be stopped: {_last_line(stopped)}")
        removed = _try(["docker", "rm", "--force", name])
        if removed.returncode != 0 and not _gone(removed):
            problems.append(f"the container {name} could not be removed: {_last_line(removed)}")
    for name in (UP_NETWORK, NETWORK):
        if networks[name] is not None:
            problems += _take_down(name)
    problems += _gateway_config_removed(root)
    for line in problems:
        print(f"  Error: {line}")
    if problems:
        return 1
    if not ran:
        left = any(state is not None for state in states.values()) or any(owner is not None for owner in networks.values())
        print("  The chat is not running." + (" What an earlier start left was removed." if left else ""))
        return 1
    print("  The chat has stopped. The databases are detached from it.")
    return 0


def measure(folder_fd, limit=None):
    """(the sum of the sizes of the regular files under the folder `folder_fd`, True), or (the sum so far, False) when it holds more than `limit` entries (`WALK_LIMIT` by default) or the tree moved under the walk. Names are counted when they are listed, so no more than `limit` are held (and one more read to tell that there are more). Iterative and one descriptor at a time: each folder is opened relative to the one above without following a link and checked to be the folder that was looked at; the way back up is `..`, checked against the folder it came from; a link is counted as an entry and never followed; a folder that cannot be opened is passed over."""
    limit = WALK_LIMIT if limit is None else limit
    fd = os.dup(folder_fd)
    total = seen = 0

    def names(fd):
        """The names of the folder `fd`, at most what is left of the bound (they are counted here); None when it holds more (one more name is read to tell)."""
        nonlocal seen
        with os.scandir(fd) as entries:
            listed = [entry.name for entry in itertools.islice(entries, limit - seen)]
            if len(listed) == limit - seen and next(entries, None) is not None:
                return None
        seen += len(listed)
        return listed

    try:
        info = os.fstat(fd)
        first = names(fd)
        if first is None:
            return total, False
        stack = [((info.st_dev, info.st_ino), first)]
        while stack:
            pending = stack[-1][1]
            if pending:
                name = pending.pop()
                try:
                    entry = os.stat(name, dir_fd=fd, follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISREG(entry.st_mode):
                    total += entry.st_size
                if not stat.S_ISDIR(entry.st_mode):
                    continue
                try:
                    child = os.open(name, _DIRECTORY_FLAGS, dir_fd=fd)
                except OSError:
                    continue
                opened = os.fstat(child)
                try:
                    if (opened.st_dev, opened.st_ino) != (entry.st_dev, entry.st_ino):
                        raise OSError()
                    listing = names(child)
                except OSError:
                    os.close(child)
                    continue
                if listing is None:
                    os.close(child)
                    return total, False
                os.close(fd)
                fd = child
                stack.append(((entry.st_dev, entry.st_ino), listing))
                continue
            stack.pop()
            if not stack:
                break
            up = os.open("..", _DIRECTORY_FLAGS, dir_fd=fd)
            os.close(fd)
            fd = up
            if (os.fstat(fd).st_dev, os.fstat(fd).st_ino) != stack[-1][0]:
                return total, False
    finally:
        os.close(fd)
    return total, True


def _size_text(size):
    for unit, factor in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if size >= factor:
            return f"{size / factor:.1f} {unit}"
    return f"{size} B"


def _work_line(root, cap):
    try:
        with _chat_folder(root, create=False) as chat_fd:
            work_fd = None if chat_fd is None else _subfolder(chat_fd, WORK_NAME, os.path.join(CHAT_FOLDER, WORK_NAME), create=False)
            if work_fd is None:
                return "Work folder: missing"
            try:
                size, complete = measure(work_fd)
            finally:
                os.close(work_fd)
    except (Refused, OSError) as error:
        return f"Work folder: cannot be measured ({error.message if isinstance(error, Refused) else 'it cannot be read'})"
    of_cap = f"the soft cap of {cap} GiB" if cap else "no saved soft cap"
    if not complete:
        return f"Work folder: more than {_size_text(size)} (more than {WALK_LIMIT:,} entries, or it changed while it was measured; not all counted), against {of_cap}"
    over = " (over the soft cap)" if cap and size > cap * (1 << 30) else ""
    return f"Work folder: {_size_text(size)} of {of_cap}{over}"


def _started_ports(root):
    """The ports of the MCP listeners of the gateway's config (written at start)."""
    try:
        with _chat_folder(root, create=False) as chat_fd:
            gateway_fd = None if chat_fd is None else _subfolder(chat_fd, GATEWAY_NAME, GATEWAY_FOLDER, create=False)
            if gateway_fd is None:
                return set()
            try:
                table = gateway_table.parse(json.loads(files.read_file(gateway_fd, CONFIG_NAME, CONFIG_MAX_BYTES) or b"null"))
            finally:
                os.close(gateway_fd)
    except (Refused, files.BrokerFileError, gateway_table.ConfigError, ValueError):
        return set()
    return {lst.port for lst in table.listeners if lst.kind == gateway_table.MCP}


def _started_databases(root):
    """The names of the databases the gateway's config (written at start) has a listener for."""
    ports = _started_ports(root)
    return [db.name for db in DATABASES if db.port in ports]


def cmd_status(root):
    """`sdp chat status`: whether each container runs, the back end, the databases attached now (and those dropped since the start), whether the query scheduler is attached now, the work folder's size against the soft cap. Exit code 0 when both containers run, 1 otherwise."""
    root = os.fspath(root)
    running = {}
    for name, shown in ((GATEWAY_CONTAINER, "Gateway"), (SANDBOX_CONTAINER, "Sandbox")):
        try:
            state = _state(name)
        except Refused as refusal:
            print(f"  Error: {refusal.message}")
            state = None
        running[name] = _ours(state, root) and state[0] == "running"
        foreign = state is not None and not _ours(state, root)
        print(f"  {shown}: {'running' if running[name] else 'not running'}{f' (a container named {name} belongs to another deployment)' if foreign else ''}")
    try:
        with _chat_folder(root, create=False) as chat_fd:
            settings = _previous(chat_fd)
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        settings = {}
    backend = settings.get("backend")
    print(f"  Back end: {f'{backend} ({BACKEND_TEXT[backend]})' if backend else 'not set up'}")
    if backend is None:
        print(SETUP_FIRST)
    if running[GATEWAY_CONTAINER]:
        try:
            listing = _docker(["docker", "ps", "--no-trunc", "--filter", f"network={UP_NETWORK}", "--filter", f"label={PROJECT_LABEL}={COMPOSE_PROJECT}", "--format", COMPOSE_FORMAT])
        except Refused as refusal:
            listing = subprocess.CompletedProcess([], 1, "", refusal.message)
        if listing.returncode != 0:
            print(f"  Error: Docker cannot say which databases are attached: {_last_line(listing)}")
        else:
            services = {service for _, service in compose_containers(listing.stdout, root)}
            now = [db.name for db in DATABASES if db.service in services]
            print(f"  Databases the chat can read now: {', '.join(now) or 'none'}")
            for name in _started_databases(root):
                if name not in now:
                    print(f"    {name}: dropped since the start (its container was stopped or replaced). To attach it again: python sdp.py chat stop, then python sdp.py chat start")
            queue_now = JOBS_SERVICE in services
            print(f"  Query queue for the assistant: {'attached' if queue_now else 'not attached'}")
            if not queue_now and QUEUE_PORT in _started_ports(root):
                print("    the query scheduler: dropped since the start (its container was stopped or replaced). To attach it again: python sdp.py chat stop, then python sdp.py chat start")
    print(f"  {_work_line(root, settings.get('work_cap_gib'))}")
    return 0 if all(running.values()) else 1
