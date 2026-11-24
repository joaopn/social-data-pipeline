"""A starting prompt's file, `prompt.json`: `{"instructions": <text>, "queue": <text>}`, the two editable parts of the role text (`claude.role`'s pair: the instructions, and the queue's instructions); the deployment's fixed facts are never in it.

The deployment keeps its saved prompt in `.sdp/chat/prompt/prompt.json` (written by `sdp chat prompt`, mounted read-only into the sandbox); the runner copies it into a new chat's folder, and every turn of that chat reads the chat's copy. Each part is 0 to 10,000 bytes of UTF-8 with no control character but the tab and the line break `\\n` and no line or paragraph separator (the rule of `control.types.Text(10000, multiline=True)`); the file is at most 64 KiB and is exactly an object with those two keys, each once.

Standard library only: the runner, the agent and the host all use it.
"""

import json
import os
import stat
import unicodedata

NAME = "prompt.json"
KEYS = ("instructions", "queue")
TEXT_MAX_BYTES = 10000
FILE_MAX_BYTES = 64 * 1024


def text_problem(text):
    """None when `text` may be a part of a starting prompt, else what is wrong with it, as the end of a sentence ("holds a control character ...")."""
    if not isinstance(text, str):
        return "is not text"
    for character in text:
        if character not in ("\t", "\n") and unicodedata.category(character) in ("Cc", "Zl", "Zp"):
            return "holds a control character (only tabs and line breaks may be in it)"
    try:
        size = len(text.encode("utf-8"))
    except UnicodeEncodeError:
        return "is not well-formed text"
    if size > TEXT_MAX_BYTES:
        return f"is longer than {TEXT_MAX_BYTES:,} bytes"
    return None


class _Duplicate(ValueError):
    pass


def _no_duplicates(pairs):
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise _Duplicate("it names a key twice")
    return dict(pairs)


def parse(data):
    """The pair (instructions, queue) of the bytes of a prompt file. Raises ValueError, whose text says what is wrong without quoting the file."""
    if len(data) > FILE_MAX_BYTES:
        raise ValueError(f"it is larger than {FILE_MAX_BYTES // 1024} KiB")
    try:
        doc = json.loads(data.decode("utf-8"), object_pairs_hook=_no_duplicates)
    except _Duplicate as error:
        raise ValueError(str(error)) from None
    except (ValueError, RecursionError):
        raise ValueError("it is not JSON") from None
    if not isinstance(doc, dict) or set(doc) != set(KEYS):
        raise ValueError("it is not an object with exactly the keys instructions and queue")
    for key in KEYS:
        problem = text_problem(doc[key])
        if problem is not None:
            raise ValueError(f"its {key} {problem}")
    return doc["instructions"], doc["queue"]


def encode(instructions, queue):
    """The bytes of a prompt file holding the two parts (checked first: raises ValueError)."""
    for key, text in zip(KEYS, (instructions, queue)):
        problem = text_problem(text)
        if problem is not None:
            raise ValueError(f"the {key} {problem}")
    return (json.dumps({"instructions": instructions, "queue": queue}, ensure_ascii=False) + "\n").encode("utf-8")


def load(name, dir_fd=None):
    """The pair of the prompt file `name` (relative to the folder `dir_fd` when given), read with one open that follows no link at its last part and does not wait, at most FILE_MAX_BYTES + 1 bytes; None when there is no such file. Raises ValueError for a file that is not a starting prompt and OSError for anything that is not a regular file or cannot be read."""
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=dir_fd)
    except FileNotFoundError:
        return None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError("not a regular file")
        parts, size = [], 0
        while size <= FILE_MAX_BYTES:
            chunk = os.read(fd, FILE_MAX_BYTES + 1 - size)
            if not chunk:
                break
            parts.append(chunk)
            size += len(chunk)
    finally:
        os.close(fd)
    return parse(b"".join(parts))
