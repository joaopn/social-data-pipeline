"""Run the chat's runner: `python -m social_data_pipeline.chat`, configured by the environment `sdp chat start` gives the sandbox; serves until SIGTERM or SIGINT, then returns 0. A setting that cannot be used ends it with one line and exit code 2.

- `SDP_CHAT_BACKEND`: `claude-code` (`chat.claude`) or `openai-compatible` (`chat.openai`).
- `SDP_CHAT_DATABASES`: the attached databases, a comma list of `postgres`, `mongo`, `starrocks`.
- `SDP_CHAT_CAP_GIB`: the work folder's soft cap in GiB, 1 to 10000.
- `SDP_CHAT_GATEWAY`: the gateway's IPv4 address.
- `SDP_CHAT_QUEUE`: `1` when the query queue is attached (the gateway's port 8004 reaches the scheduler's assistant endpoint), `0` or absent when not.
- `SDP_CHAT_MODEL`: the model (for `openai-compatible`, required): 1 to 200 visible ASCII characters, without spaces (the rule of `chat_host.parse_model`).
- `SDP_CHAT_QUERY_SECONDS`: the query time limit of the chat's MongoDB and StarRocks servers, whole seconds from 1 to 3600 (`sdp chat start` gives it); missing or empty: none is named to the assistant.
- `SDP_CHAT_MODELS` (required): the models a chat may be given. For `claude-code` a comma list of `opus`, `sonnet`, `haiku`, each at most once; for `openai-compatible` exactly `SDP_CHAT_MODEL`.
- `SDP_CHAT_SOCKET` (default `/run/sdp-chat/runner.sock`), `SDP_CHAT_WORK` (default `/work`), `SDP_CHAT_TURN_SECONDS` (default 1800).
"""

import ipaddress
import os
import re
import signal
import sys
import threading

from . import claude, openai, runner

BACKENDS = ("claude-code", "openai-compatible")
CAP_MAX_GIB = 10000
MODEL = re.compile(r"[\x21-\x7e]{1,200}")


class SettingError(Exception):
    pass


def settings(environ):
    """The runner's settings from `environ`. Raises SettingError naming the variable."""
    backend = environ.get("SDP_CHAT_BACKEND", "")
    if backend not in BACKENDS:
        raise SettingError(f"SDP_CHAT_BACKEND must be one of {', '.join(BACKENDS)}")
    names = [name for name in environ.get("SDP_CHAT_DATABASES", "").split(",") if name]
    if any(name not in claude.DATABASES for name in names) or len(set(names)) != len(names):
        raise SettingError(f"SDP_CHAT_DATABASES must be a comma list of {', '.join(claude.DATABASES)}, each at most once")
    cap = environ.get("SDP_CHAT_CAP_GIB", "")
    if not (cap.isascii() and cap.isdigit() and len(cap) <= 5 and 1 <= int(cap) <= CAP_MAX_GIB):
        raise SettingError(f"SDP_CHAT_CAP_GIB must be a whole number of GiB from 1 to {CAP_MAX_GIB}")
    seconds = environ.get("SDP_CHAT_TURN_SECONDS", "1800")
    if not (seconds.isascii() and seconds.isdigit() and len(seconds) <= 6 and int(seconds) >= 1):
        raise SettingError("SDP_CHAT_TURN_SECONDS must be a whole number of seconds from 1")
    gateway = environ.get("SDP_CHAT_GATEWAY", "")
    try:
        ipaddress.IPv4Address(gateway)
    except ValueError:
        raise SettingError("SDP_CHAT_GATEWAY must be the gateway's IPv4 address") from None
    model = environ.get("SDP_CHAT_MODEL", "")
    if backend == "openai-compatible" and not MODEL.fullmatch(model):
        raise SettingError("SDP_CHAT_MODEL must be the model's name: 1 to 200 visible ASCII characters, without spaces")
    allowed = environ.get("SDP_CHAT_MODELS", "")
    if backend == "claude-code":
        models = allowed.split(",") if allowed else []
        if not models or any(name not in claude.MODELS for name in models) or len(set(models)) != len(models):
            raise SettingError(f"SDP_CHAT_MODELS must be a comma list of {', '.join(claude.MODELS)}, each at most once")
    else:
        if allowed != model:
            raise SettingError("SDP_CHAT_MODELS must be the model of SDP_CHAT_MODEL")
        models = [model]
    queue = environ.get("SDP_CHAT_QUEUE", "0")
    if queue not in ("0", "1"):
        raise SettingError("SDP_CHAT_QUEUE must be 1 (the query queue is attached) or 0")
    try:
        query_seconds = claude.query_seconds(environ)
    except ValueError as error:
        raise SettingError(str(error)) from None
    return {"backend": backend, "databases": names, "queue": queue == "1", "query_seconds": query_seconds, "model": model, "models": models, "cap_bytes": int(cap) << 30, "turn_seconds": int(seconds), "gateway": gateway, "socket": environ.get("SDP_CHAT_SOCKET", runner.SOCKET_DEFAULT), "work": environ.get("SDP_CHAT_WORK", runner.WORK_DEFAULT)}


def main(environ=None):
    environ = os.environ if environ is None else environ
    try:
        found = settings(environ)
    except SettingError as error:
        print(f"runner: refusing to start: {error}", file=sys.stderr)
        return 2
    if found["backend"] == "claude-code":
        backend = claude.ClaudeCode(found["gateway"], found["databases"], queue=found["queue"], models=found["models"], query_seconds=found["query_seconds"])
    else:
        backend = openai.OpenAICompatible(found["gateway"], found["model"], found["databases"], queue=found["queue"], query_seconds=found["query_seconds"])
    limits = runner.Limits(turn_seconds=found["turn_seconds"])
    try:
        chats = runner.Runner(found["work"], backend, found["backend"], found["databases"], found["cap_bytes"], queue=found["queue"], limits=limits)
    except OSError:
        print("runner: refusing to start: the work folder cannot be opened", file=sys.stderr)
        return 2
    try:
        server = runner.Server(chats, found["socket"])
    except (runner.StartError, OSError) as error:
        print(f"runner: refusing to start: {error if isinstance(error, runner.StartError) else 'the socket could not be made'}", file=sys.stderr)
        chats.close()
        return 2
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    server.start()
    print(f"runner: listening (back end {found['backend']}, databases {', '.join(found['databases']) or 'none'}, query queue {'attached' if found['queue'] else 'not attached'})", flush=True)
    stop.wait()
    server.close()
    chats.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
