"""Run the chat's runner: `python -m social_data_pipeline.chat`, configured by the environment `sdp chat start` gives the sandbox; serves until SIGTERM or SIGINT, then returns 0. A setting that cannot be used ends it with one line and exit code 2.

- `SDP_CHAT_BACKEND`: `claude-code` or `openai-compatible` (the latter answers `hello` and `chat.list` and refuses `chat.send`: it is not available in this version).
- `SDP_CHAT_DATABASES`: the attached databases, a comma list of `postgres`, `mongo`, `starrocks`.
- `SDP_CHAT_CAP_GIB`: the work folder's soft cap in GiB, 1 to 10000.
- `SDP_CHAT_GATEWAY`: the gateway's IP address (for `claude-code`).
- `SDP_CHAT_SOCKET` (default `/run/sdp-chat/runner.sock`), `SDP_CHAT_WORK` (default `/work`), `SDP_CHAT_TURN_SECONDS` (default 1800).
"""

import ipaddress
import os
import signal
import sys
import threading

from . import claude, runner

BACKENDS = ("claude-code", "openai-compatible")
CAP_MAX_GIB = 10000


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
    if backend == "claude-code":
        try:
            ipaddress.IPv4Address(gateway)
        except ValueError:
            raise SettingError("SDP_CHAT_GATEWAY must be the gateway's IPv4 address") from None
    return {"backend": backend, "databases": names, "cap_bytes": int(cap) << 30, "turn_seconds": int(seconds), "gateway": gateway, "socket": environ.get("SDP_CHAT_SOCKET", runner.SOCKET_DEFAULT), "work": environ.get("SDP_CHAT_WORK", runner.WORK_DEFAULT)}


def main(environ=None):
    environ = os.environ if environ is None else environ
    try:
        found = settings(environ)
    except SettingError as error:
        print(f"runner: refusing to start: {error}", file=sys.stderr)
        return 2
    backend = claude.ClaudeCode(found["gateway"], found["databases"]) if found["backend"] == "claude-code" else None
    limits = runner.Limits(turn_seconds=found["turn_seconds"])
    try:
        chats = runner.Runner(found["work"], backend, found["backend"], found["databases"], found["cap_bytes"], limits=limits)
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
    print(f"runner: listening (back end {found['backend']}, databases {', '.join(found['databases']) or 'none'})", flush=True)
    stop.wait()
    server.close()
    chats.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
