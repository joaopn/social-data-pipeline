"""Helpers of the runner's tests: the stand-in `claude` script, plan steps, and one exchange on the socket."""

import os
import socket
import time

from social_data_pipeline.control import framing

# The stand-in: records its argv, its environment as the kernel gave it (/proc/self/environ: Python adds a locale variable to os.environ) and its standard input, then follows the steps of the plan.
STANDIN = r'''
import json, os, signal, subprocess, sys, time
plan = json.load(open(PLAN))
line = sys.stdin.buffer.readline()
environ = dict(entry.split("=", 1) for entry in open("/proc/self/environ", "rb").read().decode().split("\0") if entry)
with open(plan["record"], "a") as f:
    f.write(json.dumps({"argv": sys.argv[1:], "env": environ, "cwd": os.getcwd(), "stdin": line.decode(), "stdin_end": sys.stdin.buffer.read() == b""}) + "\n")
for kind, value in plan["steps"]:
    if kind == "out":
        sys.stdout.buffer.write(value.encode("latin-1") if isinstance(value, str) else (json.dumps(value) + "\n").encode())
        sys.stdout.buffer.flush()
    elif kind == "sleep":
        time.sleep(value)
    elif kind == "write":
        open(value[0], "w").write(value[1])
    elif kind == "ignore_term":
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    elif kind == "child":
        child = subprocess.Popen([sys.executable, "-c", "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(600)"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        open(value, "w").write(str(child.pid))
    elif kind == "detached":
        subprocess.Popen([sys.executable, "-c", "import os, sys, time; os.setsid(); open(sys.argv[1], 'w').write(str(os.getpid())); time.sleep(600)", value], stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 10
        while not (os.path.exists(value) and open(value).read()) and time.monotonic() < deadline:
            time.sleep(0.02)
    elif kind == "flood":
        line = json.dumps({"type": "assistant", "message": {"content": [{"type": "text", "text": "flood"}]}}) + "\n"
        subprocess.Popen([sys.executable, "-c", "import os, sys; os.setsid(); open(sys.argv[1], 'w').write(str(os.getpid()))\nwhile True:\n    sys.stdout.write(sys.argv[2] * 200); sys.stdout.flush()", value, line], stderr=subprocess.DEVNULL)
        deadline = time.monotonic() + 10
        while not (os.path.exists(value) and open(value).read()) and time.monotonic() < deadline:
            time.sleep(0.02)
    elif kind == "exit":
        sys.exit(value)
'''

INIT = ["out", {"type": "system", "subtype": "init", "session_id": "s"}]


def result(cost=0.25, is_error=False, **extra):
    return ["out", {"type": "result", "subtype": "success", "is_error": is_error, "total_cost_usd": cost, "duration_ms": 1200, "num_turns": 2, **extra}]


def exchange(path, document, *, raw=None, timeout=10):
    """Send one frame (or the bytes `raw`) and return the decoded reply, or None when the connection closed without one."""
    folder_fd = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.settimeout(timeout)
        connection.connect(f"/proc/self/fd/{folder_fd}/{os.path.basename(path)}")
        try:
            connection.sendall(raw if raw is not None else framing.encode(document, max_bytes=1 << 20))
        except (BrokenPipeError, ConnectionResetError):
            return None
        try:
            return framing.read_frame(connection.recv, max_bytes=4 << 20, deadline=time.monotonic() + timeout)
        except framing.FrameError:
            return None
    finally:
        connection.close()
        os.close(folder_fd)
