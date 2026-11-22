#!/usr/bin/env python3
"""A check of the web UI's Chat window against a real Docker, run by hand: `python tests/e2e/manual/check_chat_page.py [--browser PYTHON]` (Linux; the needs of `check_chat_runner.py`, which this check builds on, and an interpreter with PyYAML). With `--browser`, PYTHON is an interpreter with Playwright and `PLAYWRIGHT_BROWSERS_PATH` must be set to the folder of its browsers.

It is not collected by pytest. In a temp folder it makes a deployment with a copy of the CLI and the package, and starts the chat as `check_chat_runner.py` does (the real gateway and sandbox images under throwaway names, the gateway with its loopback test upstream and a STAND-IN API in its network namespace, the sandbox with the real Claude Code, created by `chat_host.sandbox_arguments`); the stand-in's Bash turn writes a real PNG, and a turn holding `MARKUP-TURN` gets an answer holding markup and a `javascript:` link. Then the broker (`sdp.py broker passwd`, `broker start`) and the web UI (set up and started by the host module, with throwaway names) run. Over HTTPS, with the generated certificate as the only trust root:

1. The start mounts `.sdp/chat/socket` read-only with its setting and prints no line about the Chat window; the Chat page names the back end and mongo and lists no chat.
2. A new chat; a question whose turn runs a shell command writing `plot.png`: the page shows the question, the tool call collapsed, the answer, the image inline (served as `image/png` with `sandbox`), and the download is byte for byte.
3. The answer holding markup and a `[link](javascript:...)` is shown as text: no element, no such attribute.
4. Stop on a held turn: the turn ends with the stopped error. Delete: the chat leaves the list.
5. With `--browser PYTHON`: `check_chat_page_browser.py` does the same by clicking in headless Chromium.
6. After the management password is changed with `sdp.py broker passwd`, the old session's Chat pages, the events part (by `HX-Redirect`) and a download lead to the login.

Clean-up, also when the check fails: every throwaway container, network and image, the broker and every process of the deployment, and the temp folder. What stays: the base images and Docker's build cache.
"""

import argparse
import base64
import contextlib
import html
import http.client
import io
import json
import os
import re
import shutil
import signal
import struct
import subprocess
import sys
import time
import zlib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import check_chat_runner as runner_check
from check_chat_runner import check
from check_door import free_port
from check_webui import CSP, Page

from social_data_pipeline import webui_host as host
from social_data_pipeline.setup import utils as setup_utils

MANAGEMENT = "made-up-management-password-3d8a"
NEW_MANAGEMENT = "made-up-new-management-password-b61e"
MARKUP = "<b>not bold</b> <script>alert(1)</script> [link](javascript:alert(1))"


def png():
    """A real 2x2 PNG, so that a browser can draw it."""
    def chunk(kind, data):
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))
    rows = b"".join(b"\x00" + b"\xff\x00\x00" * 2 for _ in range(2))
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", 2, 2, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b"")


PNG = png()


def standin():
    """The runner check's stand-in API, its Bash turn writing a real PNG and with an answer of markup for `MARKUP-TURN`."""
    text = runner_check.STANDIN
    command = re.search(r"^ +command = .*$", text, re.MULTILINE).group(0)
    written = command[:command.index("command")] + f"command = \"python3 -c \\\"import base64; open('plot.png','wb').write(base64.b64decode('{base64.b64encode(PNG).decode()}'))\\\" && echo MARKER-7f3a\""
    old_answer = '"Here is the plot: plot.png" if result else "plain answer"'
    assert text.count(old_answer) == 1
    return f"MARKUP = {MARKUP!r}\n" + text.replace(command, written).replace(old_answer, '"Here is the plot: plot.png" if result else (MARKUP if "MARKUP-TURN" in said else "plain answer")')


class Deployment(runner_check.Check):
    """The runner check's temp folder, names and chat, plus a copy of the CLI and the package for the broker and the web UI."""

    def __init__(self):
        super().__init__()
        self.name = "sdp-chat-page-check-" + os.urandom(4).hex()
        for name in ("sdp.py", "Dockerfile.webui", "requirements-webui.txt"):
            shutil.copy(REPO_ROOT / name, self.root / name)
            os.chmod(self.root / name, 0o644)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        for path, _, names in os.walk(self.root / "social_data_pipeline"):
            os.chmod(path, 0o755)
            for name in names:
                os.chmod(os.path.join(path, name), 0o644)
        self.env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        self.port = free_port()
        host.IMAGE = host.CONTAINER = host.NETWORK = self.name
        setup_utils.ROOT = self.root

    def cli(self, *argv, stdin=None):
        result = subprocess.run([sys.executable, str(self.root / "sdp.py"), *argv], env=self.env, capture_output=True, text=True, timeout=180, input=stdin, start_new_session=True)
        return result.returncode, result.stdout

    def own_processes(self):
        found = []
        for name in os.listdir("/proc"):
            if name.isdigit() and int(name) != os.getpid():
                with contextlib.suppress(OSError):
                    if str(self.root).encode() in Path(f"/proc/{name}/cmdline").read_bytes():
                        found.append(int(name))
        return found

    def clean_up(self):
        self.docker("rm", "--force", self.name)
        self.docker("network", "rm", self.name)
        self.docker("image", "rm", "--force", self.name, timeout=300)
        self.cli("broker", "stop")
        for pid in self.own_processes():
            with contextlib.suppress(OSError):
                os.kill(pid, signal.SIGKILL)
        left = super().clean_up()
        return left + [n for n in (self.name,) if self.docker("inspect", n).returncode == 0 or self.docker("image", "inspect", n).returncode == 0] + [f"process {pid}" for pid in self.own_processes()]


def start_webui(d):
    print("1. the broker and the web UI, with the chat's socket folder")
    check(d.cli("broker", "passwd", stdin=f"{MANAGEMENT}\n{MANAGEMENT}\n")[0] == 0 and d.cli("broker", "start")[0] == 0, "`broker passwd` and `broker start`")
    answers = [str(d.port), "", "generate"]
    # The image's build log goes to this process's own output: it is dropped.
    sys.stdout.flush()
    kept = [os.dup(1), os.dup(2)]
    with open(os.devnull, "w") as null:
        os.dup2(null.fileno(), 1)
        os.dup2(null.fileno(), 2)
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            code = host._setup(str(d.root), lambda *a, **k: answers.pop(0), lambda *a, **k: answers.pop(0), os.getuid(), os.getgid())
    finally:
        for fd, copy in zip((1, 2), kept):
            os.dup2(copy, fd)
            os.close(copy)
    check(code == 0, "the web UI's setup built its image and made the certificate")
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        code = host._start(str(d.root), os.getuid(), os.getgid())
    found = json.loads(d.docker("inspect", d.name).stdout)[0]
    mounts = [(mount["Destination"], mount["RW"]) for mount in found["Mounts"]]
    check(code == 0 and "Chat:" not in shown.getvalue() and ("/run/sdp-chat", False) in mounts and "SDP_WEBUI_CHAT_SOCKET=/run/sdp-chat/runner.sock" in found["Config"]["Env"], f"started with .sdp/chat/socket mounted read-only and its setting, no line about the Chat window: {mounts}")
    time.sleep(2)
    return d.root / ".sdp" / "webui" / "tls" / "cert.pem"


def log_in(d, cafile, password=MANAGEMENT):
    page = Page(d.port, cafile)
    status, _, _, cookies = page.request("POST", "/login", headers=page.origin, form={"password": password})
    check(status == 303 and cookies, "a login through the page")
    page.cookie = cookies[0].split(";")[0].split("=", 1)[1]
    return page, re.search(r'name="form_token" value="([^"]+)"', page.request("GET", "/")[2]).group(1)


def raw_get(page, path):
    connection = http.client.HTTPSConnection("127.0.0.1", page.port, context=page.context, timeout=60)
    connection.request("GET", path, headers={"Host": f"127.0.0.1:{page.port}", "Cookie": f"__Host-sdp_session={page.cookie}"})
    response = connection.getresponse()
    body, headers = response.read(), {}
    for key, value in response.getheaders():
        headers[key.lower()] = f"{headers[key.lower()]}, {value}" if key.lower() in headers else value
    connection.close()
    return response.status, headers, body


def text_of(page):
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", page)).split())


def ask(page, token, chat, question, until, seconds=120):
    status, headers, _, _ = page.request("POST", f"/chat/{chat}/send", headers=page.origin, form={"form_token": token, "text": question})
    check(status == 303 and headers["location"] == f"/chat/{chat}", f"{question!r} sent, the answer a redirect to the chat")
    deadline = time.monotonic() + seconds
    while True:
        text = page.request("GET", f"/chat/{chat}")[2]
        if until(text) or time.monotonic() > deadline:
            return text
        time.sleep(1)


def check_pages(d, page, token):
    print("2. a new chat, a turn that writes a plot")
    text = page.request("GET", "/chat")[2]
    check("claude-code" in text and "MongoDB · read-only" in text and "No chat yet." in text, "the Chat page names the back end and mongo and lists no chat")
    status, headers, _, _ = page.request("POST", "/chat/new", headers=page.origin, form={"form_token": token})
    chat = headers["location"].rsplit("/", 1)[1]
    check(status == 303 and re.fullmatch(r"c_[0-9a-f]{12}", chat), f"New chat leads to {chat}")
    text = ask(page, token, chat, "PLOT-TURN draw the counts", lambda t: "Stop the turn" not in t and "The turn ended" in t)
    check("PLOT-TURN draw the counts" in text and '<details class="tool call"><summary><span class="gl" aria-hidden="true">▣</span><span class="nm">Bash</span>' in text and "Here is the plot: plot.png" in text, "the page shows the question, the tool call collapsed and the answer")
    check(f'src="/chat/{chat}/image/plot.png"' in text and f'href="/chat/{chat}/file/plot.png"' in text, "the image inline and the download link")
    status, headers, body = raw_get(page, f"/chat/{chat}/image/plot.png")
    check(status == 200 and body == PNG and headers["content-type"] == "image/png" and "sandbox" in headers["content-security-policy"], f"the image is served as image/png with the sandbox policy ({headers.get('content-security-policy')})")
    status, headers, body = raw_get(page, f"/chat/{chat}/file/plot.png")
    check(status == 200 and body == PNG and headers["content-type"] == "application/octet-stream" and headers["content-disposition"] == 'attachment; filename="plot.png"' and CSP in headers["content-security-policy"], "the download is byte for byte, an attachment with the guard's policy and sandbox")

    print("3. an answer holding markup and a javascript: link")
    text = ask(page, token, chat, "MARKUP-TURN show me markup", lambda t: t.count("The turn ended") >= 2)
    attributes = " ".join(re.findall(r'="([^"]*)"', text))
    check("<b>not bold</b>" not in text and "<script>alert" not in text and "javascript:" not in attributes and MARKUP in text_of(text), "the markup and the link are shown as text, as no element and no attribute")

    print("4. stop a held turn, delete")
    page.request("POST", f"/chat/{chat}/send", headers=page.origin, form={"form_token": token, "text": "HOLD-TURN wait"})
    deadline = time.monotonic() + 60
    while not any("HOLD-TURN" in r.get("said", "") for r in d.requests()):
        runner_check.within(deadline, "the held request reached the stand-in")
        time.sleep(0.5)
    status, headers, _, _ = page.request("POST", f"/chat/{chat}/stop", headers=page.origin, form={"form_token": token})
    deadline = time.monotonic() + 60
    while "Stop the turn" in (text := page.request("GET", f"/chat/{chat}")[2]):
        runner_check.within(deadline, "the stopped turn ends")
        time.sleep(1)
    check(status == 303 and "the turn was stopped" in text and text.count("The turn ended") == 3, "Stop ends the held turn with the stopped error")
    status, headers, _, _ = page.request("POST", f"/chat/{chat}/delete", headers=page.origin, form={"form_token": token})
    check(status == 303 and headers["location"] == "/chat" and f"/chat/{chat}" not in page.request("GET", "/chat")[2], "Delete removes the chat from the list")
    return chat


def check_browser(d, browser):
    print("5. the browser")
    result = subprocess.run([browser, str(Path(__file__).with_name("check_chat_page_browser.py")), f"https://127.0.0.1:{d.port}/"], input=MANAGEMENT + "\n", capture_output=True, text=True, timeout=600)
    print(result.stdout, end="")
    check(result.returncode == 0, "the browser check passed" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-2000:]))


def check_password_change(d, cafile, chat_of):
    print("6. a session the broker has ended")
    readers = [log_in(d, cafile)[0] for _ in range(4)]
    for reader in readers:
        reader.request("GET", "/chat")
    code, _ = d.cli("broker", "passwd", stdin=f"{NEW_MANAGEMENT}\n{NEW_MANAGEMENT}\n")
    check(code == 0, "the management password changed with `sdp.py broker passwd`")
    time.sleep(6)
    for reader, (path, headers, want) in zip(readers, (("/chat", {}, 303), (f"/chat/{chat_of}", {}, 303), (f"/chat/{chat_of}/events?offset=0", {"HX-Request": "true"}, 204), (f"/chat/{chat_of}/file/plot.png", {}, 303))):
        status, answer, text, _ = reader.request("GET", path, headers=headers)
        where = answer.get("hx-redirect") if want == 204 else answer.get("location")
        check(status == want and where == "/login?why=ended" and "PLOT-TURN" not in text, f"an old session's GET {path} leads to the login ({status}, {where})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", metavar="PYTHON", help="an interpreter with Playwright: run check_chat_page_browser.py with it (needs PLAYWRIGHT_BROWSERS_PATH)")
    options = parser.parse_args()
    if options.browser and not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        parser.error("--browser needs PLAYWRIGHT_BROWSERS_PATH set to the folder of Playwright's browsers")
    runner_check.STANDIN = standin()
    d = Deployment()
    failed = None
    try:
        try:
            print(f"temp folder {d.root.name}, names {d.name} and *-check-{d.images[0].rsplit('-', 1)[1]}, Docker {d.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            runner_check.start(d)
            cafile = start_webui(d)
            page, token = log_in(d, cafile)
            chat = check_pages(d, page, token)
            if options.browser:
                check_browser(d, options.browser)
            check_password_change(d, cafile, chat)
        except Exception as error:
            failed = error
    finally:
        left = d.clean_up()
    if left:
        print(f"FAILED: left behind: {left}")
        return 1
    print("  ok: no container, network, image or process of the check is left, and the temp folder is deleted")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
