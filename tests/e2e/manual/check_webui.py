#!/usr/bin/env python3
"""A check of the web UI against a real Docker, run by hand: `python tests/e2e/manual/check_webui.py [--browser PYTHON]` (an interpreter with PyYAML; a Docker without user-id mapping that can pull `python:3.13-slim` and reach PyPI, unpkg.com and cdn.jsdelivr.net, with a `bridge` network whose gateway address this machine has, and the image `python:3.13-alpine`, which the runs use and which is not pulled). With `--browser`, PYTHON is an interpreter with Playwright and `PLAYWRIGHT_BROWSERS_PATH` must be set in the environment to the folder of its browsers. Linux only: it reads `/proc`.

It is not collected by pytest. It builds a throwaway deployment in a temp folder: a copy of the CLI, the package, `Dockerfile.webui` and `requirements-webui.txt`, and one source `demo`. The image, the container and the network get throwaway names (`sdp-webui-check-<hex>`), set on the host module before it is used. What it shows:

1. `sdp.py broker passwd` and `sdp.py broker start` through the CLI; then the host module's setup with scripted answers (a free port, the gateway address of Docker's default `bridge` network and one name as the one answer of addresses and names, a generated certificate): the image is built, the files of `.sdp/webui` are private, the certificate's fingerprint is the SHA-256 of the certificate.
2. The host module's start: the container is created and started and the broker answers from inside it.
3. Over HTTPS, with the generated certificate as the only trust root: `/healthz`; a foreign `Host` refused; the login page's headers; a wrong and a right login (a form post with the right `Origin`); the Overview lists the source `demo`; a POST with a foreign `Origin` refused. Then the actions, through the pages as a browser's forms send them: a run of the profile `parse` of `demo` (the deployment's compose file runs a program that prints a line every second and exits with code 3 on an interrupt) chosen, shown on its pending page with its command, confirmed and linked from its outcome; its output followed through the output view until it holds the program's lines; the run cancelled through its pending page, and its output view then stopping with the line of the interrupt; `db.stop` confirmed; a database admin password kept for the session; `db.unsetup_jobs` (a step-up verb) refused with a wrong management password, the pending action kept, and run with the right one; the audit view listing those calls; and no password and no token of 43 characters but the page's own form token in any page. With `--browser PYTHON`, `check_webui_browser.py` is run with that interpreter (one with Playwright) against the running page. Then the logout (the old cookie no longer works).
4. The channel of a login needs both signs of the machine itself (the local port, and the gateway's address, as which Docker shows a client on 127.0.0.1): wrong logins through 127.0.0.1 until the web UI's own limit refuses them, after which the broker's `local` budget is empty (a login sent straight to the broker's socket on `local` is `throttled`, on `remote` it is not), and a right login through the bridge address (the other port, which Docker reaches through its proxy) still succeeds: it was sent as `remote`.
5. `docker inspect`: the user, the read-only root, the dropped capabilities, the security options, the two read-only mounts and no other (no Docker socket), the network, the published ports (127.0.0.1 to 8444, the bridge address to 8443).
6. From inside the container: the broker's socket answers `hello`; a name on the internet does not resolve and an address on it cannot be reached; nothing can be written to `/run/sdp`, `/tls` or the root.
7. The container's log has one line per request with the method, the path and the status only, and no password.
8. The host module's stop; the container is gone. Then a key that cannot be read: start says the server ended, prints its last log lines (which hold nothing of the key file), removes the container and returns 1.
9. Every address: setup again on a new free port with `all` typed alone, and start. The page answers at `https://127.0.0.1:<port>/` and, with that address as `Host` and an unlisted name as `Host`, at the bridge address, which nothing listed (the certificate's name not checked there, its chain still is); a login through the bridge address succeeds. `docker port` shows the one publication on every address, and neither the next port nor the local port. The stop.

Clean-up, also when the check is interrupted: the container, the network and the image of the throwaway names are removed, so are the containers and the network of the deployment's compose project, the broker and every process of the deployment are killed, and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

import argparse
import contextlib
import hashlib
import html
import io
import http.client
import json
import os
import re
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from social_data_pipeline import webui_host as host
from social_data_pipeline.control import client as broker_client

MANAGEMENT = "made-up-management-password-a51c"
DB_PASSWORD = "made-up-db-admin-password-0f3e"
NAME = "webui-check.example"
RUN_IMAGE = "python:3.13-alpine"
PROGRAM = "import sys, time\ntry:\n    n = 0\n    while True:\n        print('line', n, flush=True)\n        n += 1\n        time.sleep(1)\nexcept KeyboardInterrupt:\n    print('interrupted', flush=True)\n    sys.exit(3)\n"
TOKEN_LIKE = re.compile(r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{43}(?![A-Za-z0-9_-])")
CSP = "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'; object-src 'none'"


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-webui-check-")).resolve()
        os.chmod(self.root, 0o700)
        self.name = "sdp-webui-check-" + os.urandom(4).hex()
        shutil.copy(REPO_ROOT / "sdp.py", self.root / "sdp.py")
        for name in ("Dockerfile.webui", "requirements-webui.txt"):
            shutil.copy(REPO_ROOT / name, self.root / name)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        os.chmod(self.root / "sdp.py", 0o644)
        for path, _, names in os.walk(self.root / "social_data_pipeline"):
            os.chmod(path, 0o755)
            for name in names:
                os.chmod(os.path.join(path, name), 0o644)
        source = self.root / "config" / "sources" / "demo"
        source.mkdir(parents=True)
        (source / "platform.yaml").write_text("data_types: [posts]\n")
        (source / "parse.yaml").write_text("{}\n")
        # The pipeline service of the profile parse: a program that prints a line every second, from an image that must be there already (it is never pulled).
        service = {"image": RUN_IMAGE, "pull_policy": "never", "profiles": ["parse"], "environment": {"SOURCE": "${SOURCE:-}"}, "command": ["python", "-u", "-c", PROGRAM]}
        (self.root / "docker-compose.yml").write_text(json.dumps({"name": self.name, "services": {"parse": service}}, indent=2))
        self.env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        self.port = free_port()
        gateway = json.loads(self.docker("network", "inspect", "bridge", "--format", "{{json .IPAM.Config}}").stdout or "[]")
        self.bridge = next((entry.get("Gateway") for entry in gateway if ":" not in entry.get("Gateway", ":")), None)
        if not self.bridge:
            raise SystemExit("Docker's bridge network has no IPv4 gateway: this check needs one as its second listen address")
        # The throwaway names, used by every function of the host module from here on.
        host.IMAGE = host.CONTAINER = host.NETWORK = self.name

    def cli(self, *argv, stdin=None):
        result = subprocess.run([sys.executable, str(self.root / "sdp.py"), *argv], env=self.env, capture_output=True, text=True, timeout=180, input=stdin, start_new_session=True)
        return result.returncode, result.stdout

    def docker(self, *argv, timeout=120):
        return subprocess.run(["docker", *argv], capture_output=True, text=True, timeout=timeout)

    def own_processes(self):
        found = []
        mark = str(self.root).encode()
        for name in os.listdir("/proc"):
            if not name.isdigit() or int(name) == os.getpid():
                continue
            try:
                arguments = Path(f"/proc/{name}/cmdline").read_bytes()
            except OSError:
                continue
            try:
                folder = os.readlink(f"/proc/{name}/cwd")
            except OSError:
                folder = None
            if folder == str(self.root) or mark in arguments:
                found.append(int(name))
        return found

    def project_containers(self):
        return self.docker("ps", "-a", "-q", "--filter", f"label=com.docker.compose.project={self.name}").stdout.split()

    def clean_up(self):
        self.docker("rm", "--force", self.name)
        self.docker("network", "rm", self.name)
        for container in self.project_containers():
            self.docker("rm", "--force", container)
        self.docker("network", "rm", f"{self.name}_default")
        self.docker("image", "rm", "--force", self.name, timeout=300)
        for pid in self.own_processes():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        time.sleep(0.5)
        left = {
            "containers": self.docker("ps", "-a", "-q", "--filter", f"name={self.name}").stdout.split() + self.project_containers(),
            "networks": self.docker("network", "ls", "-q", "--filter", f"name={self.name}").stdout.split(),
            "images": self.docker("images", "-q", self.name).stdout.split(),
            "processes": self.own_processes(),
        }
        shutil.rmtree(self.root, ignore_errors=True)
        return left


class Page:
    """HTTPS requests to the web UI at one address with its certificate as the only trust root, and the session cookie kept by hand."""

    def __init__(self, port, cafile, address="127.0.0.1"):
        self.port = port
        self.address = address
        self.context = ssl.create_default_context(cafile=str(cafile))
        self.cookie = None

    @property
    def origin(self):
        return {"Origin": f"https://{self.address}:{self.port}"}

    def request(self, method, path, *, host=None, headers=None, form=None):
        connection = http.client.HTTPSConnection(self.address, self.port, context=self.context, timeout=60)
        # The certificate names the address, so the name checked by TLS is the address; the Host header is set apart.
        sent = {"Host": host or f"{self.address}:{self.port}", **(headers or {})}
        body = None
        if form is not None:
            body = urllib.parse.urlencode(form).encode()
            sent["Content-Type"] = "application/x-www-form-urlencoded"
        if self.cookie:
            sent["Cookie"] = f"__Host-sdp_session={self.cookie}"
        connection.request(method, path, body=body, headers=sent)
        response = connection.getresponse()
        text = response.read().decode("utf-8", "replace")
        headers = {key.lower(): value for key, value in response.getheaders()}
        cookies = [value for key, value in response.getheaders() if key.lower() == "set-cookie"]
        connection.close()
        return response.status, headers, text, cookies


def check_setup(deployment):
    print("1. password, broker, setup")
    code, text = deployment.cli("broker", "passwd", stdin=f"{MANAGEMENT}\n{MANAGEMENT}\n")
    check(code == 0, "`broker passwd` sets the management password")
    code, text = deployment.cli("broker", "start")
    check(code == 0 and "started" in text, "`broker start`: " + text.strip())
    answers = [str(deployment.port), f"{deployment.bridge}, {NAME}", "generate"]

    def ask(prompt, default=None, tag=None):
        return answers.pop(0)

    def ask_choice(prompt, options, default=None, tag=None):
        return answers.pop(0)

    started = time.monotonic()
    code = host._setup(str(deployment.root), ask, ask_choice, os.getuid(), os.getgid())
    check(code == 0, f"setup built the image {deployment.name} and made the certificate in {time.monotonic() - started:.0f} s")
    folder = deployment.root / ".sdp" / "webui"
    modes = {str(path.relative_to(folder)): oct(path.stat().st_mode & 0o777) for path in [folder, *folder.rglob("*")]}
    check(modes == {".": "0o700", "config.json": "0o600", "tls": "0o700", "tls/cert.pem": "0o600", "tls/key.pem": "0o600"}, f"the state folder is private: {modes}")
    cert = (folder / "tls" / "cert.pem").read_bytes()
    der = ssl.PEM_cert_to_DER_cert(cert.decode())
    digest = hashlib.sha256(der).hexdigest().upper()
    saved = json.loads((folder / "config.json").read_text())
    check(saved["fingerprint"].replace(":", "") == digest, "the fingerprint in the settings is the SHA-256 of the certificate")
    return folder / "tls" / "cert.pem"


def check_start(deployment):
    print("2. start")
    code = host._start(str(deployment.root), os.getuid(), os.getgid())
    check(code == 0, "start created and started the container")
    probe = deployment.docker("exec", deployment.name, "python", "-m", "social_data_pipeline.webui.probe")
    check(probe.stdout.strip() == "answered", "the broker answers `hello` from inside the container")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with socket.socket() as attempt:
            if attempt.connect_ex(("127.0.0.1", deployment.port)) == 0:
                break
        time.sleep(0.25)
    time.sleep(1)


def check_https(deployment, cafile, browser):
    print("3. the page over HTTPS")
    page = Page(deployment.port, cafile)
    status, headers, text, _ = page.request("GET", "/healthz")
    check(status == 200 and text == "ok", "/healthz answers ok over HTTPS with the generated certificate as trust root")
    status, _, text, _ = page.request("GET", "/healthz", host=f"evil.example:{deployment.port}")
    check(status == 400, "a foreign Host is refused: " + text.strip())
    status, headers, text, _ = page.request("GET", "/login")
    check(status == 200 and headers.get("content-security-policy") == CSP and headers.get("x-content-type-options") == "nosniff" and headers.get("referrer-policy") == "same-origin" and headers.get("cache-control") == "no-store", "the login page has the security headers")
    check("strict-transport-security" not in headers and "server" not in headers, "no HSTS and no Server header")
    origin = {"Origin": f"https://127.0.0.1:{deployment.port}"}
    # Through the bridge address: a wrong login through 127.0.0.1 here would count against the broker's local budget, which refills one attempt every 30 seconds while the actions below run, and step 4 needs that budget whole.
    bridge = Page(deployment.port, cafile, deployment.bridge)
    status, _, text, cookies = bridge.request("POST", "/login", headers=bridge.origin, form={"password": "a-wrong-made-up-password"})
    check(status == 403 and "The password is wrong." in text and not cookies, "a wrong login is refused")
    status, headers, _, cookies = page.request("POST", "/login", headers=origin, form={"password": MANAGEMENT})
    check(status == 303 and len(cookies) == 1 and re.fullmatch(r"__Host-sdp_session=[A-Za-z0-9_-]{43}; Path=/; Secure; HttpOnly; SameSite=Strict", cookies[0]) is not None, "the right login sets the session cookie: " + re.sub(r"=[A-Za-z0-9_-]{43};", "=<id>;", cookies[0]))
    page.cookie = cookies[0].split(";")[0].split("=", 1)[1]
    status, _, text, _ = page.request("GET", "/overview")
    check(status == 200 and '<a href="/sources/demo">demo</a>' in text, "the Overview lists the source demo")
    check(MANAGEMENT not in text, "the Overview holds no password")
    token = re.search(r'name="form_token" value="([^"]+)"', text).group(1)
    status, _, text, _ = page.request("POST", "/logout", headers={"Origin": "https://evil.example"}, form={"form_token": token})
    check(status == 403, "a POST with a foreign Origin is refused")
    check_actions(deployment, page, token)
    if browser:
        result = subprocess.run([browser, str(Path(__file__).with_name("check_webui_browser.py")), f"https://127.0.0.1:{deployment.port}/"], input=MANAGEMENT + "\n", capture_output=True, text=True, timeout=300)
        print(result.stdout, end="")
        check(result.returncode == 0, "the browser check passed" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-2000:]))
    status, headers, _, cookies = page.request("POST", "/logout", headers=origin, form={"form_token": token})
    check(status == 303 and cookies and "Max-Age=0" in cookies[0], "the logout clears the cookie")
    status, headers, _, _ = page.request("GET", "/overview")
    check(status == 303 and headers.get("location") == "/login", "the old cookie no longer opens the Overview")


def pending_and_confirm(page, token, action, fields, password=None, seen=None):
    """Choose an action as its form does, look at its pending page, confirm it: (the pending page's address, its text, the confirmation's status, its headers, its text)."""
    status, headers, text, _ = page.request("POST", "/pending", headers=page.origin, form={"form_token": token, "action": action, **fields})
    if status != 303:
        raise AssertionError(f"the choice of {action} was answered {status}")
    path = headers["location"]
    _, _, pending_text, _ = page.request("GET", path)
    form = {"form_token": token}
    if password is not None:
        form["management_password"] = password
    status, headers, text, _ = page.request("POST", path + "/confirm", headers=page.origin, form=form)
    if seen is not None:
        seen += [pending_text, text]
    return path, pending_text, status, headers, text


def outcome_of(page, location, seen, seconds=120):
    """The outcome page of a confirmed action once it is no longer under way."""
    deadline = time.monotonic() + seconds
    while True:
        _, _, text, _ = page.request("GET", location)
        seen.append(text)
        if "under way" not in text or time.monotonic() > deadline:
            return text
        time.sleep(1)


def follow_output(page, run_id, until, seen, seconds=90):
    """Follow a run's output view as htmx would (each answer carries the next request) until `until(shown, end line)` holds or the view stops: (what it shows, the end line or None)."""
    _, _, text, _ = page.request("GET", f"/runs/{run_id}")
    seen.append(text)
    href = html.unescape(re.search(r'hx-get="([^"]+)" hx-trigger="load"', text).group(1))
    shown, end = "", None
    deadline = time.monotonic() + seconds
    while href and time.monotonic() < deadline:
        status, headers, part, _ = page.request("GET", href, headers={"HX-Request": "true"})
        seen.append(part)
        if status != 200:
            raise AssertionError(f"the output view was answered {status}")
        if headers.get("hx-retarget"):
            shown = ""
        shown += "".join(html.unescape(piece) for piece in re.findall(r'<div class="output-piece">(.*?)</div>', part, re.DOTALL))
        following = re.search(r'<div class="output-next" hx-get="([^"]+)" hx-trigger="([^"]+)"', part)
        found = re.search(r'<p class="output-end">(.*?)</p>', part, re.DOTALL)
        end = html.unescape(found.group(1)) if found else None
        if until(shown, end) or not following:
            break
        href = html.unescape(following.group(1))
        time.sleep(0.2 if following.group(2) == "load" else 1)
    return shown, end


def check_actions(deployment, page, token):
    print("3b. actions through the pages")
    seen = []
    status, _, text, _ = page.request("GET", "/sources/demo")
    seen.append(text)
    check(status == 200 and 'name="profile" value="parse"' in text, "the page of the source demo offers a run of parse")
    _, pending_text, status, headers, _ = pending_and_confirm(page, token, "run.start", {"profile": "parse", "source": "demo"}, seen=seen)
    check("python sdp.py run parse --source=demo" in pending_text, "the pending page shows the command `python sdp.py run parse --source=demo`")
    check(status == 303 and headers["location"].startswith("/actions/"), "the confirmation leads to the outcome page")
    outcome = outcome_of(page, headers["location"], seen)
    found = re.search(r'<a href="/runs/(r[0-9]{8}T[0-9]{6}-[0-9a-f]{6})">', outcome)
    check(found is not None, "the outcome links the run it started")
    run_id = found.group(1)
    shown, end = follow_output(page, run_id, lambda shown, end: "line 2\n" in shown, seen)
    if "line 2\n" not in shown:
        # What the run printed instead, to say why.
        print("\n".join("  | " + line for line in (shown.splitlines()[-15:] + [f"(the view's end line: {end})"])))
    check("line 0\nline 1\nline 2\n" in shown, f"the output view of {run_id} holds the program's lines: {[line for line in shown.splitlines() if line.startswith('line ')][:3]}")
    _, pending_text, status, headers, _ = pending_and_confirm(page, token, "run.cancel", {"run_id": run_id}, seen=seen)
    check(f"python sdp.py runs cancel {run_id}" in pending_text, "the cancel's pending page shows `python sdp.py runs cancel <run>`")
    outcome = outcome_of(page, headers["location"], seen)
    check("exit code 0." in outcome, "the cancel ended with exit code 0")
    shown, end = follow_output(page, run_id, lambda shown, end: end is not None and "ended" in end, seen)
    check(end is not None and "has ended" in end and "interrupted\n" in shown, f"the output view stops after the cancel, with the program's last line ({[line for line in shown.splitlines() if line.strip()][-1]!r}): {end}")
    _, _, text, _ = page.request("GET", f"/runs/{run_id}")
    check("<dd>cancelled</dd>" in text and "<dd>3</dd>" in text, "the run page says cancelled, exit code 3")
    _, pending_text, status, headers, _ = pending_and_confirm(page, token, "db.stop", {"service": ""}, seen=seen)
    check("python sdp.py db stop</code>" in pending_text and status == 303, "db.stop: the pending page shows `python sdp.py db stop`, the confirmation leads to its outcome")
    outcome = outcome_of(page, headers["location"], seen)
    found = re.search(r"exit code (-?[0-9]+)\.", outcome)
    check(found is not None, f"db.stop ran and its outcome shows its exit code ({found.group(1) if found else None})")
    _, _, status, headers, text = pending_and_confirm(page, token, "session.set_db_password", {"password": DB_PASSWORD}, MANAGEMENT, seen=seen)
    check(status == 303 and "Done." in outcome_of(page, headers["location"], seen), "a database admin password is kept for the session (with the management password)")
    _, _, text, _ = page.request("GET", "/databases")
    seen.append(text)
    check("A database admin password is kept for this session" in text, "the Databases page says one is kept")
    path, pending_text, status, _, text = pending_and_confirm(page, token, "db.unsetup_jobs", {}, "a-wrong-made-up-password", seen=seen)
    check("python sdp.py db unsetup-jobs" in pending_text and 'name="management_password"' in pending_text, "the step-up action's pending page shows its command and asks for the management password")
    check(status == 403 and "The management password given again was wrong." in text, "a wrong management password is refused")
    status, _, text, _ = page.request("GET", path)
    check(status == 200, "the pending action is kept")
    status, headers, _, _ = page.request("POST", path + "/confirm", headers=page.origin, form={"form_token": token, "management_password": MANAGEMENT})
    outcome = outcome_of(page, headers["location"], seen)
    check(status == 303 and "exit code 0." in outcome, "the right one runs it: exit code 0")
    _, _, text, _ = page.request("GET", "/system")
    seen.append(text)
    rows = re.findall(r"<tr><td>[^<]*</td><td>[^<]*</td><td>([^<]*)</td><td>([^<]*)</td><td>([^<]*)</td></tr>", text)
    for event, verb in (("call_ended", "run.start"), ("call_ended", "run.cancel"), ("call_ended", "db.stop"), ("call_refused", "db.unsetup_jobs"), ("call_ended", "db.unsetup_jobs")):
        check(any(row[0] == event and row[1] == verb for row in rows), f"the audit view lists {event} {verb}")
    check(any(row[0] == "step_up_failed" for row in rows), "the audit view lists the failed step-up")
    secrets_found = [secret for secret in (MANAGEMENT, DB_PASSWORD, "a-wrong-made-up-password") if any(secret in page_text for page_text in seen)]
    check(secrets_found == [], f"no password in any of the {len(seen)} pages and parts seen")
    tokens = {found for page_text in seen for found in TOKEN_LIKE.findall(page_text)}
    check(tokens == {token}, "no token of 43 characters in them but the page's own form token")


def check_channels(deployment, cafile):
    print("4. the channel: local through 127.0.0.1, remote through the other address")
    local = Page(deployment.port, cafile)
    refused_at = None
    for attempt in range(1, 7):
        status, _, text, _ = local.request("POST", "/login", headers=local.origin, form={"password": "a-wrong-made-up-password"})
        if status == 429:
            refused_at = attempt
            break
        check(status == 403, f"wrong login {attempt} through 127.0.0.1 refused by the broker")
    check(refused_at is not None and "too many failed logins from this address" in text, f"the web UI's own limit refuses the next one without asking the broker (attempt {refused_at})")
    socket_path = str(deployment.root / ".sdp" / "socket" / "broker.sock")
    try:
        broker_client.Client(socket_path, timeout=30).login(MANAGEMENT, channel="local")
        throttled = False
    except broker_client.BrokerError as error:
        throttled = error.code == "throttled"
    check(throttled, "the broker's local budget is empty: the wrong logins through 127.0.0.1 were sent as `local`")
    straight = broker_client.Client(socket_path, timeout=30)
    straight.login(MANAGEMENT, channel="remote")
    straight.logout()
    check(True, "the broker's remote budget is untouched")
    remote = Page(deployment.port, cafile, deployment.bridge)
    status, _, text, cookies = remote.request("POST", "/login", headers=remote.origin, form={"password": MANAGEMENT})
    check(status == 303 and cookies, f"a right login through {deployment.bridge} (the other port) succeeds while the local budget is empty: it was sent as `remote`")
    remote.cookie = cookies[0].split(";")[0].split("=", 1)[1]
    status, _, text, _ = remote.request("GET", "/overview")
    check(status == 200 and "demo" in text, "its Overview opens")


def check_inspect(deployment):
    print("5. docker inspect")
    found = json.loads(deployment.docker("inspect", deployment.name).stdout)[0]
    config, hosting = found["Config"], found["HostConfig"]
    check(config["User"] == f"{os.getuid()}:{os.getgid()}", f"it runs as the operator's uid and gid ({config['User']})")
    check(hosting["ReadonlyRootfs"] is True, "its root file system is read-only")
    check(hosting["CapDrop"] == ["ALL"] and not hosting.get("CapAdd"), "every capability is dropped")
    check(hosting["SecurityOpt"] == ["no-new-privileges"], f"security options: {hosting['SecurityOpt']}")
    check(hosting["PidsLimit"] == 256 and hosting["Memory"] == 512 * 1024 * 1024 and hosting["RestartPolicy"]["Name"] == "no", "256 processes, 512 MiB, no restart")
    mounts = sorted((mount["Type"], mount["Source"], mount["Destination"], mount["RW"]) for mount in found["Mounts"])
    expected = sorted([("bind", str(deployment.root / ".sdp" / "socket"), "/run/sdp", False), ("bind", str(deployment.root / ".sdp" / "webui" / "tls"), "/tls", False)])
    check(mounts == expected, f"exactly two mounts, both read-only: {[(mount[2], 'rw' if mount[3] else 'ro') for mount in mounts]}")
    check(hosting.get("Tmpfs") == {"/tmp": "rw,noexec,nosuid,size=16m"}, f"a /tmp of 16 MiB without exec: {hosting.get('Tmpfs')}")
    check(not any("docker.sock" in str(mount) for mount in found["Mounts"]), "no Docker socket is mounted")
    check(list(found["NetworkSettings"]["Networks"]) == [deployment.name], f"its only network is {deployment.name}")
    network = json.loads(deployment.docker("network", "inspect", deployment.name).stdout)[0]
    check(network["Options"].get("com.docker.network.bridge.enable_ip_masquerade") == "false", "the network does not masquerade")
    check(hosting["PortBindings"] == {"8444/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(deployment.port)}], "8443/tcp": [{"HostIp": deployment.bridge, "HostPort": str(deployment.port)}]}, f"published: 127.0.0.1:{deployment.port} to 8444, {deployment.bridge}:{deployment.port} to 8443, nothing else")


def in_container(deployment, program):
    return deployment.docker("exec", deployment.name, "python", "-c", program, timeout=60)


def check_inside(deployment):
    print("6. from inside the container")
    result = in_container(deployment, "import socket\ntry:\n    socket.getaddrinfo('pypi.org', 443)\n    print('resolved')\nexcept OSError:\n    print('no')\n")
    check(result.stdout.strip() == "no", "a name on the internet does not resolve")
    result = in_container(deployment, "import socket\ns = socket.socket()\ns.settimeout(5)\nprint('reached' if s.connect_ex(('1.1.1.1', 443)) == 0 else 'no')\n")
    check(result.stdout.strip() == "no", "an address on the internet cannot be reached")
    for path in ("/run/sdp/made-up-file", "/tls/made-up-file", "/app/made-up-file", "/made-up-file"):
        result = in_container(deployment, f"open({path!r}, 'w')\n")
        check(result.returncode != 0 and ("Read-only file system" in result.stderr or "Permission denied" in result.stderr), f"nothing can be written to {path.rsplit('/', 1)[0] or '/'}")
    result = in_container(deployment, "import os\nos.remove('/run/sdp/broker.sock')\n")
    check(result.returncode != 0, "the broker's socket cannot be removed")
    check((deployment.root / ".sdp" / "socket" / "broker.sock").exists(), "the broker's socket is still there")


def check_log(deployment):
    print("7. the container's log")
    result = deployment.docker("logs", deployment.name)
    lines = (result.stdout + result.stderr).splitlines()
    requests = [line for line in lines if re.search(r" INFO (GET|HEAD|POST) ", line)]
    check(requests and all(re.fullmatch(r"\S+ \S+ INFO (GET|HEAD|POST) /\S* (\d{3}|-)", line) for line in requests), f"one line per request, method, path and status only ({len(requests)} lines, e.g. {requests[-1].split(' INFO ')[1]!r})")
    check(all(MANAGEMENT not in line and "a-wrong-made-up-password" not in line for line in lines), "no password in the container's log")


def check_stop(deployment):
    print("8. stop, and a start with a key that cannot be read")
    check(host.cmd_stop(str(deployment.root)) == 0, "stop stops and removes the container")
    check(deployment.docker("ps", "-a", "-q", "--filter", f"name={deployment.name}").stdout.strip() == "", "the container is gone")
    check(host.cmd_status(str(deployment.root)) == 1, "status says it does not run (exit code 1)")
    marker = "made-up-broken-key-material-5d0e"
    key = deployment.root / ".sdp" / "webui" / "tls" / "key.pem"
    key.write_text(f"-----BEGIN PRIVATE KEY-----\n{marker}\n-----END PRIVATE KEY-----\n")
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        code = host._start(str(deployment.root), os.getuid(), os.getgid())
    text = shown.getvalue()
    print("\n".join("  | " + line for line in text.splitlines()))
    check(code == 1 and "ended right after its start" in text and "The web UI is started." not in text, "start with a broken key returns 1 and says the server ended")
    check(marker not in text, "the log lines it prints hold nothing of the key file")
    check(deployment.docker("ps", "-a", "-q", "--filter", f"name={deployment.name}").stdout.strip() == "", "the container was removed")


def wait_for(port):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        with socket.socket() as attempt:
            if attempt.connect_ex(("127.0.0.1", port)) == 0:
                return
        time.sleep(0.25)


def check_every_address(deployment):
    print("9. every address (all, typed alone)")
    port = free_port()
    answers = [str(port), "all", "generate"]
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        code = host._setup(str(deployment.root), lambda prompt, default=None, tag=None: answers.pop(0), lambda prompt, options, default=None, tag=None: answers.pop(0), os.getuid(), os.getgid())
    text = shown.getvalue()
    check(code == 0 and f"every address of this machine, port {port}:" in text and host.EVERY_WARNING in text, f"setup on port {port} with `all` alone: it says where the page answers and warns")
    shown = io.StringIO()
    with contextlib.redirect_stdout(shown):
        code = host._start(str(deployment.root), os.getuid(), os.getgid())
    text = shown.getvalue()
    print("\n".join("  | " + line for line in text.splitlines() if "every address" in line))
    check(code == 0, "start created and started the container")
    wait_for(port)
    time.sleep(1)
    cafile = deployment.root / ".sdp" / "webui" / "tls" / "cert.pem"
    check(Page(port, cafile).request("GET", "/healthz")[0] == 200, f"the page answers at https://127.0.0.1:{port}/")
    other = Page(port, cafile, deployment.bridge)
    # The certificate names only localhost and 127.0.0.1: its chain is checked against it, its name is not (what the operator clears by the fingerprint).
    other.context.check_hostname = False
    for name in (f"{deployment.bridge}:{port}", f"unlisted.example:{port}"):
        check(other.request("GET", "/healthz", host=name)[0] == 200, f"through {deployment.bridge} (another address of this machine, not listed) it answers with Host {name}")
    status, _, _, cookies = other.request("POST", "/login", headers=other.origin, form={"password": MANAGEMENT})
    check(status == 303 and cookies, f"a login through https://{deployment.bridge}:{port}/ succeeds")
    published = [line for line in deployment.docker("port", deployment.name).stdout.split("\n") if line]
    check(f"8443/tcp -> 0.0.0.0:{port}" in published and all(line.startswith("8443/tcp -> ") and line.endswith(f":{port}") for line in published), f"docker port: the one publication on every address: {published}")
    check(not any(str(port + 1) in line or "8444" in line for line in published), f"port {port + 1} is not published, nor the local port")
    check(host.cmd_stop(str(deployment.root)) == 0, "stop stops and removes the container")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", metavar="PYTHON", help="an interpreter with Playwright: run check_webui_browser.py with it against the running page (needs PLAYWRIGHT_BROWSERS_PATH in the environment)")
    options = parser.parse_args()
    if options.browser and not os.environ.get("PLAYWRIGHT_BROWSERS_PATH"):
        parser.error("--browser needs PLAYWRIGHT_BROWSERS_PATH set to the folder of Playwright's browsers")
    deployment = Deployment()
    failed = None
    try:
        try:
            print(f"deployment {deployment.root.name}, names {deployment.name}, port {deployment.port}, Docker {deployment.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            cafile = check_setup(deployment)
            check_start(deployment)
            check_https(deployment, cafile, options.browser)
            check_channels(deployment, cafile)
            check_inspect(deployment)
            check_inside(deployment)
            check_log(deployment)
            check_stop(deployment)
            check_every_address(deployment)
        except Exception as error:
            failed = error
    finally:
        left = deployment.clean_up()
    if any(left.values()):
        print(f"FAILED: left behind: {left}")
        return 1
    print("  ok: no container, network, image or process of the check is left, and the temp folder is deleted (the base image python:3.13-slim and the build cache stay)")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
