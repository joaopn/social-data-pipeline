#!/usr/bin/env python3
"""A check of the web UI against a real Docker, run by hand: `python tests/e2e/manual/check_webui.py [--browser PYTHON]` (an interpreter with PyYAML; a Docker without user-id mapping that can pull `python:3.13-slim` and reach PyPI, unpkg.com and cdn.jsdelivr.net, with a `bridge` network whose gateway address this machine has). With `--browser`, PYTHON is an interpreter with Playwright and `PLAYWRIGHT_BROWSERS_PATH` must be set in the environment to the folder of its browsers. Linux only: it reads `/proc`.

It is not collected by pytest. It builds a throwaway deployment in a temp folder: a copy of the CLI, the package, `Dockerfile.webui` and `requirements-webui.txt`, and one source `demo`. The image, the container and the network get throwaway names (`sdp-webui-check-<hex>`), set on the host module before it is used. What it shows:

1. `sdp.py broker passwd` and `sdp.py broker start` through the CLI; then the host module's setup with scripted answers (a free port, the gateway address of Docker's default `bridge` network as the one other listen address, one name, a generated certificate): the image is built, the files of `.sdp/webui` are private, the certificate's fingerprint is the SHA-256 of the certificate.
2. The host module's start: the container is created and started and the broker answers from inside it.
3. Over HTTPS, with the generated certificate as the only trust root: `/healthz`; a foreign `Host` refused; the login page's headers; a wrong and a right login (a form post with the right `Origin`); the Overview lists the source `demo`; a POST with a foreign `Origin` refused; the logout (the old cookie no longer works). With `--browser PYTHON`, `check_webui_browser.py` is run with that interpreter (one with Playwright) against the running page.
4. The channel of a login needs both signs of the machine itself (the local port, and the gateway's address, as which Docker shows a client on 127.0.0.1): wrong logins through 127.0.0.1 until the web UI's own limit refuses them, after which the broker's `local` budget is empty (a login sent straight to the broker's socket on `local` is `throttled`, on `remote` it is not), and a right login through the bridge address (the other port, which Docker reaches through its proxy) still succeeds: it was sent as `remote`.
5. `docker inspect`: the user, the read-only root, the dropped capabilities, the security options, the two read-only mounts and no other (no Docker socket), the network, the published ports (127.0.0.1 to 8444, the bridge address to 8443).
6. From inside the container: the broker's socket answers `hello`; a name on the internet does not resolve and an address on it cannot be reached; nothing can be written to `/run/sdp`, `/tls` or the root.
7. The container's log has one line per request with the method, the path and the status only, and no password.
8. The host module's stop; the container is gone. Then a key that cannot be read: start says the server ended, prints its last log lines (which hold nothing of the key file), removes the container and returns 1.

Clean-up, also when the check is interrupted: the container, the network and the image of the throwaway names are removed, the broker and every process of the deployment are killed, and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

import argparse
import contextlib
import hashlib
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
NAME = "webui-check.example"
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

    def clean_up(self):
        self.docker("rm", "--force", self.name)
        self.docker("network", "rm", self.name)
        self.docker("image", "rm", "--force", self.name, timeout=300)
        for pid in self.own_processes():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        time.sleep(0.5)
        left = {
            "containers": self.docker("ps", "-a", "-q", "--filter", f"name={self.name}").stdout.split(),
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
    answers = [str(deployment.port), deployment.bridge, NAME, "generate"]

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
    status, _, text, cookies = page.request("POST", "/login", headers=origin, form={"password": "a-wrong-made-up-password"})
    check(status == 403 and "The password is wrong." in text and not cookies, "a wrong login is refused")
    status, headers, _, cookies = page.request("POST", "/login", headers=origin, form={"password": MANAGEMENT})
    check(status == 303 and len(cookies) == 1 and re.fullmatch(r"__Host-sdp_session=[A-Za-z0-9_-]{43}; Path=/; Secure; HttpOnly; SameSite=Strict", cookies[0]) is not None, "the right login sets the session cookie: " + re.sub(r"=[A-Za-z0-9_-]{43};", "=<id>;", cookies[0]))
    page.cookie = cookies[0].split(";")[0].split("=", 1)[1]
    status, _, text, _ = page.request("GET", "/")
    check(status == 200 and '<a href="/sources/demo">demo</a>' in text, "the Overview lists the source demo")
    check(MANAGEMENT not in text, "the Overview holds no password")
    token = re.search(r'name="form_token" value="([^"]+)"', text).group(1)
    status, _, text, _ = page.request("POST", "/logout", headers={"Origin": "https://evil.example"}, form={"form_token": token})
    check(status == 403, "a POST with a foreign Origin is refused")
    if browser:
        result = subprocess.run([browser, str(Path(__file__).with_name("check_webui_browser.py")), f"https://127.0.0.1:{deployment.port}/"], input=MANAGEMENT + "\n", capture_output=True, text=True, timeout=300)
        print(result.stdout, end="")
        check(result.returncode == 0, "the browser check passed" + ("" if result.returncode == 0 else ": " + result.stderr.strip()[-2000:]))
    status, headers, _, cookies = page.request("POST", "/logout", headers=origin, form={"form_token": token})
    check(status == 303 and cookies and "Max-Age=0" in cookies[0], "the logout clears the cookie")
    status, headers, _, _ = page.request("GET", "/")
    check(status == 303 and headers.get("location") == "/login", "the old cookie no longer opens the Overview")


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
    status, _, text, _ = remote.request("GET", "/")
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
