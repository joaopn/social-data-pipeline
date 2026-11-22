#!/usr/bin/env python3
"""A check of `sdp chat setup` and `sdp chat unsetup` against a real Docker, run by hand: `python tests/e2e/manual/check_chat_setup.py` (Linux; a Docker that can pull `python:3.13-slim` and reach PyPI and the vendor's download of Claude Code).

It is not collected by pytest. It builds a throwaway deployment in a temp folder (a copy of the CLI, the package, the two Dockerfiles with their ignore files and `requirements-sandbox.txt`) and runs the CLI there as a child process without a terminal, the answers and the made-up secrets on its standard input (`getpass` reads them from there when there is no terminal). The two images get throwaway names (`sdp-gateway-check-<hex>`, `sdp-sandbox-check-<hex>`), set on the host module in the child before the CLI runs, so the real `sdp-gateway` and `sdp-sandbox` are never touched. What it shows:

1. `chat setup` with the back end claude-code and a made-up token: exit code 0, both images built, the modes of `.sdp/chat` (folders 0700, files 0600), the settings, the token file holding the token, and the token in none of the output (Docker's build output included).
2. `chat setup` again with the back end openai-compatible (the default address, a model, a made-up key, which is required without the question of keeping the stored token because the back end changed, a cap of 7): the same checks, and the key in none of the output.
3. Neither image holds `.sdp`, a `credential` folder or a `model.token` (searched from `/` as root): the build contexts, cut down by the two `<Dockerfile>.dockerignore` files, left the deployment's state folder out. The sandbox image, run as this user with a read-only root and no network (`HOME` and `CLAUDE_CONFIG_DIR` on a tmpfs), prints the pinned version from `claude --version`; both images default to the user 65534:65534.
4. The gateway image, started without a config file, refuses with its own message and exit code 2.
5. `chat unsetup` answered yes, then no: the images and the settings are gone, `.sdp/chat/work` and a chat's file in it stay. Again answered yes, then yes: `.sdp/chat` is gone; the images, already gone, are no error.

Clean-up, also when the check fails: the two throwaway images are removed and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

import json
import os
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from social_data_pipeline import chat_host as host

TOKEN = "made-up-claude-token-for-the-check-3e7a1f"
KEY = "made-up-endpoint-key-for-the-check-9c2b5d"
MODEL = "vendor/made-up-model"
# The child: the throwaway image names on the host module, then the CLI with the arguments given.
CHILD = "import sys\nsys.path.insert(0, sys.argv[1])\nfrom social_data_pipeline import chat_host\nchat_host.GATEWAY_IMAGE, chat_host.SANDBOX_IMAGE = sys.argv[2], sys.argv[3]\nimport sdp\nsys.argv = ['sdp.py', *sys.argv[4:]]\nsys.exit(sdp.main())\n"


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-chat-check-")).resolve()
        os.chmod(self.root, 0o700)
        suffix = os.urandom(4).hex()
        self.gateway = f"sdp-gateway-check-{suffix}"
        self.sandbox = f"sdp-sandbox-check-{suffix}"
        shutil.copy(REPO_ROOT / "sdp.py", self.root / "sdp.py")
        for name in (host.GATEWAY_DOCKERFILE, host.SANDBOX_DOCKERFILE, f"{host.GATEWAY_DOCKERFILE}.dockerignore", f"{host.SANDBOX_DOCKERFILE}.dockerignore", "requirements-sandbox.txt"):
            shutil.copy(REPO_ROOT / name, self.root / name)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))

    def cli(self, *argv, stdin):
        result = subprocess.run([sys.executable, "-c", CHILD, str(self.root), self.gateway, self.sandbox, *argv], cwd=str(self.root), env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True, text=True, timeout=3600, input=stdin, start_new_session=True)
        return result.returncode, result.stdout + result.stderr

    def docker(self, *argv, timeout=300):
        return subprocess.run(["docker", *argv], capture_output=True, text=True, timeout=timeout)

    def image_exists(self, name):
        return self.docker("image", "inspect", name).returncode == 0

    def clean_up(self):
        for name in (self.gateway, self.sandbox):
            self.docker("image", "rm", "--force", name)
        shutil.rmtree(self.root, ignore_errors=True)
        return [name for name in (self.gateway, self.sandbox) if self.image_exists(name)]


def modes(deployment):
    chat = deployment.root / ".sdp" / "chat"
    return {str(path.relative_to(chat)): oct(stat.S_IMODE(path.lstat().st_mode)) for path in [chat, *chat.rglob("*")]}


EXPECTED_MODES = {".": "0o700", "config.json": "0o600", "credential": "0o700", "credential/model.token": "0o600", "work": "0o700", "socket": "0o700"}


def own_lines(text):
    """The lines of the output that are not Docker's build progress (which starts each line with `#<step>` and is full of numbers), for the check that the secret's length is not printed."""
    return "\n".join(line for line in text.splitlines() if not line.startswith("#"))


def check_setup(deployment):
    print("1. chat setup, back end claude-code")
    code, text = deployment.cli("chat", "setup", stdin=f"1\n{TOKEN}\n\n")
    check(code == 0, "`chat setup` returned 0")
    check(TOKEN not in text and str(len(TOKEN)) not in own_lines(text), "the token is in none of the output (Docker's included), nor its length in the command's own lines")
    check(host.NOT_STARTED in text and "claude setup-token" in text and "under the vendor's terms" in text, "the summary and the two lines of the subscription login are printed")
    check(deployment.image_exists(deployment.gateway) and deployment.image_exists(deployment.sandbox), "both images are built")
    check(modes(deployment) == EXPECTED_MODES, f"the modes: {modes(deployment)}")
    chat = deployment.root / ".sdp" / "chat"
    saved = json.loads((chat / "config.json").read_text())
    check(saved == {"backend": "claude-code", "work_cap_gib": 10, "claude_code_version": host.CLAUDE_CODE_VERSION}, f"the settings: {saved}")
    check((chat / "credential" / "model.token").read_text() == TOKEN, "the token file holds the token")

    print("2. chat setup again, back end openai-compatible: a new secret is required for the other back end")
    code, text = deployment.cli("chat", "setup", stdin=f"2\n\n{MODEL}\n{KEY}\n7\n")
    check(code == 0, "`chat setup` returned 0")
    check("a new secret is needed" in text, "the stored token of the other back end is not offered to keep")
    check(KEY not in text and TOKEN not in text and str(len(KEY)) not in own_lines(text), "neither secret is in the output, nor the key's length in the command's own lines")
    check(host.DEFAULT_UPSTREAM in text and MODEL in text and "7 GiB" in text, "the summary names the address, the model and the cap")
    check(modes(deployment) == EXPECTED_MODES, f"the modes: {modes(deployment)}")
    saved = json.loads((chat / "config.json").read_text())
    check(saved == {"backend": "openai-compatible", "upstream": host.DEFAULT_UPSTREAM, "model": MODEL, "work_cap_gib": 7, "claude_code_version": host.CLAUDE_CODE_VERSION}, f"the settings: {saved}")
    check((chat / "credential" / "model.token").read_text() == KEY, "the token file holds the new key")


def check_images(deployment):
    print("3. the sandbox image")
    result = deployment.docker("run", "--rm", "--network", "none", "--user", f"{os.getuid()}:{os.getgid()}", "--read-only", "--tmpfs", "/home/chat:rw,size=16m,mode=1777", "-e", "HOME=/home/chat", "-e", "CLAUDE_CONFIG_DIR=/home/chat/.claude", deployment.sandbox, "claude", "--version")
    check(result.returncode == 0 and result.stdout.strip() == f"{host.CLAUDE_CODE_VERSION} (Claude Code)", f"`claude --version` as uid {os.getuid()} with a read-only root: {result.stdout.strip()!r}")
    for name in (deployment.gateway, deployment.sandbox):
        found = deployment.docker("run", "--rm", "--network", "none", "--user", "0:0", name, "find", "/", "-xdev", "(", "-name", ".sdp", "-o", "-name", host.TOKEN_NAME, "-o", "-name", "credential", ")", "-print")
        check(found.returncode == 0 and found.stdout.strip() == "", f"{name.rsplit('-', 2)[0]} holds no .sdp, no credential folder and no {host.TOKEN_NAME} (its build context left them out)")
    for name in (deployment.gateway, deployment.sandbox):
        user = deployment.docker("image", "inspect", "--format", "{{.Config.User}}", name).stdout.strip()
        check(user == "65534:65534", f"{name.rsplit('-', 2)[0]} defaults to the user {user}")
    print("4. the gateway image without a config")
    result = deployment.docker("run", "--rm", "--network", "none", "--read-only", deployment.gateway)
    check(result.returncode == 2 and "gateway: refusing to start: config: cannot be read as a JSON file" in result.stderr, f"it refuses with its own message: {result.stderr.strip()!r}")


def check_unsetup(deployment):
    print("5. chat unsetup")
    chat = deployment.root / ".sdp" / "chat"
    (chat / "work" / "chat-1").mkdir()
    (chat / "work" / "chat-1" / "notes.txt").write_text("a chat's file\n")
    code, text = deployment.cli("chat", "unsetup", stdin="y\nn\n")
    check(code == 0, "`chat unsetup` (yes, then no) returned 0")
    check(not deployment.image_exists(deployment.gateway) and not deployment.image_exists(deployment.sandbox), "both images are removed")
    check(sorted(os.listdir(chat)) == ["work"] and (chat / "work" / "chat-1" / "notes.txt").exists(), "the settings, the credential and the socket folder are gone; the work folder and the chat's file stay")
    code, text = deployment.cli("chat", "unsetup", stdin="y\ny\n")
    check(code == 0 and "There was no image" in text, "`chat unsetup` (yes, then yes) returned 0 with the images already gone")
    check(not chat.exists() and (deployment.root / ".sdp").is_dir(), ".sdp/chat is gone, .sdp stays")



def main():
    deployment = Deployment()
    failed = None
    try:
        try:
            print(f"deployment {deployment.root.name}, images {deployment.gateway} and {deployment.sandbox}, Docker {deployment.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            check_setup(deployment)
            check_images(deployment)
            check_unsetup(deployment)
        except Exception as error:
            failed = error
    finally:
        left = deployment.clean_up()
    if left:
        print(f"FAILED: left behind: {left}")
        return 1
    print("  ok: no image of the check is left, and the temp folder is deleted (the base image python:3.13-slim and the build cache stay)")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
