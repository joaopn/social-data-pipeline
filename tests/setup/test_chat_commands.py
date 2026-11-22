"""Tests for the `chat` commands of the CLI (`social_data_pipeline/chat_host.py`, wired in `sdp.py`): `setup`, `unsetup` and the bare `sdp chat`.

The contracts pinned here, each test naming the bug class it catches:

- The answers: every refused form of the endpoint's address (another scheme, userinfo, a query, a fragment, `%`, `..`, a bad port, a bad host or path), of the model name and of the work folder's cap; the secret refused when empty, too long or not visible ASCII, without the refusal holding it.
- `setup` writes `.sdp/chat` and its folders 0700 and its files 0600 whatever the umask; the secret reaches no output, no Docker argument list and no environment, and its length is not printed; a second setup keeps or replaces the credential as answered (no question of the secret when kept); a link or a folder of another user in place of each folder is refused and left; the two `docker build` argument lists are pinned; the pinned Claude Code version equals the Dockerfile's.
- `unsetup` asks first (a default answer removes nothing and asks nothing more), removes the two images, the settings, the credential and the socket folder, keeps `work/` unless the second answer is yes, follows no link (a linked `.sdp/chat` refused; a link inside pointing at a file outside removed as a link, the file kept), and takes a missing piece for no error.
- `sdp chat setup|unsetup` reach the host module with the deployment folder; `import sdp` does not load it; `sdp chat` alone prints the group's help; every prompt has a tag listed in `tests/e2e/questions.yaml`.

Docker is a stand-in that records every call; nothing reaches a real Docker.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import sdp
from social_data_pipeline import chat_host as host

REPO_ROOT = Path(__file__).resolve().parents[2]
# A made-up secret of 53 characters (a length printed nowhere else).
MARKER = "made-up-chat-secret-marker-7f3a9c1e5b2d4f6a8c0e2b4d6f"
OTHER = "made-up-other-secret-0b1c2d"
CHAT = Path(".sdp") / "chat"


class FakeDocker:
    """Records every call (its argument list and keywords) and answers like Docker for `build` and `image rm`. `fail_build` names an image whose build fails; `images` the images that exist."""

    def __init__(self):
        self.calls = []
        self.keywords = []
        self.fail_build = None
        self.images = {host.GATEWAY_IMAGE, host.SANDBOX_IMAGE}

    def __call__(self, arguments, **keywords):
        self.calls.append(list(arguments))
        self.keywords.append(dict(keywords))
        if arguments[1] == "build":
            return self.answer(1 if arguments[arguments.index("-t") + 1] == self.fail_build else 0)
        if arguments[1:3] == ["image", "rm"]:
            if arguments[3] not in self.images:
                return self.answer(1, stderr=f"Error response from daemon: No such image: {arguments[3]}:latest")
            self.images.discard(arguments[3])
            return self.answer(0, f"Untagged: {arguments[3]}:latest")
        raise AssertionError(f"an unexpected Docker call: {arguments}")

    @staticmethod
    def answer(code, stdout="", stderr=""):
        return subprocess.CompletedProcess([], code, stdout, stderr)


@pytest.fixture
def root(tmp_path):
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    return folder.resolve()


@pytest.fixture
def docker(monkeypatch):
    fake = FakeDocker()
    monkeypatch.setattr(host, "_docker", fake)
    return fake


def scripted(answers, secret=MARKER):
    """`ask`, `ask_choice`, `ask_bool` answering from one list in order (the default when it is empty), an `ask_secret` answering `secret`, and the tags asked."""
    asked = []

    def ask(prompt, default=None, tag=None):
        asked.append(tag)
        answer = answers.pop(0) if answers else ""
        return str(default) if answer == "" and default is not None else answer

    def ask_choice(prompt, options, default=None, tag=None):
        asked.append(tag)
        return answers.pop(0) if answers else default

    def ask_bool(prompt, default=True, tag=None):
        asked.append(tag)
        return answers.pop(0) if answers else default

    def ask_secret(prompt, tag):
        asked.append(tag)
        return secret

    return ask, ask_choice, ask_bool, ask_secret, asked


def set_up(root, answers=None, secret=MARKER):
    ask, ask_choice, ask_bool, ask_secret, asked = scripted(list(answers if answers is not None else ["claude-code", "10"]), secret)
    return host._setup(str(root), ask, ask_choice, ask_bool, ask_secret), asked


OPENAI = ["openai-compatible", "https://openrouter.ai/api/v1", "vendor/model-1.5", "25"]


# ── the answers ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("answer", [
    "http://openrouter.ai/api/v1", "ftp://openrouter.ai/v1", "HTTPS://openrouter.ai/v1", "openrouter.ai/api/v1", "",
    "https://user@openrouter.ai/v1", "https://user:pw@openrouter.ai/v1", "https://openrouter.ai/v1@elsewhere",
    "https://openrouter.ai/v1?key=1", "https://openrouter.ai/v1#part", "https://openrouter.ai/%2e%2e/v1", "https://openrouter.ai/v%31",
    "https://openrouter.ai/a/../b", "https://openrouter.ai/..", "https://openrouter..ai/v1", "https://openrouter.ai/.hidden",
    "https://openrouter.ai:0/v1", "https://openrouter.ai:65536/v1", "https://openrouter.ai:/v1", "https:///v1", "https://[::1]/v1",
    "https://openrouter.ai/v 1", "https://open router.ai/v1", "https://openrouter.ai\\v1", "https://\u00f6penrouter.ai/v1", "https://openrouter.ai/v1\n",
])
def test_refused_addresses(answer):
    """Bug class: an address the gateway's table would refuse at start (or worse, one it would take: a user name sent upstream, a query or `..` that changes where a request goes) accepted at setup."""
    if answer.endswith("\n"):
        answer = "https://openrouter.ai/v1\nhttps://elsewhere.example/v1"
    with pytest.raises(host.Refused):
        host.parse_upstream(answer)


@pytest.mark.parametrize("answer", ["https://openrouter.ai/api/v1", "https://openrouter.ai/api/v1/", "https://127.0.0.1:8443/v1", "https://openrouter.ai", " https://openrouter.ai/api/v1 "])
def test_accepted_addresses(answer):
    """Bug class: the default or a plain address with a port refused."""
    assert host.parse_upstream(answer) == answer.strip()


@pytest.mark.parametrize("answer", ["", "   ", "two words", "tab\there", "x" * 201, "mod\u00e8le", "model\x7f"])
def test_refused_model_names(answer):
    """Bug class: a model name with a space, a control or a non-ASCII character (which the gateway's table refuses), or an empty or over-long one, accepted."""
    with pytest.raises(host.Refused):
        host.parse_model(answer)


def test_accepted_model_names():
    assert host.parse_model(" vendor/model-1.5:free ") == "vendor/model-1.5:free" and host.parse_model("x" * 200) == "x" * 200


@pytest.mark.parametrize("answer", ["0", "10001", "-1", "+5", "1.5", "ten", "", "\u0663", "1e3"])
def test_refused_caps(answer):
    """Bug class: a cap of zero, a negative, a fraction or a non-ASCII digit taken as a size."""
    with pytest.raises(host.Refused):
        host.parse_cap(answer)


@pytest.mark.parametrize("secret", ["", "   \n", "x" * 4097, "t\u00f6k\u00e9n-made-up", "two made-up words", "made-up\ttab", "made-up\x00nul"])
def test_refused_secrets(root, docker, secret, capsys):
    """Bug class: an empty secret saved (the gateway then refuses to start, far from the cause), a pasted one with a space or a foreign character saved, or the refusal printing what was typed."""
    with pytest.raises(host.Refused) as caught:
        set_up(root, secret=secret)
    assert secret.strip() == "" or secret.strip() not in caught.value.message
    assert str(len(secret)) not in caught.value.message
    assert not (root / CHAT).exists() and docker.calls == []
    assert secret.strip() == "" or secret.strip() not in capsys.readouterr().out


def test_a_secret_of_4096_characters_is_taken_stripped(root, docker):
    """Bug class: the surrounding white space of a pasted secret kept, or the limit off by one."""
    secret = "y" * 4096
    assert set_up(root, secret=f"  {secret}\n")[0] == 0
    assert (root / CHAT / "credential" / "model.token").read_bytes() == secret.encode()


# ── setup ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("mask", [0o000, 0o022, 0o077])
def test_setup_gives_every_folder_and_file_its_mode_whatever_the_umask(root, docker, mask):
    """Bug class: the credential or its folder readable by other users under a permissive umask, or made unusable under a strict one."""
    old = os.umask(mask)
    try:
        assert set_up(root)[0] == 0
    finally:
        os.umask(old)
    chat = root / CHAT
    modes = {str(path.relative_to(root)): stat.S_IMODE(path.lstat().st_mode) for path in [root / ".sdp", chat, *chat.rglob("*")]}
    assert modes == {".sdp": 0o700, ".sdp/chat": 0o700, ".sdp/chat/config.json": 0o600, ".sdp/chat/credential": 0o700, ".sdp/chat/credential/model.token": 0o600, ".sdp/chat/work": 0o700, ".sdp/chat/socket": 0o700}


def test_setup_makes_an_existing_folder_private(root, docker):
    """Bug class: a work or credential folder of the operator left open to others by an earlier hand-made `mkdir`."""
    (root / CHAT / "credential").mkdir(parents=True)
    os.chmod(root / CHAT / "credential", 0o755)
    assert set_up(root)[0] == 0
    assert stat.S_IMODE((root / CHAT / "credential").stat().st_mode) == 0o700


@pytest.mark.parametrize("answers", [["claude-code", "10"], OPENAI], ids=["claude-code", "openai-compatible"])
def test_the_secret_is_in_no_output_no_argument_and_no_environment(root, docker, capsys, answers):
    """Bug class: the secret printed in the summary or an error, passed to Docker as an argument or a build argument, or put into an environment that a child process (Docker) inherits."""
    before = dict(os.environ)
    assert set_up(root, answers)[0] == 0
    captured = capsys.readouterr()
    shown = captured.out + captured.err
    assert MARKER not in shown and str(len(MARKER)) not in shown
    assert all(MARKER not in word for call in docker.calls for word in call)
    assert all(set(keywords) <= {"capture", "timeout"} for keywords in docker.keywords)
    assert dict(os.environ) == before and all(MARKER not in value for value in os.environ.values())
    assert (root / CHAT / "credential" / "model.token").read_text() == MARKER
    assert MARKER not in (root / CHAT / "config.json").read_text()


def test_setup_saves_the_answers_and_prints_the_summary(root, docker, capsys):
    """Bug class: the address or model of the endpoint lost, a summary that leaves out what was set up, or one that claims the chat runs."""
    assert set_up(root, OPENAI)[0] == 0
    saved = json.loads((root / CHAT / "config.json").read_text())
    assert saved == {"backend": "openai-compatible", "upstream": "https://openrouter.ai/api/v1", "model": "vendor/model-1.5", "work_cap_gib": 25, "claude_code_version": host.CLAUDE_CODE_VERSION}
    out = capsys.readouterr().out
    for text in ("openai-compatible", "https://openrouter.ai/api/v1", "vendor/model-1.5", "25 GiB", "Set up. Nothing runs yet: starting the chat is not part of this version."):
        assert text in out
    assert set_up(root, ["claude-code", False, "10"])[0] == 0
    assert json.loads((root / CHAT / "config.json").read_text()) == {"backend": "claude-code", "work_cap_gib": 10, "claude_code_version": host.CLAUDE_CODE_VERSION}
    out = capsys.readouterr().out
    assert "claude setup-token" in out and "YOUR Claude subscription, under the vendor's terms for it" in out


def test_setup_asks_again_after_a_refused_answer(root, docker, capsys):
    """Bug class: a refused address, model or cap ending the setup, or taken anyway."""
    assert set_up(root, ["openai-compatible", "http://openrouter.ai/v1", "https://openrouter.ai/v1", "two words", "m", "0", "3"])[0] == 0
    saved = json.loads((root / CHAT / "config.json").read_text())
    assert (saved["upstream"], saved["model"], saved["work_cap_gib"]) == ("https://openrouter.ai/v1", "m", 3)


@pytest.mark.parametrize("replace, expected", [(False, MARKER), (True, OTHER)])
def test_a_second_setup_keeps_or_replaces_the_credential(root, docker, capsys, replace, expected):
    """Bug class: a second setup that asks the secret again and overwrites it unasked, or that cannot replace it; the earlier answers lost as defaults."""
    assert set_up(root, OPENAI)[0] == 0
    code, asked = set_up(root, ["openai-compatible", "", "", replace, ""], secret=OTHER)
    assert code == 0
    assert (root / CHAT / "credential" / "model.token").read_text() == expected
    assert ("chat_secret" in asked) is replace and "chat_replace_credential" in asked
    saved = json.loads((root / CHAT / "config.json").read_text())
    assert (saved["model"], saved["work_cap_gib"]) == ("vendor/model-1.5", 25)
    assert ("Credential: saved" if replace else "Credential: kept") in capsys.readouterr().out


FOLDERS = [".sdp", ".sdp/chat", ".sdp/chat/credential", ".sdp/chat/work", ".sdp/chat/socket"]


@pytest.mark.parametrize("folder", FOLDERS)
def test_a_link_in_place_of_a_folder_is_refused_and_left(root, docker, tmp_path, folder):
    """Bug class: a link planted in place of a state folder followed, so that the credential is written (or a mode changed) in a folder another user chose."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(mode=0o755)
    path = root / folder
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.symlink_to(elsewhere)
    with pytest.raises(host.Refused):
        set_up(root)
    assert path.is_symlink() and os.listdir(elsewhere) == [] and stat.S_IMODE(elsewhere.stat().st_mode) == 0o755
    assert not any(p.name == "model.token" for p in root.rglob("*"))


@pytest.mark.parametrize("folder", FOLDERS)
def test_a_folder_of_another_user_is_refused(root, docker, monkeypatch, folder):
    """Bug class: a state folder that another user made (and can therefore read or replace the credential in) used."""
    for name in FOLDERS:
        (root / name).mkdir(mode=0o700)
    inode = os.lstat(root / folder).st_ino
    real_fstat = os.fstat

    def fstat(fd):
        info = real_fstat(fd)
        if info.st_ino == inode:
            return os.stat_result((info.st_mode, info.st_ino, info.st_dev, info.st_nlink, os.geteuid() + 1, info.st_gid, info.st_size, 0, 0, 0))
        return info

    monkeypatch.setattr(host.os, "fstat", fstat)
    with pytest.raises(host.Refused):
        set_up(root)
    assert not (root / CHAT / "credential" / "model.token").exists()


def test_the_build_argument_lists_are_pinned(root, docker, capsys):
    """Bug class: an image built from the wrong Dockerfile, under the wrong name or with another context; a build argument that could carry a secret."""
    set_up(root)
    assert docker.calls == [
        ["docker", "build", "-f", f"{root}/Dockerfile.gateway", "-t", "sdp-gateway", str(root)],
        ["docker", "build", "-f", f"{root}/Dockerfile.sandbox", "-t", "sdp-sandbox", str(root)],
    ]
    out = capsys.readouterr().out
    assert "Building the image sdp-gateway ..." in out and "Building the image sdp-sandbox ..." in out


def test_a_failed_build_is_said_and_the_settings_stay(root, docker):
    """Bug class: a failed build reported as a set-up chat."""
    docker.fail_build = host.SANDBOX_IMAGE
    with pytest.raises(host.Refused) as caught:
        set_up(root)
    assert "sdp-sandbox could not be built" in caught.value.message
    assert (root / CHAT / "config.json").exists()


def test_the_pinned_version_is_the_dockerfiles():
    """Bug class: the version saved in the settings drifting from the one the image installs."""
    text = (REPO_ROOT / "Dockerfile.sandbox").read_text()
    assert re.findall(r"^ARG CLAUDE_CODE_VERSION=(\S+)$", text, re.M) == [host.CLAUDE_CODE_VERSION]
    assert "sha256sum -c" in text


def test_setup_refuses_a_deployment_folder_docker_cannot_mount(tmp_path, docker):
    """Bug class: a deployment path with a comma set up, to fail later at the first mount."""
    folder = tmp_path / "a,b"
    folder.mkdir()
    with pytest.raises(host.Refused):
        set_up(folder.resolve())
    assert docker.calls == []


# ── unsetup ─────────────────────────────────────────────────────────────────


def with_a_chat(root):
    set_up(root)
    (root / CHAT / "work" / "chat-1").mkdir()
    (root / CHAT / "work" / "chat-1" / "notes.txt").write_text("a chat's file\n")


def test_unsetup_with_the_default_answer_removes_nothing(root, docker, capsys):
    """Bug class: an unsetup that removes before or without the question."""
    with_a_chat(root)
    docker.calls.clear()
    asked = []
    assert host.cmd_unsetup(root, lambda: False, lambda: asked.append(1) or True) == 0
    assert docker.calls == [] and asked == []
    assert sorted(p.name for p in (root / CHAT).iterdir()) == ["config.json", "credential", "socket", "work"]
    out = capsys.readouterr().out
    assert "sdp-gateway and sdp-sandbox" in out and "Nothing was removed." in out


def test_unsetup_removes_exactly_what_it_says_and_keeps_the_chats(root, docker, capsys):
    """Bug class: the chats deleted with the settings although the operator kept them, or the credential left behind."""
    with_a_chat(root)
    docker.calls.clear()
    assert host.cmd_unsetup(root, lambda: True, lambda: False) == 0
    assert docker.calls == [["docker", "image", "rm", "sdp-gateway"], ["docker", "image", "rm", "sdp-sandbox"]]
    assert sorted(p.name for p in (root / CHAT).iterdir()) == ["work"]
    assert (root / CHAT / "work" / "chat-1" / "notes.txt").read_text() == "a chat's file\n"
    assert "Kept .sdp/chat/work" in capsys.readouterr().out


def test_unsetup_deletes_the_chats_when_asked(root, docker):
    """Bug class: a yes to the second question leaving the chats, or removing `.sdp` with the broker's files."""
    with_a_chat(root)
    (root / ".sdp" / "broker-file").write_text("not the chat's\n")
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert not (root / CHAT).exists() and (root / ".sdp" / "broker-file").exists()


def test_unsetup_follows_no_link_inside(root, docker, tmp_path):
    """Bug class: a link planted inside the chat's folders followed by the removal, deleting a file outside."""
    with_a_chat(root)
    marker = tmp_path / "outside.txt"
    marker.write_text("kept\n")
    (tmp_path / "outside").mkdir()
    (tmp_path / "outside" / "inner.txt").write_text("kept too\n")
    (root / CHAT / "credential" / "planted").symlink_to(marker)
    (root / CHAT / "work" / "chat-1" / "planted").symlink_to(tmp_path / "outside")
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert not (root / CHAT).exists()
    assert marker.read_text() == "kept\n" and (tmp_path / "outside" / "inner.txt").read_text() == "kept too\n"


@pytest.mark.parametrize("linked", [".sdp/chat", ".sdp/chat/credential", ".sdp/chat/config.json"])
def test_unsetup_refuses_a_link_in_place_of_a_piece(root, docker, tmp_path, capsys, linked):
    """Bug class: `.sdp/chat` (or a piece of it) replaced by a link, and the removal deleting what the link points at."""
    with_a_chat(root)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "keep.txt").write_text("not to be removed\n")
    target = elsewhere if linked != ".sdp/chat/config.json" else elsewhere / "keep.txt"
    path = root / linked
    if path.is_dir():
        os.rename(path, tmp_path / "moved")
    else:
        path.unlink()
    path.symlink_to(target)
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 1
    assert path.is_symlink() and (elsewhere / "keep.txt").read_text() == "not to be removed\n"
    assert "it was left as it is" in capsys.readouterr().out


def test_unsetup_takes_missing_pieces_for_no_error(root, docker, capsys):
    """Bug class: an unsetup that fails on a second run, or when the images were removed by hand."""
    docker.images = set()
    assert host.cmd_unsetup(root, lambda: True, lambda: False) == 0
    out = capsys.readouterr().out
    assert "There was no image sdp-gateway." in out and "There was no .sdp/chat to remove." in out


# ── the CLI ─────────────────────────────────────────────────────────────────


def test_importing_sdp_does_not_load_the_chat_module():
    """Bug class: the CLI loading the chat's module (and the gateway's) at its start, for every command."""
    probe = "import json, sys\nimport sdp\nprint(json.dumps(sorted(sys.modules)))\n"
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=60, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    assert result.returncode == 0, result.stderr
    assert [name for name in json.loads(result.stdout) if name.startswith(("social_data_pipeline.chat_host", "social_data_pipeline.gateway"))] == []


def test_bare_chat_prints_the_groups_help():
    """Bug class: `sdp chat` without a command doing nothing, or failing."""
    result = subprocess.run([sys.executable, str(REPO_ROOT / "sdp.py"), "chat"], capture_output=True, text=True, timeout=30, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    assert result.returncode == 0 and result.stdout.startswith("usage: sdp.py chat")
    assert "setup" in result.stdout and "unsetup" in result.stdout


def test_setup_reaches_the_host_module_with_the_deployment_folder(root, monkeypatch):
    """Bug class: a command wired to the wrong function, or given the current directory instead of the deployment folder."""
    seen = []
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(host, "cmd_setup", lambda folder: seen.append(folder) or 7)
    args = sdp.build_parser().parse_args(["chat", "setup"])
    assert args.func(args) == 7 and seen == [root]


def test_unsetup_is_wired_with_two_confirmations(root, monkeypatch):
    """Bug class: an unsetup that removes without the question, or deletes the chats on the first yes."""
    seen = []
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(host, "cmd_unsetup", lambda folder, confirm, confirm_work: seen.append((folder, confirm(), confirm_work())) or 0)
    answers = iter(["y", ""])
    monkeypatch.setattr("builtins.input", lambda text="": next(answers))
    args = sdp.build_parser().parse_args(["chat", "unsetup"])
    assert args.func(args) == 0 and seen == [(root, True, False)]


def test_every_prompt_has_a_tag_listed_in_the_questions(root, docker, monkeypatch, capsys):
    """Bug class: a prompt without a tag (the E2E harness waits on it until its timeout), the secret asked with echo, or a tag missing from the dictionary of questions."""
    import getpass

    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setenv("SDP_TAGGED_MODE", "1")
    answers = iter(["2", "", "vendor/model", ""])
    prompts, secret_prompts = [], []
    monkeypatch.setattr("builtins.input", lambda text="": prompts.append(text) or next(answers))
    monkeypatch.setattr(getpass, "getpass", lambda text="": secret_prompts.append(text) or MARKER)
    args = sdp.build_parser().parse_args(["chat", "setup"])
    assert args.func(args) == 0
    shown = capsys.readouterr().out + "".join(prompts)
    assert len(secret_prompts) == 1 and "[chat_secret]" in secret_prompts[0] and MARKER not in shown
    questions = yaml.safe_load((REPO_ROOT / "tests" / "e2e" / "questions.yaml").read_text())
    for tag in ("chat_backend", "chat_upstream", "chat_model", "chat_work_cap"):
        assert f"[{tag}]" in shown and tag in questions
    for tag in ("chat_secret", "chat_replace_credential", "chat_unsetup_confirm", "chat_unsetup_work"):
        assert tag in questions


# ── review round 1 ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("answer", ["1" * 5000, "123456", "010000"])
def test_a_cap_of_too_many_digits_is_refused_before_it_is_converted(answer):
    """Bug class: a typed cap of more than 4300 digits ending the setup with a traceback (`int` refuses it with a ValueError of its own)."""
    with pytest.raises(host.Refused):
        host.parse_cap(answer)


@pytest.mark.parametrize("saved", ['"' + "9" * 5000 + '"', "9" * 5000, "123456", "true"])
def test_a_saved_cap_that_cannot_be_used_falls_back_to_the_default(root, docker, saved):
    """Bug class: a hand-edited cap in `config.json` breaking every later setup with a traceback."""
    (root / CHAT).mkdir(parents=True)
    (root / CHAT / "config.json").write_text('{"backend": "claude-code", "work_cap_gib": ' + saved + "}\n")
    assert set_up(root, ["claude-code", ""])[0] == 0
    assert json.loads((root / CHAT / "config.json").read_text())["work_cap_gib"] == host.DEFAULT_CAP_GIB


@pytest.mark.parametrize("dockerfile", ["Dockerfile.gateway", "Dockerfile.sandbox"])
def test_each_build_context_holds_only_what_its_dockerfile_copies(dockerfile):
    """Bug class: the deployment folder (with `.sdp/chat/credential`) sent to Docker as the build context, or a COPY whose file the ignore file leaves out."""
    lines = [line for line in (REPO_ROOT / f"{dockerfile}.dockerignore").read_text().splitlines() if line and not line.startswith("#")]
    assert lines[0] == "*"
    included = {line[1:].rstrip("/") for line in lines if line.startswith("!")}
    copied = {source.rstrip("/") for line in (REPO_ROOT / dockerfile).read_text().splitlines() if line.startswith("COPY ") for source in line.split()[1:-1]}
    assert included == copied and not any(name.startswith(".sdp") for name in included)


@pytest.mark.parametrize("first, second", [(["claude-code", "10"], OPENAI), (OPENAI, ["claude-code", "10"])], ids=["to-openai", "to-claude-code"])
def test_changing_the_back_end_needs_a_new_secret(root, docker, capsys, first, second):
    """Bug class: a Claude token kept as the key of an OpenAI-compatible endpoint (or the reverse) because "keep" is the default answer."""
    assert set_up(root, first)[0] == 0
    capsys.readouterr()
    code, asked = set_up(root, list(second), secret=OTHER)
    assert code == 0 and "chat_replace_credential" not in asked and "chat_secret" in asked
    assert (root / CHAT / "credential" / "model.token").read_text() == OTHER
    assert "a new secret is needed" in capsys.readouterr().out


def test_unsetup_removes_a_tree_3000_folders_deep(root, docker):
    """Bug class: a recursive removal failing on a deep tree the sandbox left in `work/` (recursion limit, or one descriptor per level)."""
    set_up(root)
    fd = os.open(root / CHAT / "work", os.O_RDONLY | os.O_DIRECTORY)
    for _ in range(3000):
        os.mkdir("d", dir_fd=fd)
        child = os.open("d", os.O_RDONLY | os.O_DIRECTORY, dir_fd=fd)
        os.close(fd)
        fd = child
    os.close(fd)
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert not (root / CHAT).exists()


def test_unsetup_removes_closed_folders_fifos_and_links_without_following(root, docker, tmp_path):
    """Bug class: a folder of mode 000 or 0500 left by the sandbox stopping the removal, a FIFO opened (the removal hangs), or a link followed out of the work folder."""
    set_up(root)
    work = root / CHAT / "work"
    for mode in (0o000, 0o500):
        folder = work / f"closed-{mode:o}"
        folder.mkdir()
        (folder / "inside.txt").write_text("x\n")
        os.chmod(folder, mode)
    os.mkfifo(work / "pipe")
    marker = tmp_path / "outside.txt"
    marker.write_text("kept\n")
    (work / "link").symlink_to(marker)
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert not (root / CHAT).exists() and marker.read_text() == "kept\n"


def test_unsetup_removes_leftover_temporary_files_and_says_what_stayed(root, docker, capsys):
    """Bug class: a temporary file of an interrupted write (one of the credential's holds the secret) left behind, or `.sdp/chat` left without a word."""
    set_up(root)
    (root / CHAT / "config.json.0a1b2c3d.tmp").write_text("{}\n")
    (root / CHAT / "credential" / "model.token.0a1b2c3d.tmp").write_text("made-up-leftover\n")
    (root / CHAT / "stray.txt").write_text("not the chat's\n")
    capsys.readouterr()
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert sorted(os.listdir(root / CHAT)) == ["stray.txt"]
    assert ".sdp/chat was not removed: 1 other entry stayed in it." in capsys.readouterr().out


def test_setup_sweeps_a_leftover_temporary_token(root, docker):
    """Bug class: a copy of a secret left in `credential/` by an interrupted write, kept by every later setup."""
    set_up(root)
    leftover = root / CHAT / "credential" / "model.token.0a1b2c3d.tmp"
    leftover.write_text("made-up-leftover\n")
    assert set_up(root, ["claude-code", False, ""])[0] == 0
    assert sorted(os.listdir(root / CHAT / "credential")) == ["model.token"]


@pytest.mark.parametrize("failure", ["build", "folder"])
def test_the_secret_is_in_nothing_on_a_failure_path(root, docker, capsys, tmp_path, failure):
    """Bug class: the secret in a refusal, a traceback or Docker's arguments when the setup fails after it was typed (a failed build, a folder refused when the files are written)."""
    if failure == "build":
        docker.fail_build = host.GATEWAY_IMAGE
    else:
        (tmp_path / "elsewhere").mkdir()
        (root / CHAT).mkdir(parents=True)
        (root / CHAT / "work").symlink_to(tmp_path / "elsewhere")
    before = dict(os.environ)
    with pytest.raises(host.Refused) as caught:
        set_up(root)
    captured = capsys.readouterr()
    assert MARKER not in caught.value.message + captured.out + captured.err
    assert all(MARKER not in word for call in docker.calls for word in call)
    assert dict(os.environ) == before


# ── review round 2 ──────────────────────────────────────────────────────────


class TooManyNames(Exception):
    pass


def test_unsetup_removes_many_sibling_folders_in_linear_time(root, docker, monkeypatch):
    """Bug class: a removal that lists a folder again after each emptied subfolder: 200,000 siblings (seconds for the sandbox to make) cost about 2x10^10 names read, hours."""
    set_up(root)
    count = 20000
    work_fd = os.open(root / CHAT / "work", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for index in range(count):
            os.mkdir(f"d{index}", dir_fd=work_fd)
    finally:
        os.close(work_fd)
    read = [0]
    real_listdir = os.listdir

    def listdir(where="."):
        names = real_listdir(where)
        read[0] += len(names)
        if read[0] > 4 * count:
            raise TooManyNames(read[0])
        return names

    monkeypatch.setattr(host.os, "listdir", listdir)
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert not (root / CHAT).exists() and read[0] <= 4 * count


def stat_as(real, name, **changes):
    """An `os.stat` that reports the entry `name` (looked at relative to a folder) with some fields changed."""
    order = ("st_mode", "st_ino", "st_dev", "st_nlink", "st_uid", "st_gid", "st_size")

    def fake(path, *args, **keywords):
        info = real(path, *args, **keywords)
        if path == name and "dir_fd" in keywords:
            fields = [changes.get(key, getattr(info, key)) for key in order]
            return os.stat_result((*fields, 0, 0, 0))
        return info

    return fake


@pytest.mark.parametrize("chmod_error", [None, ValueError, NotImplementedError])
def test_a_link_swapped_in_mid_walk_is_reported_not_followed(root, docker, monkeypatch, tmp_path, capsys, chmod_error):
    """Bug class: an entry that turns into a link between its look and its change of mode followed, or ending the unsetup with a traceback (ValueError or NotImplementedError from the change of mode)."""
    set_up(root)
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o755)
    (outside / "keep.txt").write_text("kept\n")
    (root / CHAT / "work" / "swapped").symlink_to(outside)
    real_stat, real_chmod = os.stat, os.chmod
    monkeypatch.setattr(host.os, "stat", stat_as(real_stat, "swapped", st_mode=stat.S_IFDIR | 0o700))
    if chmod_error is not None:
        def chmod(path, *args, **keywords):
            if path == "swapped":
                raise chmod_error("made-up refusal of the change of mode")
            return real_chmod(path, *args, **keywords)

        monkeypatch.setattr(host.os, "chmod", chmod)
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 1
    out = capsys.readouterr().out
    assert "1 entry could not be removed" in out and "Traceback" not in out
    assert (outside / "keep.txt").read_text() == "kept\n" and stat.S_IMODE(outside.stat().st_mode) == 0o755


def test_the_walk_does_not_cross_into_another_file_system(root, docker, monkeypatch, capsys):
    """Bug class: a removal that follows a mount point the sandbox's folder holds into another file system."""
    set_up(root)
    mounted = root / CHAT / "work" / "mounted"
    mounted.mkdir()
    (mounted / "theirs.txt").write_text("on another file system\n")
    monkeypatch.setattr(host.os, "stat", stat_as(os.stat, "mounted", st_dev=os.stat(mounted).st_dev + 1))
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 1
    assert (mounted / "theirs.txt").exists()
    assert ".sdp/chat/work: 1 entry could not be removed" in capsys.readouterr().out


@pytest.mark.parametrize("config", [None, "not json", '{"work_cap_gib": 10}'], ids=["missing", "unreadable", "no-back-end"])
def test_a_token_of_an_unknown_back_end_needs_a_new_secret(root, docker, capsys, config):
    """Bug class: a token kept although nothing says which back end it is for."""
    set_up(root)
    if config is None:
        (root / CHAT / "config.json").unlink()
    else:
        (root / CHAT / "config.json").write_text(config)
    capsys.readouterr()
    code, asked = set_up(root, ["claude-code", "10"], secret=OTHER)
    assert code == 0 and "chat_replace_credential" not in asked and "chat_secret" in asked
    assert (root / CHAT / "credential" / "model.token").read_text() == OTHER
    assert "back end is not known" in capsys.readouterr().out


def test_the_sweep_leaves_anything_but_a_regular_file(root, docker):
    """Bug class: a folder (or a link) named like a leftover temporary file unlinked, or making every later setup refuse."""
    set_up(root)
    odd = root / CHAT / "credential" / "model.token.0a1b2c3d.tmp"
    odd.mkdir()
    assert set_up(root, ["claude-code", False, ""])[0] == 0
    assert odd.is_dir()
