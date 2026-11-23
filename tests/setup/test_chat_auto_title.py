"""Tests for the deployment's switch of generated chat titles: `CHAT_AUTO_TITLE` of `.env` (`social_data_pipeline/chat_host.py` `auto_title`, `auto_title_setting`, `sandbox_arguments`) and `sdp chat auto-title [on|off]` (`sdp.py` `cmd_chat_auto_title`, `do_chat_auto_title`).

The contracts pinned here, each test naming the bug class it catches:

- The value is read as `sdp` reads `.env`: absent or empty is on (the default), `on` and `off` are themselves, anything else (`ON` included) is refused naming the key and the command that sets it; `chat start` refuses such a value before making anything.
- The sandbox is always told the setting (`SDP_CHAT_AUTO_TITLE=on|off`), so the runner never guesses.
- `chat auto-title` without a value shows the setting and whether it is the default; with `on` or `off` it writes `.env` through the setup's writer (every other line kept), restarts nothing and says the change waits for the chat's next start; any other value is refused by the parser and changes nothing; `chat settings` prints what it printed before.

Docker is the stand-in of `test_chat_start.py`, which records every call.
"""

from __future__ import annotations

import os

import pytest

import sdp
from social_data_pipeline import chat_host as host
from social_data_pipeline.setup import utils
from test_chat_start import MONGO_ID, UID, GID, ChatDocker, made_calls, set_up

ENV_LINES = ["# The deployment's settings", "DATA_PATH=./data", "", "MONGO_MCP_USER=reader", "# CHAT_AUTO_TITLE=off", "CHAT_AUTO_TITLE=on", "JOBS_PORT=8050"]
REFUSAL = "CHAT_AUTO_TITLE in the deployment's .env is neither on nor off. Set it again: python sdp.py chat auto-title on|off"


@pytest.fixture
def root(tmp_path, monkeypatch):
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    folder = folder.resolve()
    monkeypatch.setattr(sdp, "ROOT", folder)
    monkeypatch.setattr(utils, "ROOT", folder)
    return folder


@pytest.fixture
def docker(monkeypatch, root):
    fake = ChatDocker(root)
    monkeypatch.setattr(host, "_docker", fake)
    return fake


def auto_title(*words):
    """Run `python sdp.py chat auto-title <words>` through the real parser."""
    args = sdp.build_parser().parse_args(["chat", "auto-title", *words])
    return args.func(args)


# ── the value of .env ───────────────────────────────────────────────────────


@pytest.mark.parametrize("text, on, default", [
    (None, True, True), ("", True, True), ("CHAT_AUTO_TITLE=\n", True, True), ("# CHAT_AUTO_TITLE=off\n", True, True),
    ("CHAT_AUTO_TITLE=on\n", True, False), ("CHAT_AUTO_TITLE=off\n", False, False), ("CHAT_AUTO_TITLE=off\nCHAT_AUTO_TITLE = on \n", True, False), ("X=1\n  CHAT_AUTO_TITLE=off  \n", False, False),
])
def test_the_switch_is_read_as_sdp_reads_the_env(root, text, on, default):
    """Bug class: a missing or empty key not taken as on (the default the maintainer chose), a commented-out line read, or another line than the last one winning."""
    if text is not None:
        (root / ".env").write_text(text)
    assert host.auto_title(str(root)) is on
    assert host.auto_title_setting(str(root)) == {"on": on, "default": default, "problem": None}


@pytest.mark.parametrize("value", ["ON", "Off", "yes", "1", "true", "o n", "оn"])
def test_any_other_value_is_refused_naming_the_key_and_the_command(root, value):
    """Bug class: a value nobody can tell the meaning of silently taken as on or off (the chat then spends, or does not, against what the operator meant), or a refusal that names neither the key nor the way to set it."""
    (root / ".env").write_text(f"CHAT_AUTO_TITLE={value}\n")
    with pytest.raises(host.Refused) as refusal:
        host.auto_title(str(root))
    assert refusal.value.message == REFUSAL
    assert host.auto_title_setting(str(root)) == {"on": None, "default": False, "problem": REFUSAL}


# ── what the sandbox is told ────────────────────────────────────────────────


@pytest.mark.parametrize("on, entry", [(True, "SDP_CHAT_AUTO_TITLE=on"), (False, "SDP_CHAT_AUTO_TITLE=off")])
def test_the_sandbox_is_always_told_the_setting(on, entry):
    """Bug class: the switch not reaching the runner (titles made while the operator switched them off), or the entry left out for one value so the runner falls back to its own default."""
    arguments = host.sandbox_arguments("/srv/d", UID, GID, "10.201.0.2", "claude-code", ["mongo"], 7, auto_title=on)
    assert arguments[-3:] == ["-e", entry, "sdp-sandbox"]
    assert sum(1 for item in arguments if item.startswith("SDP_CHAT_AUTO_TITLE=")) == 1


@pytest.mark.parametrize("text, entry", [(None, "SDP_CHAT_AUTO_TITLE=on"), ("CHAT_AUTO_TITLE=off\n", "SDP_CHAT_AUTO_TITLE=off")])
def test_start_gives_the_sandbox_the_value_of_the_env(root, docker, text, entry):
    """Bug class: `chat start` reading another value than `.env` holds, or not passing it on."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    if text is not None:
        (root / ".env").write_text(text)
    assert host.cmd_start(root) == 0
    sandbox = next(call for call in docker.calls if call[1] == "create" and "sdp-sandbox" in call)
    assert entry in sandbox


def test_start_refuses_a_malformed_switch_before_making_anything(root, docker, capsys):
    """Bug class: a chat started on a guess of a value nobody set, or what a refused start made left behind."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    (root / ".env").write_text("CHAT_AUTO_TITLE=maybe\n")
    assert host.cmd_start(root) == 1
    assert made_calls(docker) == [] and not any(call[1] == "compose" for call in docker.calls)
    assert REFUSAL in capsys.readouterr().out


# ── sdp chat auto-title ─────────────────────────────────────────────────────


def test_setting_it_replaces_only_its_line_and_restarts_nothing(root, docker, capsys):
    """Bug class: a write that drops or reorders other lines of .env (its passwords and ports) or adds a second line for the key, a change said to be in force at once, or a chat stopped or started by it."""
    (root / ".env").write_text("\n".join(ENV_LINES) + "\n")

    assert auto_title("off") == 0

    # The setup's writer puts the value where the key first appears (a commented-out line of it included) and drops its later lines.
    assert (root / ".env").read_text().splitlines() == ["# The deployment's settings", "DATA_PATH=./data", "", "MONGO_MCP_USER=reader", "CHAT_AUTO_TITLE=off", "JOBS_PORT=8050"]
    assert docker.calls == []
    assert capsys.readouterr().out == "  Updated:   .env (CHAT_AUTO_TITLE=off)\n  It takes effect at the chat's next start: python sdp.py chat stop, then python sdp.py chat start. Nothing was restarted.\n"
    assert host.auto_title(str(root)) is False


def test_a_missing_key_is_added_and_every_other_line_kept(root, docker):
    """Bug class: the value lost when .env does not hold the key yet, or a .env created without it."""
    (root / ".env").write_text("A=1\nB=2\n")
    assert auto_title("on") == 0
    assert (root / ".env").read_text() == "A=1\nB=2\nCHAT_AUTO_TITLE=on\n"
    os.remove(root / ".env")
    assert auto_title("off") == 0
    assert (root / ".env").read_text() == "CHAT_AUTO_TITLE=off\n"


@pytest.mark.parametrize("text", ["ON", "yes", ""])
def test_another_value_is_refused_and_changes_nothing(root, docker, capsys, text):
    """Bug class: a value the chat's start would refuse written to .env, after which `chat start` fails."""
    (root / ".env").write_text("A=1\n")
    with pytest.raises(SystemExit) as end:
        auto_title(text)
    assert end.value.code == 2
    assert (root / ".env").read_text() == "A=1\n" and docker.calls == []
    assert "invalid choice" in capsys.readouterr().err


def test_the_action_refuses_another_value_before_writing(root):
    """Bug class: the action (which the broker calls without the parser) writing a value the parser would have refused."""
    (root / ".env").write_text("A=1\n")
    for value in ("ON", "", None, True):
        with pytest.raises(sdp.ActionError):
            sdp.do_chat_auto_title(value=value)
    assert (root / ".env").read_text() == "A=1\n"


@pytest.mark.parametrize("text, line, code", [
    ("A=1\n", "  Generated chat titles: on (the default: CHAT_AUTO_TITLE is not set in .env)", 0),
    ("CHAT_AUTO_TITLE=on\n", "  Generated chat titles: on (CHAT_AUTO_TITLE in .env)", 0),
    ("CHAT_AUTO_TITLE=off\n", "  Generated chat titles: off (CHAT_AUTO_TITLE in .env)", 0),
    ("CHAT_AUTO_TITLE=x\n", f"  Error: {REFUSAL}.", 1),
])
def test_without_a_value_the_setting_is_shown(root, docker, capsys, text, line, code):
    """Bug class: the shown setting not the one a start would use (the default not said as such), or a malformed value shown as on or off."""
    (root / ".env").write_text(text)

    assert auto_title() == code

    assert capsys.readouterr().out.splitlines() == [
        "",
        line,
        "  A new chat gets a short title made from its first question by the chat's model, a few seconds after it is sent; a title you set is never replaced; its cost counts in the chat's Cost.",
        "  Change it: python sdp.py chat auto-title on|off (on when it is not set); a change takes effect at the chat's next start.",
        "",
    ]
    assert docker.calls == []


def test_chat_settings_prints_what_it_printed_before_and_its_data_carries_the_switch(root, capsys):
    """Bug class: an existing command's output changed by the new setting (2026-11-21 directive), or the broker's `chat.settings` not carrying the switch the web UI shows."""
    (root / ".env").write_text("CHAT_AUTO_TITLE=off\n")

    assert sdp.cmd_chat_settings(None) == 0

    assert "title" not in capsys.readouterr().out
    data = sdp.get_chat_settings()
    assert data["auto_title"] == {"on": False, "default": False, "problem": None}
    assert set(data) == {"query_limit", "auto_title"}
