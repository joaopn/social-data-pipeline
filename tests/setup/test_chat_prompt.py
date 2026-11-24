"""Tests for the deployment's starting prompt of new chats on the host: `.sdp/chat/prompt/prompt.json` (`social_data_pipeline/chat_host.py` `prompt_setting`, `saved_prompt`, `save_prompt`, `reset_prompt`) and `sdp chat prompt [--instructions-file=<path>] [--queue-file=<path>] [--reset]` (`sdp.py` `cmd_chat_prompt`, `do_chat_prompt`).

The contracts pinned here, each test naming the bug class it catches:

- No file is the built-in prompt; a malformed file is a `problem` naming the file and the recovery, never the built-in prompt.
- Setting writes both parts (a part not given is the one in force: saved, else built-in); a text the runner would refuse is refused before anything is written; a file's `\\r\\n` is read as `\\n`; a reset removes the file and is no error without one; the parser refuses a reset with a file flag.
- `chat.settings` carries the prompt as the broker sends it (wrapped in `data`), and `chat settings` prints what it printed before.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import sdp
from social_data_pipeline import chat_host as host
from social_data_pipeline.chat import claude
from social_data_pipeline.chat import prompt as chat_prompt
from social_data_pipeline.control.broker import Broker
from social_data_pipeline.setup import utils


@pytest.fixture
def root(tmp_path, monkeypatch):
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    folder = folder.resolve()
    monkeypatch.setattr(sdp, "ROOT", folder)
    monkeypatch.setattr(utils, "ROOT", folder)
    return folder


def saved_file(root):
    return root / ".sdp" / "chat" / "prompt" / "prompt.json"


def write_saved(root, data):
    path = saved_file(root)
    path.parent.mkdir(parents=True, mode=0o700)
    path.write_bytes(data)


def chat_prompt_command(*words):
    """Run `python sdp.py chat prompt <words>` through the real parser."""
    args = sdp.build_parser().parse_args(["chat", "prompt", *words])
    return args.func(args)


def test_no_file_is_the_built_in_prompt_and_a_saved_one_is_shown(root):
    """Bug class: the built-in parts or the fixed facts shown from another place than the role text uses, or a saved file not read back as it was written."""
    data = host.prompt_setting(str(root))
    assert data == {"saved": None, "builtin": {"instructions": claude.INSTRUCTIONS, "queue": claude.QUEUE_INSTRUCTIONS}, "facts": {"deployment": claude.DEPLOYMENT_FACTS, "queue": claude.QUEUE_FACTS}, "max_bytes": 10000, "problem": None}
    write_saved(root, chat_prompt.encode("Be brief.\n\tAlways.", ""))
    assert host.prompt_setting(str(root))["saved"] == {"instructions": "Be brief.\n\tAlways.", "queue": ""}


@pytest.mark.parametrize("data", [b"", b"not json", b'{"instructions": "a"}', b'{"instructions": "a", "queue": "b", "x": "c"}', b'{"instructions": "a", "queue": 1}', b'{"instructions": "a\\rb", "queue": ""}', b'{"instructions": "a", "instructions": "b", "queue": ""}', json.dumps({"instructions": "x" * 10001, "queue": ""}).encode(), b"[]"])
def test_a_malformed_saved_prompt_is_a_problem_naming_the_file_and_the_recovery(root, data, capsys):
    """Bug class: a malformed file silently shown (or used) as the built-in prompt, so the operator believes a prompt is in force that no new chat gets (the runner refuses every new chat); or a problem that does not say which file and what to run."""
    write_saved(root, data)
    problem = host.prompt_setting(str(root))["problem"]
    assert problem.startswith(".sdp/chat/prompt/prompt.json ") and "python sdp.py chat prompt --reset" in problem
    assert host.prompt_setting(str(root))["saved"] is None
    assert chat_prompt_command() == 1
    assert problem in capsys.readouterr().out


def test_setting_both_parts_then_one_keeps_the_other_and_a_reset_goes_back(root, capsys):
    """Bug class: a part not given replaced by the built-in one (or left empty) when a saved one is in force, a reset that fails without a file, or a file left behind."""
    assert sdp.do_chat_prompt(instructions="mine", queue="queue rule") == 0
    assert json.loads(saved_file(root).read_text()) == {"instructions": "mine", "queue": "queue rule"}
    assert sdp.do_chat_prompt(instructions="newer") == 0
    assert json.loads(saved_file(root).read_text()) == {"instructions": "newer", "queue": "queue rule"}
    assert "chats already made keep the prompt they were made with" in capsys.readouterr().out
    assert sdp.do_chat_prompt(reset=True) == 0
    assert not saved_file(root).exists()
    assert sdp.do_chat_prompt(reset=True) == 0
    assert sdp.do_chat_prompt(queue="only the queue") == 0
    assert json.loads(saved_file(root).read_text()) == {"instructions": claude.INSTRUCTIONS, "queue": "only the queue"}


@pytest.mark.parametrize("fields", [{"instructions": "a\rb"}, {"queue": "a\x00b"}, {"instructions": "a\u2028b"}, {"instructions": "x" * 10001}, {"queue": "\u00e9" * 5001}, {"instructions": "a\ud800"}, {}, {"reset": True, "queue": "b"}, {"reset": "yes"}])
def test_a_text_the_runner_would_refuse_or_a_mixed_call_writes_nothing(root, fields):
    """Bug class: a file written that the runner then refuses (no new chat can be made), multi-byte text counted in characters, or a call that does neither a set nor a reset changing something."""
    write_saved(root, chat_prompt.encode("kept", "kept too"))
    before = saved_file(root).read_bytes()
    with pytest.raises(sdp.ActionError):
        sdp.do_chat_prompt(**fields)
    assert saved_file(root).read_bytes() == before


def test_the_files_are_read_with_windows_line_endings_made_unix_ones(root, tmp_path):
    """Bug class: a file saved on Windows refused for its `\\r` (or stored with it, after which the runner refuses every new chat)."""
    (tmp_path / "ins.txt").write_bytes(b"Be brief.\r\nAnswer in French.\r\n")
    (tmp_path / "queue.txt").write_bytes("Queue r\u00e8gle.".encode())
    assert chat_prompt_command(f"--instructions-file={tmp_path / 'ins.txt'}", "--queue-file", str(tmp_path / "queue.txt")) == 0
    assert json.loads(saved_file(root).read_text()) == {"instructions": "Be brief.\nAnswer in French.\n", "queue": "Queue r\u00e8gle."}


@pytest.mark.parametrize("words", [["--reset", "--instructions-file=a"], ["--queue-file=a", "--reset"], ["--instructions-file=a", "--queue-file=b", "--reset"]])
def test_the_parser_refuses_a_reset_with_a_file_flag_in_either_order(root, words):
    """Bug class: a reset and a new text in one command, one of which would be dropped without a word."""
    with pytest.raises(SystemExit) as exit_:
        sdp.build_parser().parse_args(["chat", "prompt", *words])
    assert exit_.value.code == 2
    assert not saved_file(root).exists()


def test_chat_settings_carries_the_prompt_as_the_broker_sends_it_and_prints_what_it_printed_before(root, capsys):
    """Bug class: B102 again (a parser written against the bare getter result while the broker wraps every read in `data`), or `chat settings`' output changed by the new setting (2026-11-21 directive)."""
    write_saved(root, chat_prompt.encode("mine", "queue rule"))
    child = SimpleNamespace(stdout=json.dumps(sdp.get_chat_settings()).encode(), stderr=b"", trimmed={})
    reply = Broker._getter_reply(None, "exited", 0, child)
    assert reply["ok"] is True
    assert reply["result"] == {"data": sdp.get_chat_settings()}
    assert reply["result"]["data"]["prompt"]["saved"] == {"instructions": "mine", "queue": "queue rule"}
    assert sdp.cmd_chat_settings(None) == 0
    assert "prompt" not in capsys.readouterr().out.lower()
