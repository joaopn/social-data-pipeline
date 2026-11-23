"""The model of a chat: the runner's allow-list (`hello`, `chat.new`, `chat.send`), the chat keeping its model in its folder, `--model` on Claude Code's command line, and the runner's setting of the list (`SDP_CHAT_MODELS`)."""

import json

import pytest
from chat_helpers import INIT, result

from social_data_pipeline.chat import __main__ as main
from social_data_pipeline.chat import claude, openai

BASE = {"SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_CAP_GIB": "10", "SDP_CHAT_GATEWAY": "10.0.0.2"}


def error_of(reply):
    assert reply["ok"] is False and set(reply["error"]) == {"code", "message"}
    return reply["error"]["code"]


def model_flags(record):
    argv = record["argv"]
    return argv[argv.index("--model"):] if "--model" in argv else []


def listed(r, chat):
    return next(entry for entry in r.call("chat.list")["result"]["chats"] if entry["id"] == chat)


def test_hello_names_the_allow_list_and_the_default_of_each_back_end(chat_runner):
    """Bug class: the web UI offering models the runner refuses, or none of the ones it takes."""
    r = chat_runner()
    found = r.call("hello")["result"]
    assert found["models"] == ["opus", "sonnet", "haiku"] and found["model_default"] is None
    r = chat_runner(backend=openai.OpenAICompatible("127.0.0.1", "vendor/m-1", ["mongo"]))
    found = r.call("hello")["result"]
    assert found["models"] == ["vendor/m-1"] and found["model_default"] == "vendor/m-1"


@pytest.mark.parametrize("model, code", [
    ("claude-opus-5-5", "model_not_allowed"), ("Opus", "model_not_allowed"), ("gpt-4o", "model_not_allowed"), ("", "model_not_allowed"), ("opus ", "model_not_allowed"),
    (5, "bad_request"), (["opus"], "bad_request"), (True, "bad_request"),
])
def test_a_model_outside_the_allow_list_is_refused_and_changes_nothing(chat_runner, model, code):
    """Bug class: a model the gateway would forward (or one the back end does not know) taken into a chat, or a refused send that still ran a turn or changed the chat's model."""
    r = chat_runner()
    assert error_of(r.call("chat.new", model=model)) == code
    assert r.call("chat.list")["result"]["chats"] == []
    chat = r.call("chat.new", model="haiku")["result"]["chat"]
    assert error_of(r.call("chat.send", chat=chat, text="hi", model=model)) == code
    assert r.call("chat.events", chat=chat, offset=0, limit=10)["result"]["events"] == [] and r.records() == []
    assert listed(r, chat)["model"] == "haiku"


def test_an_openai_compatible_back_end_takes_only_its_configured_model(chat_runner):
    r = chat_runner(backend=openai.OpenAICompatible("127.0.0.1", "vendor/m-1", ["mongo"]))
    assert error_of(r.call("chat.new", model="opus")) == "model_not_allowed"
    chat = r.call("chat.new", model="vendor/m-1")["result"]["chat"]
    assert listed(r, chat)["model"] == "vendor/m-1"


def test_the_chat_keeps_its_model_across_turns_and_restarts_and_a_send_changes_it(chat_runner):
    """Bug class: a turn run on another model than the chat's (the picker's choice lost, or applied to one turn only), or the choice forgotten when the runner restarts."""
    r = chat_runner()
    chat = r.call("chat.new", model="haiku")["result"]["chat"]
    turns = [({}, ["--model", "haiku"]), ({"model": "opus"}, ["--model", "opus"]), ({}, ["--model", "opus"]), ({"model": None}, [])]
    for fields, flags in turns:
        r.plan([INIT, result()])
        assert r.call("chat.send", chat=chat, text="hi", **fields)["ok"] is True
        r.wait(chat)
        assert model_flags(r.records()[-1]) == flags
    assert listed(r, chat)["model"] is None
    r.plan([INIT, result()])
    r.call("chat.send", chat=chat, text="hi", model="sonnet")
    r.wait(chat)
    assert json.loads((r.work / chat / "meta.json").read_text())["model"] == "sonnet"
    r.server.close()
    r.runner.close()
    r = chat_runner()
    assert listed(r, chat)["model"] == "sonnet"
    r.call("chat.send", chat=chat, text="hi")
    r.wait(chat)
    assert model_flags(r.records()[-1]) == ["--model", "sonnet"]


def test_a_remembered_model_outside_the_allow_list_is_dropped(chat_runner):
    """Bug class: a model written into a chat's folder (by the sandbox, or under another allow-list) passed to the back end unchecked."""
    r = chat_runner()
    chat = r.call("chat.new", model="opus")["result"]["chat"]
    r.server.close()
    r.runner.close()
    meta = json.loads((r.work / chat / "meta.json").read_text())
    (r.work / chat / "meta.json").write_text(json.dumps({**meta, "model": "claude-opus-5-5"}))
    r = chat_runner(backend=claude.ClaudeCode("127.0.0.1", ["mongo"], models=("haiku",)))
    assert listed(r, chat)["model"] is None and r.call("hello")["result"]["models"] == ["haiku"]
    assert error_of(r.call("chat.new", model="opus")) == "model_not_allowed"


def test_claude_codes_command_line_gets_the_model_only_when_one_is_set():
    """Bug class: the chosen model never reaching Claude Code, or `--model` with an empty name."""
    args = ("/usr/local/bin/claude", "/work/c_0123456789ab/home", "5f6cec57-5f0d-4a8c-a927-8480b9f00bb1", True, ["mongo"])
    plain = claude.command(*args)
    assert "--model" not in plain and claude.command(*args, model="") == plain
    assert claude.command(*args, model="sonnet") == plain + ["--model", "sonnet"]


@pytest.mark.parametrize("environ, models", [
    ({**BASE, "SDP_CHAT_MODELS": "opus,sonnet,haiku"}, ["opus", "sonnet", "haiku"]),
    ({**BASE, "SDP_CHAT_MODELS": "haiku"}, ["haiku"]),
    ({**BASE, "SDP_CHAT_BACKEND": "openai-compatible", "SDP_CHAT_MODEL": "vendor/m-1", "SDP_CHAT_MODELS": "vendor/m-1"}, ["vendor/m-1"]),
])
def test_the_setting_gives_the_allow_list(environ, models):
    assert main.settings(environ)["models"] == models


@pytest.mark.parametrize("environ", [
    BASE, {**BASE, "SDP_CHAT_MODELS": ""}, {**BASE, "SDP_CHAT_MODELS": "opus,opus"}, {**BASE, "SDP_CHAT_MODELS": "opus,claude-opus-5-5"}, {**BASE, "SDP_CHAT_MODELS": "opus,"},
    {**BASE, "SDP_CHAT_BACKEND": "openai-compatible", "SDP_CHAT_MODEL": "vendor/m-1"},
    {**BASE, "SDP_CHAT_BACKEND": "openai-compatible", "SDP_CHAT_MODEL": "vendor/m-1", "SDP_CHAT_MODELS": "vendor/m-2"},
])
def test_a_missing_or_wrong_allow_list_refuses_the_start(environ):
    """Bug class: a sandbox started without its allow-list (every model taken, or none), or with names its back end does not know."""
    with pytest.raises(main.SettingError, match="SDP_CHAT_MODELS"):
        main.settings(environ)
