"""The OpenAI-compatible back end in the runner: the agent run as a real process (through a wrapper that records its argv and environment and points it at a stand-in endpoint on loopback), its pinned command line and environment, stop, and the entry point's settings."""

import json
import os
import stat
import sys

import pytest
import standins
from standins import gone, reply, tool_call

from social_data_pipeline import chat_host
from social_data_pipeline.chat import __main__ as main
from social_data_pipeline.chat import agent, claude, openai, runner
from social_data_pipeline.chat import prompt as chat_prompt

MODEL = "vendor/model-1:free"
# Runs as the agent's `python`: records what it was given (its environment as the kernel gave it), then runs the agent with the model's port set to the stand-in's.
WRAPPER = r'''
import json, os, sys
environ = dict(entry.split("=", 1) for entry in open("/proc/self/environ", "rb").read().decode().split("\0") if entry)
with open(RECORD, "a") as f:
    f.write(json.dumps({"argv": sys.argv[1:], "env": environ, "cwd": os.getcwd(), "pid": os.getpid()}) + "\n")
from social_data_pipeline.chat import agent, claude
claude.MODEL_PORT = PORT
sys.exit(agent.main(sys.argv[sys.argv.index("-m") + 2:]))
'''


def answer(doc):
    """The stand-in model: `PLOT` gets a `run_shell` that writes plot.png, then a text; `HOLD` a `run_shell` that waits in the background; anything else a text."""
    messages = doc["messages"]
    asked = [m["content"] for m in messages if m["role"] == "user"][-1]
    if messages[-1]["role"] == "tool":
        return 200, reply("Here is plot.png")
    if "PLOT" in asked:
        return 200, reply(calls=[tool_call("run_shell", {"command": "printf png > plot.png && echo written"})])
    if "HOLD" in asked:
        return 200, reply(calls=[tool_call("run_shell", {"command": "sleep 600 & echo $! > ../held.pid; wait"})])
    return 200, reply("plain answer")


@pytest.fixture
def openai_runner(chat_runner, tmp_path):
    endpoint = standins.serve(standins.Model, answer=answer)
    record = tmp_path / "agent-record.jsonl"
    wrapper = tmp_path / "python"
    wrapper.write_text(f"#!{sys.executable}\nRECORD = {str(record)!r}\nPORT = {endpoint.server_address[1]}\n" + WRAPPER)
    wrapper.chmod(0o755)
    back = openai.OpenAICompatible("127.0.0.1", MODEL, [], python=str(wrapper), grace=0.5, join=0.5)
    r = chat_runner(backend=back, databases=())
    r.endpoint = endpoint
    r.agent_records = lambda: [json.loads(line) for line in record.read_text().splitlines()]
    yield r
    standins.stop(endpoint)


def test_a_turn_runs_the_agent_and_its_lines_become_events_and_the_history_resumes(openai_runner):
    """Bug class: the agent's progress lines not read as the chat's events, the history not kept or not given to the next turn."""
    r = openai_runner
    assert r.call("hello")["result"]["backend"] == "openai-compatible"
    chat = r.new()
    assert r.call("chat.send", chat=chat, text="PLOT the counts")["result"] == {"offset": 0}
    events = r.wait(chat)
    assert [e["type"] for e in events] == ["user", "tool", "tool_result", "text", "file", "end"]
    assert events[1]["name"] == "run_shell" and events[2] == {**events[2], "text": "written\n[exit code 0]", "is_error": False} and events[3]["text"] == "Here is plot.png"
    assert events[4]["name"] == "plot.png" and {k: events[5][k] for k in ("is_error", "cost_usd", "turns")} == {"is_error": False, "cost_usd": None, "turns": 2}
    history = r.work / chat / "home" / agent.HISTORY_NAME
    assert stat.S_IMODE(os.stat(history).st_mode) == 0o600
    r.call("chat.send", chat=chat, text="and again")
    assert [e["type"] for e in r.wait(chat)][6:] == ["user", "text", "end"]
    sent = r.endpoint.seen[-1]["doc"]["messages"]
    assert [m["role"] for m in sent] == ["system", "user", "assistant", "tool", "assistant", "user"] and sent[1]["content"] == "PLOT the counts"
    assert all(entry["doc"]["model"] == MODEL for entry in r.endpoint.seen)


def test_the_agent_gets_exactly_its_environment_and_command_line(openai_runner, monkeypatch):
    """Bug class (security): the runner's environment (a variable planted in it) passed on to the agent and so to the model's shell commands."""
    monkeypatch.setenv("SDP_CHAT_LEAK_MARKER", "leaked")
    r = openai_runner
    chat = r.new()
    r.call("chat.send", chat=chat, text="hi")
    r.wait(chat)
    tmp = str(r.tmp / f"sdp-chat-{chat}")
    record = r.agent_records()[0]
    assert record["env"] == {
        "HOME": f"{tmp}/home", "TMPDIR": tmp, "MPLCONFIGDIR": f"{tmp}/matplotlib", "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": openai.PACKAGE_ROOT, "SDP_CHAT_GATEWAY": "127.0.0.1", "SDP_CHAT_MODEL": MODEL, "SDP_CHAT_DATABASES": "",
    }
    assert record["argv"] == ["-P", "-s", "-m", "social_data_pipeline.chat.agent", "--home", str(r.work / chat / "home"), "--files", str(r.work / chat / "files")]
    assert record["cwd"] == str(r.work / chat / "files")


def test_the_agent_runs_on_the_chats_prompt_file(openai_runner):
    """Bug class: the OpenAI-compatible back end ignoring the chat's starting prompt (its system message the built-in role text), or given another file than the chat's own copy."""
    r = openai_runner
    pair = ("Answer in French.\n\n\tAlways.", "Queue rule.")
    os.makedirs(r.prompt_folder)
    with open(os.path.join(r.prompt_folder, chat_prompt.NAME), "wb") as handle:
        handle.write(chat_prompt.encode(*pair))
    chat = r.new()
    r.call("chat.send", chat=chat, text="hi")
    assert [e["type"] for e in r.wait(chat)] == ["user", "text", "end"]
    assert r.endpoint.seen[-1]["doc"]["messages"][0] == {"role": "system", "content": claude.role(False, None, pair)}
    argv = r.agent_records()[0]["argv"]
    assert argv[argv.index("--prompt-file") + 1] == str(r.work / chat / chat_prompt.NAME)


def test_the_command_line_and_environment_are_pinned():
    assert openai.command("/usr/local/bin/python", "/work/c_0123456789ab/home", "/work/c_0123456789ab/files") == [
        "/usr/local/bin/python", "-P", "-s", "-m", "social_data_pipeline.chat.agent", "--home", "/work/c_0123456789ab/home", "--files", "/work/c_0123456789ab/files",
    ]
    assert openai.environment("/tmp/s/home", "/tmp/s", "10.201.0.2", "m", ["postgres", "mongo"]) == {
        "HOME": "/tmp/s/home", "TMPDIR": "/tmp/s", "MPLCONFIGDIR": "/tmp/s/matplotlib", "PATH": "/usr/local/bin:/usr/bin:/bin",
        "PYTHONPATH": openai.PACKAGE_ROOT, "SDP_CHAT_GATEWAY": "10.201.0.2", "SDP_CHAT_MODEL": "m", "SDP_CHAT_DATABASES": "postgres,mongo",
    }
    assert os.path.isfile(os.path.join(openai.PACKAGE_ROOT, "social_data_pipeline", "chat", "agent.py"))


def test_stop_ends_the_agent_and_its_shell_child(openai_runner):
    """Bug class: a stopped turn leaving the model's shell command running (it is in a process group of its own, which the runner's stop does not reach)."""
    r = openai_runner
    chat = r.new()
    r.call("chat.send", chat=chat, text="HOLD on")
    held = r.work / chat / "held.pid"
    standins.wait_for(lambda: held.exists() and held.read_text().strip(), 20, "the shell command runs")
    assert r.call("chat.stop", chat=chat)["result"] == {"stopping": True}
    events = r.wait(chat)
    assert [e["type"] for e in events][-2:] == ["error", "end"] and events[-2]["message"] == runner.STOPPED
    standins.wait_for(lambda: gone(int(held.read_text())) and gone(r.agent_records()[0]["pid"]), 5, "the agent and the shell command are gone")


@pytest.mark.parametrize("model, ok", [("vendor/model-1:free", True), ("", False), ("a b", False), ("x" * 200, True), ("x" * 201, False), ("é", False)])
def test_the_entry_point_requires_the_model_by_the_rule_of_setup(model, ok):
    """Bug class: the sandbox started without the model (every request refused by the gateway), or taking a name setup would refuse."""
    environ = {"SDP_CHAT_BACKEND": "openai-compatible", "SDP_CHAT_CAP_GIB": "10", "SDP_CHAT_GATEWAY": "10.0.0.2", "SDP_CHAT_MODEL": model, "SDP_CHAT_MODELS": model}
    try:
        assert chat_host.parse_model(model) == model and ok
    except chat_host.Refused:
        assert not ok
    if ok:
        assert main.settings(environ)["model"] == model
    else:
        with pytest.raises(main.SettingError, match="SDP_CHAT_MODEL"):
            main.settings(environ)
    assert main.settings({**environ, "SDP_CHAT_BACKEND": "claude-code", "SDP_CHAT_MODEL": "", "SDP_CHAT_MODELS": "opus,sonnet,haiku"})["backend"] == "claude-code"
    with pytest.raises(main.SettingError, match="SDP_CHAT_GATEWAY"):
        main.settings({**environ, "SDP_CHAT_MODEL": "m", "SDP_CHAT_GATEWAY": "gateway"})

