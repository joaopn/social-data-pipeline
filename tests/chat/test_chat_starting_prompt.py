"""The chat's starting prompt in the runner: `chat.new` copies the deployment's saved prompt into the new chat (or refuses `prompt_unreadable`), and every turn runs on the chat's own copy (`chat/runner.py` `_deployment_prompt`, `_chat_prompt`, `_op_chat_new`, `_turn`), here with Claude Code's stand-in; the OpenAI-compatible agent's `--prompt-file` is tested in `test_chat_openai.py`."""

import json
import os

from social_data_pipeline.chat import claude, runner
from social_data_pipeline.chat import prompt as chat_prompt

PAIR = ("Answer in French.\n\n\tAlways.", "Queue rule.")


def save(folder, data):
    os.makedirs(folder, mode=0o700, exist_ok=True)
    with open(os.path.join(folder, chat_prompt.NAME), "wb") as handle:
        handle.write(data)


def role_of(record):
    argv = record["argv"]
    return argv[argv.index("--append-system-prompt") + 1]


def test_a_chat_keeps_the_prompt_it_was_made_with_and_a_new_one_takes_the_saved_one(chat_runner):
    """Bug class: a chat that changes its prompt when the deployment's prompt changes (the operator's earlier chats go on under other instructions), a new chat that does not get the saved prompt without a restart, or a chat made without a saved prompt that does not run on the built-in one."""
    r = chat_runner()
    plain = r.new()
    assert not (r.work / plain / chat_prompt.NAME).exists()
    save(r.prompt_folder, chat_prompt.encode(*PAIR))
    made = r.new()
    assert chat_prompt.parse((r.work / made / chat_prompt.NAME).read_bytes()) == PAIR
    save(r.prompt_folder, chat_prompt.encode("Changed later.", ""))

    for chat in (plain, made):
        r.call("chat.send", chat=chat, text="hi")
        r.wait(chat)

    assert [role_of(record) for record in r.records()] == [claude.role(False), claude.role(False, None, PAIR)]


def test_a_malformed_saved_prompt_refuses_the_new_chat_and_makes_nothing(chat_runner):
    """Bug class: a malformed file silently taken as the built-in prompt (the operator believes their prompt is in force), or a chat folder left behind by the refusal."""
    r = chat_runner()
    for data in (b'{"instructions": "a"}', b"\xff", json.dumps({"instructions": "a\rb", "queue": ""}).encode(), b" " * (chat_prompt.FILE_MAX_BYTES + 1)):
        save(r.prompt_folder, data)
        assert r.call("chat.new") == {"ok": False, "error": {"code": "prompt_unreadable", "message": runner.MESSAGES["prompt_unreadable"]}}
    os.remove(os.path.join(r.prompt_folder, chat_prompt.NAME))
    os.symlink("/etc/hostname", os.path.join(r.prompt_folder, chat_prompt.NAME))
    assert r.call("chat.new")["error"]["code"] == "prompt_unreadable"
    assert os.listdir(r.work) == [] and r.call("chat.list")["result"] == {"chats": []}


def test_a_chat_whose_prompt_cannot_be_read_ends_the_turn_with_the_error_and_runs_nothing(chat_runner):
    """Bug class: a turn that runs on the built-in prompt when the chat's own file is broken, or a turn that ends without its `end`."""
    r = chat_runner()
    chat = r.new()
    (r.work / chat / chat_prompt.NAME).write_text('{"instructions": 5, "queue": ""}')
    r.call("chat.send", chat=chat, text="hi")
    events = r.wait(chat)
    assert [e["type"] for e in events] == ["user", "error", "end"] and events[1]["message"] == "the chat's starting prompt cannot be read" and events[2]["is_error"] is True
    assert r.records() == []
