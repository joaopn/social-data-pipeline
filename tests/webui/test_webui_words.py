"""Tests for `webui.words`: one sentence for every error code the broker can answer and every reason of `BrokerUnreachable`, and a fallback for the rest.

The codes are read out of the broker's source (its syntax tree), so that a code added there without a sentence here fails this test.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from social_data_pipeline.control import client, errors, sessions
from social_data_pipeline.webui import words

REPO_ROOT = Path(__file__).resolve().parents[2]
CODE = re.compile(r"[a-z][a-z_]*")


def broker_codes():
    """Every code `control/broker.py` can answer: the keys of MESSAGES, every literal code given to `_error`, `_Refusal` and `_refuse`, the codes of the sessions module it passes on, and the codes of RunNotStarted (named in its docstring)."""
    tree = ast.parse((REPO_ROOT / "social_data_pipeline" / "control" / "broker.py").read_text())
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "MESSAGES" for target in node.targets):
            found |= {key.value for key in node.value.keys}
        if isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
            # The code is the first argument of `_error` and `_Refusal`, the third of `_refuse(number, verb, code, field)`.
            position = {"_error": 0, "_Refusal": 0, "_refuse": 2}.get(name)
            if position is not None and len(node.args) > position:
                argument = node.args[position]
                if isinstance(argument, ast.Constant) and isinstance(argument.value, str) and CODE.fullmatch(argument.value):
                    found.add(argument.value)
    found |= {sessions.UNAUTHORIZED, sessions.THROTTLED, sessions.NO_PASSWORD_SET, sessions.PASSWORD_FILE_NOT_USABLE, sessions.STEP_UP_FAILED, sessions.SESSION_ENDED}
    found |= set(re.findall(r"`([a-z_]+)` \(", errors.RunNotStarted.__doc__))
    return found


def test_the_parse_finds_the_codes_it_must():
    """Bug class: a parse of the broker's source that finds nothing (a renamed helper), which would let the next test pass with an empty list."""
    codes = broker_codes()
    assert {"bad_request", "request_too_large", "unauthorized", "throttled", "no_password_set", "password_file_not_usable", "step_up_failed", "session_ended", "unknown_verb", "not_available", "invalid_input", "step_up_required", "run_active", "docker_unreachable", "store_not_usable", "password_not_allowed_here", "audit_not_writable", "broker_folder_not_usable", "busy", "timeout", "too_large", "failed", "start_failed", "stopping", "internal"} <= codes
    assert {"docker_not_reachable", "not_the_deployment", "cli_not_loadable", "record_not_possible"} <= codes


def test_every_code_the_broker_can_answer_has_a_sentence():
    """Bug class: a code of the broker shown to the operator as the fallback ("a reason the web UI does not know"), or as the raw code."""
    missing = broker_codes() - set(words.CODE_SENTENCES)
    assert missing == set()


def test_every_reason_of_no_reply_has_a_sentence():
    """Bug class: a broker that is not running, one that refuses the container's user and one that cannot be opened all shown with one vague line."""
    assert set(words.REASON_SENTENCES) == set(client.REASONS)
    sentences = [words.sentence(client.BrokerUnreachable(reason)) for reason in client.REASONS]
    assert len(set(sentences)) == len(sentences)


@pytest.mark.parametrize("code", ["no_such_code", "", None, 7])
def test_an_unknown_code_gets_the_fallback(code):
    """Bug class: an unknown code raising (a KeyError where a sentence was due) or echoed onto the page."""
    error = client.BrokerError(code, "a message")
    assert words.sentence(error) == words.FALLBACK
    assert words.code_sentence(code) == words.FALLBACK


def test_no_sentence_carries_the_text_of_the_error():
    """Bug class: the broker's message, or the hostile `errors` text of a failed command, put into the sentence (and from there rendered as if the web UI said it)."""
    error = client.BrokerError("failed", "<b>made-up message</b>", details={"errors": "<script>made-up</script>"})
    shown = words.sentence(error)
    assert shown == words.CODE_SENTENCES["failed"]
    assert "made-up" not in shown
