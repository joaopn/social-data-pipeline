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


# ── the scheduler's control socket ──────────────────────────────────────────


def door_codes():
    """Every code the scheduler's control socket (`jobs/door.py`) can answer: the keys of MESSAGES and every literal code given to `_error` and `_Refusal`."""
    tree = ast.parse((REPO_ROOT / "social_data_pipeline" / "jobs" / "door.py").read_text())
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == "MESSAGES" for target in node.targets):
            found |= {key.value for key in node.value.keys}
        if isinstance(node, ast.Call):
            name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
            if name in ("_error", "_Refusal") and node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                found.add(node.args[0].value)
    return found


def test_the_parse_of_the_scheduler_finds_its_codes():
    """Bug class: a parse of the socket's source that finds nothing, which would let the next test pass with an empty list."""
    assert {"bad_request", "not_pending", "changed", "too_large", "explain_failed", "explain_timeout", "busy", "timeout", "outcome_unknown", "not_done", "preview_not_available", "preview_failed", "unknown_target", "not_found", "unavailable", "internal", "request_too_large"} <= door_codes()


def test_every_code_of_the_scheduler_has_a_sentence():
    """Bug class: a refusal of the scheduler (a job that changed, one that is no longer pending, an EXPLAIN that may still run) shown as the fallback or the raw code."""
    assert door_codes() - set(words.DOOR_CODE_SENTENCES) == set()
    for code in door_codes():
        assert words.door_sentence(client.BrokerError(code, "x")) == words.DOOR_CODE_SENTENCES[code]


def test_every_way_the_scheduler_cannot_be_reached_has_its_own_sentence():
    """Bug class: no settings, a control folder that fails its check, no socket, permission denied and a refused user all shown with one vague line, so the operator does not know what to do."""
    from social_data_pipeline.webui import scheduler

    reasons = ["no_settings", *scheduler.FOLDER_REASONS, *scheduler.SOCKET_REASONS, *client.REASONS]
    assert set(words.DOOR_REASON_SENTENCES) == set(reasons)
    sentences = [words.door_sentence(scheduler.SchedulerUnavailable(reason)) for reason in ["no_settings", *scheduler.FOLDER_REASONS, *scheduler.SOCKET_REASONS]] + [words.door_sentence(client.BrokerUnreachable(reason)) for reason in client.REASONS]
    assert len(set(sentences)) == len(sentences) == len(reasons)
    assert "restart the web UI" in words.door_sentence(scheduler.SchedulerUnavailable("no_settings"))
    assert "not running" in words.door_sentence(client.BrokerUnreachable(client.NO_SOCKET)) and "image was built before" in words.door_sentence(client.BrokerUnreachable(client.NO_SOCKET))
    assert words.door_sentence(client.BrokerError("made_up_code", "x")) == words.DOOR_FALLBACK


def test_the_sentences_of_what_may_still_happen():
    """Bug class: an EXPLAIN that timed out or a change whose outcome is unknown worded as if nothing happened, so the operator repeats it."""
    assert "may still be running" in words.DOOR_CODE_SENTENCES["explain_timeout"]
    assert "may still happen" in words.DOOR_CODE_SENTENCES["outcome_unknown"]
    assert "nothing was done" in words.DOOR_CODE_SENTENCES["timeout"]


def test_each_refusal_of_the_socket_names_its_fix():
    """Bug class: a socket of another owner refused with no word on what to do, although the scheduler gives its socket to the owner of its data folder when it starts."""
    assert "python sdp.py db start jobs" in words.DOOR_REASON_SENTENCES["socket_other_owner"]
    assert "nothing was sent" in words.DOOR_CODE_SENTENCES["settings_not_read"]
