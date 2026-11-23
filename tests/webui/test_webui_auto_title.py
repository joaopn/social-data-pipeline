"""Tests for the switch of generated chat titles in the settings box's section "This deployment" (`GET /settings/deployment`, the table entries `chat.settings` and `chat.auto_title`).

The contracts pinned here:

- The part shows the setting in force (and whether it is the default) beside the query limit, with a form that posts `chat.auto_title` to the pending page and says the change waits for the chat's next start; a failed or misshapen read of the switch offers no form for it and leaves the query limit's form alone.
- Saving goes through the pending page, asks no management password (class `write`) and sends exactly the value chosen; the pending page's sentence says what `sdp chat auto-title` does, pinned whole.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import DONE, choose, confirm, form_token, log_in, pending_path

from social_data_pipeline.control import client as broker_client
from social_data_pipeline.webui import actions

# What `sdp.py` `do_chat_auto_title` does, clause by clause: it checks the value, writes the one key with the setup's writer (`update_env_file`: the line where the key first appears, a commented-out one included, is replaced and its later lines dropped; a missing key is appended; a missing .env is created holding it), prints, and restarts nothing; the sandbox reads it at its start (`chat_host._start`, `sandbox_arguments`).
SENTENCE = (
    "It writes CHAT_AUTO_TITLE to .env, one value for the whole deployment, and keeps every other line of the file. "
    "It restarts nothing: the chat's sandbox reads the setting when it starts, so the change takes effect at the chat's next start (python sdp.py chat stop, then python sdp.py chat start), and a running chat keeps the setting it started with. "
    "While it is on, a new chat gets a short title made from its first question by the chat's model a few seconds after it is sent (a title you set is never replaced, and its cost counts in the chat's Cost); while it is off, a new chat keeps the start of its first question as its title."
)


def answer(on=True, default=True, problem=None, auto_title=True):
    data = {"query_limit": {"seconds": 90, "default": True, "default_seconds": 90, "max_seconds": 3600, "problem": None}}
    if auto_title:
        data["auto_title"] = {"on": on, "default": default, "problem": problem}
    return data


@pytest.fixture
def client(make_app, make_client):
    made = make_client(make_app(action_wait_seconds=0.5))
    assert log_in(made).status_code == 303
    return made


@pytest.fixture
def token(client):
    return form_token(client)


@pytest.mark.parametrize("reply, line, selected", [
    (answer(), "Generated chat titles: on (the default).", "on"),
    (answer(on=True, default=False), "Generated chat titles: on.", "on"),
    (answer(on=False, default=False), "Generated chat titles: off.", "off"),
])
def test_the_part_shows_the_switch_in_its_form(client, broker, reply, line, selected):
    """Bug class: the form preselecting another value than the one in force (a save then changes what the operator did not touch), the default not said, the next-start rule not said, or the query limit's form lost beside it."""
    broker.answers["chat.settings"] = reply

    text = client.get("/settings/deployment").text

    assert line in text
    assert 'name="action" value="chat.auto_title"' in text and 'name="action" value="chat.query_limit"' in text
    other = "off" if selected == "on" else "on"
    assert f'<option value="{selected}" selected>' in text and f'<option value="{other}">' in text
    assert "A change applies at the chat's next start." in text
    # No password field: the switch is a `write` action.
    assert 'type="password"' not in text


@pytest.mark.parametrize("reply", [
    answer(auto_title=False),
    answer(on=None, default=False, problem=None),
    answer(on="on", default=False),
    answer(on=True, default="yes"),
    {**answer(), "auto_title": []},
])
def test_a_misshapen_switch_offers_no_form_for_it_and_keeps_the_limit(client, broker, reply):
    """Bug class: a form drawn from an answer that is not the setting in force (a broker of another version, a hostile shape), or one bad part taking the other setting's form with it."""
    broker.answers["chat.settings"] = reply

    text = client.get("/settings/deployment").text

    assert "Generated chat titles could not be read" in text and 'value="chat.auto_title"' not in text
    assert 'value="chat.query_limit"' in text


def test_a_problem_is_shown_with_the_form(client, broker):
    """Bug class: a malformed value in .env hidden (the chat then refuses to start and the page said nothing), or no way offered to set it right."""
    broker.answers["chat.settings"] = answer(on=None, default=False, problem="CHAT_AUTO_TITLE in the deployment's .env is neither on nor off. Set it again: python sdp.py chat auto-title on|off")

    text = client.get("/settings/deployment").text

    assert "Generated chat titles: CHAT_AUTO_TITLE in the deployment&#39;s .env is neither on nor off." in text
    assert 'value="chat.auto_title"' in text and "selected" not in text.split('value="chat.auto_title"')[1]


def test_a_failed_read_offers_no_form_at_all(client, broker):
    """Bug class: a failed read that breaks the box or offers forms filled with nothing."""
    broker.answers["chat.settings"] = broker_client.BrokerError("internal", "x")

    text = client.get("/settings/deployment").text

    assert "<form" not in text and "Generated chat titles could not be read" in text


def test_saving_asks_no_password_and_sends_the_value_chosen(client, broker, token):
    """Bug class: the switch asking the management password the maintainer said it does not need, a value other than the one chosen sent, or the pending page not saying what the command does."""
    broker.answers["chat.auto_title"] = dict(DONE)
    path = pending_path(choose(client, "chat.auto_title", token, value="off"))
    page = client.get(path).text
    assert SENTENCE.replace("'", "&#39;") in page
    assert "python sdp.py chat auto-title off" in page
    broker.requests.clear()

    assert confirm(client, path, token).status_code == 303

    assert broker.requests == [{"verb": "chat.auto_title", "fields": {"value": "off"}, "step_up": None}]


@pytest.mark.parametrize("value", ["ON", "yes", "", "1"])
def test_another_value_is_refused_before_anything_is_kept(client, broker, token, value):
    """Bug class: a value `chat start` would refuse reaching the pending page (and, confirmed, the broker)."""
    broker.requests.clear()

    response = choose(client, "chat.auto_title", token, value=value)

    assert response.status_code == 400 and broker.requests == []


def test_the_sentence_is_the_actions_whatever_the_value():
    """Bug class: a confirm sentence that drifts from what `do_chat_auto_title` does (the area rule: a confirm page's sentence is a claim about the CLI)."""
    entry = actions.TABLE["chat.auto_title"]
    assert entry.cls == "write" and entry.form == "python sdp.py chat auto-title on|off"
    for value in ("on", "off"):
        assert entry.also({"value": value}) == SENTENCE
        assert entry.command({"value": value}) == f"python sdp.py chat auto-title {value}"
