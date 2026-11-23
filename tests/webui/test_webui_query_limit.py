"""Tests for the settings box's section "This deployment": the read-only query limit for chats (`GET /settings/deployment`, the table entries `chat.settings` and `chat.query_limit`).

The contracts pinned here:

- The part shows the limit in force in a form that posts `chat.query_limit` to the pending page; a read that failed or came back in another shape offers no form.
- Saving goes through the pending page and sends nothing to the broker without the management password; a value outside 1 to 3600 is refused before anything is kept.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import DONE, PASSWORD, choose, confirm, form_token, log_in, pending_path

from social_data_pipeline.control import client as broker_client


def answer(seconds=90, default=True, problem=None):
    return {"query_limit": {"seconds": seconds, "default": default, "default_seconds": 90, "max_seconds": 3600, "problem": problem}}


@pytest.fixture
def client(make_app, make_client):
    made = make_client(make_app(action_wait_seconds=0.5))
    assert log_in(made).status_code == 303
    return made


@pytest.fixture
def token(client):
    return form_token(client)


def test_the_part_shows_the_limit_in_its_form(client, broker):
    """Bug class: the field filled with another value than the one in force (a save then changes what the operator did not touch), the default not said, or the note's claims missing."""
    broker.answers["chat.settings"] = answer(120, default=False)

    page = client.get("/settings/deployment")

    assert page.status_code == 200
    text = page.text
    assert 'action="/pending"' in text and 'name="action" value="chat.query_limit"' in text and 'name="form_token"' in text
    assert 'name="seconds" min="1" max="3600" step="1" required value="120"' in text and "Now: 120 s." in text
    assert "PostgreSQL ignores it (its MCP server keeps its own limit)" in text
    broker.answers["chat.settings"] = answer()
    assert "Now: 90 s (the default)." in client.get("/settings/deployment").text


@pytest.mark.parametrize("reply", [broker_client.BrokerError("internal", "x"), {"query_limit": {"seconds": True, "default": False, "default_seconds": 90, "problem": None}}, {"query_limit": {"seconds": 5000, "default": False, "default_seconds": 90, "problem": None}}, [], None])
def test_a_failed_or_misshapen_read_offers_no_form(client, broker, reply):
    """Bug class: a form drawn from an answer that is not the limit in force (an empty or wrong field the operator then saves), or a failed read that breaks the box."""
    broker.answers["chat.settings"] = reply

    page = client.get("/settings/deployment")

    assert page.status_code == 200 and "could not be read" in page.text and "<form" not in page.text


def test_saving_asks_for_the_management_password_before_anything_is_sent(client, broker, token):
    """Bug class: a change of the deployment's settings sent without the step-up (a stolen session could set it), or with another value than the one typed."""
    broker.answers["chat.query_limit"] = dict(DONE)
    path = pending_path(choose(client, "chat.query_limit", token, seconds="300"))
    broker.requests.clear()

    response = confirm(client, path, token)

    assert response.status_code == 400 and "Type the management password" in response.text
    assert broker.requests == []
    assert confirm(client, path, token, PASSWORD).status_code == 303
    assert broker.requests == [{"verb": "chat.query_limit", "fields": {"seconds": 300}, "step_up": True}]


@pytest.mark.parametrize("value", ["0", "3601", "abc", "1.5", ""])
def test_a_value_outside_the_bounds_is_refused_before_anything_is_kept(client, broker, token, value):
    """Bug class: a limit the compose file and `chat start` refuse reaching the pending page (and, confirmed, the broker)."""
    broker.requests.clear()

    response = choose(client, "chat.query_limit", token, seconds=value)

    assert response.status_code == 400 and broker.requests == []
