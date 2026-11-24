"""Tests for the starting prompt's floating box (`GET /settings/prompt`, `POST /settings/prompt`, the table entries `chat.settings` and `chat.prompt`).

The contracts pinned here:

- The box shows each part of the prompt in force: the saved one, else the built-in one, escaped.
- Save sends exactly `chat.prompt` with both texts, Windows line endings made Unix ones, with one click (no pending page), and answers the box saying it was saved.
"""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette.testclient")

from webui_helpers import ORIGIN, form_token, log_in

BUILTIN = {"instructions": "Built-in instructions.", "queue": "Built-in queue <part> & more."}


def settings(saved=None):
    prompt = {"saved": saved, "builtin": BUILTIN, "facts": {"deployment": "Facts.", "queue": "Queue facts."}, "max_bytes": 10000, "problem": None}
    return {"data": {"query_limit": {"seconds": 90, "default": True, "default_seconds": 90, "max_seconds": 3600, "problem": None}, "auto_title": {"on": True, "default": True, "problem": None}, "prompt": prompt}}


@pytest.fixture
def client(make_app, make_client):
    made = make_client(make_app())
    assert log_in(made).status_code == 303
    return made


def test_the_box_shows_the_saved_part_else_the_built_in_one(client, broker):
    """Bug class: the box filled from the wrong part (the built-in text over a saved one, or nothing when none is saved), so a save overwrites what the operator did not touch; or a text drawn as markup."""
    broker.answers["chat.settings"] = {"data": {**settings()["data"], "prompt": {**settings()["data"]["prompt"], "saved": {"instructions": "Mine <b>bold</b>", "queue": "My queue."}}}}
    text = client.get("/settings/prompt").text
    assert "Mine &lt;b&gt;bold&lt;/b&gt;" in text and "<b>bold</b>" not in text
    assert "My queue." in text and "Built-in instructions." not in text

    broker.answers["chat.settings"] = settings()
    text = client.get("/settings/prompt").text
    assert "Built-in queue &lt;part&gt; &amp; more." in text and "Built-in instructions." in text


def test_save_sends_both_texts_with_unix_line_endings(client, broker):
    """Bug class: the textarea's CRLF line endings stored as they came, a part dropped or renamed on the way, or the save routed through the pending page instead of one click."""
    broker.answers["chat.settings"] = settings()
    broker.answers["chat.prompt"] = {}
    token = form_token(client)

    answer = client.post("/settings/prompt", data={"form_token": token, "instructions": "Line one.\r\nLine two.", "queue": "Queue."}, headers={"origin": ORIGIN, "hx-request": "true"})

    assert answer.status_code == 200
    assert [request for request in broker.requests if request["verb"] == "chat.prompt"] == [{"verb": "chat.prompt", "fields": {"instructions": "Line one.\nLine two.", "queue": "Queue."}, "step_up": None}]
    assert "Saved" in answer.text
