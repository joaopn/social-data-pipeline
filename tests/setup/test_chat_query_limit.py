"""Tests for `sdp chat query-limit <seconds>` (`sdp.py` `do_chat_query_limit`, `social_data_pipeline/chat_host.py` `apply_query_limit`).

The contracts pinned here, each test naming the bug class it catches:

- The value is written to `.env` through the setup's writer and nothing else of the file changes; a value that is not a whole number of seconds from 1 to 3600 changes nothing and asks Docker nothing.
- When the chat runs, exactly the copies of the MCP servers that run now are removed and started again with the new value in their environment and attached again under their alias; the gateway, the sandbox and the networks are left as they are. When it does not run, only `.env` is written.
- A copy that cannot be started again is named with the recovery, the value stays written; an image without the query-limit label is refused before any copy is removed.

Docker is the stand-in of `test_chat_start.py`, which records every call and the environment of a call given one.
"""

from __future__ import annotations

import argparse

import pytest

import sdp
from social_data_pipeline import chat_host as host
from social_data_pipeline.setup import utils
from test_chat_start import COPY_IDS, MONGO_COPY, MONGO_ID, STARROCKS_ID, ChatDocker, set_up

STARROCKS_COPY = COPY_IDS["chat-starrocks-mcp"]
ENV_LINES = ["# The deployment's settings", "DATA_PATH=./data", "", "MONGO_MCP_USER=reader", "# CHAT_QUERY_TIMEOUT_SECONDS=10", "CHAT_QUERY_TIMEOUT_SECONDS=45", "JOBS_PORT=8050"]


@pytest.fixture
def root(tmp_path, monkeypatch):
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    folder = folder.resolve()
    monkeypatch.setattr(sdp, "ROOT", folder)
    monkeypatch.setattr(utils, "ROOT", folder)
    (folder / ".env").write_text("\n".join(ENV_LINES) + "\n")
    return folder


@pytest.fixture
def docker(monkeypatch, root):
    fake = ChatDocker(root)
    monkeypatch.setattr(host, "_docker", fake)
    return fake


def query_limit(seconds):
    return sdp.cmd_chat_query_limit(argparse.Namespace(seconds=seconds))


def changes(docker):
    """The calls that change something."""
    return [call for call in docker.calls if call[1] in ("create", "start", "stop", "rm", "compose") or call[1:3] in (["network", "create"], ["network", "connect"], ["network", "disconnect"], ["network", "rm"])]


def running_chat(root, docker, *databases):
    """A chat started with the MCP servers of `databases` running, and the calls of the start forgotten."""
    set_up(root)
    for container, service in databases:
        docker.add(container, service)
    assert host.cmd_start(root) == 0
    docker.calls.clear()
    docker.envs.clear()


def test_the_value_replaces_only_its_line_and_nothing_starts_when_the_chat_does_not_run(root, docker, capsys):
    """Bug class: a write that drops or reorders other lines of .env (its passwords and ports), adds a second line for the key, or a chat that does not run being started by the change."""
    assert query_limit("120") == 0

    # The setup's writer puts the value where the key first appears (a commented-out line of it included) and drops its later lines.
    assert (root / ".env").read_text().splitlines() == ["# The deployment's settings", "DATA_PATH=./data", "", "MONGO_MCP_USER=reader", "CHAT_QUERY_TIMEOUT_SECONDS=120", "JOBS_PORT=8050"]
    assert changes(docker) == []
    out = capsys.readouterr().out
    assert "Updated:   .env (CHAT_QUERY_TIMEOUT_SECONDS=120)" in out and "The chat is not running" in out
    assert host.query_seconds(str(root)) == 120


def test_a_missing_key_is_added_and_every_other_line_kept(root, docker):
    """Bug class: the value lost when .env does not hold the key yet (the default of 90 then stays in force while the command said it changed)."""
    (root / ".env").write_text("A=1\nB=2\n")

    assert query_limit("3600") == 0

    assert (root / ".env").read_text() == "A=1\nB=2\nCHAT_QUERY_TIMEOUT_SECONDS=3600\n"


@pytest.mark.parametrize("text", ["0", "3601", "12345", "-5", "+5", "1.5", "90s", " 90", "", "abc", "\u0663\u0660"])
def test_a_value_outside_the_bounds_changes_nothing(root, docker, capsys, text):
    """Bug class: a limit the compose file and the chat's start would refuse (or read as another number) written to .env, after which `chat start` fails; or a refusal that does not name the bounds."""
    before = (root / ".env").read_text()

    assert query_limit(text) == 1

    assert (root / ".env").read_text() == before and docker.calls == []
    assert "whole number of seconds from 1 to 3600" in capsys.readouterr().out


def test_without_a_value_the_limit_in_force_is_shown(root, docker, capsys):
    """Bug class: the shown limit not the one a start would use (the default not said as such), or PostgreSQL not named as ignoring it."""
    (root / ".env").write_text("A=1\n")

    assert query_limit(None) == 0

    out = capsys.readouterr().out
    assert "Read-only query limit for chats: 90 s (the default: CHAT_QUERY_TIMEOUT_SECONDS is not set in .env)" in out and "PostgreSQL ignores it" in out
    assert docker.calls == []


def test_a_running_chat_gets_its_copies_started_again_with_the_new_value(root, docker, capsys):
    """Bug class: the copies left on the old limit (the change then waits for a restart nobody does), the new value not the one Compose is given, a copy not attached again under its alias (the gateway's listener then reaches nothing), or the gateway or the sandbox restarted, which cuts off the running turns."""
    running_chat(root, docker, (MONGO_ID, "mongo-mcp"), (STARROCKS_ID, "starrocks-mcp"))
    sandbox_before = docker.containers["sdp-sandbox"]

    assert query_limit("300") == 0

    mongo, starrocks = (db for db in host.DATABASES if db.chat_service)
    assert changes(docker) == [
        ["docker", "rm", "--force", MONGO_COPY],
        host.copy_arguments(str(root), mongo),
        ["docker", "network", "connect", "--alias", "sdp-chat-mongo-mcp", "sdp-chat-up", MONGO_COPY],
        ["docker", "rm", "--force", STARROCKS_COPY],
        host.copy_arguments(str(root), starrocks),
        ["docker", "network", "connect", "--alias", "sdp-chat-starrocks-mcp", "sdp-chat-up", STARROCKS_COPY],
    ]
    composed = [index for index, call in enumerate(docker.calls) if call[1] == "compose"]
    assert [docker.envs[index]["CHAT_QUERY_TIMEOUT_SECONDS"] for index in composed] == ["300", "300"]
    assert docker.containers["sdp-sandbox"] is sandbox_before and {entry["status"] for entry in docker.containers.values()} == {"running"}
    assert set(docker.networks) == {"sdp-chat", "sdp-chat-up"} and {MONGO_ID, STARROCKS_ID} <= set(docker.compose)
    out = capsys.readouterr().out
    assert "mongo: the chat's MCP server was started again with the new limit." in out and "starrocks: the chat's MCP server was started again" in out
    assert "keep their own client-side waits until python sdp.py chat stop, then python sdp.py chat start" in out


def test_only_the_copies_that_run_are_started_again(root, docker):
    """Bug class: a copy started for a database the running chat never attached (an MCP server switched off or stopped since), which the gateway has no listener for."""
    running_chat(root, docker, (MONGO_ID, "mongo-mcp"))

    assert query_limit("60") == 0

    assert [call[-1] for call in changes(docker)] == [MONGO_COPY, "chat-mongo-mcp", MONGO_COPY]


def test_a_copy_that_does_not_start_again_is_named_with_the_recovery(root, docker, capsys):
    """Bug class: a failed restart reported as done (the chat then has no way to that database and nobody knows), the other copy skipped because of it, or the written value rolled back."""
    running_chat(root, docker, (MONGO_ID, "mongo-mcp"), (STARROCKS_ID, "starrocks-mcp"))
    docker.fail = lambda arguments: "container sdp-chat-starrocks-mcp-1 exited (1)" if arguments[1] == "compose" and arguments[-1] == "chat-starrocks-mcp" else None

    assert query_limit("200") == 1

    out = capsys.readouterr().out
    assert "Error: the chat's copy of the MCP server of starrocks (chat-starrocks-mcp) did not start: container sdp-chat-starrocks-mcp-1 exited (1). The chat cannot read starrocks until it is started again: python sdp.py chat stop, then python sdp.py chat start" in out
    assert "mongo: the chat's MCP server was started again with the new limit." in out
    assert host.query_seconds(str(root)) == 200


def test_an_image_without_the_label_is_refused_before_any_copy_is_removed(root, docker, capsys):
    """Bug class: a working copy removed and then not started again because its image would ignore the limit, which leaves the chat without that database."""
    running_chat(root, docker, (MONGO_ID, "mongo-mcp"), (STARROCKS_ID, "starrocks-mcp"))
    docker.labels["sdp-starrocks-mcp"] = ""

    assert query_limit("200") == 1

    assert changes(docker) == []
    out = capsys.readouterr().out
    assert "the image sdp-starrocks-mcp of the MCP server of starrocks was built before the query time limit" in out and "takes effect on the chat's databases at the next start" in out
