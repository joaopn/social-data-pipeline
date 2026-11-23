"""The gateway's allow-list of models: a model listener of either kind forwards only a body that names a model of its table, and the table of `sdp chat start` holds the model ids the pinned Claude Code sends for the runner's allow-list.

Bug classes: a chat (or code in the sandbox) spending the operator's credential on a model outside the list; a table that takes no list (every model forwarded) or one the gateway refuses at its start.
"""

import copy
import json

import pytest
from gateway_helpers import exchange, parse, request

from social_data_pipeline import chat_host
from social_data_pipeline.chat import claude
from social_data_pipeline.gateway import table
from social_data_pipeline.gateway.table import MODEL_ANTHROPIC, MODEL_OPENAI, ConfigError

ANTHROPIC = {"listen": "127.0.0.1", "credential_file": "/run/c", "listeners": [{"port": 8080, "kind": "model-anthropic", "beta_allowed": [], "models": ["claude-opus-5-5"]}]}


@pytest.mark.parametrize("kind, model", [
    (MODEL_ANTHROPIC, "claude-opus-4-1"), (MODEL_ANTHROPIC, "opus"), (MODEL_ANTHROPIC, "X"), (MODEL_ANTHROPIC, None), (MODEL_ANTHROPIC, 5), (MODEL_ANTHROPIC, ["x"]),
    (MODEL_OPENAI, "m-2"), (MODEL_OPENAI, None), (MODEL_OPENAI, ["m-1"]),
])
def test_a_body_naming_a_model_outside_the_list_is_refused_and_never_forwarded(gw, kind, model):
    g = gw(kind)
    body = json.dumps({"model": model, "max_tokens": 1}).encode()
    status, _, answer = parse(exchange(g.port, request("/chat/completions" if kind == MODEL_OPENAI else "/v1/messages", body=body)))
    assert (status, json.loads(answer)) == (403, {"error": "model"})
    assert g.up.records == []


@pytest.mark.parametrize("target", ["/v1/messages", "/v1/messages/count_tokens?beta=true"])
def test_a_body_without_a_model_is_refused_on_the_anthropic_listener(gw, target):
    g = gw(MODEL_ANTHROPIC)
    assert parse(exchange(g.port, request(target, body=b'{"max_tokens":1}')))[0] == 403
    assert g.up.records == []


def test_each_model_of_the_list_is_forwarded(gw):
    g = gw(MODEL_ANTHROPIC, models=frozenset({"claude-opus-5-5", "claude-haiku-5-5"}))
    for model in ("claude-opus-5-5", "claude-haiku-5-5"):
        assert parse(exchange(g.port, request(body=json.dumps({"model": model}).encode())))[0] == 200
    assert len(g.up.records) == 2


@pytest.mark.parametrize("value", [table.ConfigError, [], ["a b"], "claude-opus-5-5", [5]])
def test_an_anthropic_listener_needs_a_list_of_models(value):
    doc = copy.deepcopy(ANTHROPIC)
    if value is table.ConfigError:
        del doc["listeners"][0]["models"]
    else:
        doc["listeners"][0]["models"] = value
    with pytest.raises(ConfigError, match=r"^listeners\[0\]\.models:"):
        table.parse(doc)
    assert table.parse(ANTHROPIC).listeners[0].models == frozenset({"claude-opus-5-5"})


def test_chat_start_gives_the_gateway_the_ids_of_the_runners_allow_list():
    """The gateway's list and the runner's are one list: each alias the runner takes has its measured id, and nothing else is forwarded."""
    config = chat_host.gateway_config("10.201.0.2", {"backend": "claude-code"}, [])
    assert config["listeners"][0]["models"] == ["claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"]
    assert tuple(chat_host.CLAUDE_MODEL_IDS) == claude.MODELS == chat_host.CLAUDE_MODELS
    assert table.parse(config).listeners[0].models == frozenset(chat_host.CLAUDE_MODEL_IDS.values())
