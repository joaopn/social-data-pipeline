"""The gateway factory: a gateway with one listener in front of a stand-in upstream, both stopped at the end."""

import time
from types import SimpleNamespace

import pytest
from gateway_helpers import CRED, StandIn, ok_json

from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import ANTHROPIC_UPSTREAM, MCP, MODEL_ANTHROPIC, MODEL_OPENAI, Limits, Listener, Table, Upstream


@pytest.fixture
def gw():
    """`gw(kind, respond=..., limits=..., clock=..., **listener fields)` starts a gateway with one listener in front of a stand-in; both are stopped at the end, and no log line may hold the credential."""
    started = []

    def make(kind=MODEL_ANTHROPIC, respond=ok_json, limits=None, clock=time.monotonic, **fields):
        up = StandIn(respond)
        local = Upstream("http", "127.0.0.1", up.port)
        if kind == MODEL_ANTHROPIC:
            fields = {"beta_allowed": frozenset({"b1", "b2"}), "tool_types_allowed": frozenset({"bash_20250124"}), **fields}
            lst = Listener(0, kind, ANTHROPIC_UPSTREAM, **fields)
        elif kind == MODEL_OPENAI:
            lst = Listener(0, kind, Upstream("http", "127.0.0.1", up.port, "/api/v1"), **{"model": "m-1", **fields})
        else:
            lst = Listener(0, MCP, local, **{"paths": ("/mcp", "/messages/"), **fields})
        log = []
        gateway = Gateway(Table("127.0.0.1", (lst,), "unused", limits or Limits()), CRED, log=log.append, clock=clock, test_upstream=local)
        gateway.start()
        run = SimpleNamespace(port=gateway.ports[0], up=up, log=log, gateway=gateway)
        started.append(run)
        return run

    yield make
    for run in started:
        run.gateway.stop()
        run.up.close()
        assert not any(CRED in line for line in run.log)
