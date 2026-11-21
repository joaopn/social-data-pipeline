"""Tests for `webui.serve`, the container's entry point, without uvicorn: a stand-in `uvicorn` module is put into `sys.modules` for the test and the module's sockets are stand-ins too, so that what `main()` hands over can be looked at.

The contracts pinned here: the server's settings exactly (the certificate and key of the mounted folder, h11 only, no WebSocket, one process, no `Server` header, no forwarded headers believed, no access log of uvicorn's, the limit of connections, the keep-alive time); two IPv4 sockets bound to every address on 8443 and 8444, in that order, handed to one server; the application served is the guarded one `from_environment` builds; a setting it refuses ends the process before any socket is bound.
"""

from __future__ import annotations

import importlib
import sys
import types

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("starlette")
pytest.importorskip("jinja2")

from social_data_pipeline.webui import guard

ENVIRONMENT = {
    "SDP_WEBUI_HOSTS": "localhost,127.0.0.1,pipeline.example",
    "SDP_WEBUI_PORT": "9443",
    "SDP_WEBUI_SOCKET": "/run/sdp/broker.sock",
    "SDP_WEBUI_GATEWAY": "172.30.0.1",
}
EXPECTED_SETTINGS = {
    "ssl_certfile": "/tls/cert.pem",
    "ssl_keyfile": "/tls/key.pem",
    "http": "h11",
    "ws": "none",
    "workers": 1,
    "server_header": False,
    "proxy_headers": False,
    "access_log": False,
    "limit_concurrency": 256,
    "timeout_keep_alive": 5,
}


class StandInSocket:
    made = []

    def __init__(self, family, kind):
        self.family, self.kind = family, kind
        self.options = []
        self.bound = None
        StandInSocket.made.append(self)

    def setsockopt(self, level, name, value):
        self.options.append((level, name, value))

    def bind(self, address):
        self.bound = address


def serve_with_stand_in(monkeypatch):
    """Import `webui.serve` against a stand-in uvicorn and return (`run(environment)`, the module): `run` runs `main()` and returns what it handed over (the configs, the servers' runs, the sockets made). Everything goes through `monkeypatch`, so that its undo leaves neither `sys.modules` nor the package's attribute `serve` holding the module tied to the stand-in."""
    import social_data_pipeline.webui as package

    configs, runs = [], []

    class Config:
        def __init__(self, app, **settings):
            configs.append((app, settings))

    class Server:
        def __init__(self, config):
            self.config = config

        def run(self, sockets=None):
            runs.append((self.config, list(sockets) if sockets is not None else None))

    stand_in = types.ModuleType("uvicorn")
    stand_in.Config = Config
    stand_in.Server = Server
    monkeypatch.setitem(sys.modules, "uvicorn", stand_in)
    # `delitem` notes nothing for a key that is missing, so the entry the import makes would outlive the test: the key is first noted with `setitem` (undone last: back to missing, or to the module that was there) and then removed.
    monkeypatch.setitem(sys.modules, "social_data_pipeline.webui.serve", sys.modules.get("social_data_pipeline.webui.serve"))
    monkeypatch.delitem(sys.modules, "social_data_pipeline.webui.serve")
    # The import sets the package's attribute `serve`; noted here so that the undo removes it again (or puts back what was there).
    monkeypatch.setattr(package, "serve", None, raising=False)
    serve = importlib.import_module("social_data_pipeline.webui.serve")
    socket_module = types.SimpleNamespace(socket=StandInSocket, AF_INET=2, SOCK_STREAM=1, SOL_SOCKET=1, SO_REUSEADDR=2)
    monkeypatch.setattr(serve, "socket", socket_module)
    StandInSocket.made = []

    def run(environment):
        for name in ENVIRONMENT:
            monkeypatch.delenv(name, raising=False)
        for name, value in environment.items():
            monkeypatch.setenv(name, value)
        serve.main()
        return configs, runs, StandInSocket.made

    return run, serve


@pytest.fixture
def served(monkeypatch):
    return serve_with_stand_in(monkeypatch)[0]


def test_the_stand_in_leaves_nothing_behind():
    """Bug class: the module imported against the stand-in uvicorn left in `sys.modules` or as the package's attribute after the test, so that a later `from social_data_pipeline.webui import serve` gets a module whose uvicorn is a stand-in."""
    import social_data_pipeline.webui as package

    before_module = sys.modules.get("social_data_pipeline.webui.serve")
    before_attribute = getattr(package, "serve", None)
    with pytest.MonkeyPatch.context() as patch:
        _, tied = serve_with_stand_in(patch)
        assert sys.modules["social_data_pipeline.webui.serve"] is tied and package.serve is tied
    assert sys.modules.get("social_data_pipeline.webui.serve") is before_module is not tied
    assert getattr(package, "serve", None) is before_attribute
    assert getattr(package, "serve", None) is not tied


def test_the_server_gets_exactly_its_settings(served):
    """Bug class: a setting of the server dropped or changed (forwarded headers believed, which would let a client name its own address; uvicorn's access log with client addresses and query strings; a WebSocket library; no limit of connections; a `Server` header; the certificate of another folder)."""
    configs, runs, _ = served(ENVIRONMENT)
    assert len(configs) == 1
    assert configs[0][1] == EXPECTED_SETTINGS


def test_two_sockets_on_8443_and_8444_go_to_one_server(served):
    """Bug class: one port only (every login local, or none), the ports swapped, a socket bound to the loopback of the container only (Docker's published ports would reach nothing), or two servers with two applications (two tables of sessions)."""
    configs, runs, sockets = served(ENVIRONMENT)
    assert [(made.family, made.kind, made.bound) for made in sockets] == [(2, 1, ("0.0.0.0", 8443)), (2, 1, ("0.0.0.0", 8444))]
    assert len(runs) == 1 and runs[0][1] == sockets


def test_the_application_is_the_guarded_one_from_the_environment(served):
    """Bug class: the FastAPI application served without its guard (no Host, Origin, body or form-token checks, no security headers), or one built from other settings than the container's."""
    configs, _, _ = served(ENVIRONMENT)
    application = configs[0][0]
    assert isinstance(application, guard.Guard)
    assert application.allowed_hosts == {"localhost:9443", "127.0.0.1:9443", "pipeline.example:9443"}


def test_a_setting_it_refuses_ends_it_before_any_socket(served):
    """Bug class: a server that binds its ports (and is reported running) with a setting it cannot use."""
    broken = {**ENVIRONMENT, "SDP_WEBUI_GATEWAY": "not-an-address"}
    with pytest.raises(SystemExit) as caught:
        served(broken)
    assert "SDP_WEBUI_GATEWAY" in str(caught.value) and "not-an-address" not in str(caught.value)
    assert StandInSocket.made == []
