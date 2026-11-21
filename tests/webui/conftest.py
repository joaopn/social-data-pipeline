"""The fixtures of the web UI's tests: the stand-in broker, the clock, the settings, and the builders of the application and of its clients.

Nothing here imports FastAPI, Starlette or Jinja2 at load time: the tests of the pure modules (`config`, `words`, `cleaning`, `limiter`, `sessions`) run without them, and each route test module skips itself through `pytest.importorskip` where they are missing.
"""

from __future__ import annotations

import pytest
from webui_helpers import GATEWAY, HOST, HOSTS, LISTEN_PORT, Clock, StandInBroker


@pytest.fixture
def broker():
    """The stand-in broker. After the test, every call it received is checked against the web UI's table (property a): no route of any test may have made the broker receive a verb, an operation or a field outside it."""
    from social_data_pipeline.webui import actions

    made = StandInBroker()
    yield made
    for request in made.requests:
        action = actions.TABLE.get(request["verb"])
        assert action is not None, f"the broker received {request['verb']!r}, which is not in the table"
        assert set(request["fields"]) <= set(action.inputs), f"the broker received a field outside the table for {request['verb']}"
    for call in made.calls:
        if call[0] == "call":
            assert call[2] in actions.TABLE and not actions.TABLE[call[2]].op, f"the broker received {call[2]!r}, which is not a verb of the table"


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def settings():
    from social_data_pipeline.webui import config

    return config.Settings(hosts=HOSTS, port=8443, socket_path="/run/sdp/broker.sock", gateway=GATEWAY)


@pytest.fixture
def make_app(broker, clock, settings):
    """Build the application with the stand-in broker: `make_app(**limits)`, and `settings=` to replace the settings."""
    from social_data_pipeline.webui import app as webapp

    def make(settings=settings, **limits):
        return webapp.create_app(settings, broker=broker.factory, clock=clock, limits=webapp.Limits(**limits))

    return make


@pytest.fixture
def make_client():
    """A client of an application: `make_client(app, address=GATEWAY, server_port=LISTEN_PORT)`. It does not follow redirects; its requests carry `Host: localhost:8443` (the published port) and arrive on the container port `server_port` (8443, or 8444 for the local port), whatever the client's address. `base_url` replaces both."""
    from starlette.testclient import TestClient

    made = []

    def make(app, address=GATEWAY, server_port=LISTEN_PORT, base_url=None):
        if base_url is None:
            client = TestClient(app, base_url=f"https://localhost:{server_port}", headers={"host": HOST}, client=(address, 50000), follow_redirects=False)
        else:
            client = TestClient(app, base_url=base_url, client=(address, 50000), follow_redirects=False)
        made.append(client)
        return client

    yield make
    for client in made:
        client.close()
