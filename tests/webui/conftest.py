"""The fixtures of the web UI's tests: the stand-in broker, the stand-in scheduler's control socket, the clock, the settings, the results folder, and the builders of the application and of its clients.

Nothing here imports FastAPI, Starlette or Jinja2 at load time: the tests of the pure modules (`config`, `words`, `cleaning`, `limiter`, `sessions`) run without them, and each route test module skips itself through `pytest.importorskip` where they are missing.
"""

from __future__ import annotations

import pytest
from webui_helpers import GATEWAY, HOST, HOSTS, LISTEN_PORT, Clock, StandInBroker, StandInDoor, good_lstat


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
            assert call[2] in actions.TABLE and not actions.TABLE[call[2]].op and actions.TABLE[call[2]].door is None, f"the broker received {call[2]!r}, which is not a verb of the table"


@pytest.fixture
def door(broker):
    """The stand-in scheduler's control socket, sharing the broker's timeline. After the test, every request it received is checked against the web UI's table (property i): an operation of a `queue.*` entry with exactly that entry's fields."""
    from social_data_pipeline.webui import actions

    made = StandInDoor(broker.timeline)
    yield made
    entries = {action.door: action for action in actions.TABLE.values() if action.door is not None}
    for request in made.requests:
        action = entries.get(request.get("op"))
        assert action is not None, f"the scheduler received {request.get('op')!r}, which is not in the table"
        assert set(request) - {"op"} == set(action.inputs), f"the scheduler received other fields than the table's for {action.name}"
        actions.check_fields(action, {key: value for key, value in request.items() if key != "op"})


@pytest.fixture
def results(tmp_path):
    """The results folder the web UI reads the parts from (its read-only mount in the container)."""
    folder = tmp_path / "results"
    folder.mkdir()
    return folder


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def settings():
    from social_data_pipeline.webui import config

    return config.Settings(hosts=HOSTS, port=8443, socket_path="/run/sdp/broker.sock", gateway=GATEWAY)


@pytest.fixture
def make_scheduler(door, results):
    """The way to the stand-in scheduler: `make_scheduler(socket_path=..., results=..., lstat=..., uid=...)`; by default the control folder and its socket look right (a folder 0700 and a socket 0600, of uid 1000)."""
    from social_data_pipeline.webui import scheduler

    def make(socket_path="/run/sdp-jobs/door.sock", results=str(results), lstat=good_lstat, uid=lambda: 1000):
        return scheduler.Scheduler(socket_path, results, connect=door.connect, lstat=lstat, uid=uid)

    return make


@pytest.fixture
def make_app(broker, clock, settings, make_scheduler):
    """Build the application with the stand-in broker and the stand-in scheduler: `make_app(**limits)`, `settings=` to replace the settings, `scheduler=` the way to the scheduler."""
    from social_data_pipeline.webui import app as webapp

    def make(settings=settings, scheduler=None, **limits):
        return webapp.create_app(settings, broker=broker.factory, scheduler=scheduler if scheduler is not None else make_scheduler(), clock=clock, limits=webapp.Limits(**limits))

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
