"""The `secret_file` of an `mcp` listener: the loader, the reading of the file, and the `x-sdp-assistant` header the gateway adds upstream.

Bug classes: the secret sent on another listener (to a database's MCP server) or not at all; a client's own `x-sdp-assistant` reaching an upstream (anything in the sandbox could then claim to be the gateway); the file read through a link or as something other than one hex value; the secret in a log line or an error text.
"""

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
from gateway_helpers import CRED, WAIT, StandIn, exchange, parse, request

from social_data_pipeline.gateway import table
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import ANTHROPIC_UPSTREAM, MCP, MODEL_ANTHROPIC, ConfigError, Limits, Listener, Table, Upstream

REPO = Path(__file__).resolve().parents[2]
SECRET = "0123456789abcdef" * 4
CLIENT = "client-sent-value-4c1d"
MCP_DOC = {"port": 8004, "kind": "mcp", "upstream": "http://sdp-chat-jobs:8050", "paths": ["/assistant/mcp", "/assistant/results/"], "secret_file": "/run/sdp-gateway/assistant.secret"}


def config(*listeners):
    return {"listen": "127.0.0.1", "credential_file": "/run/c", "listeners": [{"port": 8080, "kind": "model-anthropic", "beta_allowed": [], "models": ["m"]}, *listeners]}


def test_secret_file_is_taken_on_an_mcp_listener():
    parsed = table.parse(config(MCP_DOC, {"port": 8002, "kind": "mcp", "upstream": "http://m:3000", "paths": ["/mcp"]}))
    assert parsed.listeners[1].secret_file == "/run/sdp-gateway/assistant.secret" and parsed.listeners[2].secret_file == ""


@pytest.mark.parametrize("listener,key", [
    ({"port": 8001, "kind": "model-anthropic", "beta_allowed": [], "models": ["m"], "secret_file": "/s"}, "listeners[1].secret_file: unknown key"),
    ({"port": 8001, "kind": "model-openai", "upstream": "https://h/v1", "model": "m", "secret_file": "/s"}, "listeners[1].secret_file: unknown key"),
    ({**MCP_DOC, "secret_file": "relative/path"}, "listeners[1].secret_file: must be an absolute path"),
    ({**MCP_DOC, "secret_file": ""}, "listeners[1].secret_file: must be an absolute path"),
    ({**MCP_DOC, "secret_file": 5}, "listeners[1].secret_file: must be an absolute path"),
])
def test_secret_file_is_refused_on_other_kinds_and_in_other_shapes(listener, key):
    with pytest.raises(ConfigError) as err:
        table.parse(config(listener))
    assert str(err.value) == key


@pytest.mark.parametrize("make", ["missing", "link", "fifo", "directory", "short", "uppercase", "not hex", "two values", "too large"])
def test_a_secret_file_that_is_not_one_hex_value_refuses_without_its_content(tmp_path, make):
    path = tmp_path / "secret"
    real = tmp_path / "real"
    real.write_text(SECRET)
    if make == "link":
        path.symlink_to(real)
    elif make == "fifo":
        os.mkfifo(path)
    elif make == "directory":
        path.mkdir()
    elif make != "missing":
        path.write_text({"short": SECRET[:62], "uppercase": SECRET.upper(), "not hex": SECRET[:-1] + "g", "two values": SECRET + " " + SECRET, "too large": SECRET + "\n" * 200}[make])
    with pytest.raises(ConfigError) as err:
        table.read_secret(str(path), "listeners[1].secret_file")
    assert str(err.value).startswith("listeners[1].secret_file:") and SECRET[:16] not in str(err.value).lower() and err.value.__cause__ is None


def test_a_secret_file_is_read_stripped(tmp_path):
    (tmp_path / "secret").write_text(f"  {SECRET}\n")
    assert table.read_secret(str(tmp_path / "secret"), "k") == SECRET


def test_a_listener_with_a_secret_file_needs_its_secret_and_only_an_mcp_listener_takes_one():
    mcp = Listener(0, MCP, Upstream("http", "127.0.0.1", 1), paths=("/mcp",), secret_file="/s")
    with pytest.raises(ConfigError, match=r"^listeners\[0\]\.secret_file:"):
        Gateway(Table("127.0.0.1", (mcp,)), "")
    model = Listener(0, MODEL_ANTHROPIC, ANTHROPIC_UPSTREAM)
    with pytest.raises(ConfigError, match=r"^listeners\[0\]\.secret_file:"):
        Gateway(Table("127.0.0.1", (model,), "c"), CRED, secrets=[SECRET])


def test_the_header_goes_only_to_the_listener_with_the_secret_and_a_clients_never(tmp_path):
    ups = [StandIn() for _ in range(3)]
    listeners = (
        Listener(0, MCP, Upstream("http", "127.0.0.1", ups[0].port), paths=("/assistant/mcp", "/assistant/results/"), secret_file="/run/s"),
        Listener(0, MCP, Upstream("http", "127.0.0.1", ups[1].port), paths=("/mcp",)),
        Listener(0, MODEL_ANTHROPIC, ANTHROPIC_UPSTREAM, models=frozenset({"x"})),
    )
    log = []
    gateway = Gateway(Table("127.0.0.1", listeners, "c", Limits()), CRED, secrets=[SECRET, "", ""], log=log.append, test_upstream=Upstream("http", "127.0.0.1", ups[2].port))
    gateway.start()
    try:
        sent = [("x-sdp-assistant", CLIENT), ("X-SDP-Assistant", CLIENT)]
        assert parse(exchange(gateway.ports[0], request("/assistant/mcp", headers=sent, body=b"{}")))[0] == 200
        assert parse(exchange(gateway.ports[0], request("/assistant/results/pg_0a0a0a0a/0", method="GET", headers=sent, body=b"")))[0] == 200
        assert parse(exchange(gateway.ports[1], request("/mcp", headers=sent, body=b"{}")))[0] == 200
        assert parse(exchange(gateway.ports[2], request("/v1/messages", headers=sent)))[0] == 200
    finally:
        gateway.stop()
        for up in ups:
            up.close()
    for rec in ups[0].records:
        assert [v for n, v in rec.headers if n == "x-sdp-assistant"] == [SECRET]
    assert len(ups[0].records) == 2
    for rec in ups[1].records + ups[2].records:
        assert "x-sdp-assistant" not in [n for n, _ in rec.headers] and SECRET.encode() not in rec.raw
    assert all(CLIENT.encode() not in rec.raw for up in ups for rec in up.records)
    assert len(log) == 4 and not any(SECRET in line for line in log)


def test_main_reads_the_secret_and_refuses_a_bad_one_without_its_content(tmp_path):
    (tmp_path / "secret").write_text(SECRET[:-2] + "zz")
    doc = {"listen": "127.0.0.1", "listeners": [{**MCP_DOC, "port": 1, "upstream": "http://127.0.0.1:2", "secret_file": str(tmp_path / "secret")}]}
    (tmp_path / "bad.json").write_text(json.dumps(doc))
    bad = subprocess.run([sys.executable, "-m", "social_data_pipeline.gateway", str(tmp_path / "bad.json")], cwd=REPO, capture_output=True, text=True, timeout=WAIT)
    assert bad.returncode == 2 and "listeners[0].secret_file: must hold one value" in bad.stderr and SECRET[:16] not in bad.stdout + bad.stderr
    (tmp_path / "secret").write_text(SECRET)
    up = StandIn()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    doc["listeners"][0].update(port=port, upstream=f"http://127.0.0.1:{up.port}")
    (tmp_path / "ok.json").write_text(json.dumps(doc))
    proc = subprocess.Popen([sys.executable, "-m", "social_data_pipeline.gateway", str(tmp_path / "ok.json")], cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert proc.stdout.readline() == f"gateway: mcp listening on port {port}\n"
        assert parse(exchange(port, request("/assistant/mcp", body=b"{}")))[0] == 200
        deadline = time.monotonic() + WAIT
        while not up.records and time.monotonic() < deadline:
            time.sleep(0.01)
        assert [v for n, v in up.records[0].headers if n == "x-sdp-assistant"] == [SECRET]
        proc.terminate()
        out, err = proc.communicate(timeout=WAIT)
        assert SECRET not in out + err
    finally:
        proc.kill()
        proc.wait(timeout=WAIT)
        up.close()
