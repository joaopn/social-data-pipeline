"""The gateway's config, its credential file and its command line.

Bug classes: a config that is wrong or unknown starting a gateway anyway (a wildcard bind, an `http://` model upstream, an unknown key ignored); the credential leaking through an error text; the package pulling in anything beyond the standard library.
"""

import copy
import json
import os
import selectors
import signal
import socket
import subprocess
import sys
from pathlib import Path

import pytest
from gateway_helpers import CRED, WAIT

from social_data_pipeline.gateway import table
from social_data_pipeline.gateway.server import Gateway
from social_data_pipeline.gateway.table import ConfigError, Upstream

REPO = Path(__file__).resolve().parents[2]
GOOD = {
    "listen": "127.0.0.1",
    "credential_file": "/run/secrets/credential",
    "limits": {"head_timeout": 5, "requests_per_minute": 30},
    "listeners": [
        {"port": 8001, "kind": "model-anthropic", "beta_allowed": ["b1"], "tool_types_allowed": ["bash_20250124"]},
        {"port": 8002, "kind": "model-openai", "upstream": "https://openrouter.ai/api/v1", "model": "vendor/m-1"},
        {"port": 8003, "kind": "mcp", "upstream": "http://mcp-upstream:8000", "paths": ["/sse", "/messages/"]},
    ],
}


def test_a_good_config_parses():
    t = table.parse(GOOD)
    anthropic, openai, mcp = t.listeners
    assert (t.listen, t.limits.head_timeout, t.limits.requests_per_minute, t.limits.max_connections) == ("127.0.0.1", 5.0, 30, 32)
    assert anthropic.upstream == table.ANTHROPIC_UPSTREAM and anthropic.beta_allowed == {"b1"}
    assert openai.upstream == Upstream("https", "openrouter.ai", 443, "/api/v1") and openai.model == "vendor/m-1"
    assert mcp.upstream == Upstream("http", "mcp-upstream", 8000) and mcp.paths == ("/sse", "/messages/")


def changed(path, value):
    doc = copy.deepcopy(GOOD)
    *parents, last = path
    node = doc
    for key in parents:
        node = node[key]
    if value is KeyError:
        del node[last]
    else:
        node[last] = value
    return doc


@pytest.mark.parametrize("path,value,key", [
    (["listen"], "0.0.0.0", "listen"),
    (["listen"], "::", "listen"),
    (["listen"], "", "listen"),
    (["listen"], "gateway", "listen"),
    (["listen"], KeyError, "listen"),
    (["extra"], 1, "extra"),
    (["credential_file"], KeyError, "credential_file"),
    (["credential_file"], 5, "credential_file"),
    (["limits", "head_timeout"], 0, "limits.head_timeout"),
    (["limits", "max_connections"], True, "limits.max_connections"),
    (["limits", "max_connections"], 1.5, "limits.max_connections"),
    (["limits", "other"], 1, "limits.other"),
    (["listeners"], [], "listeners"),
    (["listeners", 2, "port"], 8001, "listeners"),
    (["listeners", 0, "port"], 0, "listeners[0].port"),
    (["listeners", 0, "port"], 65536, "listeners[0].port"),
    (["listeners", 0, "port"], "8001", "listeners[0].port"),
    (["listeners", 0, "port"], True, "listeners[0].port"),
    (["listeners", 0, "kind"], "proxy", "listeners[0].kind"),
    (["listeners", 0, "upstream"], "http://127.0.0.1:1", "listeners[0].upstream"),
    (["listeners", 0, "beta_allowed"], KeyError, "listeners[0].beta_allowed"),
    (["listeners", 0, "beta_allowed"], ["a,b"], "listeners[0].beta_allowed"),
    (["listeners", 1, "upstream"], "http://openrouter.ai/api/v1", "listeners[1].upstream"),
    (["listeners", 1, "upstream"], "https://user@openrouter.ai/api/v1", "listeners[1].upstream"),
    (["listeners", 1, "upstream"], "https://openrouter.ai/api/../v1", "listeners[1].upstream"),
    (["listeners", 1, "upstream"], "https://openrouter.ai/api?x=1", "listeners[1].upstream"),
    (["listeners", 1, "upstream"], "https://openrouter.ai:0/api", "listeners[1].upstream"),
    (["listeners", 1, "model"], "", "listeners[1].model"),
    (["listeners", 1, "paths"], ["/x"], "listeners[1].paths"),
    (["listeners", 2, "upstream"], "https://mcp-upstream:8000", "listeners[2].upstream"),
    (["listeners", 2, "upstream"], "http://mcp-upstream:8000/sse", "listeners[2].upstream"),
    (["listeners", 2, "paths"], [], "listeners[2].paths"),
    (["listeners", 2, "paths"], ["/a/../b"], "listeners[2].paths"),
    (["listeners", 2, "paths"], ["sse"], "listeners[2].paths"),
    (["listeners", 2, "paths"], ["//sse"], "listeners[2].paths"),
])
def test_a_wrong_config_refuses_naming_the_key(path, value, key):
    with pytest.raises(ConfigError) as err:
        table.parse(changed(path, value))
    assert str(err.value).startswith(key + ":")


def test_the_server_refuses_an_http_model_upstream_without_the_test_argument():
    t = table.Table("127.0.0.1", (table.Listener(1, table.MODEL_OPENAI, Upstream("http", "127.0.0.1", 1), model="m"),), "x")
    with pytest.raises(ConfigError, match=r"^listeners\[0\]\.upstream:"):
        Gateway(t, CRED)
    with pytest.raises(ValueError):
        Gateway(t, CRED, test_upstream=Upstream("http", "upstream-host", 1))


@pytest.mark.parametrize("make", ["missing", "directory", "link", "fifo", "empty", "binary", "two words"])
def test_a_credential_file_that_is_not_one_regular_value_refuses(tmp_path, make):
    path = tmp_path / "cred"
    real = tmp_path / "real"
    real.write_text(CRED)
    if make == "directory":
        path.mkdir()
    elif make == "link":
        path.symlink_to(real)
    elif make == "fifo":
        os.mkfifo(path)
    elif make != "missing":
        path.write_bytes({"empty": b" \n", "binary": CRED.encode() + b"\xff", "two words": CRED.encode() + b" " + CRED.encode()}[make])
    with pytest.raises(ConfigError) as err:
        table.read_credential(str(path))
    assert str(err.value).startswith("credential_file:") and CRED not in str(err.value) and err.value.__cause__ is None


def test_a_credential_file_is_read_stripped(tmp_path):
    (tmp_path / "cred").write_text(f"  {CRED}\n")
    assert table.read_credential(str(tmp_path / "cred")) == CRED


def run_main(config_path):
    return subprocess.Popen([sys.executable, "-m", "social_data_pipeline.gateway", str(config_path)], cwd=REPO, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def test_main_serves_until_sigterm_and_refuses_a_bad_config(tmp_path):
    (tmp_path / "cred").write_text(CRED)
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    doc = {"listen": "127.0.0.1", "credential_file": str(tmp_path / "cred"), "listeners": [{"port": port, "kind": "model-anthropic", "beta_allowed": []}]}
    (tmp_path / "ok.json").write_text(json.dumps(doc))
    proc = run_main(tmp_path / "ok.json")
    try:
        with selectors.DefaultSelector() as sel:
            sel.register(proc.stdout, selectors.EVENT_READ)
            assert sel.select(WAIT), "no start-up line"
            assert proc.stdout.readline() == f"gateway: model-anthropic listening on port {port}\n"
        busy = run_main(tmp_path / "ok.json")
        out, err = busy.communicate(timeout=WAIT)
        assert busy.returncode == 2 and f"listeners[0].port: cannot bind port {port}" in err
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=WAIT)
        assert proc.returncode == 0 and CRED not in out + err
    finally:
        proc.kill()
        proc.wait(timeout=WAIT)
    (tmp_path / "bad.json").write_text(json.dumps({**doc, "listen": "0.0.0.0"}))
    bad = run_main(tmp_path / "bad.json")
    out, err = bad.communicate(timeout=WAIT)
    assert bad.returncode == 2 and "listen: must be one IP address" in err


def test_the_package_imports_only_the_standard_library():
    code = "import sys, social_data_pipeline.gateway.__main__; print(' '.join(sorted(sys.modules)))"
    loaded = subprocess.run([sys.executable, "-S", "-c", code], cwd=REPO, capture_output=True, text=True, check=True, timeout=WAIT).stdout.split()
    ours = {m for m in loaded if m.split(".")[0] == "social_data_pipeline"}
    assert ours == {"social_data_pipeline", "social_data_pipeline.gateway", "social_data_pipeline.gateway.__main__", "social_data_pipeline.gateway.server", "social_data_pipeline.gateway.rules", "social_data_pipeline.gateway.table"}
    assert {m.split(".")[0] for m in loaded} - {"social_data_pipeline"} <= set(sys.stdlib_module_names) | {"__main__"}
