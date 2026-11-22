"""Tests for the web UI's pure parts: `config` (the settings from the environment and the rule for host names), `limiter` (failed logins per address, with the clock moved by the test), `sessions` (the browser sessions and their one-use form tokens), and the import boundary of the package (property m).

None of these needs FastAPI, except the import test, which skips without it.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from webui_helpers import Clock

from social_data_pipeline.webui import config, limiter, overview
from social_data_pipeline.webui.sessions import WebSessions

REPO_ROOT = Path(__file__).resolve().parents[2]
GOOD_ENVIRONMENT = {
    "SDP_WEBUI_HOSTS": "localhost,127.0.0.1,[2001:db8::5],pipeline.example",
    "SDP_WEBUI_PORT": "8443",
    "SDP_WEBUI_SOCKET": "/run/sdp/broker.sock",
    "SDP_WEBUI_GATEWAY": "172.30.0.1",
}


# ── config ──────────────────────────────────────────────────────────────────


def test_good_settings_are_read():
    """Bug class: settings read into the wrong fields, or the Host values built without the port."""
    settings = config.load_settings(GOOD_ENVIRONMENT)
    assert settings == config.Settings(hosts=("localhost", "127.0.0.1", "[2001:db8::5]", "pipeline.example"), port=8443, socket_path="/run/sdp/broker.sock", gateway="172.30.0.1")
    assert settings.allowed_hosts() == {"localhost:8443", "127.0.0.1:8443", "[2001:db8::5]:8443", "pipeline.example:8443"}


def test_the_default_https_port_may_be_left_out_only_when_it_is_the_port():
    """Bug class: a page on port 443 refused for the Host a browser sends there (without the port), or a page on another port accepting a Host without its port."""
    settings = config.load_settings({**GOOD_ENVIRONMENT, "SDP_WEBUI_PORT": "443"})
    assert {"localhost", "localhost:443", "[2001:db8::5]", "pipeline.example"} <= settings.allowed_hosts()
    assert "localhost" not in config.load_settings(GOOD_ENVIRONMENT).allowed_hosts()


def test_the_local_host_port_is_optional():
    """Bug class: today's settings (without it) refused or given extra hosts, or the local host port read into hosts accepted on every port."""
    assert config.load_settings(GOOD_ENVIRONMENT).local_host_port is None and config.load_settings(GOOD_ENVIRONMENT).local_hosts() == frozenset()
    settings = config.load_settings({**GOOD_ENVIRONMENT, "SDP_WEBUI_LOCAL_HOST_PORT": "8444"})
    assert settings.local_host_port == 8444 and settings.local_hosts() == {"127.0.0.1:8444", "localhost:8444"}
    assert settings.allowed_hosts() == config.load_settings(GOOD_ENVIRONMENT).allowed_hosts()


@pytest.mark.parametrize("name, value", [
    ("SDP_WEBUI_LOCAL_HOST_PORT", "0"), ("SDP_WEBUI_LOCAL_HOST_PORT", "65536"), ("SDP_WEBUI_LOCAL_HOST_PORT", "08444"), ("SDP_WEBUI_LOCAL_HOST_PORT", "8443"), ("SDP_WEBUI_LOCAL_HOST_PORT", "x"),
    ("SDP_WEBUI_HOSTS", None), ("SDP_WEBUI_HOSTS", ""), ("SDP_WEBUI_HOSTS", "localhost,,x"), ("SDP_WEBUI_HOSTS", "*.example"), ("SDP_WEBUI_HOSTS", "Localhost"),
    ("SDP_WEBUI_HOSTS", "2001:db8::5"), ("SDP_WEBUI_HOSTS", "[fe80::1%eth0]"), ("SDP_WEBUI_HOSTS", "localhost,localhost"), ("SDP_WEBUI_HOSTS", "010.0.0.1"),
    ("SDP_WEBUI_PORT", None), ("SDP_WEBUI_PORT", "0"), ("SDP_WEBUI_PORT", "65536"), ("SDP_WEBUI_PORT", "84 43"), ("SDP_WEBUI_PORT", "08443"), ("SDP_WEBUI_PORT", "\u0668\u0664"),
    ("SDP_WEBUI_SOCKET", None), ("SDP_WEBUI_SOCKET", "relative/broker.sock"), ("SDP_WEBUI_SOCKET", "/" + "s" * 200),
    ("SDP_WEBUI_GATEWAY", None), ("SDP_WEBUI_GATEWAY", "gateway"), ("SDP_WEBUI_GATEWAY", "172.030.0.1"), ("SDP_WEBUI_GATEWAY", "fd00::1"), ("SDP_WEBUI_GATEWAY", "127.0.0.1"), ("SDP_WEBUI_GATEWAY", "0.0.0.0"),
])
def test_a_missing_or_malformed_setting_is_refused_with_its_name(name, value):
    """Bug class: a web UI that starts with a setting it cannot use (every Host refused, a socket path that cannot be connected to, a gateway that is no IPv4 address, so that no login is ever local) instead of saying which one is wrong; a message that echoes the value."""
    environment = dict(GOOD_ENVIRONMENT)
    if value is None:
        del environment[name]
    else:
        environment[name] = value
    with pytest.raises(config.ConfigError) as caught:
        config.load_settings(environment)
    assert name in caught.value.message
    if value:
        assert value not in caught.value.message


@pytest.mark.parametrize("name", ["*", "*.example", "Pipeline.example", "pipeline.example.", "a..b", "-" * 64, "a" * 254, "under_score", "10.0.0.1", ""])
def test_the_host_name_rule_refuses_what_is_not_a_plain_lower_case_name(name):
    """Bug class: a wildcard or a malformed name accepted as a Host value or a certificate name."""
    assert config.host_name_problem(name) is not None


@pytest.mark.parametrize("name", ["localhost", "pipeline.example", "a-b.c-d.example", "x" * 63 + ".example", "host1"])
def test_the_host_name_rule_accepts_plain_names(name):
    """Bug class: a rule so strict that the names an operator really uses are refused."""
    assert config.host_name_problem(name) is None


def test_the_plain_name_pattern_is_the_registrys():
    """Bug class: the web UI's copy of the registry's name pattern drifting from it, so that a name the broker would refuse becomes a link (or the reverse)."""
    from social_data_pipeline.control import types

    assert overview.PLAIN_NAME == types.NAME_PATTERN


# ── limiter ─────────────────────────────────────────────────────────────────


def fail(table, address, times=1):
    for _ in range(times):
        attempt = table.begin(address)
        assert attempt is not None
        table.end(attempt, failed=True)


def test_the_sixth_failure_in_five_minutes_is_refused_and_another_address_is_not():
    """Bug class: a limit that never refuses, one that counts every address together (a stranger locks the operator out), or one that a success elsewhere clears."""
    clock = Clock()
    table = limiter.LoginLimiter(clock)
    fail(table, "192.0.2.7", 5)
    assert table.begin("192.0.2.7") is None
    success = table.begin("192.0.2.8")
    table.end(success, failed=False)
    assert table.begin("192.0.2.7") is None
    assert table.begin("192.0.2.9") is not None


def test_attempts_under_way_count_until_they_end():
    """Bug class: five logins sent at once all passing the check before any failure is counted, so that a burst gets more guesses than the limit."""
    table = limiter.LoginLimiter(Clock())
    pending = [table.begin("192.0.2.7") for _ in range(5)]
    assert all(pending) and table.begin("192.0.2.7") is None
    table.end(pending[0], failed=False)
    assert table.begin("192.0.2.7") is not None


def test_the_window_is_five_minutes():
    """Bug class: a limit that never lifts, or one that lifts early."""
    clock = Clock()
    table = limiter.LoginLimiter(clock)
    fail(table, "192.0.2.7", 5)
    clock.now += 299.9
    assert table.begin("192.0.2.7") is None
    clock.now += 0.1
    assert table.begin("192.0.2.7") is not None


def test_ipv6_is_counted_by_its_64_network():
    """Bug class: an IPv6 attacker taking a new address of its /64 for every guess."""
    table = limiter.LoginLimiter(Clock())
    fail(table, "2001:db8:1:2::1", 3)
    fail(table, "2001:db8:1:2:ffff:ffff:ffff:ffff", 2)
    assert table.begin("2001:db8:1:2::abcd") is None
    assert table.begin("2001:db8:1:3::1") is not None
    assert limiter.address_key("::ffff:192.0.2.7") == "192.0.2.7"


def test_the_table_is_bounded_and_the_oldest_address_leaves():
    """Bug class: a table that grows with every address that ever tried (memory without bound), or one that evicts the newest entry and so never holds a busy attacker."""
    clock = Clock()
    table = limiter.LoginLimiter(clock, addresses_max=3)
    for number in range(1, 5):
        fail(table, f"192.0.2.{number}", 5)
        clock.now += 1
    assert table.addresses() == 3
    assert table.begin("192.0.2.1") is not None
    assert table.begin("192.0.2.4") is None


# ── sessions ────────────────────────────────────────────────────────────────


def test_a_session_has_a_new_random_id_and_keeps_the_token_out_of_its_repr():
    """Bug class: guessable or reused session ids, or a token that shows up wherever a session is printed."""
    sessions = WebSessions(Clock())
    first_id, first = sessions.create("made-up-broker-token-1")
    second_id, _ = sessions.create("made-up-broker-token-2")
    assert first_id != second_id and len(first_id) >= 43
    assert sessions.get(first_id) is first
    assert "made-up-broker-token-1" not in repr(first) and first.form_token not in repr(first)


def test_a_form_token_is_compared_whole_and_belongs_to_one_session():
    """Bug class: a comparison that accepts a prefix, a longer string, nothing or another session's token; or a token that changes while its session lives."""
    sessions = WebSessions(Clock())
    first_id, first = sessions.create("t1")
    second_id, second = sessions.create("t2")
    token = first.form_token
    assert not sessions.form_token_matches(first_id, token[:-1])
    assert not sessions.form_token_matches(first_id, token + "x")
    assert not sessions.form_token_matches(first_id, second.form_token)
    assert not sessions.form_token_matches(first_id, None)
    assert not sessions.form_token_matches("made-up-unknown-id", token)
    assert sessions.form_token_matches(first_id, token)
    assert sessions.form_token_matches(first_id, token) and first.form_token == token


def test_the_table_keeps_at_most_its_bound_and_forgets_the_oldest():
    """Bug class: sessions without bound (a login loop fills the memory), or the newest session forgotten instead of the oldest."""
    sessions = WebSessions(Clock(), sessions_max=3)
    ids = [sessions.create(f"t{number}")[0] for number in range(4)]
    assert len(sessions) == 3
    assert sessions.get(ids[0]) is None and all(sessions.get(session_id) for session_id in ids[1:])


def test_at_most_64_sessions_by_default():
    """Bug class: a session table so large that its outcomes, held lines and pending actions (about 1.6 MiB a session) can fill the container's 512 MiB."""
    from social_data_pipeline.webui import sessions

    assert sessions.SESSIONS_MAX == 64
    # The application's own default, where the real web packages are there (the jobs tests put a stand-in FastAPI in their place otherwise).
    pytest.importorskip("starlette.testclient")
    pytest.importorskip("jinja2")
    from social_data_pipeline.webui import app as webapp

    assert webapp.Limits().sessions_max == 64


# ── imports (m) ─────────────────────────────────────────────────────────────


def test_the_web_package_and_the_client_load_nothing_of_the_host_side():
    """Bug class: the web UI importing the CLI, the broker, the registry, the setup package or PyYAML; the image holds none of them, so the container would not start (or would carry code it must not run)."""
    pytest.importorskip("fastapi")
    pytest.importorskip("jinja2")
    probe = "import json, sys\nimport social_data_pipeline.webui.app, social_data_pipeline.webui.probe, social_data_pipeline.control.client\nprint(json.dumps(sorted(sys.modules)))\n"
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=60, env={"PYTHONDONTWRITEBYTECODE": "1", "PATH": "/usr/bin:/bin"})
    assert result.returncode == 0, result.stderr
    loaded = json.loads(result.stdout)
    assert "fastapi" in loaded
    forbidden = [name for name in loaded if name == "sdp" or name == "yaml" or name.startswith(("yaml.", "social_data_pipeline.setup", "social_data_pipeline.control.broker", "social_data_pipeline.control.verbs", "social_data_pipeline.control.types", "social_data_pipeline.webui_host"))]
    assert forbidden == []


def test_the_image_copies_what_the_package_imports():
    """Bug class: a module of the control package imported by the web UI but not copied into the image (the container fails at its start), or more of the host side copied than it needs."""
    dockerfile = (REPO_ROOT / "Dockerfile.webui").read_text()
    copied = set()
    for line in dockerfile.splitlines():
        if line.startswith("COPY ") and "social_data_pipeline/control/" in line:
            copied |= {Path(part).name for part in line.split()[1:-1]}
    assert copied == {"__init__.py", "errors.py", "framing.py", "client.py"}
    imported = set()
    for path in (REPO_ROOT / "social_data_pipeline" / "webui").glob("*.py"):
        for line in path.read_text().splitlines():
            if line.startswith("from social_data_pipeline.control import"):
                imported |= {name.strip().split(" as ")[0] + ".py" for name in line.split("import", 1)[1].split(",")}
    assert imported <= copied
