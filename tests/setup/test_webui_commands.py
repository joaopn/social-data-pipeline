"""Tests for the `webui` commands of the CLI (`social_data_pipeline/webui_host.py`, wired in `sdp.py`): `setup`, `start`, `stop`, `status`, `unsetup`, and the bare `sdp webui`.

The contracts pinned here, each test naming the bug class it catches:

- The answers: the one question's answer sorted into addresses and names, `all` and its other spellings (alone or beside other entries) as every address, and a refused entry named; every refused form of a listen address (multicast, a zone, 127.0.0.1 again, a duplicate, anything but an IP address) and of a name (a wildcard, upper case, a trailing dot, an empty or over-long label, an over-long name, a number as last label, localhost again, a duplicate); the port's range.
- The `docker create` argument list, pinned literally for a setting with two addresses and two names and for every address (one publication on every address, `SDP_WEBUI_ANY_HOST=1`); what setup, start and status print in both modes; a saved setting of every address (`0.0.0.0` or `::`) read back, and offered again as the default; the certificate's `openssl` argument list, with every value its own argument.
- `start`: refused when not set up, under rootless Docker or user-namespace remapping, for a container of another deployment; "already running" with exit code 0; a failed create or start removes the container and returns 1 with Docker's last line; a server that ends right after its start is reported with its last log lines, removed, and the command returns 1; the broker is asked from inside the container. 127.0.0.1 is published to the local port 8444, every other listen address (IPv6 ones included) to 8443.
- `unsetup` removes nothing through a symbolic link: a linked `.sdp` is refused and what it points at stays.
- The Queue window's mounts (`_queue_mounts`): the `docker create` list pinned with none, one and both; `start` makes `data/jobs/control` 0700 whatever the umask; a linked or foreign control folder, a missing or linked `data/jobs`: no mount and the line; the results folder from `.env` (by its key), its default, a comma in its path, `data/jobs` itself, a folder above it (through a link too), a folder inside the control folder: each with its line and the right mounts; `.env` is never printed; `status` says which are mounted.
- The Chat window's mount (`_chat_folder`): pinned with all three optional mounts; `.sdp/chat/socket` present (mounted with its setting), missing, a link (itself or a folder above it), of another user, open to others: the line and no mount; `status` says whether it is mounted.
- `stop` and `status` exit codes; `unsetup` asks first and removes the container, the network, the image and `.sdp/webui`.
- `setup` refuses without a management password before asking anything; the state folder and its files have modes 0700 and 0600; no argument list holds the key or a password; every prompt has a tag listed in `tests/e2e/questions.yaml`.
- `import sdp` loads neither the web package nor the control package; `sdp webui` alone prints the group's help.

Docker is a stand-in that records every argument list and answers from a small model of one container and one network; nothing reaches a real Docker.
"""

from __future__ import annotations

import base64
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

import sdp
from social_data_pipeline import webui_host as host
from social_data_pipeline.control import broker, passwords

REPO_ROOT = Path(__file__).resolve().parents[2]
UID, GID = 1000, 1000
CERT_PEM = b"-----BEGIN CERTIFICATE-----\n" + base64.b64encode(b"made-up certificate body") + b"\n-----END CERTIFICATE-----\n"
KEY_PEM = b"-----BEGIN PRIVATE KEY-----\nmade-up-key-material-77\n-----END PRIVATE KEY-----\n"


def fake_scrypt(password, *, salt, n, r, p, maxmem, dklen):
    return b"k" * dklen


class FakeDocker:
    """Records every argument list in `calls` and answers like Docker for one container `sdp-webui` and one network `sdp-webui`. `container` is None or (status, root label); `fail` maps a command word (`create`, `start`, `build`, `exec`) to the stderr of a failure; `security` is what `docker info` lists."""

    def __init__(self, root):
        self.root = str(root)
        self.calls = []
        self.container = None
        self.network = None
        self.ipam = [{"Subnet": "172.30.0.0/16", "Gateway": "172.30.0.1"}]
        self.driver = "bridge"
        self.fail = {}
        self.security = ["name=seccomp,profile=builtin", "name=cgroupns"]
        self.probe = "answered"
        # The server ends right after `docker start` answered 0 (a key it cannot read).
        self.exits_at_start = False

    def __call__(self, arguments, *, capture=True, timeout=600):
        self.calls.append(list(arguments))
        assert arguments[0] == "docker"
        word = arguments[1]
        if word in self.fail:
            return self.answer(1, stderr=f"first line\n{self.fail[word]}\n")
        if word == "info":
            return self.answer(0, json.dumps(self.security))
        if arguments[1:3] == ["container", "inspect"] and "{{json .Mounts}}" in arguments:
            created = next((call for call in reversed(self.calls) if call[1] == "create"), [])
            mounts = [dict(part.split("=", 1) for part in created[index + 1].split(",") if "=" in part) for index, word in enumerate(created) if word == "--mount"]
            return self.answer(0, json.dumps([{"Source": mount["source"], "Destination": mount["target"]} for mount in mounts]))
        if arguments[1:3] == ["container", "inspect"]:
            if self.container is None:
                return self.answer(1, stderr="Error: No such container: sdp-webui")
            status, label = self.container
            return self.answer(0, f"{json.dumps(status)}\n{json.dumps({host.ROOT_LABEL: label} if label else {})}\n")
        if word == "ps":
            return self.answer(0, "")
        if arguments[1:3] == ["network", "inspect"]:
            if self.network is None:
                return self.answer(1, stderr="Error: network sdp-webui not found")
            return self.answer(0, json.dumps(self.driver) + "\n" + json.dumps(self.network) + "\n" + json.dumps(self.ipam) + "\n")
        if arguments[1:3] == ["network", "create"]:
            self.network = {"com.docker.network.bridge.enable_ip_masquerade": "false"}
            return self.answer(0, "made-up-network-id")
        if arguments[1:3] == ["network", "rm"]:
            if self.network is None:
                return self.answer(1, stderr="Error response from daemon: network sdp-webui not found")
            self.network = None
            return self.answer(0, "sdp-webui")
        if arguments[1:3] == ["image", "rm"]:
            return self.answer(0, "Untagged: sdp-webui:latest")
        if word == "create":
            self.container = ("created", self.root)
            return self.answer(0, "made-up-container-id")
        if word == "start":
            self.container = ("running", self.root)
            return self.answer(0, "sdp-webui")
        if word == "exec":
            if self.exits_at_start:
                self.container = ("exited", self.container[1])
            return self.answer(0 if not self.exits_at_start else 1, self.probe + "\n")
        if word == "logs":
            return self.answer(0, "", "INFO Started server process [1]\nssl.SSLError: [SSL] PEM lib (_ssl.c:4123)\n")
        if word == "stop":
            self.container = ("exited", self.container[1])
            return self.answer(0, "sdp-webui")
        if word == "rm":
            self.container = None
            return self.answer(0, "sdp-webui")
        if word == "build":
            return self.answer(0)
        if word == "run":
            return self.openssl(arguments)
        raise AssertionError(f"an unexpected Docker call: {arguments}")

    def openssl(self, arguments):
        mount = arguments[arguments.index("--mount") + 1]
        folder = Path(dict(part.split("=", 1) for part in mount.split(",") if "=" in part)["source"])
        command = arguments[arguments.index(host.IMAGE) + 1:]
        if command[:2] == ["openssl", "req"]:
            for name, content in ((host.NEW_CERT_NAME, CERT_PEM), (host.NEW_KEY_NAME, KEY_PEM)):
                (folder / name).write_bytes(content)
                os.chmod(folder / name, 0o644)
            return self.answer(0, stderr="-----\n")
        if "-enddate" in command:
            return self.answer(0, "notAfter=Feb 23 19:43:49 2029 GMT\n")
        if "-pubkey" in command or command[:2] == ["openssl", "pkey"]:
            return self.answer(0, "-----BEGIN PUBLIC KEY-----\nmade-up\n-----END PUBLIC KEY-----\n")
        raise AssertionError(f"an unexpected openssl call: {command}")

    @staticmethod
    def answer(code, stdout="", stderr=""):
        return subprocess.CompletedProcess([], code, stdout, stderr)

    def words(self):
        return [" ".join(call[1:3]) if call[1] in ("network", "image", "container") else call[1] for call in self.calls]


@pytest.fixture
def root(tmp_path):
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    return folder.resolve()


@pytest.fixture
def docker(root, monkeypatch):
    fake = FakeDocker(root)
    monkeypatch.setattr(host, "_docker", fake)
    return fake


def set_management_password(root):
    os.makedirs(root / ".sdp", mode=0o700, exist_ok=True)
    passwords.set_password(broker.broker_folder(root), "made-up-management-pw-31", scrypt=fake_scrypt)


def scripted(answers):
    """An `ask` that answers in order and records the prompts with their tags."""
    asked = []

    def ask(prompt, default=None, tag=None):
        asked.append(tag)
        return answers.pop(0) if answers else ("" if default is None else str(default))

    def ask_choice(prompt, options, default=None, tag=None):
        asked.append(tag)
        return answers.pop(0) if answers else default

    return ask, ask_choice, asked


def set_up(root, docker, answers=None):
    set_management_password(root)
    ask, ask_choice, _ = scripted(list(answers if answers is not None else ["8443", "192.0.2.10, 2001:db8::5, pipeline.example, ops.example", "generate"]))
    return host._setup(str(root), ask, ask_choice, UID, GID)


# ── the answers ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("answer, says", [
    ("0.0.0.0", "every address"), ("::", "every address"),
    ("224.0.0.1", "multicast"), ("ff02::1", "multicast"),
    ("fe80::1%eth0", "zone"),
    ("127.0.0.1", "always served on 127.0.0.1"), ("::1", "always served on 127.0.0.1"), ("127.0.0.2", "always served on 127.0.0.1"), ("127.255.0.9", "always served on 127.0.0.1"), ("[::1]", "always served on 127.0.0.1"), ("::ffff:127.0.0.1", "always served on 127.0.0.1"),
    ("192.0.2.1, 192.0.2.1", "twice"), ("2001:db8::5, 2001:db8:0::5", "twice"),
    ("pipeline.example", "not an IP address"), ("192.0.2.0/24", "not an IP address"), ("192.0.2.256", "not an IP address"), ("010.0.0.1", "not an IP address"),
])
def test_refused_listen_addresses(answer, says):
    """Bug class: a listen address that publishes the page on every network (0.0.0.0, ::), one Docker cannot publish (a zone, multicast) or a duplicate, accepted and found out only when the container fails to start, or never."""
    with pytest.raises(host.Refused) as caught:
        host.parse_addresses(answer)
    assert says in caught.value.message


def test_the_one_answer_is_sorted_into_addresses_and_names():
    """Bug class: a name taken for an address or the reverse (published, or left out of the certificate), or the order of the `Host` values changed."""
    assert host.parse_reach(" 192.0.2.10, pipeline.example ,[2001:db8::5],, ops.example", 8443) == host.WebuiConfig(port=8443, addresses=("192.0.2.10", "2001:db8::5"), names=("pipeline.example", "ops.example"))
    assert host.parse_reach("", 9443) == host.WebuiConfig(port=9443)


@pytest.mark.parametrize("word", ["all", "ALL", "any", "*", "0.0.0.0", "::", "[::]", "0:0::0"])
def test_each_word_for_every_address_is_every_address(word):
    """Bug class: one spelling of "every address" refused or published as an address, or the entries beside it lost."""
    assert host.parse_reach(word, 8443) == host.WebuiConfig(port=8443, every=True)
    assert host.parse_reach(f"192.0.2.10, {word}, pipeline.example", 8443) == host.WebuiConfig(port=8443, addresses=("192.0.2.10",), names=("pipeline.example",), every=True)


@pytest.mark.parametrize("answer, says", [
    ("under_score.example", "'under_score.example'"), ("192.0.2.300", "'192.0.2.300'"), ("Pipeline.example", "'Pipeline.example'"), ("fe80::1%eth0", "'fe80::1%eth0'"),
    ("224.0.0.1", "'224.0.0.1'"), ("pipeline.example, 192.0.2.1, pipeline.example", "'pipeline.example' is named twice"), ("192.0.2.1, all, 192.0.2.1", "'192.0.2.1' is named twice"),
    ("localhost", "localhost is always there"), ("all, 127.0.0.1", "always served on 127.0.0.1"),
])
def test_a_refused_entry_of_the_one_answer_is_named(answer, says):
    """Bug class: an entry that is neither an address, a name nor every address accepted, or refused without saying which."""
    with pytest.raises(host.Refused) as caught:
        host.parse_reach(answer, 8443)
    assert says in caught.value.message


def test_listen_addresses_are_read_in_their_short_form():
    """Bug class: an address kept as typed (`2001:0db8::0005`), which then differs from the certificate's and the Host check's form."""
    assert host.parse_addresses("") == ()
    assert host.parse_addresses(" 192.0.2.10 , 2001:0db8::0005, [2001:db8::6], 169.254.1.1") == ("192.0.2.10", "2001:db8::5", "2001:db8::6", "169.254.1.1")


@pytest.mark.parametrize("answer", ["*.example", "*", "Pipeline.example", "pipeline.example.", "a..b", "x" * 64 + ".example", ".".join(["a" * 63] * 4), "under_score.example", "localhost", "a.example, a.example", "10.0.0.1", "host.123", "space name"])
def test_refused_names(answer):
    """Bug class: a wildcard, a name a browser will never send as Host, or a name that would be taken for an address, written into the certificate and the Host check."""
    with pytest.raises(host.Refused):
        host.parse_names(answer)


def test_names_are_read():
    """Bug class: good names refused, or empty parts of a list kept as names."""
    assert host.parse_names("") == ()
    assert host.parse_names("pipeline.example, ops-1.example,,box") == ("pipeline.example", "ops-1.example", "box")


@pytest.mark.parametrize("answer", ["1023", "65536", "0", "x", "", "84 43", "-1", "\u0668\u0664\u0664\u0663"])
def test_refused_ports(answer):
    """Bug class: a privileged or out-of-range port accepted, which Docker refuses at start."""
    with pytest.raises(host.Refused):
        host.parse_port(answer)
    assert host.parse_port("8443") == 8443 and host.parse_port("1024") == 1024


# ── argument lists ──────────────────────────────────────────────────────────


def test_the_create_argument_list_is_pinned():
    """Bug class: one option of the container's confinement dropped or loosened (a writable root, a capability, the Docker socket, a third mount, a port on every address) by a change that no other test would notice."""
    cfg = host.WebuiConfig(port=8443, addresses=("192.0.2.10", "2001:db8::5"), names=("pipeline.example", "ops.example"))
    assert host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1") == [
        "docker", "create",
        "--name", "sdp-webui",
        "--label", "sdp.webui.root=/srv/deploy",
        "--restart", "no",
        "--user", "1000:1001",
        "--read-only",
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--pids-limit", "256",
        "--memory", "512m",
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m",
        "--network", "sdp-webui",
        "--mount", "type=bind,source=/srv/deploy/.sdp/socket,target=/run/sdp,readonly",
        "--mount", "type=bind,source=/srv/deploy/.sdp/webui/tls,target=/tls,readonly",
        "-p", "127.0.0.1:8443:8444",
        "-p", "192.0.2.10:8443:8443",
        "-p", "[2001:db8::5]:8443:8443",
        "-e", "SDP_WEBUI_HOSTS=localhost,127.0.0.1,192.0.2.10,[2001:db8::5],pipeline.example,ops.example",
        "-e", "SDP_WEBUI_PORT=8443",
        "-e", "SDP_WEBUI_SOCKET=/run/sdp/broker.sock",
        "-e", "SDP_WEBUI_GATEWAY=172.30.0.1",
        "sdp-webui",
    ]


PINNED_START = [
    "docker", "create",
    "--name", "sdp-webui",
    "--label", "sdp.webui.root=/srv/deploy",
    "--restart", "no",
    "--user", "1000:1001",
    "--read-only",
    "--cap-drop", "ALL",
    "--security-opt", "no-new-privileges",
    "--pids-limit", "256",
    "--memory", "512m",
    "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m",
    "--network", "sdp-webui",
    "--mount", "type=bind,source=/srv/deploy/.sdp/socket,target=/run/sdp,readonly",
    "--mount", "type=bind,source=/srv/deploy/.sdp/webui/tls,target=/tls,readonly",
]
PINNED_PORTS_AND_SETTINGS = [
    "-p", "127.0.0.1:8443:8444",
    "-e", "SDP_WEBUI_HOSTS=localhost,127.0.0.1",
    "-e", "SDP_WEBUI_PORT=8443",
    "-e", "SDP_WEBUI_SOCKET=/run/sdp/broker.sock",
    "-e", "SDP_WEBUI_GATEWAY=172.30.0.1",
]


def test_the_create_argument_list_with_the_queue_mounts_is_pinned():
    """Bug class: the scheduler's folders mounted writable, at another place, or with the socket's setting but not its folder (or the reverse), or the results folder mounted without its setting; a new mount slipping in with them."""
    cfg = host.WebuiConfig(port=8443)
    both = host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1", "/srv/deploy/data/jobs/control", "/srv/results")
    assert both == [
        *PINNED_START,
        "--mount", "type=bind,source=/srv/deploy/data/jobs/control,target=/run/sdp-jobs,readonly",
        "--mount", "type=bind,source=/srv/results,target=/run/sdp-results,readonly",
        *PINNED_PORTS_AND_SETTINGS,
        "-e", "SDP_WEBUI_QUEUE_SOCKET=/run/sdp-jobs/door.sock",
        "-e", "SDP_WEBUI_RESULTS=/run/sdp-results",
        "sdp-webui",
    ]
    assert host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1", "/srv/deploy/data/jobs/control") == [
        *PINNED_START,
        "--mount", "type=bind,source=/srv/deploy/data/jobs/control,target=/run/sdp-jobs,readonly",
        *PINNED_PORTS_AND_SETTINGS,
        "-e", "SDP_WEBUI_QUEUE_SOCKET=/run/sdp-jobs/door.sock",
        "sdp-webui",
    ]
    assert host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1") == [*PINNED_START, *PINNED_PORTS_AND_SETTINGS, "sdp-webui"]


def test_the_create_argument_list_with_the_chat_mount_is_pinned():
    """Bug class: the chat's socket folder mounted writable (the web UI could then replace the runner's socket), at another place, or without its setting; with all three optional mounts, one of them dropped or reordered."""
    cfg = host.WebuiConfig(port=8443)
    chat_mount = ["--mount", "type=bind,source=/srv/deploy/.sdp/chat/socket,target=/run/sdp-chat,readonly"]
    chat_setting = ["-e", "SDP_WEBUI_CHAT_SOCKET=/run/sdp-chat/runner.sock"]
    assert host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1", chat_folder="/srv/deploy/.sdp/chat/socket") == [*PINNED_START, *chat_mount, *PINNED_PORTS_AND_SETTINGS, *chat_setting, "sdp-webui"]
    assert host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1", "/srv/deploy/data/jobs/control", "/srv/results", "/srv/deploy/.sdp/chat/socket") == [
        *PINNED_START,
        "--mount", "type=bind,source=/srv/deploy/data/jobs/control,target=/run/sdp-jobs,readonly",
        "--mount", "type=bind,source=/srv/results,target=/run/sdp-results,readonly",
        *chat_mount,
        *PINNED_PORTS_AND_SETTINGS,
        "-e", "SDP_WEBUI_QUEUE_SOCKET=/run/sdp-jobs/door.sock",
        "-e", "SDP_WEBUI_RESULTS=/run/sdp-results",
        *chat_setting,
        "sdp-webui",
    ]


@pytest.mark.parametrize("address", ["2001:db8::5", "192.0.2.10", "169.254.1.1"])
def test_only_127_0_0_1_is_published_to_the_local_port(address):
    """Bug class: a listen address other than 127.0.0.1 (an IPv6 one, which Docker may proxy from the gateway, or the IPv6 loopback) published to the local port, so that whoever reaches it logs in as `local`."""
    cfg = host.WebuiConfig(port=9443, addresses=(address,))
    arguments = host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1")
    published = [arguments[index + 1] for index, word in enumerate(arguments) if word == "-p"]
    shown = f"[{address}]" if ":" in address else address
    assert published == ["127.0.0.1:9443:8444", f"{shown}:9443:8443"]


def test_the_create_argument_list_for_every_address_is_pinned():
    """Bug class: every address published beside a publication of 127.0.0.1 (Docker refuses the port twice) or on a second port, the any-host setting missing (the page refuses every address it was not told), or the addresses listed beside `all` published apart."""
    cfg = host.WebuiConfig(port=8443, addresses=("192.0.2.10",), names=("pipeline.example",), every=True)
    assert host.create_arguments("/srv/deploy", 1000, 1001, cfg, "172.30.0.1") == [
        *PINNED_START,
        "-p", "8443:8443",
        "-e", "SDP_WEBUI_HOSTS=localhost,127.0.0.1,192.0.2.10,pipeline.example",
        "-e", "SDP_WEBUI_PORT=8443",
        "-e", "SDP_WEBUI_SOCKET=/run/sdp/broker.sock",
        "-e", "SDP_WEBUI_GATEWAY=172.30.0.1",
        "-e", "SDP_WEBUI_ANY_HOST=1",
        "sdp-webui",
    ]
    assert host.subject_alt_names(cfg) == "DNS:localhost,IP:127.0.0.1,IP:192.0.2.10,DNS:pipeline.example"


def test_the_certificate_argument_list_passes_each_value_as_one_argument():
    """Bug class: the names reaching openssl through a shell (an injected option or command), or a key of another kind or lifetime."""
    cfg = host.WebuiConfig(port=8443, addresses=("192.0.2.10",), names=("pipeline.example",))
    arguments = host.certificate_arguments("/srv/deploy", 1000, 1001, cfg)
    assert arguments[:arguments.index("sdp-webui") + 1] == ["docker", "run", "--rm", "--network", "none", "--user", "1000:1001", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--mount", "type=bind,source=/srv/deploy/.sdp/webui/tls,target=/tls", "sdp-webui"]
    command = arguments[arguments.index("sdp-webui") + 1:]
    assert command == ["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-noenc", "-keyout", "/tls/new-key.pem", "-out", "/tls/new-cert.pem", "-days", "825", "-subj", "/CN=sdp-webui", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:192.0.2.10,DNS:pipeline.example", "-addext", "extendedKeyUsage=serverAuth", "-addext", "keyUsage=critical,digitalSignature", "-addext", "basicConstraints=critical,CA:FALSE"]
    assert "sh" not in arguments and "-c" not in arguments


# ── start ───────────────────────────────────────────────────────────────────


def test_start_creates_starts_and_asks_the_broker_from_inside(root, docker, capsys):
    """Bug class: a start that skips the network, the mount source or the check of the broker, or that says "started" when Docker refused."""
    assert set_up(root, docker) == 0
    docker.calls.clear()
    capsys.readouterr()

    assert host._start(str(root), UID, GID) == 0
    assert docker.words() == ["info", "container inspect", "ps", "network inspect", "network create", "network inspect", "create", "start", "exec", "container inspect"]
    assert docker.calls[6] == host.create_arguments(str(root), UID, GID, host.read_config(str(root))[0], "172.30.0.1")
    assert docker.calls[8] == ["docker", "exec", "sdp-webui", "python", "-m", "social_data_pipeline.webui.probe"]
    out = capsys.readouterr().out
    assert "The web UI is started." in out and host.PROBE_LINES["answered"] in out and "https://[2001:db8::5]:8443/" in out
    assert stat.S_IMODE(os.lstat(root / ".sdp" / "socket").st_mode) == 0o700


@pytest.mark.parametrize("probe", ["no_socket", "refused", "no_reply", "permission_denied"])
def test_start_says_what_the_broker_answered(root, docker, capsys, probe):
    """Bug class: a start that reports success while the container cannot reach the broker, or does not say why."""
    set_up(root, docker)
    docker.probe = probe
    capsys.readouterr()
    assert host._start(str(root), UID, GID) == 0
    assert host.PROBE_LINES[probe] in capsys.readouterr().out


@pytest.mark.parametrize("step", ["create", "start"])
def test_a_failed_create_or_start_removes_the_container_and_returns_1(root, docker, capsys, step):
    """Bug class: a container left in the state `created` (Docker leaves one when a published address does not exist), which blocks the next start under the same name; a failure reported without Docker's reason."""
    set_up(root, docker)
    docker.fail[step] = "Error response from daemon: cannot assign requested address"
    capsys.readouterr()
    assert host._start(str(root), UID, GID) == 1
    assert docker.calls[-1] == ["docker", "rm", "--force", "sdp-webui"]
    out = capsys.readouterr().out
    assert "cannot assign requested address" in out and "every listen address" in out
    assert "first line" not in out


def test_a_server_that_ends_right_after_its_start_is_reported_and_removed(root, docker, capsys):
    """Bug class: "started" and exit code 0 for a container whose server ended at once (a key it cannot read, a setting it refuses): `docker start` answers 0 for it."""
    set_up(root, docker)
    docker.exits_at_start = True
    docker.probe = "no_setting"
    capsys.readouterr()
    assert host._start(str(root), UID, GID) == 1
    out = capsys.readouterr().out
    assert "The web UI is started." not in out
    assert "ended right after its start" in out and "ssl.SSLError: [SSL] PEM lib" in out
    assert docker.words()[-3:] == ["container inspect", "logs", "rm"] and docker.container is None
    assert ["docker", "logs", "--tail", "20", "sdp-webui"] in docker.calls


def test_a_server_that_does_not_serve_is_reported_and_removed(root, docker, capsys):
    """Bug class: a container whose server is still importing (and then ends on a key it cannot read) taken for a running web UI because it had not ended yet when it was looked at."""
    set_up(root, docker)
    docker.probe = "not_serving"
    capsys.readouterr()
    assert host._start(str(root), UID, GID) == 1
    out = capsys.readouterr().out
    assert "The web UI is started." not in out and "ended right after its start" in out
    assert docker.container is None


@pytest.mark.parametrize("option, says", [("name=rootless", "rootless"), ("name=userns", "remaps user ids")])
def test_start_is_refused_under_user_id_mapping(root, docker, option, says):
    """Bug class: a web UI started under rootless Docker or userns-remap, where the broker sees another uid and closes every connection without a word."""
    set_up(root, docker)
    docker.security = ["name=seccomp,profile=builtin", option]
    docker.calls.clear()
    with pytest.raises(host.Refused) as caught:
        host._start(str(root), UID, GID)
    assert says in caught.value.message
    assert docker.words() == ["info"]


def test_start_when_running_says_so_with_exit_code_0(root, docker, capsys):
    """Bug class: a second start that fails, or that replaces the running container."""
    set_up(root, docker)
    host._start(str(root), UID, GID)
    docker.calls.clear()
    capsys.readouterr()
    assert host._start(str(root), UID, GID) == 0
    assert capsys.readouterr().out == "  The web UI is already running.\n"
    assert "create" not in docker.words() and "rm" not in docker.words()


def test_start_refuses_a_container_of_another_deployment(root, docker):
    """Bug class: the web UI of another deployment folder taken for this one ("already running") or removed."""
    set_up(root, docker)
    docker.container = ("running", "/another/deployment")
    with pytest.raises(host.Refused):
        host._start(str(root), UID, GID)
    assert "rm" not in docker.words()


def test_start_when_not_set_up_says_how_to_set_it_up(root, docker, capsys):
    """Bug class: a start that runs Docker without settings, or fails with a traceback."""
    assert host.cmd_start(root) == 1
    assert "not set up" in capsys.readouterr().out and docker.calls == []


# ── stop, status, unsetup ───────────────────────────────────────────────────


def test_stop_and_status_exit_codes(root, docker, capsys):
    """Bug class: scripts that cannot tell a running web UI from a stopped one by the exit code."""
    assert host.cmd_status(root) == 1
    assert host.cmd_stop(root) == 1
    set_up(root, docker)
    host._start(str(root), UID, GID)
    capsys.readouterr()
    assert host.cmd_status(root) == 0
    out = capsys.readouterr().out
    assert "Web UI: running" in out and "Listen addresses: 127.0.0.1, 192.0.2.10, 2001:db8::5" in out and "Names: localhost, pipeline.example, ops.example" in out
    assert host.certificate_fingerprint(CERT_PEM) in out and "Feb 23 19:43:49 2029 GMT" in out
    docker.calls.clear()
    assert host.cmd_stop(root) == 0
    assert docker.words()[-2:] == ["stop", "rm"]
    assert host.cmd_status(root) == 1
    assert host.cmd_stop(root) == 1
    docker.container = ("exited", str(root))
    assert host.cmd_stop(root) == 1
    assert docker.container is None


def test_unsetup_asks_first_and_then_removes_everything(root, docker, capsys):
    """Bug class: an unsetup that removes without asking, or that leaves the container, the network, the image or the settings with the certificate's key behind."""
    set_up(root, docker)
    host._start(str(root), UID, GID)
    docker.calls.clear()
    assert host.cmd_unsetup(root, lambda: False) == 0
    assert docker.calls == [] and (root / ".sdp" / "webui").exists()
    assert host.cmd_unsetup(root, lambda: True) == 0
    assert docker.words() == ["container inspect", "rm", "network rm", "image rm"]
    assert not (root / ".sdp" / "webui").exists() and (root / ".sdp" / "broker").exists()
    assert host.cmd_unsetup(root, lambda: True) == 0


def test_start_reads_the_gateway_and_refuses_a_network_without_one(root, docker):
    """Bug class: a container started with no gateway (no login could ever be local), or with an IPv6 or made-up one."""
    set_up(root, docker)
    docker.network = {"com.docker.network.bridge.enable_ip_masquerade": "false"}
    docker.ipam = [{"Subnet": "fd00::/64", "Gateway": "fd00::1"}]
    with pytest.raises(host.Refused) as caught:
        host._start(str(root), UID, GID)
    assert "no IPv4 gateway" in caught.value.message and "create" not in docker.words()
    docker.ipam = [{"Subnet": "fd00::/64", "Gateway": "fd00::1"}, {"Subnet": "10.9.0.0/24", "Gateway": "10.9.0.1"}]
    assert host._start(str(root), UID, GID) == 0
    assert "SDP_WEBUI_GATEWAY=10.9.0.1" in next(call for call in docker.calls if call[1] == "create")


@pytest.mark.parametrize("driver", ["macvlan", "ipvlan", "overlay", "host", ""])
def test_start_refuses_a_network_that_is_not_a_bridge(root, docker, driver):
    """Bug class: a network named sdp-webui made by someone else with another driver (a macvlan network whose "gateway" is the LAN's router) used as it is: the container's local port could then be reached from the LAN without the published port, and logins from the router's address would count as local."""
    set_up(root, docker)
    docker.network = {"com.docker.network.bridge.enable_ip_masquerade": "false"}
    docker.driver = driver
    with pytest.raises(host.Refused) as caught:
        host._start(str(root), UID, GID)
    assert "not a bridge network" in caught.value.message and "docker network rm sdp-webui" in caught.value.message
    assert "create" not in docker.words()


def test_a_saved_loopback_listen_address_is_refused_at_start(root, docker, capsys):
    """Bug class: a settings file written before loopback addresses were refused (or by hand) still publishing `::1` or `127.0.0.2` to the port where logins count as remote, or failing with a line that does not say why."""
    set_up(root, docker)
    path = root / ".sdp" / "webui" / "config.json"
    saved = json.loads(path.read_text())
    saved["addresses"] = ["::1"]
    path.write_text(json.dumps(saved))
    docker.calls.clear()
    capsys.readouterr()
    assert host.cmd_start(root) == 1
    out = capsys.readouterr().out
    assert host.LOOPBACK_REFUSED in out and "webui setup" in out
    assert docker.calls == []


def test_unsetup_says_whether_the_settings_were_there(root, docker, capsys):
    """Bug class: "Removed .sdp/webui." printed when there was nothing to remove (the operator believes an old folder existed and is gone)."""
    set_up(root, docker)
    capsys.readouterr()
    assert host.cmd_unsetup(root, lambda: True) == 0
    assert "  Removed .sdp/webui.\n" in capsys.readouterr().out
    assert host.cmd_unsetup(root, lambda: True) == 0
    out = capsys.readouterr().out
    assert "Removed .sdp/webui." not in out and "  There was no .sdp/webui to remove.\n" in out


def test_unsetup_follows_no_link(root, docker, tmp_path, capsys):
    """Bug class: `.sdp` replaced by a link to another folder, and unsetup removing that folder's `webui` (a check of the last part of the path only, then a removal that follows a link higher up)."""
    elsewhere = tmp_path / "elsewhere"
    (elsewhere / "webui" / "tls").mkdir(parents=True)
    (elsewhere / "webui" / "keep.txt").write_text("not to be removed\n")
    (root / ".sdp").symlink_to(elsewhere)
    assert host.cmd_unsetup(root, lambda: True) == 1
    assert (elsewhere / "webui" / "keep.txt").read_text() == "not to be removed\n"
    assert (elsewhere / "webui" / "tls").is_dir()
    assert "is a link or not a folder of this user" in capsys.readouterr().out


def test_unsetup_follows_no_link_inside(root, docker, tmp_path):
    """Bug class: a link inside `.sdp/webui` (planted as `tls`) followed by the removal."""
    set_up(root, docker)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (elsewhere / "keep.txt").write_text("kept\n")
    (root / ".sdp" / "webui" / "planted").symlink_to(elsewhere)
    assert host.cmd_unsetup(root, lambda: True) == 0
    assert not (root / ".sdp" / "webui").exists() and (elsewhere / "keep.txt").read_text() == "kept\n"


# ── setup ───────────────────────────────────────────────────────────────────


def test_setup_refuses_without_a_management_password_before_asking(root, docker):
    """Bug class: a web UI set up (and its image built) that nobody can log in to, the operator finding out only at the login page."""
    ask, ask_choice, asked = scripted([])
    with pytest.raises(host.Refused) as caught:
        host._setup(str(root), ask, ask_choice, UID, GID)
    assert "broker passwd" in caught.value.message
    assert asked == [] and docker.calls == []
    assert not (root / ".sdp" / "webui").exists()


def test_setup_writes_private_files_and_prints_the_fingerprint(root, docker, capsys):
    """Bug class: a key or settings readable by other users (a umask of 022), the key left under a temporary name, or a setup that does not show the fingerprint the browser will ask the operator to trust."""
    old = os.umask(0o022)
    try:
        assert set_up(root, docker) == 0
    finally:
        os.umask(old)
    folder = root / ".sdp" / "webui"
    assert stat.S_IMODE(folder.stat().st_mode) == 0o700 and stat.S_IMODE((folder / "tls").stat().st_mode) == 0o700
    for path in (folder / "config.json", folder / "tls" / "cert.pem", folder / "tls" / "key.pem"):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600, path
    assert sorted(os.listdir(folder / "tls")) == ["cert.pem", "key.pem"]
    saved = json.loads((folder / "config.json").read_text())
    assert saved == {"port": 8443, "addresses": ["192.0.2.10", "2001:db8::5"], "names": ["pipeline.example", "ops.example"], "certificate": "generate", "fingerprint": host.certificate_fingerprint(CERT_PEM), "not_after": "Feb 23 19:43:49 2029 GMT"}
    out = capsys.readouterr().out
    assert host.certificate_fingerprint(CERT_PEM) in out and "https://pipeline.example:8443/" in out
    assert ["docker", "build", "-f", os.path.join(str(root), "Dockerfile.webui"), "-t", "sdp-webui", str(root)] in docker.calls


def test_with_the_operators_certificate_nothing_is_said_about_the_names_in_it(root, docker, tmp_path, capsys):
    """Bug class: in the every-address mode, setup and status listing the names of a certificate they did not make (the operator's own may hold other names, and then no browser warning comes)."""
    certificate, key = tmp_path / "mine.crt", tmp_path / "mine.key"
    certificate.write_bytes(CERT_PEM)
    key.write_bytes(KEY_PEM)
    assert set_up(root, docker, ["9443", "all", "own", str(certificate), str(key)]) == 0
    out = capsys.readouterr().out
    assert "The page answers on every address of this machine, port 9443" in out
    assert "The certificate is for" not in out
    assert host.cmd_status(root) in (0, 1)
    status = capsys.readouterr().out
    assert "Listen addresses: all" in status and "In the certificate" not in status


def test_setup_with_the_operators_certificate_copies_it_and_puts_no_secret_on_a_command_line(root, docker, tmp_path):
    """Bug class: the operator's key passed to Docker or openssl as an argument (visible in the process list), linked instead of copied (so the container mounts a file outside the state folder), or a key that does not belong to the certificate accepted."""
    certificate, key = tmp_path / "mine.crt", tmp_path / "mine.key"
    certificate.write_bytes(CERT_PEM)
    key.write_bytes(KEY_PEM)
    assert set_up(root, docker, ["9443", "", "own", str(certificate), str(key)]) == 0
    tls = root / ".sdp" / "webui" / "tls"
    assert (tls / "key.pem").read_bytes() == KEY_PEM and not (tls / "key.pem").is_symlink()
    assert stat.S_IMODE((tls / "key.pem").stat().st_mode) == 0o600
    flat = "\n".join(" ".join(call) for call in docker.calls)
    assert "made-up-key-material" not in flat and "PRIVATE KEY" not in flat and "made-up-management-pw" not in flat


def test_setup_with_a_key_of_another_certificate_is_refused(root, docker, tmp_path, monkeypatch):
    """Bug class: a certificate and a key that do not match, found out only when the container cannot start."""
    certificate, key = tmp_path / "mine.crt", tmp_path / "mine.key"
    certificate.write_bytes(CERT_PEM)
    key.write_bytes(KEY_PEM)
    real = docker.openssl

    def mismatched(arguments):
        answer = real(arguments)
        if "pkey" in arguments:
            answer.stdout = "-----BEGIN PUBLIC KEY-----\nanother\n-----END PUBLIC KEY-----\n"
        return answer

    monkeypatch.setattr(docker, "openssl", mismatched)
    with pytest.raises(host.Refused) as caught:
        set_up(root, docker, ["9443", "", "own", str(certificate), str(key)])
    assert "not the key of the certificate" in caught.value.message
    assert not (root / ".sdp" / "webui" / "config.json").exists()


def test_every_setup_prompt_has_a_tag_listed_in_the_questions(root, docker, monkeypatch, capsys):
    """Bug class: a prompt without a tag (the E2E harness waits on it until its timeout), or a tag missing from the dictionary of questions."""
    set_management_password(root)
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setenv("SDP_TAGGED_MODE", "1")
    answers = iter(["8443", "", "1"])
    prompts = []
    monkeypatch.setattr("builtins.input", lambda text="": prompts.append(text) or next(answers))
    args = sdp.build_parser().parse_args(["webui", "setup"])
    assert args.func(args) == 0
    shown = capsys.readouterr().out + "".join(prompts)
    questions = yaml.safe_load((REPO_ROOT / "tests" / "e2e" / "questions.yaml").read_text())
    for tag in ("webui_port", "webui_addresses", "webui_certificate"):
        assert f"[{tag}]" in shown and tag in questions
    for tag in ("webui_cert_path", "webui_key_path", "webui_unsetup_confirm"):
        assert tag in questions


def test_setup_asks_again_after_a_refused_answer(root, docker, capsys):
    """Bug class: a refused answer ending the setup (the operator starts over) or taken anyway."""
    assert set_up(root, docker, ["80", "8443", "*.example", "192.0.2.300, all", "all", "generate"]) == 0
    out = capsys.readouterr().out
    assert "1024 to 65535" in out and "wildcard" in out and "'192.0.2.300'" in out
    assert host.read_config(str(root))[0] == host.WebuiConfig(port=8443, every=True)


def test_setup_refuses_while_the_container_exists(root, docker):
    """Bug class: the certificate replaced under a running web UI."""
    set_up(root, docker)
    host._start(str(root), UID, GID)
    with pytest.raises(host.Refused):
        set_up(root, docker)


# ── the CLI ─────────────────────────────────────────────────────────────────


def test_importing_sdp_loads_neither_the_web_package_nor_the_control_package():
    """Bug class: the CLI loading the web UI or the broker's package at its start (slower for every command, and the web package's imports on the host for nothing)."""
    probe = "import json, sys\nimport sdp\nprint(json.dumps(sorted(sys.modules)))\n"
    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, cwd=str(REPO_ROOT), timeout=60, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    assert result.returncode == 0, result.stderr
    loaded = json.loads(result.stdout)
    assert [name for name in loaded if name.startswith(("social_data_pipeline.webui", "social_data_pipeline.control", "fastapi", "starlette"))] == []


def test_bare_webui_prints_the_groups_help():
    """Bug class: `sdp webui` without a command doing nothing, or failing, where `sdp broker` prints its help."""
    result = subprocess.run([sys.executable, str(REPO_ROOT / "sdp.py"), "webui"], capture_output=True, text=True, timeout=30, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
    assert result.returncode == 0
    assert result.stdout.startswith("usage: sdp.py webui")
    for command in ("setup", "start", "stop", "status", "unsetup"):
        assert command in result.stdout


@pytest.mark.parametrize("command, function", [("setup", "cmd_setup"), ("start", "cmd_start"), ("stop", "cmd_stop"), ("status", "cmd_status")])
def test_each_command_reaches_its_function_with_the_deployment_folder(root, monkeypatch, command, function):
    """Bug class: a command wired to the wrong function, or given the current directory instead of the deployment folder."""
    seen = []
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(host, function, lambda folder: seen.append(folder) or 7)
    args = sdp.build_parser().parse_args(["webui", command])
    assert args.func(args) == 7 and seen == [root]


def test_unsetup_is_wired_with_a_confirmation(root, monkeypatch):
    """Bug class: an unsetup that removes without the question."""
    seen = []
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(host, "cmd_unsetup", lambda folder, confirm: seen.append((folder, confirm())) or 0)
    monkeypatch.setattr("builtins.input", lambda text="": "n")
    args = sdp.build_parser().parse_args(["webui", "unsetup"])
    assert args.func(args) == 0 and seen == [(root, False)]


# ── the Queue window's mounts ───────────────────────────────────────────────

ME = os.getuid(), os.getgid()
ENV_MARKER = "made-up-env-marker-5e1d"


@pytest.fixture
def deployment(root, docker):
    """A set-up web UI whose deployment folder has a `.env` with a marker in a key the start must not print; `data/jobs` is made by each test."""
    set_up(root, docker)
    (root / ".env").write_text(f"OTHER_SECRET={ENV_MARKER}\n")
    docker.calls.clear()
    return root


def start_mounts(root, docker, capsys):
    """Start the web UI; returns (its exit code, the Queue's two mount sources or None, the output)."""
    capsys.readouterr()
    code = host._start(str(root), *ME)
    out = capsys.readouterr().out
    created = next(call for call in docker.calls if call[1] == "create")
    mounts = {dict(part.split("=", 1) for part in created[index + 1].split(",") if "=" in part)["target"]: created[index + 1] for index, word in enumerate(created) if word == "--mount"}
    settings = {word.split("=", 1)[0] for index, word in enumerate(created) if index and created[index - 1] == "-e"}
    queue = mounts.get("/run/sdp-jobs")
    results = mounts.get("/run/sdp-results")
    assert (queue is not None) == ("SDP_WEBUI_QUEUE_SOCKET" in settings) and (results is not None) == ("SDP_WEBUI_RESULTS" in settings)
    docker.calls.clear()
    host.cmd_stop(root)
    assert ENV_MARKER not in out
    return code, mount_source(queue), mount_source(results), out


def mount_source(mount):
    return dict(part.split("=", 1) for part in mount.split(",") if "=" in part)["source"] if mount else None


def make_jobs(root, results=True):
    (root / "data" / "jobs").mkdir(parents=True)
    if results:
        (root / "data" / "jobs" / "results").mkdir()


@pytest.mark.parametrize("mask", [0o000, 0o277])
def test_start_makes_the_control_folder_0700_and_mounts_both(deployment, docker, capsys, mask):
    """Bug class: the control folder made with the umask's mode (0777 under the umask 0 that Docker's helpers use; 0500 under a strict one, which the scheduler cannot use), or not made at all, so that the scheduler opens the socket in a folder others can enter, or the web UI mounts nothing."""
    make_jobs(deployment)
    old = os.umask(mask)
    try:
        code, queue, results, out = start_mounts(deployment, docker, capsys)
    finally:
        os.umask(old)
    assert code == 0 and "Queue:" not in out
    control = deployment / "data" / "jobs" / "control"
    assert stat.S_IMODE(os.lstat(control).st_mode) == 0o700 and os.lstat(control).st_uid == ME[0]
    assert queue == str(control) and results == str(deployment / "data" / "jobs" / "results")


def test_without_data_jobs_there_are_no_queue_mounts_and_a_line(deployment, docker, capsys):
    """Bug class: a mount of a folder that does not exist (Docker refuses, or makes it as root, after which the scheduler refuses it), or no word on why the Queue window is not there."""
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert code == 0 and queue is None and results is None
    assert "the Queue window will not be available: the scheduler's data folder data/jobs is not there" in out
    assert not (deployment / "data").exists()


def test_a_linked_data_jobs_is_refused(deployment, docker, capsys, tmp_path):
    """Bug class: `data/jobs` replaced by a link to a folder of someone else's choosing, whose `control` folder would then be mounted (and made there)."""
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (deployment / "data").mkdir()
    (deployment / "data" / "jobs").symlink_to(elsewhere)
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert queue is None and results is None and "is a link, not a folder" in out
    assert list(elsewhere.iterdir()) == []


def test_a_linked_control_folder_is_refused_and_left(deployment, docker, capsys, tmp_path):
    """Bug class: a link planted as `data/jobs/control` followed: its target mounted into the web UI, or given mode 0700."""
    make_jobs(deployment)
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(mode=0o755)
    (deployment / "data" / "jobs" / "control").symlink_to(elsewhere)
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert code == 0 and queue is None and results is None
    assert "data/jobs/control is a link or not a folder; it was left as it is" in out
    assert (deployment / "data" / "jobs" / "control").is_symlink() and stat.S_IMODE(elsewhere.stat().st_mode) == 0o755


def test_a_control_folder_of_another_user_is_refused(deployment, docker, capsys, monkeypatch):
    """Bug class: a control folder that another user made (and can therefore replace the socket in) mounted as the scheduler's."""
    make_jobs(deployment)
    control = deployment / "data" / "jobs" / "control"
    control.mkdir(mode=0o700)
    inode = os.lstat(control).st_ino
    real_fstat = os.fstat

    def fstat(fd):
        info = real_fstat(fd)
        if info.st_ino == inode:
            return os.stat_result((info.st_mode, info.st_ino, info.st_dev, info.st_nlink, ME[0] + 1, info.st_gid, info.st_size, 0, 0, 0))
        return info

    monkeypatch.setattr(host.os, "fstat", fstat)
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert queue is None and results is None and "data/jobs/control belongs to another user than you" in out


def test_an_open_control_folder_is_said(deployment, docker, capsys):
    """Bug class: a control folder open to others mounted without a word, while the scheduler keeps its socket closed and the page refuses it."""
    make_jobs(deployment)
    control = deployment / "data" / "jobs" / "control"
    control.mkdir()
    os.chmod(control, 0o750)
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert queue == str(control) and "is open to the group or to others" in out and stat.S_IMODE(os.lstat(control).st_mode) == 0o750


def write_env(root, value):
    (root / ".env").write_text(f"OTHER_SECRET={ENV_MARKER}\nJOBS_RESULT_ROOT={value}\n")


def test_the_results_folder_comes_from_the_env_key(deployment, docker, capsys, tmp_path):
    """Bug class: the default results folder mounted while the deployment keeps its results elsewhere (the downloads then find nothing), or the setting read relative to the current folder instead of the deployment's."""
    make_jobs(deployment, results=False)
    elsewhere = tmp_path / "results-elsewhere"
    elsewhere.mkdir()
    write_env(deployment, str(elsewhere))
    assert start_mounts(deployment, docker, capsys)[2] == str(elsewhere)
    (deployment / "kept").mkdir()
    write_env(deployment, "./kept")
    assert start_mounts(deployment, docker, capsys)[2] == str(deployment / "kept")


def test_the_default_results_folder_and_a_missing_one(deployment, docker, capsys):
    """Bug class: no downloads although the default results folder is there, or a mount of a missing folder (which fails the start)."""
    make_jobs(deployment, results=False)
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert queue is not None and results is None and "downloads will not be available: the scheduler's results folder (JOBS_RESULT_ROOT) is not there" in out
    (deployment / "data" / "jobs" / "results").mkdir()
    assert start_mounts(deployment, docker, capsys)[2] == str(deployment / "data" / "jobs" / "results")


@pytest.mark.parametrize("case, says", [
    ("comma", "holds a comma"),
    ("jobs", "is data/jobs or a folder above it"),
    ("above", "is data/jobs or a folder above it"),
    ("above_linked", "is data/jobs or a folder above it"),
    ("inside_control", "lies inside data/jobs/control"),
])
def test_a_results_folder_that_must_not_be_mounted(deployment, docker, capsys, tmp_path, case, says):
    """Bug class: a results folder (which the database servers can write) that is the scheduler's data folder, holds it (directly or through a link), or lies inside the control folder, mounted into the web UI: the job store or the socket's folder would then be in reach of what the database servers write, or a comma would give Docker's mount option another field."""
    make_jobs(deployment)
    if case == "comma":
        folder = tmp_path / "a,b"
        folder.mkdir()
        value = str(folder)
    elif case == "jobs":
        value = "./data/jobs"
    elif case == "above":
        value = "./data"
    elif case == "above_linked":
        (tmp_path / "link").symlink_to(deployment / "data")
        value = str(tmp_path / "link")
    else:
        (deployment / "data" / "jobs" / "control").mkdir(mode=0o700)
        (deployment / "data" / "jobs" / "control" / "inner").mkdir()
        value = "./data/jobs/control/inner"
    write_env(deployment, value)
    code, queue, results, out = start_mounts(deployment, docker, capsys)
    assert code == 0 and queue == str(deployment / "data" / "jobs" / "control") and results is None
    assert "downloads will not be available" in out and says in out


def test_status_says_which_queue_folders_are_mounted_and_prints_no_env(deployment, docker, capsys):
    """Bug class: a status that does not say why the Queue window is missing, or that prints the deployment's `.env`."""
    make_jobs(deployment, results=False)
    host._start(str(deployment), *ME)
    capsys.readouterr()
    host.cmd_status(deployment)
    out = capsys.readouterr().out
    assert "Queue: the scheduler's control folder is mounted" in out and "the results folder is not mounted" in out and ENV_MARKER not in out


def test_the_results_folder_is_read_from_the_env_of_the_deployment_started(deployment, docker, capsys, tmp_path, monkeypatch):
    """Bug class: the results folder read from the `.env` next to the installed code (`setup.utils.ROOT`) instead of the deployment's that is started, so another deployment's results would be mounted."""
    from social_data_pipeline.setup import utils

    other = tmp_path / "other-code"
    other.mkdir()
    (other / ".env").write_text("JOBS_RESULT_ROOT=/made-up/other/results\n")
    monkeypatch.setattr(utils, "ROOT", other)
    make_jobs(deployment, results=False)
    (deployment / "mine").mkdir()
    write_env(deployment, "./mine")
    assert start_mounts(deployment, docker, capsys)[2] == str(deployment / "mine")


@pytest.mark.parametrize("text", [
    "", "JOBS_RESULT_ROOT=./a\n", "  JOBS_RESULT_ROOT = ./b  \n", "# JOBS_RESULT_ROOT=./c\nJOBS_RESULT_ROOT=./d\n", "JOBS_RESULT_ROOT=./e\nJOBS_RESULT_ROOT=./f\n",
    "OTHER=1\n\n\nJOBS_RESULT_ROOT=./g=h\n", "JOBS_RESULT_ROOT\n", "JOBS_RESULT_ROOT=\n", "\tJOBS_RESULT_ROOT=./i\r\n",
])
def test_the_env_is_read_by_the_projects_rules(tmp_path, monkeypatch, text):
    """Bug class: the host module reading `.env` by other rules than the rest of the CLI (a quote, a space, a comment, a second line of the key taken differently), so that the web UI mounts another results folder than the scheduler writes to."""
    from social_data_pipeline.setup import utils

    (tmp_path / ".env").write_text(text)
    monkeypatch.setattr(utils, "ROOT", tmp_path)
    assert host._env_of(str(tmp_path)) == utils.load_env()
    assert host._env_of(str(tmp_path / "nowhere")) == {}


# ── the Chat window's mount ─────────────────────────────────────────────────


def chat_mount(root, docker, capsys):
    """Start the web UI; returns (the chat's mount source or None, whether its setting was given, the output)."""
    capsys.readouterr()
    assert host._start(str(root), *ME) == 0
    out = capsys.readouterr().out
    created = next(call for call in docker.calls if call[1] == "create")
    mounts = [created[index + 1] for index, word in enumerate(created) if word == "--mount" and "target=/run/sdp-chat," in created[index + 1]]
    setting = "SDP_WEBUI_CHAT_SOCKET=/run/sdp-chat/runner.sock" in created
    docker.calls.clear()
    host.cmd_stop(root)
    return (mount_source(mounts[0]) if mounts else None), setting, out


def make_chat_socket(root, mode=0o700):
    folder = root / ".sdp" / "chat" / "socket"
    folder.mkdir(parents=True, exist_ok=True)
    os.chmod(folder, mode)
    return folder


def test_start_mounts_the_chats_socket_folder_read_only(deployment, docker, capsys):
    """Bug class: a Chat window without its way to the runner although the chat runs, or the folder mounted without the setting."""
    folder = make_chat_socket(deployment)
    source, setting, out = chat_mount(deployment, docker, capsys)
    assert source == str(folder) and setting and "Chat:" not in out


@pytest.mark.parametrize("case, says", [
    ("missing", "the chat's socket folder .sdp/chat/socket is not there. Set the chat up and start it"),
    ("linked", ".sdp/chat/socket or a folder above it is a link"),
    ("linked_above", ".sdp/chat/socket or a folder above it is a link"),
    ("open", ".sdp/chat/socket is open to the group or to others"),
    ("foreign", ".sdp/chat/socket belongs to another user than you"),
])
def test_a_chat_socket_folder_that_fails_its_check_is_not_mounted(deployment, docker, capsys, tmp_path, monkeypatch, case, says):
    """Bug class: a folder of someone else's choosing (a link, another user's, one others can enter and plant a socket in) mounted as the chat's, or a mount of a folder that is not there (Docker refuses the start)."""
    if case in ("linked", "linked_above"):
        elsewhere = tmp_path / "elsewhere"
        (elsewhere / "socket").mkdir(parents=True, mode=0o700)
        if case == "linked":
            (deployment / ".sdp" / "chat").mkdir()
            (deployment / ".sdp" / "chat" / "socket").symlink_to(elsewhere / "socket")
        else:
            (deployment / ".sdp" / "chat").symlink_to(elsewhere)
    elif case == "open":
        make_chat_socket(deployment, 0o750)
    elif case == "foreign":
        inode = make_chat_socket(deployment).stat().st_ino
        real_fstat = os.fstat

        def fstat(fd):
            info = real_fstat(fd)
            return os.stat_result((info.st_mode, info.st_ino, info.st_dev, info.st_nlink, ME[0] + 1, info.st_gid, info.st_size, 0, 0, 0)) if info.st_ino == inode else info

        monkeypatch.setattr(host.os, "fstat", fstat)
    source, setting, out = chat_mount(deployment, docker, capsys)
    assert source is None and not setting and f"Chat: the Chat window will not be available: {says}" in out


def test_status_says_whether_the_chats_folder_is_mounted(deployment, docker, capsys):
    """Bug class: a status that does not say why the Chat window is missing."""
    host._start(str(deployment), *ME)
    capsys.readouterr()
    host.cmd_status(deployment)
    assert "Chat: the chat's socket folder is not mounted, so the Chat window is not available" in capsys.readouterr().out
    host.cmd_stop(deployment)
    make_chat_socket(deployment)
    host._start(str(deployment), *ME)
    capsys.readouterr()
    host.cmd_status(deployment)
    assert "Chat: the chat's socket folder is mounted" in capsys.readouterr().out


# ── what setup, start and status print, in both modes ──────────────────────


def printed(root, docker, capsys, answers):
    """What setup, start and status print after their first lines: (setup's lines from "The web UI is set up.", start's from "The web UI is started.", status's from "Set up: yes")."""
    capsys.readouterr()
    assert set_up(root, docker, answers) == 0
    setup = capsys.readouterr().out
    assert host._start(str(root), UID, GID) == 0
    start = capsys.readouterr().out
    assert host.cmd_status(root) == 0
    status = capsys.readouterr().out
    return setup[setup.index("  The web UI is set up."):], start[start.index("  The web UI is started."):], status[status.index("  Set up: yes"):]


def test_what_a_list_of_addresses_prints(root, docker, capsys):
    """Bug class: an address the page answers on missing from what the operator is told, or a line of the every-address mode in the list mode."""
    fingerprint = host.certificate_fingerprint(CERT_PEM)
    addresses = "    https://localhost:9443/\n    https://127.0.0.1:9443/\n    https://192.0.2.10:9443/\n    https://pipeline.example:9443/\n"
    setup, start, status = printed(root, docker, capsys, ["9443", "pipeline.example, 192.0.2.10", "generate"])
    assert setup == f"  The web UI is set up.\n  Certificate SHA-256 fingerprint: {fingerprint}\n  Valid until: Feb 23 19:43:49 2029 GMT\n  Once started (python sdp.py webui start), the page answers on:\n" + addresses
    assert start == f"  The web UI is started.\n  Broker: {host.PROBE_LINES['answered']}\n  Certificate SHA-256 fingerprint: {fingerprint}\n  The page answers on:\n" + addresses
    assert status == f"  Set up: yes\n  Port: 9443\n  Listen addresses: 127.0.0.1, 192.0.2.10\n  Names: localhost, pipeline.example\n  Certificate SHA-256 fingerprint: {fingerprint}\n  Certificate valid until: Feb 23 19:43:49 2029 GMT\n" + addresses


def test_what_every_address_prints(root, docker, capsys):
    """Bug class: the operator told of addresses or ports the page does not answer on, not warned that every address includes networks not chosen, or not told why the browser warns about the certificate under another address."""
    fingerprint = host.certificate_fingerprint(CERT_PEM)
    every = f"  The page answers on every address of this machine, port 9443: https://<address or name>:9443/\n  {host.EVERY_WARNING}\n"
    setup, start, status = printed(root, docker, capsys, ["9443", "all, pipeline.example", "generate"])
    assert host.EVERY_WARNING == "Warning: every address includes networks you did not choose (a VPN, an interface added later, Docker's own networks). The page needs the management password everywhere."
    assert setup == f"  The web UI is set up.\n  Certificate SHA-256 fingerprint: {fingerprint}\n  Valid until: Feb 23 19:43:49 2029 GMT\n  Once started (python sdp.py webui start):\n" + every + "  The certificate is for localhost, 127.0.0.1, pipeline.example: opened under another address, the browser shows its certificate warning; accept it only when the fingerprint it shows is the one above.\n"
    assert start == f"  The web UI is started.\n  Broker: {host.PROBE_LINES['answered']}\n  Certificate SHA-256 fingerprint: {fingerprint}\n" + every
    assert status == f"  Set up: yes\n  Port: 9443\n  Listen addresses: all\n  In the certificate: localhost, 127.0.0.1, pipeline.example\n  Certificate SHA-256 fingerprint: {fingerprint}\n  Certificate valid until: Feb 23 19:43:49 2029 GMT\n" + every
    saved = json.loads((root / ".sdp" / "webui" / "config.json").read_text())
    assert saved["addresses"] == ["0.0.0.0"] and saved["names"] == ["pipeline.example"]
    created = next(call for call in docker.calls if call[1] == "create")
    assert [created[index + 1] for index, word in enumerate(created) if word == "-p"] == ["9443:8443"] and "SDP_WEBUI_ANY_HOST=1" in created


@pytest.mark.parametrize("addresses", [["0.0.0.0", "192.0.2.10"], ["::"], ["192.0.2.10", "0.0.0.0", "::"]])
def test_a_saved_every_address_of_the_previous_version_reads_as_every_address(root, docker, addresses):
    """Bug class: a settings file of the version that published 0.0.0.0 and :: as addresses refused at start, or read as a list (the page then refuses every address it was not told)."""
    set_up(root, docker)
    path = root / ".sdp" / "webui" / "config.json"
    saved = json.loads(path.read_text())
    saved["addresses"] = addresses
    path.write_text(json.dumps(saved))
    cfg = host.read_config(str(root))[0]
    assert cfg.every and cfg.addresses == tuple(address for address in addresses if address not in ("0.0.0.0", "::"))


@pytest.mark.parametrize("answer, default", [("192.0.2.10, pipeline.example, 2001:db8::5", "192.0.2.10, 2001:db8::5, pipeline.example"), ("pipeline.example, any", "all, pipeline.example"), ("", "")])
def test_a_second_setup_offers_the_previous_answer(root, docker, answer, default):
    """Bug class: a second setup that offers an empty answer (accepting it drops every address and name), or offers `0.0.0.0` where the operator typed `all`."""
    set_up(root, docker, ["8443", answer, "generate"])
    defaults = {}

    def ask(prompt, default=None, tag=None):
        defaults[tag] = default
        return "" if default is None else str(default)

    assert host._setup(str(root), ask, lambda prompt, options, default=None, tag=None: default, UID, GID) == 0
    assert defaults["webui_addresses"] == default
    assert host.reach_answer(host.read_config(str(root))[0]) == default
