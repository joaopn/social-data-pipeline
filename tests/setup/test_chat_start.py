"""Tests for `sdp chat start`, `sdp chat stop` and `sdp chat status` (`social_data_pipeline/chat_host.py`, wired in `sdp.py`), and the check `chat unsetup` gained.

The contracts pinned here, each test naming the bug class it catches:

- The two `docker create` argument lists, pinned literally: no published port, no secret in an environment, nothing mounted but what is listed (the sandbox never gets the credential, the broker's socket, Docker's socket or the data folder).
- The inspect template that tells whether an MCP container's read-only user key is set prints a fixed word and nothing of the environment; the compose listing filters by project, service and not one-off, and the working-dir label must equal the deployment folder.
- The gateway's config for each back end and each set of attached databases loads with the real `gateway.table` loader; the gateway's address is the second host address of the network's subnet.
- `start` refuses without a setup, under user-id mapping, while a container runs, with a container of another deployment, and with no attachable database (one "why" line per database), changing nothing; a database without the key is never attached; a step that fails in the middle removes what the start made; the token is in no output, argument or environment.
- `stop` and `status`: their outputs and exit codes; a database container replaced since the start shows as dropped; a stopped container attached to the network is detached before the network is removed (else it could no longer start).
- The work folder's size: links are not followed, the walk is bounded; `unsetup` refuses while a container exists; the CLI parses the three commands.

Docker is a stand-in that records every call and answers from a small model of containers and networks; nothing reaches a real Docker.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

import sdp
from social_data_pipeline import chat_host as host
from social_data_pipeline.gateway import table as gateway_table

UID, GID = 1000, 1000
MARKER = "made-up-chat-token-marker-5d1e9a7c3b"
MONGO_ID = "a" * 64
POSTGRES_ID = "b" * 64
STARROCKS_ID = "c" * 64
NEW_MONGO_ID = "d" * 64
CHAT = Path(".sdp") / "chat"


class ChatDocker:
    """Records every argument list in `calls` and answers from a model: `containers` (the chat's, by name: status and root label), `networks` (by name: labels, internal, subnet, members), `compose` (the MCP containers by id: service, working folder, whether the read-only user key is set, running, aliases), `images`. `fail(arguments)` returns a stderr for a call that is to fail, else None; `gateway_exits` makes the gateway end at its start."""

    def __init__(self, root):
        self.root = str(root)
        self.calls = []
        self.images = {host.GATEWAY_IMAGE, host.SANDBOX_IMAGE}
        self.security = ["name=seccomp,profile=builtin", "name=cgroupns"]
        self.containers = {}
        self.networks = {}
        self.compose = {}
        self.aliases = {}
        self.fail = lambda arguments: None
        self.gateway_exits = False
        self.subnets = 0
        # An older Docker: it stores the gateway mode option and still gives the machine a gateway address.
        self.ignores_isolation = False
        # A Docker that makes the network again with another subnet than the one given.
        self.moves_subnet = False
        # The gateway says only its model listener listens (it never gets to the others).
        self.gateway_slow = False

    def add(self, container_id, service, *, key=True, folder=None, running=True):
        self.compose[container_id] = {"service": service, "dir": folder or self.root, "key": key, "running": running}

    @staticmethod
    def answer(code, stdout="", stderr=""):
        return subprocess.CompletedProcess([], code, stdout, stderr)

    def exists(self, container):
        return container in self.containers or container in self.compose

    def members(self, network, every):
        found = self.networks.get(network, {}).get("members", [])
        return [c for c in found if every or (self.compose[c]["running"] if c in self.compose else self.containers[c]["status"] == "running")]

    def __call__(self, arguments, *, capture=True, timeout=600):
        arguments = list(arguments)
        self.calls.append(arguments)
        assert arguments[0] == "docker"
        failed = self.fail(arguments)
        if failed is not None:
            return self.answer(1, stderr=failed)
        words = arguments[1:3]
        if arguments[1] == "info":
            return self.answer(0, json.dumps(self.security))
        if words == ["image", "inspect"]:
            return self.answer(0, "sha256:made-up") if arguments[-1] in self.images else self.answer(1, stderr=f"Error: No such image: {arguments[-1]}")
        if words == ["container", "inspect"]:
            return self.inspect(arguments)
        if arguments[1] == "ps":
            return self.ps(arguments)
        if arguments[1] == "network":
            return self.network(arguments)
        if arguments[1] == "create":
            name = arguments[arguments.index("--name") + 1]
            label = arguments[arguments.index("--label") + 1].split("=", 1)[1]
            self.containers[name] = {"status": "created", "label": label, "arguments": arguments}
            self.networks[arguments[arguments.index("--network") + 1]]["members"].append(name)
            return self.answer(0, "made-up-id")
        if arguments[1] == "start":
            name = arguments[2]
            self.containers[name]["status"] = "exited" if name == host.GATEWAY_CONTAINER and self.gateway_exits else "running"
            return self.answer(0, name)
        if arguments[1] == "logs":
            if self.gateway_exits:
                return self.answer(0, "", "gateway: refusing to start: listen: must be one IP address, not a wildcard\n")
            config = json.loads((Path(self.root) / CHAT / "gateway" / "config.json").read_text())
            listeners = config["listeners"][:1] if self.gateway_slow else config["listeners"]
            return self.answer(0, "".join(f"gateway: {item['kind']} listening on port {item['port']}\n" for item in listeners))
        if arguments[1] == "stop":
            self.containers[arguments[-1]]["status"] = "exited"
            return self.answer(0, arguments[-1])
        if arguments[1] == "rm":
            name = arguments[-1]
            if name not in self.containers:
                return self.answer(1, stderr=f"Error response from daemon: No such container: {name}")
            del self.containers[name]
            for network in self.networks.values():
                if name in network["members"]:
                    network["members"].remove(name)
            return self.answer(0, name)
        raise AssertionError(f"an unexpected Docker call: {arguments}")

    def inspect(self, arguments):
        template, target = arguments[4], arguments[5]
        if target in self.compose and template.startswith("{{range .Config.Env}}"):
            return self.answer(0, host.KEY_SET if self.compose[target]["key"] else "")
        if target in self.containers and template.startswith("{{json .State.Status}}"):
            entry = self.containers[target]
            return self.answer(0, f"{json.dumps(entry['status'])}\n{json.dumps({host.ROOT_LABEL: entry['label']})}\n")
        return self.answer(1, stderr=f"Error: No such container: {target}")

    def ps(self, arguments):
        every = "-a" in arguments
        filters = [arguments[index + 1] for index, word in enumerate(arguments) if word == "--filter"]
        form = arguments[arguments.index("--format") + 1]
        if any(item.startswith("name=") for item in filters):
            name = filters[0].removeprefix("name=^/").removesuffix("$")
            return self.answer(0, "made-up-id\n" if name in self.containers else "")
        network = next((item.removeprefix("network=") for item in filters if item.startswith("network=")), None)
        if network is not None:
            found = self.members(network, every)
        else:
            found = [c for c in self.compose if self.compose[c]["running"]]
        labels = [item.removeprefix("label=") for item in filters if item.startswith("label=")]
        if labels:
            found = [c for c in found if c in self.compose and all(self.matches(c, label) for label in labels)]
        if form == "{{.ID}}":
            return self.answer(0, "".join(f"{c}\n" for c in found))
        return self.answer(0, "".join(json.dumps([c, self.compose[c]["dir"], self.compose[c]["service"]]) + "\n" for c in found))

    def matches(self, container, label):
        key, _, value = label.partition("=")
        entry = self.compose[container]
        return {host.PROJECT_LABEL: "sdp", host.SERVICE_LABEL: entry["service"], host.ONEOFF_LABEL: "False"}.get(key) == value

    def network(self, arguments):
        word = arguments[2]
        if word == "ls":
            return self.answer(0, "".join(f"{name}\n" for name in ["bridge", *self.networks]))
        name = arguments[-1]
        if word == "inspect":
            if name not in self.networks:
                return self.answer(1, stderr=f"Error response from daemon: network {name} not found")
            entry = self.networks[name]
            if arguments[4] == "{{json .Labels}}":
                return self.answer(0, json.dumps(entry["labels"]) + "\n")
            assert arguments[4] == "{{json .Internal}}\n{{json .Options}}\n{{json .IPAM.Config}}"
            ipam = {"Subnet": entry["subnet"]}
            if not entry["options"] or self.ignores_isolation:
                ipam["Gateway"] = entry["subnet"].rsplit(".", 1)[0] + ".1"
            return self.answer(0, f"{json.dumps(entry['internal'])}\n{json.dumps(entry['options'])}\n{json.dumps([ipam])}\n")
        if word == "create":
            assert name not in self.networks
            label = arguments[arguments.index("--label") + 1].split("=", 1)
            subnet = arguments[arguments.index("--subnet") + 1] if "--subnet" in arguments and not self.moves_subnet else f"10.201.{self.subnets}.0/24"
            self.subnets += 1
            options = dict([arguments[arguments.index("-o") + 1].split("=", 1)]) if "-o" in arguments else {}
            self.networks[name] = {"labels": {label[0]: label[1]}, "internal": "--internal" in arguments, "options": options, "subnet": subnet, "members": [], "arguments": arguments}
            return self.answer(0, "made-up-network-id")
        if word == "rm":
            if name not in self.networks:
                return self.answer(1, stderr=f"Error response from daemon: network {name} not found")
            if self.members(name, every=False):
                return self.answer(1, stderr=f"Error response from daemon: error while removing network: network {name} has active endpoints")
            del self.networks[name]
            return self.answer(0, name)
        network, container = arguments[-2], arguments[-1]
        if not self.exists(container):
            return self.answer(1, stderr=f"Error response from daemon: No such container: {container}")
        members = self.networks[network]["members"]
        if word == "connect":
            members.append(container)
            if "--alias" in arguments:
                self.aliases[container] = arguments[arguments.index("--alias") + 1]
            return self.answer(0)
        if word == "disconnect":
            if container not in members:
                return self.answer(1, stderr=f"Error response from daemon: container {container} is not connected to network {network}")
            members.remove(container)
            return self.answer(0)
        raise AssertionError(f"an unexpected Docker call: {arguments}")


@pytest.fixture
def root(tmp_path):
    folder = tmp_path / "d"
    folder.mkdir(mode=0o700)
    return folder.resolve()


@pytest.fixture
def docker(monkeypatch, root):
    fake = ChatDocker(root)
    monkeypatch.setattr(host, "_docker", fake)
    return fake


def set_up(root, backend="claude-code", token=MARKER):
    """The state `chat setup` leaves, written directly."""
    chat = root / CHAT
    for folder in (root / ".sdp", chat, chat / "credential", chat / "work", chat / "socket"):
        folder.mkdir(mode=0o700, exist_ok=True)
    saved = {"backend": backend, "work_cap_gib": 10, "claude_code_version": host.CLAUDE_CODE_VERSION}
    if backend == "openai-compatible":
        saved.update(upstream="https://openrouter.ai/api/v1", model="vendor/model-1")
    (chat / "config.json").write_text(json.dumps(saved))
    (chat / "credential" / "model.token").write_text(token)
    os.chmod(chat / "credential" / "model.token", 0o600)


def start(root):
    return host.cmd_start(root)


def made_calls(docker):
    """The calls that make or change something."""
    return [call for call in docker.calls if call[1] in ("create", "start") or call[1:3] in (["network", "create"], ["network", "connect"])]


# ── the argument lists ──────────────────────────────────────────────────────


def test_the_gateway_argument_list_is_pinned():
    """Bug class: a published port, a writable mount, a secret in an environment, a missing confinement flag, or the gateway on another network than the internal one."""
    assert host.gateway_arguments("/srv/d", UID, GID, "10.201.0.2") == [
        "docker", "create", "--name", "sdp-gateway", "--label", "sdp.chat.root=/srv/d", "--restart", "no",
        "--user", "1000:1000", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--pids-limit", "128", "--memory", "256m", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=16m",
        "--network", "sdp-chat", "--ip", "10.201.0.2",
        "--mount", "type=bind,source=/srv/d/.sdp/chat/gateway,target=/run/sdp-gateway,readonly",
        "--mount", "type=bind,source=/srv/d/.sdp/chat/credential,target=/run/sdp-credential,readonly",
        "sdp-gateway",
    ]


def test_the_sandbox_argument_list_is_pinned():
    """Bug class: the sandbox given the credential, the broker's or Docker's socket, the data folder, a second network, a published port, or a secret in its environment."""
    assert host.sandbox_arguments("/srv/d", UID, GID, "10.201.0.2", "claude-code", ["mongo", "starrocks"], 7) == [
        "docker", "create", "--name", "sdp-sandbox", "--label", "sdp.chat.root=/srv/d", "--restart", "no",
        "--user", "1000:1000", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--init", "--pids-limit", "512", "--memory", "4g", "--tmpfs", "/tmp:rw,nosuid,nodev,size=1g",
        "--network", "sdp-chat",
        "--mount", "type=bind,source=/srv/d/.sdp/chat/work,target=/work",
        "--mount", "type=bind,source=/srv/d/.sdp/chat/socket,target=/run/sdp-chat",
        "-e", "SDP_CHAT_GATEWAY=10.201.0.2", "-e", "HOME=/work",
        "-e", "SDP_CHAT_BACKEND=claude-code", "-e", "SDP_CHAT_DATABASES=mongo,starrocks", "-e", "SDP_CHAT_CAP_GIB=7",
        "sdp-sandbox",
    ]


def test_the_key_template_prints_a_fixed_word_and_nothing_of_the_environment():
    """Bug class: a template that prints an environment entry (the same list holds the database passwords) to tell whether the read-only user is set."""
    template = host.key_template("MCP_MONGODB_USER")
    assert template == '{{range .Config.Env}}{{if and (ne . "MCP_MONGODB_USER=") (eq (index (split . "=") 0) "MCP_MONGODB_USER")}}read-only-user-set{{end}}{{end}}'
    assert "{{.}}" not in template and "json" not in template and "print" not in template


def test_the_compose_listing_is_filtered_by_project_service_and_not_one_off():
    """Bug class: a container of another compose project, a one-off `compose run` container, or another service attached as the database's MCP server."""
    assert host.compose_listing("mongo-mcp") == [
        "docker", "ps", "--no-trunc",
        "--filter", "label=com.docker.compose.project=sdp", "--filter", "label=com.docker.compose.service=mongo-mcp", "--filter", "label=com.docker.compose.oneoff=False",
        "--format", '[{{json .ID}},{{json (.Label "com.docker.compose.project.working_dir")}},{{json (.Label "com.docker.compose.service")}}]',
    ]


def test_the_working_folder_label_must_be_the_deployment_folder(root, tmp_path):
    """Bug class: the MCP server of another deployment on the same Docker (or a relative working folder) taken as this one's."""
    link = tmp_path / "link"
    link.symlink_to(root)
    lines = "\n".join([json.dumps([MONGO_ID, str(link), "mongo-mcp"]), json.dumps([POSTGRES_ID, str(tmp_path), "postgres-mcp"]), json.dumps([STARROCKS_ID, "d", "starrocks-mcp"]), json.dumps(["short", str(root), "mongo-mcp"]), "not json"])
    assert host.compose_containers(lines, str(root)) == [(MONGO_ID, "mongo-mcp")]


# ── the gateway's config ────────────────────────────────────────────────────


@pytest.mark.parametrize("names", [("mongo",), ("postgres", "starrocks"), ("postgres", "mongo", "starrocks")])
@pytest.mark.parametrize("backend", ["claude-code", "openai-compatible"])
def test_the_gateway_config_loads_with_the_gateways_own_loader(tmp_path, backend, names):
    """Bug class: a config the gateway refuses at its start (it then ends and the chat never works), a model listener without the measured beta list, or an MCP listener on the wrong port, upstream or paths."""
    settings = {"backend": backend, "upstream": "https://openrouter.ai/api/v1", "model": "vendor/model-1"}
    databases = [db for db in host.DATABASES if db.name in names]
    path = tmp_path / "config.json"
    path.write_text(json.dumps(host.gateway_config("10.201.0.2", settings, databases)))
    table = gateway_table.load(str(path))
    assert table.listen == "10.201.0.2" and table.credential_file == "/run/sdp-credential/model.token"
    model = table.listeners[0]
    assert model.port == 8080
    if backend == "claude-code":
        assert model.kind == "model-anthropic" and model.beta_allowed == frozenset(host.BETA_ALLOWED) and model.tool_types_allowed == frozenset()
        assert len(host.BETA_ALLOWED) == 10 and "oauth-2025-04-20" in host.BETA_ALLOWED
    else:
        assert model.kind == "model-openai" and model.model == "vendor/model-1" and (model.upstream.host, model.upstream.base) == ("openrouter.ai", "/api/v1")
    expected = {"postgres": (8001, "sdp-chat-postgres-mcp", 8000, ("/sse", "/messages/")), "mongo": (8002, "sdp-chat-mongo-mcp", 3000, ("/mcp",)), "starrocks": (8003, "sdp-chat-starrocks-mcp", 9000, ("/mcp",))}
    assert [(lst.port, lst.upstream.host, lst.upstream.port, lst.paths) for lst in table.listeners[1:]] == [expected[name] for name in names]
    assert all(lst.kind == "mcp" and lst.upstream.scheme == "http" for lst in table.listeners[1:])


@pytest.mark.parametrize(("subnet", "gateway", "expected"), [("10.201.0.0/24", "", "10.201.0.2"), ("10.201.0.0/24", "10.201.0.1", "10.201.0.2"), ("172.30.0.0/16", "172.30.0.2", "172.30.0.3")])
def test_the_gateways_address_is_the_subnets_second_host_address(subnet, gateway, expected):
    """Bug class: the gateway given the network's own gateway address (the bridge on the host) or an address outside the subnet."""
    assert host.gateway_address(subnet, gateway) == expected


@pytest.mark.parametrize("subnet", ["fd00::/64", "10.201.0.0/31", "not a subnet"])
def test_a_subnet_without_room_or_not_ipv4_is_refused(subnet):
    """Bug class: an IPv6 or too small subnet giving the gateway no usable address, or a traceback."""
    with pytest.raises(host.Refused):
        host.gateway_address(subnet, "")


# ── start ───────────────────────────────────────────────────────────────────


def test_start_attaches_the_database_with_a_read_only_user(root, docker, capsys):
    """Bug class: the networks, the gateway's address, its config or the order of the steps wrong (the gateway must be on both networks before it starts), or the summary not naming what the chat can read and why the rest was left out."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(POSTGRES_ID, "postgres-mcp", key=False)
    assert start(root) == 0
    assert docker.networks["sdp-chat"]["internal"] and "--subnet" in docker.networks["sdp-chat"]["arguments"] and docker.networks["sdp-chat"]["options"] == {"com.docker.network.bridge.gateway_mode_ipv4": "isolated"}
    assert not docker.networks["sdp-chat-up"]["internal"]
    assert all(entry["labels"] == {host.ROOT_LABEL: str(root)} for entry in docker.networks.values())
    address = host.gateway_address(docker.networks["sdp-chat"]["subnet"])
    config = root / CHAT / "gateway" / "config.json"
    assert oct(config.stat().st_mode & 0o777) == "0o600" and oct(config.parent.stat().st_mode & 0o777) == "0o700"
    table = gateway_table.load(str(config))
    assert table.listen == address and [lst.port for lst in table.listeners] == [8080, 8002]
    assert made_calls(docker) == [
        ["docker", "network", "create", "--driver", "bridge", "--label", f"{host.ROOT_LABEL}={root}", "sdp-chat-up"],
        host.internal_network_arguments(str(root)),
        host.internal_network_arguments(str(root), docker.networks["sdp-chat"]["subnet"]),
        host.gateway_arguments(str(root), os.getuid(), os.getgid(), address),
        ["docker", "network", "connect", "sdp-chat-up", "sdp-gateway"],
        ["docker", "start", "sdp-gateway"],
        ["docker", "network", "connect", "--alias", "sdp-chat-mongo-mcp", "sdp-chat-up", MONGO_ID],
        host.sandbox_arguments(str(root), os.getuid(), os.getgid(), address, "claude-code", ["mongo"], host.DEFAULT_CAP_GIB),
        ["docker", "start", "sdp-sandbox"],
    ]
    out = capsys.readouterr().out
    assert "The chat is started." in out and "Back end: claude-code" in out and "The chat can read: mongo" in out
    assert "postgres: left out: its MCP server is not on a read-only user" in out and "starrocks: left out: its MCP server does not run" in out
    assert "The Chat page itself is not part of this version yet." in out


def test_a_database_without_the_read_only_user_key_is_never_attached(root, docker):
    """Bug class (security): an MCP server on the database's admin user put in the chat's reach because it runs."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(POSTGRES_ID, "postgres-mcp", key=False)
    docker.add(STARROCKS_ID, "starrocks-mcp", key=False)
    assert start(root) == 0
    connected = [call[-1] for call in docker.calls if call[1:3] == ["network", "connect"]]
    assert connected == ["sdp-gateway", MONGO_ID]
    assert [lst.port for lst in gateway_table.load(str(root / CHAT / "gateway" / "config.json")).listeners] == [8080, 8002]


def test_with_no_attachable_database_start_refuses_and_makes_nothing(root, docker, tmp_path, capsys):
    """Bug class: a chat started that can read nothing, a "why" line missing or wrong, or a refusal that leaves a network or a container behind."""
    set_up(root)
    docker.add(POSTGRES_ID, "postgres-mcp", key=False)
    docker.add(MONGO_ID, "mongo-mcp", folder=str(tmp_path))
    docker.add(STARROCKS_ID, "starrocks-mcp", running=False)
    assert start(root) == 1
    assert made_calls(docker) == [] and docker.networks == {} and docker.containers == {}
    out = capsys.readouterr().out
    assert "no database can be read by the chat" in out
    assert "postgres: its MCP server is not on a read-only user" in out
    assert "mongo: its MCP server does not run" in out and "starrocks: its MCP server does not run" in out
    for command in ("python sdp.py db setup", "python sdp.py db setup-mcp", "python sdp.py db start"):
        assert command in out
    assert not (root / CHAT / "gateway" / "config.json").exists()


@pytest.mark.parametrize("missing", ["config", "token", "gateway image", "sandbox image"])
def test_start_refuses_without_a_completed_setup(root, docker, capsys, missing):
    """Bug class: a start that runs on half a setup (no settings, no token, an image missing) and fails later with Docker's words."""
    set_up(root)
    if missing == "config":
        (root / CHAT / "config.json").unlink()
    elif missing == "token":
        (root / CHAT / "credential" / "model.token").unlink()
    else:
        docker.images.discard(host.GATEWAY_IMAGE if missing == "gateway image" else host.SANDBOX_IMAGE)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 1
    assert made_calls(docker) == []
    assert "python sdp.py chat setup" in capsys.readouterr().out


@pytest.mark.parametrize("option", ["name=rootless", "name=userns"])
def test_start_refuses_docker_with_user_id_mapping(root, docker, capsys, option):
    """Bug class: the sandbox's files landing on the host owned by a mapped uid, the gateway unable to read the operator's token."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.security = ["name=seccomp,profile=builtin", option]
    assert start(root) == 1
    assert made_calls(docker) == [] and "user-id mapping" in capsys.readouterr().out


@pytest.mark.parametrize("name", ["sdp-gateway", "sdp-sandbox"])
def test_start_refuses_while_a_container_runs(root, docker, capsys, name):
    """Bug class: a second start replacing a running chat's networks under it."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.containers[name] = {"status": "running", "label": str(root)}
    assert start(root) == 1
    assert made_calls(docker) == [] and docker.containers[name]["status"] == "running"
    assert "already runs" in capsys.readouterr().out


def test_start_refuses_a_container_or_network_of_another_deployment(root, docker, capsys):
    """Bug class: the chat of another deployment on the same Docker removed or taken over."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.networks["sdp-chat-up"] = {"labels": {host.ROOT_LABEL: "/elsewhere"}, "internal": False, "subnet": "10.9.0.0/24", "members": []}
    assert start(root) == 1
    assert made_calls(docker) == [] and "sdp-chat-up" in docker.networks
    assert "another deployment" in capsys.readouterr().out


def test_start_removes_leftovers_that_do_not_run(root, docker, capsys):
    """Bug class: a container or network left by an interrupted start blocking every later start."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.containers["sdp-gateway"] = {"status": "exited", "label": str(root)}
    docker.networks["sdp-chat"] = {"labels": {host.ROOT_LABEL: str(root)}, "internal": True, "subnet": "10.9.0.0/24", "members": ["sdp-gateway"]}
    assert start(root) == 0
    assert ["docker", "rm", "--force", "sdp-gateway"] in docker.calls and ["docker", "network", "rm", "sdp-chat"] in docker.calls
    assert docker.containers["sdp-gateway"]["status"] == "running"


def failing(word, target):
    """A `fail` for the call that starts with `word` and ends with `target` (for `create`, the image, named as the container)."""
    return lambda arguments: "Error response from daemon: made-up failure" if arguments[1:1 + len(word)] == word and arguments[-1] == target else None


@pytest.mark.parametrize(("step", "word", "target"), [
    ("gateway start", ["start"], "sdp-gateway"),
    ("database attach", ["network", "connect"], MONGO_ID),
    ("sandbox create", ["create"], "sdp-sandbox"),
    ("sandbox start", ["start"], "sdp-sandbox"),
])
def test_a_failing_step_removes_what_the_start_made(root, docker, capsys, step, word, target):
    """Bug class: a failed start leaving a container, a network or a database attached to the chat's network (the next start then refuses, or the database stays in reach)."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(POSTGRES_ID, "postgres-mcp", key=False)
    docker.fail = failing(word, target)
    assert start(root) == 1
    assert docker.containers == {} and docker.networks == {}
    assert not (root / CHAT / "gateway" / "config.json").exists()
    out = capsys.readouterr().out
    assert "made-up failure" in out and "What this start made was removed" in out


def test_a_gateway_that_ends_at_its_start_is_reported_and_removed(root, docker, capsys):
    """Bug class: a start that says the chat runs while the gateway refused its config and ended."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.gateway_exits = True
    assert start(root) == 1
    assert docker.containers == {} and docker.networks == {}
    assert "gateway: refusing to start" in capsys.readouterr().out


@pytest.mark.parametrize("backend", ["claude-code", "openai-compatible"])
def test_the_token_is_in_no_output_argument_or_environment(root, docker, capsys, backend):
    """Bug class (security): the model's secret read by the host side and put in an argument list, an environment, the gateway's config or a line of output."""
    set_up(root, backend)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    host.cmd_status(root)
    assert host.cmd_stop(root) == 0
    assert MARKER not in capsys.readouterr().out
    assert all(MARKER not in word for call in docker.calls for word in call)
    assert MARKER not in json.dumps(host.gateway_config("10.201.0.2", host._settings_or_refuse(str(root)), list(host.DATABASES)))


# ── stop and status ─────────────────────────────────────────────────────────


def test_stop_removes_everything_and_detaches_the_databases(root, docker, capsys):
    """Bug class: a database left attached to a network that is then removed (it could no longer start), or a container or network of the chat left."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    docker.compose[MONGO_ID]["running"] = False
    assert host.cmd_stop(root) == 0
    assert docker.containers == {} and docker.networks == {}
    assert ["docker", "network", "disconnect", "sdp-chat-up", MONGO_ID] in docker.calls
    assert not (root / CHAT / "gateway" / "config.json").exists()
    assert "The chat has stopped." in capsys.readouterr().out


def test_stop_takes_a_database_gone_meanwhile_for_no_error(root, docker, capsys):
    """Bug class: a stop that fails because a database's container was removed while the chat ran."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    docker.compose[MONGO_ID]["running"] = False
    docker.fail = lambda arguments: f"Error response from daemon: No such container: {MONGO_ID}" if arguments[1:3] == ["network", "disconnect"] and arguments[-1] == MONGO_ID else None
    assert host.cmd_stop(root) == 0
    assert docker.containers == {} and docker.networks == {}


def test_stop_when_nothing_runs_is_exit_code_1(root, docker, capsys):
    """Bug class: a stop that says it stopped something that did not run."""
    assert host.cmd_stop(root) == 1
    assert "The chat is not running." in capsys.readouterr().out


def test_status_names_the_attached_database_and_one_replaced_since(root, docker, capsys):
    """Bug class: a status that shows what start attached rather than what is attached now (a database whose container was replaced is out of the chat's reach)."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    capsys.readouterr()
    assert host.cmd_status(root) == 0
    out = capsys.readouterr().out
    assert "Gateway: running" in out and "Sandbox: running" in out and "Back end: claude-code" in out
    assert "Databases the chat can read now: mongo" in out and "dropped" not in out
    del docker.compose[MONGO_ID]
    docker.networks["sdp-chat-up"]["members"].remove(MONGO_ID)
    docker.add(NEW_MONGO_ID, "mongo-mcp")
    assert host.cmd_status(root) == 0
    out = capsys.readouterr().out
    assert "Databases the chat can read now: none" in out
    assert "mongo: dropped since the start" in out and "python sdp.py chat stop" in out and "python sdp.py chat start" in out


def test_status_when_nothing_runs_is_exit_code_1(root, docker, capsys):
    """Bug class: a status that answers 0 for a chat that does not run."""
    set_up(root)
    assert host.cmd_status(root) == 1
    out = capsys.readouterr().out
    assert "Gateway: not running" in out and "Sandbox: not running" in out and "Work folder: 0 B of the soft cap of 10 GiB" in out


# ── the work folder's size ──────────────────────────────────────────────────


def test_the_walk_sums_the_files_and_follows_no_link(tmp_path):
    """Bug class: a link planted in the work folder (the sandbox writes there) making the size count a folder or a file outside."""
    work = tmp_path / "work"
    (work / "a" / "b").mkdir(parents=True)
    (work / "a" / "one").write_bytes(b"x" * 100)
    (work / "a" / "b" / "two").write_bytes(b"y" * 23)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "big").write_bytes(b"z" * 10000)
    (work / "a" / "to-folder").symlink_to(outside)
    (work / "to-file").symlink_to(outside / "big")
    fd = os.open(work, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert host.measure(fd) == (123, True)
        assert host.measure(fd, limit=3)[1] is False
    finally:
        os.close(fd)


def test_the_walk_stops_at_its_bound_and_status_says_more_than(root, docker, capsys, monkeypatch):
    """Bug class: a status that walks millions of entries the sandbox made, or prints a partial size as the whole."""
    set_up(root)
    for index in range(6):
        (root / CHAT / "work" / f"f{index}").write_bytes(b"x")
    monkeypatch.setattr(host, "WALK_LIMIT", 5)
    host.cmd_status(root)
    assert "more than 5 entries" in capsys.readouterr().out


# ── unsetup, the CLI ────────────────────────────────────────────────────────


@pytest.mark.parametrize("name", ["sdp-gateway", "sdp-sandbox"])
def test_unsetup_refuses_while_a_container_exists(root, docker, capsys, name):
    """Bug class: the credential and the work folder removed under a running sandbox and gateway."""
    set_up(root)
    docker.containers[name] = {"status": "running", "label": str(root)}
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 1
    assert "stop it first: python sdp.py chat stop" in capsys.readouterr().out
    assert (root / CHAT / "credential" / "model.token").exists() and not any(call[1] == "image" for call in docker.calls)


@pytest.mark.parametrize("command", ["start", "stop", "status"])
def test_the_cli_reaches_the_host_module_with_the_deployment_folder(root, monkeypatch, command):
    """Bug class: a command missing from the parser, or wired to the wrong function or folder."""
    seen = []
    monkeypatch.setattr(sdp, "ROOT", root)
    monkeypatch.setattr(host, f"cmd_{command}", lambda folder: seen.append(folder) or 7)
    args = sdp.build_parser().parse_args(["chat", command])
    assert args.func(args) == 7 and seen == [root]


# ── review round 1 ──────────────────────────────────────────────────────────


def test_the_internal_network_arguments_are_pinned():
    """Bug class: `sdp-chat` made as a plain internal bridge, which gives this machine the network's gateway address (through which the sandbox reaches every service of the machine that listens on all addresses)."""
    assert host.internal_network_arguments("/srv/d") == ["docker", "network", "create", "--internal", "--ipv6=false", "-o", "com.docker.network.bridge.gateway_mode_ipv4=isolated", "--label", "sdp.chat.root=/srv/d", "sdp-chat"]
    assert host.internal_network_arguments("/srv/d", "10.201.0.0/24") == ["docker", "network", "create", "--internal", "--ipv6=false", "-o", "com.docker.network.bridge.gateway_mode_ipv4=isolated", "--subnet", "10.201.0.0/24", "--label", "sdp.chat.root=/srv/d", "sdp-chat"]


ISOLATED = {"com.docker.network.bridge.gateway_mode_ipv4": "isolated"}


@pytest.mark.parametrize(("internal", "options", "ipam"), [
    (False, ISOLATED, [{"Subnet": "10.201.0.0/24"}]),
    (True, {}, [{"Subnet": "10.201.0.0/24"}]),
    (True, {"com.docker.network.bridge.gateway_mode_ipv4": "nat"}, [{"Subnet": "10.201.0.0/24"}]),
    (True, ISOLATED, [{"Subnet": "10.201.0.0/24", "Gateway": "10.201.0.1"}]),
    (True, ISOLATED, []),
    (True, ISOLATED, None),
])
def test_the_read_back_refuses_each_wrong_shape(internal, options, ipam):
    """Bug class: an older Docker that stores the gateway mode and still gives the machine an address on the network, or a network not internal, taken for an isolated one."""
    with pytest.raises(host.Refused):
        host.isolated_subnet(internal, options, ipam)
    assert host.isolated_subnet(True, ISOLATED, [{"Subnet": "10.201.0.0/24"}]) == "10.201.0.0/24"


def test_start_refuses_and_cleans_up_when_the_network_comes_back_with_a_gateway(root, docker, capsys):
    """Bug class: the sandbox started on a network on which the machine has an address."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.ignores_isolation = True
    assert start(root) == 1
    assert docker.networks == {} and docker.containers == {}
    out = capsys.readouterr().out
    assert "a gateway address of this machine" in out and "a newer Docker is needed" in out


def test_start_refuses_when_docker_refuses_the_gateway_mode(root, docker, capsys):
    """Bug class: a fall back to a plain internal network when Docker does not know the isolated gateway mode."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.fail = lambda arguments: "Error response from daemon: invalid option com.docker.network.bridge.gateway_mode_ipv4" if arguments[1:3] == ["network", "create"] and "--internal" in arguments else None
    assert start(root) == 1
    assert docker.networks == {} and docker.containers == {}
    assert [call for call in docker.calls if call[1:3] == ["network", "create"] and "--internal" in call and "-o" not in call] == []
    out = capsys.readouterr().out
    assert "cannot keep the machine itself off the chat's network; a newer Docker is needed" in out and "invalid option com.docker.network.bridge.gateway_mode_ipv4" in out


def test_the_subnet_coming_back_different_is_refused(root, docker, capsys):
    """Bug class: the gateway given an address outside the network it is created on."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.moves_subnet = True
    assert start(root) == 1
    assert docker.networks == {} and "was not made with the subnet" in capsys.readouterr().out


def test_a_failing_gateway_connect_cleans_up(root, docker, capsys):
    """Bug class: a gateway on the internal network only (no way to the model) left behind, or its network."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.fail = failing(["network", "connect"], "sdp-gateway")
    assert start(root) == 1
    assert docker.containers == {} and docker.networks == {}
    assert "the gateway could not be connected" in capsys.readouterr().out


def test_the_wait_for_the_gateway_runs_out(root, docker, capsys, monkeypatch):
    """Bug class: a start that waits forever on a gateway that never listens on every port, or reports it as started."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.gateway_slow = True
    monkeypatch.setattr(host, "GATEWAY_WAIT_SECONDS", -1)
    monkeypatch.setattr(host.time, "sleep", lambda seconds: None)
    assert start(root) == 1
    assert docker.containers == {} and docker.networks == {}
    out = capsys.readouterr().out
    assert "did not listen within" in out and "model-anthropic listening on port 8080" in out


@pytest.mark.parametrize("failure", ["gateway ends", "sandbox create"])
def test_the_token_is_in_nothing_on_a_failure_path(root, docker, capsys, failure):
    """Bug class (security): a failure path (the gateway's last log lines printed, a failed create) printing or passing the token."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    if failure == "gateway ends":
        docker.gateway_exits = True
    else:
        docker.fail = failing(["create"], "sdp-sandbox")
    assert start(root) == 1
    out = capsys.readouterr().out
    assert ("gateway: refusing to start" in out) == (failure == "gateway ends")
    assert MARKER not in out and all(MARKER not in word for call in docker.calls for word in call)


def test_a_docker_failure_during_clean_up_does_not_stop_it(root, docker, capsys, monkeypatch):
    """Bug class: Docker failing on one removal ending the clean-up (the networks and the database attachment left) and hiding the failure that started it."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    real = docker.__call__

    def flaky(arguments, **keywords):
        if arguments[1] == "start" and arguments[-1] == "sdp-sandbox":
            return docker.answer(1, stderr="Error response from daemon: the first failure")
        if arguments[1] == "rm" and arguments[-1] == "sdp-gateway":
            raise host.Refused("Docker cannot be run, or did not answer in time")
        return real(arguments, **keywords)

    monkeypatch.setattr(host, "_docker", flaky)
    assert start(root) == 1
    assert docker.networks == {} and "sdp-sandbox" not in docker.containers
    out = capsys.readouterr().out
    assert out.index("the first failure") < out.index("Docker cannot be run, or did not answer in time")
    assert "the container sdp-gateway could not be removed" in out


def test_the_walk_reads_no_more_names_than_its_bound(tmp_path, monkeypatch):
    """Bug class: a bounded walk that still lists (and holds) every name of a folder the sandbox filled with millions of entries."""
    work = tmp_path / "work"
    for folder in range(3):
        (work / f"f{folder}").mkdir(parents=True)
        for index in range(200):
            (work / f"f{folder}" / f"n{index}").write_bytes(b"x")
    returned = []
    real = os.scandir

    class Counted:
        def __init__(self, fd):
            self.inner = real(fd)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.inner.close()

        def __iter__(self):
            return self

        def __next__(self):
            entry = next(self.inner)
            returned.append(entry.name)
            return entry

    monkeypatch.setattr(host.os, "scandir", Counted)
    fd = os.open(work, os.O_RDONLY | os.O_DIRECTORY)
    try:
        assert host.measure(fd, limit=250)[1] is False
        assert len(returned) <= 251
        returned.clear()
        assert host.measure(fd, limit=603) == (600, True) and len(returned) == 603
    finally:
        os.close(fd)


# ── review round 2 ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("ipv6", [{"Subnet": "fd00:1::/64"}, {"Subnet": "fd00:1::/64", "Gateway": "fd00:1::1"}])
def test_a_dual_stack_network_is_refused_with_the_ipv6_line(ipv6):
    """Bug class: an IPv6 entry passing unchecked (the isolated gateway mode is IPv4's: the machine can keep an IPv6 address on the network) or refused with the wrong advice."""
    with pytest.raises(host.Refused) as refused:
        host.isolated_subnet(True, ISOLATED, [{"Subnet": "10.201.0.0/24"}, ipv6])
    assert "came back with IPv6" in refused.value.message and "IPv4-only" in refused.value.message and "default-network-opts" in refused.value.message
    assert "newer Docker" not in refused.value.message


def test_another_failure_of_the_first_create_is_reported_without_the_docker_advice(root, docker, capsys):
    """Bug class: an unrelated failure (no free subnet, a name in use) reported as a Docker too old for the isolated mode."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.fail = lambda arguments: "Error response from daemon: all predefined address pools have been fully subnetted" if arguments[1:3] == ["network", "create"] and "--internal" in arguments else None
    assert start(root) == 1
    out = capsys.readouterr().out
    assert "the chat's network could not be made: Error response from daemon: all predefined address pools have been fully subnetted" in out and "newer Docker" not in out
    assert docker.networks == {}


def test_a_leftover_network_that_cannot_be_removed_prints_every_problem(root, docker, capsys):
    """Bug class: only the first of several failures of a leftover's clean-up shown, hiding which container is still attached."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(POSTGRES_ID, "postgres-mcp", key=False)
    docker.networks["sdp-chat-up"] = {"labels": {host.ROOT_LABEL: str(root)}, "internal": False, "options": {}, "subnet": "10.9.0.0/24", "members": [MONGO_ID, POSTGRES_ID]}
    docker.fail = lambda arguments: f"Error response from daemon: made-up refusal for {arguments[-1][:4]}" if arguments[1:3] == ["network", "disconnect"] else None
    assert start(root) == 1
    out = capsys.readouterr().out
    assert "made-up refusal for aaaa" in out and "made-up refusal for bbbb" in out
