"""Tests for `sdp chat start`, `sdp chat stop` and `sdp chat status` (`social_data_pipeline/chat_host.py`, wired in `sdp.py`), and the check `chat unsetup` gained.

The contracts pinned here, each test naming the bug class it catches:

- The two `docker create` argument lists, pinned literally: no published port, no secret in an environment, nothing mounted but what is listed (the sandbox never gets the credential, the broker's socket, Docker's socket or the data folder).
- The inspect template that tells whether an MCP container's read-only user key is set prints a fixed word and nothing of the environment; the compose listing filters by project, service and not one-off, and the working-dir label must equal the deployment folder.
- The gateway's config for each back end and each set of attached databases loads with the real `gateway.table` loader; the gateway's address is the second host address of the network's subnet.
- `start` refuses without a setup, under user-id mapping, while a container runs, with a container of another deployment, and with no attachable database (one "why" line per database), changing nothing; a database without the key is never attached; a step that fails in the middle removes what the start made; the token is in no output, argument or environment.
- `stop` and `status`: their outputs and exit codes; a database container replaced since the start shows as dropped; a stopped container attached to the network is detached before the network is removed (else it could no longer start).
- The query time limit: `CHAT_QUERY_TIMEOUT_SECONDS` read from `.env` as `sdp` reads it (missing or empty: 90; malformed or outside 1 to 3600: start refuses naming the key, making nothing); for MongoDB and StarRocks the chat's own copy of the MCP server is started (its `docker compose` argument list pinned, the checked value in its environment) and attached in place of the shared server, PostgreSQL stays on its shared one; stop and a failed start remove the copies; the sandbox and the output are told the limit.
- `rebuild`: both images built first, as setup builds them, then the chat stopped and started (the argument lists in order); a chat that does not run is started; refused before any Docker call without a completed setup; a failed build stops nothing.
- The work folder's size: links are not followed, the walk is bounded; `unsetup` refuses while a container exists; the CLI parses the three commands.

Docker is a stand-in that records every call (and the environment of a call given one) and answers from a small model of containers and networks, a `docker compose up` of a copy adding its container; nothing reaches a real Docker.
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
# The containers the stand-in gives the chat's copies of the MongoDB and StarRocks MCP servers.
COPY_IDS = {"chat-mongo-mcp": "1" * 64, "chat-starrocks-mcp": "2" * 64}
MONGO_COPY = COPY_IDS["chat-mongo-mcp"]
CHAT = Path(".sdp") / "chat"


class ChatDocker:
    """Records every argument list in `calls` and answers from a model: `containers` (the chat's, by name: status and root label), `networks` (by name: labels, internal, subnet, members), `compose` (the MCP containers by id: service, working folder, whether the read-only user key is set, running, aliases), `images`. `fail(arguments)` returns a stderr for a call that is to fail, else None; `gateway_exits` makes the gateway end at its start."""

    def __init__(self, root):
        self.root = str(root)
        self.calls = []
        # The environment given with each call that had one, by the index of the call.
        self.envs = {}
        self.images = {host.GATEWAY_IMAGE, host.SANDBOX_IMAGE}
        # The query-limit label of the MCP images the chat's copies run, by image (an image not here is missing).
        self.labels = {"sdp-mongo-mcp": "1", "sdp-starrocks-mcp": "1"}
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

    def __call__(self, arguments, *, capture=True, timeout=600, env=None):
        arguments = list(arguments)
        if env is not None:
            self.envs[len(self.calls)] = env
        self.calls.append(arguments)
        assert arguments[0] == "docker"
        failed = self.fail(arguments)
        if failed is not None:
            return self.answer(1, stderr=failed)
        if arguments[1] == "compose":
            assert arguments[2:4] == ["--project-directory", self.root] and "up" in arguments
            self.add(COPY_IDS[arguments[-1]], arguments[-1])
            return self.answer(0)
        words = arguments[1:3]
        if arguments[1] == "info":
            return self.answer(0, json.dumps(self.security))
        if words == ["image", "inspect"] and "sdp.query-limit" in arguments[4]:
            return self.answer(0, self.labels[arguments[-1]] + "\n") if arguments[-1] in self.labels else self.answer(1, stderr=f"Error: No such image: {arguments[-1]}")
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
        if arguments[1] == "build":
            return self.answer(0)
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
            if name in self.compose:
                del self.compose[name]
                for network in self.networks.values():
                    if name in network["members"]:
                        network["members"].remove(name)
                return self.answer(0, name)
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
            found = [c for c in self.compose if every or self.compose[c]["running"]]
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


@pytest.mark.parametrize("backend, model, extra", [("claude-code", None, ["-e", "SDP_CHAT_MODELS=opus,sonnet,haiku"]), ("openai-compatible", "vendor/model-1:free", ["-e", "SDP_CHAT_MODEL=vendor/model-1:free", "-e", "SDP_CHAT_MODELS=vendor/model-1:free"])])
def test_the_sandbox_argument_list_is_pinned(backend, model, extra):
    """Bug class: the sandbox given the credential, the broker's or Docker's socket, the data folder, a second network, a published port, or a secret in its environment; the OpenAI-compatible agent not told its model."""
    assert host.sandbox_arguments("/srv/d", UID, GID, "10.201.0.2", backend, ["mongo", "starrocks"], 7, model) == [
        "docker", "create", "--name", "sdp-sandbox", "--label", "sdp.chat.root=/srv/d", "--restart", "no",
        "--user", "1000:1000", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--init", "--pids-limit", "512", "--memory", "4g", "--tmpfs", "/tmp:rw,nosuid,nodev,size=1g",
        "--network", "sdp-chat",
        "--mount", "type=bind,source=/srv/d/.sdp/chat/work,target=/work",
        "--mount", "type=bind,source=/srv/d/.sdp/chat/socket,target=/run/sdp-chat",
        "-e", "SDP_CHAT_GATEWAY=10.201.0.2", "-e", "HOME=/work",
        "-e", f"SDP_CHAT_BACKEND={backend}", "-e", "SDP_CHAT_DATABASES=mongo,starrocks", "-e", "SDP_CHAT_CAP_GIB=7", *extra,
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
        assert model.models == frozenset({"claude-opus-5-5", "claude-sonnet-5-5", "claude-haiku-5-5"})
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
        ["docker", "network", "connect", "--alias", "sdp-chat-mongo-mcp", "sdp-chat-up", MONGO_COPY],
        host.sandbox_arguments(str(root), os.getuid(), os.getgid(), address, "claude-code", ["mongo"], host.DEFAULT_CAP_GIB, query_seconds=90),
        ["docker", "start", "sdp-sandbox"],
    ]
    out = capsys.readouterr().out
    assert "The chat is started." in out and "Back end: claude-code" in out and "The chat can read: mongo" in out
    assert "postgres: left out: its MCP server is not on a read-only user" in out and "starrocks: left out: its MCP server does not run" in out
    assert host.CHAT_WINDOW in out


def test_a_database_without_the_read_only_user_key_is_never_attached(root, docker):
    """Bug class (security): an MCP server on the database's admin user put in the chat's reach because it runs."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(POSTGRES_ID, "postgres-mcp", key=False)
    docker.add(STARROCKS_ID, "starrocks-mcp", key=False)
    assert start(root) == 0
    connected = [call[-1] for call in docker.calls if call[1:3] == ["network", "connect"]]
    assert connected == ["sdp-gateway", MONGO_COPY]
    assert [call[-1] for call in docker.calls if call[1] == "compose"] == ["chat-mongo-mcp"]
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
    ("database attach", ["network", "connect"], MONGO_COPY),
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
    assert [word for call in docker.calls for word in call if word.startswith("SDP_CHAT_MODEL=")] == (["SDP_CHAT_MODEL=vendor/model-1"] if backend == "openai-compatible" else [])
    assert MARKER not in json.dumps(host.gateway_config("10.201.0.2", host._settings_or_refuse(str(root)), list(host.DATABASES)))


# ── stop and status ─────────────────────────────────────────────────────────


def test_stop_removes_everything_and_detaches_the_databases(root, docker, capsys):
    """Bug class: a database left attached to a network that is then removed (it could no longer start), or a container or network of the chat left."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    docker.compose[MONGO_COPY]["running"] = False
    docker.networks["sdp-chat-up"]["members"].append(MONGO_ID)
    assert host.cmd_stop(root) == 0
    assert docker.containers == {} and docker.networks == {}
    assert ["docker", "rm", "--force", MONGO_COPY] in docker.calls and MONGO_COPY not in docker.compose
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


def changing_calls(docker):
    """The calls that build, make, start, stop or remove something, in order."""
    return [call for call in docker.calls if call[1] in ("build", "create", "start", "stop", "rm") or call[1:3] in (["network", "create"], ["network", "rm"], ["network", "connect"], ["network", "disconnect"])]


def test_rebuild_builds_both_images_first_then_stops_and_starts(root, docker, capsys):
    """Bug class: a rebuild that stops the chat before the build (down for the whole build), builds other images than setup does, or does not start it again on the new images."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    docker.calls.clear()
    capsys.readouterr()

    assert host.cmd_rebuild(root) == 0
    address = host.gateway_address(docker.networks["sdp-chat"]["subnet"])
    assert changing_calls(docker) == [
        *host.build_arguments(str(root)),
        ["docker", "stop", "-t", "10", "sdp-sandbox"],
        ["docker", "rm", "--force", "sdp-sandbox"],
        ["docker", "stop", "-t", "10", "sdp-gateway"],
        ["docker", "rm", "--force", "sdp-gateway"],
        ["docker", "rm", "--force", MONGO_COPY],
        ["docker", "network", "rm", "sdp-chat-up"],
        ["docker", "network", "rm", "sdp-chat"],
        ["docker", "network", "create", "--driver", "bridge", "--label", f"{host.ROOT_LABEL}={root}", "sdp-chat-up"],
        host.internal_network_arguments(str(root)),
        ["docker", "network", "rm", "sdp-chat"],
        host.internal_network_arguments(str(root), docker.networks["sdp-chat"]["subnet"]),
        host.gateway_arguments(str(root), os.getuid(), os.getgid(), address),
        ["docker", "network", "connect", "sdp-chat-up", "sdp-gateway"],
        ["docker", "start", "sdp-gateway"],
        ["docker", "network", "connect", "--alias", "sdp-chat-mongo-mcp", "sdp-chat-up", MONGO_COPY],
        host.sandbox_arguments(str(root), os.getuid(), os.getgid(), address, "claude-code", ["mongo"], host.DEFAULT_CAP_GIB, query_seconds=90),
        ["docker", "start", "sdp-sandbox"],
    ]
    assert all(docker.containers[name]["status"] == "running" for name in ("sdp-gateway", "sdp-sandbox"))
    out = capsys.readouterr().out
    assert out.index("Building the image sdp-gateway") < out.index("Building the image sdp-sandbox") < out.index("The chat has stopped.") < out.index("The chat is started.")


def test_rebuild_of_a_chat_that_does_not_run_starts_it(root, docker, capsys):
    """Bug class: a rebuild that fails, or stops short of the start, because stop answers 1 for a chat that did not run."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert host.cmd_rebuild(root) == 0
    assert [call[1] for call in changing_calls(docker)][:3] == ["build", "build", "network"]
    out = capsys.readouterr().out
    assert "The chat is not running." in out and "The chat is started." in out and "Error" not in out


@pytest.mark.parametrize("missing", ["config", "token"])
def test_rebuild_without_a_completed_setup_is_refused_before_any_docker_call(root, docker, capsys, missing):
    """Bug class: a rebuild that builds and restarts a chat that cannot start (no settings, no credential), or that fails with a traceback."""
    set_up(root)
    (root / CHAT / ("config.json" if missing == "config" else "credential/model.token")).unlink()
    assert host.cmd_rebuild(root) == 1
    assert docker.calls == []
    out = capsys.readouterr().out
    assert out.startswith("  Error: ") and "python sdp.py chat setup" in out


@pytest.mark.parametrize("image", ["sdp-gateway", "sdp-sandbox"])
def test_a_failed_rebuild_leaves_the_running_chat_untouched(root, docker, capsys, image):
    """Bug class: a failed build that stops the chat anyway, leaving the operator without the assistant until the build is fixed."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    assert start(root) == 0
    docker.calls.clear()
    docker.fail = lambda arguments: "failed to solve: process did not complete successfully" if arguments[1] == "build" and arguments[-2] == image else None
    capsys.readouterr()

    assert host.cmd_rebuild(root) == 1
    assert docker.calls == host.build_arguments(str(root))[:1 if image == "sdp-gateway" else 2]
    assert all(docker.containers[name]["status"] == "running" for name in ("sdp-gateway", "sdp-sandbox"))
    assert f"the image {image} could not be built (see Docker's output above). The running chat was not touched" in capsys.readouterr().out


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
    del docker.compose[MONGO_COPY]
    docker.networks["sdp-chat-up"]["members"].remove(MONGO_COPY)
    docker.add(NEW_MONGO_ID, "chat-mongo-mcp")
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


@pytest.mark.parametrize("command", ["start", "stop", "rebuild", "status"])
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


# ── the query queue ─────────────────────────────────────────────────────────

JOBS_ID = "e" * 64
QUEUE_SECRET = "0123456789abcdef" * 4
ENV_MARKER = "made-up-env-password-marker-1f3a"


def with_scheduler(root, docker, *, control=True, mode=0o700):
    """The scheduler's container running and, with `control`, its control folder as the scheduler makes it."""
    docker.add(JOBS_ID, "jobs")
    if control:
        (root / "data" / "jobs" / "control").mkdir(parents=True)
        os.chmod(root / "data" / "jobs" / "control", mode)
    return root / "data" / "jobs" / "control" / "assistant.secret"


def test_the_sandbox_argument_list_with_the_queue_is_pinned():
    """Bug class: the sandbox's runner not told the queue is there, or anything else of the list changed with it."""
    arguments = host.sandbox_arguments("/srv/d", UID, GID, "10.201.0.2", "claude-code", ["mongo"], 7, queue=True)
    assert arguments[-3:] == ["-e", "SDP_CHAT_QUEUE=1", "sdp-sandbox"]
    assert arguments[:-3] == host.sandbox_arguments("/srv/d", UID, GID, "10.201.0.2", "claude-code", ["mongo"], 7)[:-1]


def test_start_attaches_the_scheduler_behind_a_new_secret(root, docker, capsys):
    """Bug class (security): the scheduler reached without the secret, the secret file open to others, written through a link or left half-written, the gateway's copy different, the secret in an argument, an environment or a line of output; the scheduler attached under another alias or the sandbox not told."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    path = with_scheduler(root, docker)
    (root / ".env").write_text(f"POSTGRES_PASSWORD={ENV_MARKER}\nJOBS_PORT=9123\n")
    assert start(root) == 0
    secret = path.read_text().strip()
    copy = root / CHAT / "gateway" / "assistant.secret"
    assert len(secret) == 64 and int(secret, 16) >= 0 and copy.read_text().strip() == secret
    assert oct(path.stat().st_mode & 0o777) == "0o600" and oct(copy.stat().st_mode & 0o777) == "0o600"
    assert [name for name in os.listdir(path.parent)] == ["assistant.secret"]
    table = gateway_table.load(str(root / CHAT / "gateway" / "config.json"))
    queue = table.listeners[-1]
    assert (queue.port, queue.kind, queue.upstream, queue.paths, queue.secret_file) == (8004, "mcp", gateway_table.Upstream("http", "sdp-chat-jobs", 9123), ("/assistant/mcp", "/assistant/results/"), "/run/sdp-gateway/assistant.secret")
    assert ["docker", "network", "connect", "--alias", "sdp-chat-jobs", "sdp-chat-up", JOBS_ID] in docker.calls
    address = host.gateway_address(docker.networks["sdp-chat"]["subnet"])
    assert host.sandbox_arguments(str(root), os.getuid(), os.getgid(), address, "claude-code", ["mongo"], 10, queue=True, query_seconds=90) in docker.calls
    out = capsys.readouterr().out
    assert "The query queue is attached" in out and "cannot use the query queue" not in out
    assert host.cmd_status(root) == 0 and "Query queue for the assistant: attached" in capsys.readouterr().out
    assert host.cmd_stop(root) == 0
    texts = [capsys.readouterr().out, json.dumps(docker.calls), (root / CHAT / "gateway" / "config.json").read_text() if (root / CHAT / "gateway" / "config.json").exists() else ""]
    assert not any(secret in text or ENV_MARKER in text for text in texts)
    assert ["docker", "network", "disconnect", "sdp-chat-up", JOBS_ID] in docker.calls
    assert not copy.exists() and path.read_text().strip() == secret


@pytest.mark.parametrize("existing, kept", [(QUEUE_SECRET + "\n", True), (QUEUE_SECRET[:60], False), (QUEUE_SECRET.upper(), False), ("x" * 300, False)])
def test_a_valid_secret_is_kept_and_an_invalid_one_replaced(root, docker, existing, kept):
    """Bug class: a restart of the chat changing a secret that is still good, or keeping one that the scheduler refuses (the queue then answers 404 to everything)."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    path = with_scheduler(root, docker)
    path.write_text(existing)
    os.chmod(path, 0o600)
    assert start(root) == 0
    now = path.read_text().strip()
    assert (now == QUEUE_SECRET) == kept and len(now) == 64
    assert (root / CHAT / "gateway" / "assistant.secret").read_text().strip() == now


def test_a_secret_open_to_others_is_replaced(root, docker):
    """Bug class: a secret others could have read kept in use."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    path = with_scheduler(root, docker)
    path.write_text(QUEUE_SECRET)
    os.chmod(path, 0o644)
    assert start(root) == 0
    assert path.read_text().strip() != QUEUE_SECRET and oct(path.stat().st_mode & 0o777) == "0o600"


@pytest.mark.parametrize("case", ["not running", "no control folder", "open control folder", "control folder link", "secret link", "secret fifo", "bad port"])
def test_without_a_usable_scheduler_start_goes_on_without_the_queue(root, docker, capsys, tmp_path, case):
    """Bug class: a chat that fails to start because the scheduler is not there, a queue listener without a secret behind it, a secret written through a link or into a folder others can read, or the line not saying why."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    outside = tmp_path / "outside.secret"
    outside.write_text("left alone\n")
    if case == "not running":
        with_scheduler(root, docker)
    elif case == "no control folder":
        with_scheduler(root, docker, control=False)
    elif case == "open control folder":
        with_scheduler(root, docker, mode=0o750)
    elif case == "control folder link":
        with_scheduler(root, docker, control=False)
        (root / "data" / "jobs").mkdir(parents=True)
        (tmp_path / "elsewhere").mkdir(mode=0o700)
        (root / "data" / "jobs" / "control").symlink_to(tmp_path / "elsewhere")
    elif case == "secret link":
        with_scheduler(root, docker).symlink_to(outside)
    elif case == "secret fifo":
        os.mkfifo(with_scheduler(root, docker))
    else:
        with_scheduler(root, docker)
        (root / ".env").write_text("JOBS_PORT=99999\n")
    if case == "not running":
        del docker.compose[JOBS_ID]
    assert start(root) == 0
    out = capsys.readouterr().out
    why = {"not running": "the query scheduler does not run", "no control folder": "data/jobs/control is missing", "open control folder": "open to the group or to others", "control folder link": "is a link or not a folder of yours", "secret link": "is a link or not a regular file of yours", "secret fifo": "is a link or not a regular file of yours", "bad port": "JOBS_PORT in the deployment's .env is not a port number"}[case]
    line = [text for text in out.splitlines() if "cannot use the query queue" in text]
    assert len(line) == 1 and why in line[0]
    assert [lst.port for lst in gateway_table.load(str(root / CHAT / "gateway" / "config.json")).listeners] == [8080, 8002]
    assert not (root / CHAT / "gateway" / "assistant.secret").exists()
    assert not any("SDP_CHAT_QUEUE=1" in word for call in docker.calls for word in call)
    assert not any(JOBS_ID in call for call in docker.calls if call[1:3] == ["network", "connect"])
    assert outside.read_text() == "left alone\n" and not (tmp_path / "elsewhere" / "assistant.secret").exists()


@pytest.mark.parametrize("text, port", [(None, 8050), ("", 8050), ("JOBS_PORT=\n", 8050), ("# JOBS_PORT=1\nJOBS_PORT = 8100\nJOBS_PORT=8200\n", 8200), ("X=1\n  JOBS_PORT=8300  \n", 8300)])
def test_the_schedulers_port_is_read_as_sdp_reads_the_env(root, text, port):
    """Bug class: the gateway sent to another port than the scheduler's own (Compose's default when the key is empty, the last line winning as in `sdp`)."""
    if text is not None:
        (root / ".env").write_text(text)
    assert host.jobs_port(str(root)) == port


def test_status_shows_the_queue_dropped_when_the_scheduler_left(root, docker, capsys):
    """Bug class: a status that says the assistant can use the queue after the scheduler's container was replaced."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    with_scheduler(root, docker)
    assert start(root) == 0
    docker.networks["sdp-chat-up"]["members"].remove(JOBS_ID)
    capsys.readouterr()
    host.cmd_status(root)
    out = capsys.readouterr().out
    assert "Query queue for the assistant: not attached" in out and "the query scheduler: dropped since the start" in out


def test_a_failed_start_removes_the_gateways_copy_and_unsetup_leaves_the_schedulers(root, docker, capsys, monkeypatch):
    """Bug class: the secret left in the gateway's folder after a failed start or an unsetup, or the scheduler's own file (in its folder) removed by the chat."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    path = with_scheduler(root, docker)
    docker.fail = failing(["start"], "sdp-sandbox")
    assert start(root) == 1
    assert not (root / CHAT / "gateway" / "assistant.secret").exists() and path.exists()
    docker.fail = lambda arguments: None
    assert start(root) == 0 and host.cmd_stop(root) == 0
    (root / CHAT / "gateway" / "assistant.secret").write_text(QUEUE_SECRET)
    monkeypatch.setattr(host, "_docker", lambda arguments, **keywords: docker.answer(0) if arguments[1:3] == ["image", "rm"] else docker(arguments, **keywords))
    assert host.cmd_unsetup(root, lambda: True, lambda: True) == 0
    assert not (root / CHAT).exists() and path.exists()


# ── the query time limit ────────────────────────────────────────────────────


@pytest.mark.parametrize("text, seconds", [(None, 90), ("", 90), ("CHAT_QUERY_TIMEOUT_SECONDS=\n", 90), ("CHAT_QUERY_TIMEOUT_SECONDS=1\n", 1), ("# CHAT_QUERY_TIMEOUT_SECONDS=5\nCHAT_QUERY_TIMEOUT_SECONDS = 30\nCHAT_QUERY_TIMEOUT_SECONDS=3600\n", 3600), ("X=1\n  CHAT_QUERY_TIMEOUT_SECONDS=0120  \n", 120)])
def test_the_query_limit_is_read_as_sdp_reads_the_env(root, text, seconds):
    """Bug class: a missing or empty key not taken as the compose file's default of 90, or another line than the last one winning."""
    if text is not None:
        (root / ".env").write_text(text)
    assert host.query_seconds(str(root)) == seconds


@pytest.mark.parametrize("value", ["0", "3601", "12345", "abc", "9.5", "-1", "90s", "٣٠"])
def test_a_malformed_query_limit_is_refused_naming_the_key(root, value):
    """Bug class: a malformed or out-of-bounds limit silently replaced by the default (the chat then runs on another limit than the operator set), or a refusal that does not say which key."""
    (root / ".env").write_text(f"CHAT_QUERY_TIMEOUT_SECONDS={value}\n")
    with pytest.raises(host.Refused) as refusal:
        host.query_seconds(str(root))
    assert "CHAT_QUERY_TIMEOUT_SECONDS" in refusal.value.message and "1 to 3600" in refusal.value.message


def test_start_refuses_a_malformed_query_limit_before_making_anything(root, docker, capsys):
    """Bug class: a chat started on another limit than the one written, or what a refused start made left behind."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    (root / ".env").write_text("CHAT_QUERY_TIMEOUT_SECONDS=4000\n")
    assert start(root) == 1
    assert made_calls(docker) == [] and not any(call[1] == "compose" for call in docker.calls)
    assert "CHAT_QUERY_TIMEOUT_SECONDS in the deployment's .env" in capsys.readouterr().out


def test_the_copy_argument_list_is_pinned():
    """Bug class: the copy started without its database's profile (Compose refuses its `depends_on`), with its dependencies (the database recreated under the running shared server), in another project folder, or without waiting (a copy whose entrypoint refused taken as running)."""
    mongo, starrocks = (db for db in host.DATABASES if db.chat_service)
    assert host.copy_arguments("/srv/d", mongo) == ["docker", "compose", "--project-directory", "/srv/d", "--profile", "mongo", "--profile", "chat_mongo_mcp", "up", "--detach", "--no-deps", "--wait", "--wait-timeout", "60", "chat-mongo-mcp"]
    assert host.copy_arguments("/srv/d", starrocks) == ["docker", "compose", "--project-directory", "/srv/d", "--profile", "starrocks", "--profile", "chat_starrocks_mcp", "up", "--detach", "--no-deps", "--wait", "--wait-timeout", "60", "chat-starrocks-mcp"]
    assert [db.name for db in host.DATABASES if not db.chat_service] == ["postgres"]


def test_start_attaches_the_copies_with_the_limit_and_postgres_on_its_shared_server(root, docker, capsys):
    """Bug class: the shared servers (which other MCP clients use) attached instead of the chat's copies, the copy given another limit than the one checked (a shell export winning over .env), PostgreSQL's shared server replaced, the sandbox not told the limit, or the output not naming it."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(POSTGRES_ID, "postgres-mcp")
    docker.add(STARROCKS_ID, "starrocks-mcp")
    (root / ".env").write_text("CHAT_QUERY_TIMEOUT_SECONDS=45\n")
    assert start(root) == 0
    copies = [(index, call) for index, call in enumerate(docker.calls) if call[1] == "compose"]
    assert [call for _, call in copies] == [host.copy_arguments(str(root), db) for db in host.DATABASES if db.chat_service]
    assert all(docker.envs[index]["CHAT_QUERY_TIMEOUT_SECONDS"] == "45" for index, _ in copies)
    connected = [(call[-3], call[-1]) for call in docker.calls if call[1:3] == ["network", "connect"] and "--alias" in call]
    assert connected == [("sdp-chat-postgres-mcp", POSTGRES_ID), ("sdp-chat-mongo-mcp", MONGO_COPY), ("sdp-chat-starrocks-mcp", COPY_IDS["chat-starrocks-mcp"])]
    sandbox = next(call for call in docker.calls if call[1] == "create" and "sdp-sandbox" in call)
    assert "SDP_CHAT_QUERY_SECONDS=45" in sandbox
    out = capsys.readouterr().out
    assert "Query time limit: 45 s on MongoDB and StarRocks" in out and "PostgreSQL ignores it (its MCP server keeps its own limit: 30 s in restricted mode, the default)" in out
    assert ["docker", "image", "inspect", "--format", '{{index .Config.Labels "sdp.query-limit"}}', "sdp-mongo-mcp"] in docker.calls
    assert host.cmd_status(root) == 0 and "Databases the chat can read now: postgres, mongo, starrocks" in capsys.readouterr().out
    assert host.cmd_stop(root) == 0
    assert not set(COPY_IDS.values()) & set(docker.compose) and {MONGO_ID, POSTGRES_ID, STARROCKS_ID} <= set(docker.compose)


def test_a_copy_that_does_not_start_is_removed_with_what_the_start_made(root, docker, capsys):
    """Bug class: a copy left behind (exited, or running outside the chat) after a failed start, or the start going on without it."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(MONGO_COPY, "chat-mongo-mcp", running=False)
    docker.fail = lambda arguments: "container sdp-chat-mongo-mcp-1 exited (1)" if arguments[1] == "compose" else None
    assert start(root) == 1
    assert MONGO_COPY not in docker.compose and MONGO_ID in docker.compose
    assert docker.containers == {} and docker.networks == {}
    out = capsys.readouterr().out
    assert "the chat's copy of the MCP server of mongo (chat-mongo-mcp) did not start: container sdp-chat-mongo-mcp-1 exited (1)" in out and "What this start made was removed." in out


@pytest.mark.parametrize("label, said", [("", "was built before the query time limit and would ignore it"), (None, "is missing")])
def test_an_mcp_image_without_the_query_limit_label_is_refused_naming_the_build(root, docker, capsys, label, said):
    """Bug class: the chat's copy run on an MCP image built before the limit (after a pull, until someone rebuilds it), which ignores the limit while start says it is enforced; or a refusal that leaves what the start made behind or does not say how to rebuild."""
    set_up(root)
    docker.add(MONGO_ID, "mongo-mcp")
    docker.add(STARROCKS_ID, "starrocks-mcp")
    if label is None:
        del docker.labels["sdp-starrocks-mcp"]
    else:
        docker.labels["sdp-starrocks-mcp"] = label
    assert start(root) == 1
    assert docker.containers == {} and docker.networks == {} and not set(COPY_IDS.values()) & set(docker.compose)
    assert [call[-1] for call in docker.calls if call[1] == "compose"] == ["chat-mongo-mcp"]
    assert not (root / CHAT / "gateway" / "config.json").exists()
    out = capsys.readouterr().out
    assert f"the image sdp-starrocks-mcp of the MCP server of starrocks {said}" in out
    assert "docker compose --profile starrocks --profile starrocks_mcp build starrocks-mcp, then run python sdp.py chat start again" in out and "What this start made was removed." in out
