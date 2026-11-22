#!/usr/bin/env python3
"""A check of `sdp chat start`, `status` and `stop` and of the sandbox's isolation against a real Docker, run by hand: `python tests/e2e/manual/check_chat_isolation.py` (Linux, with `ip`; a Docker without user-id mapping that can pull `python:3.13-slim` and reach PyPI and the vendor's download of Claude Code, and the image `python:3.13-alpine`, which the stand-ins use and which is not pulled).

It is not collected by pytest. It builds a throwaway deployment in a temp folder (a copy of the CLI, the package, the two Dockerfiles with their ignore files and `requirements-sandbox.txt`) and runs the CLI there as a child process, `chat setup` with a made-up token on its standard input. The images, the two containers and the two networks of the chat get throwaway names, set on the host module in the child before the CLI runs. Three stand-ins on a network of their own, as Compose would make them: one made to look like the deployment's `mongo-mcp` (the compose labels, `MCP_MONGODB_USER=reader`, a tiny HTTP server on port 3000 answering `/mcp`), one like `postgres-mcp` without its read-only user key (and a made-up password in its environment), and a helper publishing a port on every address of this machine. What it shows:

1. `chat start` returns 0, names mongo as read, postgres left out because its MCP server is not on a read-only user and starrocks because it does not run, and prints no secret. The read-back of `sdp-chat` is internal, carries the isolated gateway mode and has no gateway, and this machine has no IPv4 and no global IPv6 address on its bridge (a link-local IPv6 address of the bridge interface itself is reported in a note, and from inside the sandbox no interface but loopback has an IPv6 address and that link-local address is not reached on port 22 nor on the published port).
2. From inside the sandbox: the gateway's model port refuses a wrong path with the gateway's own error (its log says so: nothing was forwarded); its port for mongo reaches the stand-in; its ports for postgres and starrocks are closed. Not reached: an address and a name on the internet, the stand-in by its own address and by its alias, the gateway's address on `sdp-chat-up`, and the published port and port 22 through each network's gateway address and each address of this machine (the same published port is reached from a container on Docker's default bridge, so the refusals mean something). Absent: the broker's socket folder, Docker's socket, the deployment's data folder, any mount of the deployment but the work and socket folders, a file named `model.token` anywhere, the token's text in any process's environment. The sandbox runs as this user, its root is read-only, it has no capabilities, and its writes to `/work` and `/run/sdp-chat` land in the deployment owned by this user.
3. From a container on `sdp-chat-up`: the gateway's ports are closed on its address there. From this machine: `docker inspect` of both containers holds no token.
4. `chat status` names mongo as attached; after the stand-in is replaced by a new container with the same labels it shows mongo as dropped. `chat unsetup` refuses while the chat runs. `chat stop` returns 0 and leaves no container or network of the chat; the stand-ins run on, on their own network only.

Clean-up, also when the check fails: every throwaway container, network and image is removed and the temp folder is deleted. What stays: the base image `python:3.13-slim` and Docker's build cache.
"""

import ipaddress
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT))

from social_data_pipeline import chat_host as host

TOKEN = "made-up-claude-token-for-the-isolation-check-6b2d"
STANDIN_SECRET = "made-up-standin-password-91fe"
ALPINE = "python:3.13-alpine"
# The child: the throwaway names on the host module, then the CLI with the arguments given.
CHILD = "import json, sys\nsys.path.insert(0, sys.argv[1])\nfrom social_data_pipeline import chat_host as h\nn = json.loads(sys.argv[2])\nh.GATEWAY_IMAGE, h.SANDBOX_IMAGE, h.GATEWAY_CONTAINER, h.SANDBOX_CONTAINER, h.NETWORK, h.UP_NETWORK = n\nh.CONTAINERS, h.NETWORKS = (n[2], n[3]), (n[4], n[5])\nimport sdp\nsys.argv = ['sdp.py', *sys.argv[3:]]\nsys.exit(sdp.main())\n"
SERVER = "import http.server, sys\nclass H(http.server.BaseHTTPRequestHandler):\n    def answer(self):\n        n = int(self.headers.get('Content-Length') or 0)\n        self.rfile.read(n)\n        body = b'{\"stand-in\": \"' + sys.argv[1].encode() + b'\"}'\n        self.send_response(200 if self.path.startswith('/mcp') or sys.argv[1] == 'host' else 404)\n        self.send_header('Content-Type', 'application/json')\n        self.send_header('Content-Length', str(len(body)))\n        self.end_headers()\n        self.wfile.write(body)\n    do_GET = do_POST = answer\nhttp.server.ThreadingHTTPServer(('0.0.0.0', int(sys.argv[2])), H).serve_forever()\n"
# Run inside the sandbox; its task comes on standard input (so nothing of it is in a command line), its answer is one JSON line.
PROBE = r'''
import concurrent.futures, http.client, json, os, socket, sys
spec = json.loads(sys.stdin.read())
gateway = os.environ["SDP_CHAT_GATEWAY"]
out = {"gateway_env": gateway}

def request(port, method, path, body=None):
    try:
        c = http.client.HTTPConnection(gateway, port, timeout=15)
        c.request(method, path, body=body, headers={"Content-Type": "application/json"} if body else {})
        r = c.getresponse()
        return [r.status, r.read().decode("utf-8", "replace")]
    except OSError as e:
        return [0, type(e).__name__]

def connects(target):
    try:
        with socket.create_connection((target[0], target[1]), timeout=3):
            return True
    except OSError:
        return False

def resolves(name):
    try:
        socket.getaddrinfo(name, 443)
        return True
    except OSError:
        return False

out["wrong_path"] = request(8080, "GET", "/wrong")
out["mcp"] = request(8002, "POST", "/mcp", b'{"jsonrpc": "2.0", "id": 1, "method": "ping"}')
out["gateway_open"] = {str(p): connects((gateway, p)) for p in (8080, 8001, 8002, 8003)}
with concurrent.futures.ThreadPoolExecutor(32) as pool:
    out["reached"] = [t for t, ok in zip(spec["targets"], pool.map(connects, spec["targets"])) if ok]
    out["resolved"] = [n for n, ok in zip(spec["names"], pool.map(resolves, spec["names"])) if ok]
out["present"] = [p for p in spec["paths"] if os.path.lexists(p)]
out["inet6_interfaces"] = sorted({line.split()[-1] for line in open("/proc/net/if_inet6")}) if os.path.exists("/proc/net/if_inet6") else []
out["link_local"] = {}
if spec["link_local"]:
    scope = socket.if_nametoindex("eth0")
    for port in spec["link_local_ports"]:
        try:
            with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as s6:
                s6.settimeout(3)
                s6.connect((spec["link_local"], port, 0, scope))
            out["link_local"][str(port)] = "connected"
        except OSError as e:
            out["link_local"][str(port)] = e.strerror or type(e).__name__
out["mounts"] = [line.split()[3] + " " + line.split()[4] for line in open("/proc/self/mountinfo")]
found = []
for top, folders, names in os.walk("/"):
    if top in ("/proc", "/sys", "/dev"):
        folders[:] = []
        continue
    found += [os.path.join(top, n) for n in names + folders if n == spec["token_name"]]
out["token_files"] = found
environs = 0
for pid in [p for p in os.listdir("/proc") if p.isdigit()]:
    try:
        environs += spec["token"] in open(f"/proc/{pid}/environ", "rb").read().decode("latin-1")
    except OSError:
        pass
out["token_in_environ"] = environs + any(spec["token"] in v for v in os.environ.values())
out["ids"] = [os.getuid(), os.getgid()]
out["cap_eff"] = next(line.split()[1] for line in open("/proc/self/status") if line.startswith("CapEff:"))
try:
    open("/root-write-probe", "w").close()
    out["root_write"] = True
except OSError:
    out["root_write"] = False
for name, path in (("work_write", "/work/isolation-probe.txt"), ("socket_write", "/run/sdp-chat/isolation-probe.txt")):
    with open(path, "w") as f:
        f.write("written in the sandbox\n")
    out[name] = True
print(json.dumps(out))
'''
# Run in a helper container on `sdp-chat-up`: which of the given (address, port) pairs accept a connection.
REACH = "import json, socket, sys\nr = []\nfor h, p in json.loads(sys.argv[1]):\n    try:\n        socket.create_connection((h, p), timeout=3).close()\n        r.append([h, p])\n    except OSError:\n        pass\nprint(json.dumps(r))\n"


def check(condition, what):
    if not condition:
        raise AssertionError(what)
    print(f"  ok: {what}")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class Deployment:
    def __init__(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp-chat-isolation-")).resolve()
        os.chmod(self.root, 0o700)
        suffix = os.urandom(4).hex()
        self.names = [f"sdp-gateway-check-{suffix}", f"sdp-sandbox-check-{suffix}", f"sdp-gateway-check-{suffix}", f"sdp-sandbox-check-{suffix}", f"sdp-chat-check-{suffix}", f"sdp-chat-up-check-{suffix}"]
        self.gateway, self.sandbox, self.network, self.up = self.names[2], self.names[3], self.names[4], self.names[5]
        self.project = f"sdp-check-project-{suffix}"
        self.mongo, self.postgres, self.helper = f"sdp-check-mongo-{suffix}", f"sdp-check-postgres-{suffix}", f"sdp-check-host-{suffix}"
        self.port = free_port()
        shutil.copy(REPO_ROOT / "sdp.py", self.root / "sdp.py")
        for name in (host.GATEWAY_DOCKERFILE, host.SANDBOX_DOCKERFILE, f"{host.GATEWAY_DOCKERFILE}.dockerignore", f"{host.SANDBOX_DOCKERFILE}.dockerignore", "requirements-sandbox.txt"):
            shutil.copy(REPO_ROOT / name, self.root / name)
        shutil.copytree(REPO_ROOT / "social_data_pipeline", self.root / "social_data_pipeline", ignore=shutil.ignore_patterns("__pycache__"))
        # The broker's socket folder and a data folder, which the sandbox must not see.
        (self.root / ".sdp" / "socket").mkdir(parents=True, mode=0o700)
        (self.root / "data" / "database").mkdir(parents=True)
        (self.root / "data" / "database" / "a-table").write_text("made-up rows\n")

    def cli(self, *argv, stdin=""):
        result = subprocess.run([sys.executable, "-c", CHILD, str(self.root), json.dumps(self.names), *argv], cwd=str(self.root), env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}, capture_output=True, text=True, timeout=3600, input=stdin, start_new_session=True)
        return result.returncode, result.stdout + result.stderr

    def docker(self, *argv, timeout=300, stdin=None):
        return subprocess.run(["docker", *argv], capture_output=True, text=True, timeout=timeout, input=stdin)

    def standin(self, name, service, environment, port):
        labels = [f"com.docker.compose.project={host.COMPOSE_PROJECT}", f"com.docker.compose.service={service}", f"com.docker.compose.project.working_dir={self.root}", "com.docker.compose.oneoff=False"]
        arguments = ["run", "-d", "--name", name, "--network", self.project, *[w for label in labels for w in ("--label", label)], *[w for e in environment for w in ("-e", e)], ALPINE, "python", "-c", SERVER, service, str(port)]
        result = self.docker(*arguments)
        if result.returncode != 0:
            raise AssertionError(f"the stand-in {service} did not start: {result.stderr.strip()}")
        return result.stdout.strip()

    def networks_of(self, name):
        return sorted(json.loads(self.docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", name).stdout or "{}"))

    def address(self, name, network):
        return json.loads(self.docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", name).stdout)[network]["IPAddress"]

    def network_gateway(self, network):
        """The network's IPv4 gateway address, or None (the sandbox's network has none)."""
        return next((entry["Gateway"] for entry in json.loads(self.docker("network", "inspect", "--format", "{{json .IPAM.Config}}", network).stdout) if ":" not in entry["Subnet"] and entry.get("Gateway")), None)

    def leftovers(self):
        containers = [n for n in (self.gateway, self.sandbox) if self.docker("container", "inspect", n).returncode == 0]
        networks = [n for n in (self.network, self.up) if self.docker("network", "inspect", n).returncode == 0]
        return containers, networks

    def clean_up(self):
        for name in (self.sandbox, self.gateway, self.mongo, self.postgres, self.helper):
            self.docker("rm", "--force", name)
        for name in (self.up, self.network, self.project):
            for container in self.docker("ps", "-a", "-q", "--filter", f"network={name}").stdout.split():
                self.docker("network", "disconnect", "--force", name, container)
            self.docker("network", "rm", name)
        for name in self.names[:2]:
            self.docker("image", "rm", "--force", name)
        shutil.rmtree(self.root, ignore_errors=True)
        containers, networks = self.leftovers()
        images = [n for n in self.names[:2] if self.docker("image", "inspect", n).returncode == 0]
        others = [n for n in (self.mongo, self.postgres, self.helper, self.project) if self.docker("inspect", n).returncode == 0]
        return containers + networks + images + others


def host_addresses():
    """This machine's IPv4 addresses, from `ip`."""
    output = subprocess.run(["ip", "-o", "-4", "addr", "show"], capture_output=True, text=True, timeout=30).stdout
    return sorted({str(ipaddress.ip_interface(line.split()[3]).ip) for line in output.splitlines() if len(line.split()) > 3})


def check_start(d):
    print("1. chat setup and chat start")
    code, text = d.cli("chat", "setup", stdin=f"1\n{TOKEN}\n\n")
    check(TOKEN not in text, "`chat setup` printed no token")
    check(code == 0, "`chat setup` with a made-up token built both images" + ("" if code == 0 else ": " + text.strip()[-1500:]))
    d.docker("network", "create", d.project)
    d.standin(d.mongo, "mongo-mcp", ["MCP_MONGODB_USER=reader", "MCP_PORT=3000"], 3000)
    d.standin(d.postgres, "postgres-mcp", ["POSTGRES_MCP_USER=", f"DATABASE_URI=postgresql://postgres:{STANDIN_SECRET}@postgres:5432/datasets"], 8000)
    helper = d.docker("run", "-d", "--name", d.helper, "-p", f"{d.port}:8080", ALPINE, "python", "-c", SERVER, "host", "8080")
    check(helper.returncode == 0, "the helper publishes a port on every address of this machine")
    time.sleep(2)
    code, text = d.cli("chat", "start")
    print("    | " + "\n    | ".join(text.rstrip().splitlines()))
    check(code == 0 and "The chat is started." in text, "`chat start` returned 0")
    check("The chat can read: mongo" in text, "it names mongo as read")
    check("postgres: left out: its MCP server is not on a read-only user" in text and "starrocks: left out: its MCP server does not run" in text, "postgres is left out (not on a read-only user) and starrocks (not running), each with its reason")
    check(TOKEN not in text and STANDIN_SECRET not in text, "its output holds neither the token nor the stand-in's password")
    check(d.networks_of(d.postgres) == [d.project], "the postgres stand-in (without the key) is not attached")
    check(d.networks_of(d.mongo) == sorted([d.project, d.up]), "the mongo stand-in is attached to the chat's upstream network and stays on its own")
    internal, options, ipam, network_id = d.docker("network", "inspect", "--format", "{{json .Internal}}\n{{json .Options}}\n{{json .IPAM.Config}}\n{{json .Id}}", d.network).stdout.strip().split("\n")
    check(json.loads(internal) is True and json.loads(options) == {"com.docker.network.bridge.gateway_mode_ipv4": "isolated"} and [sorted(entry) for entry in json.loads(ipam)] == [["Subnet"]], f"the read-back of {d.network.rsplit('-', 1)[0]}: internal {internal}, options {options}, IPAM keys {[sorted(entry) for entry in json.loads(ipam)]} (no gateway)")
    if shutil.which("ip"):
        bridge = subprocess.run(["ip", "-o", "addr", "show", f"br-{json.loads(network_id)[:12]}"], capture_output=True, text=True, timeout=30)
        lines = bridge.stdout.splitlines()
        link_local = [line for line in lines if " inet6 fe80:" in line]
        check(bridge.returncode == 0 and not any(" inet " in line for line in lines) and not any(" inet6 " in line and " scope global" in line for line in lines), "this machine has no IPv4 address and no global IPv6 address on the bridge of the sandbox's network")
        if link_local:
            print("  note: the bridge interface has its own link-local IPv6 address (fe80::/64); the sandbox is checked below to have no IPv6 address and not to reach it")
            return link_local[0].split(" inet6 ", 1)[1].split()[0].split("/")[0]
    else:
        print("  note: `ip` is missing: the bridge of the sandbox's network was not looked at")
    return None


def check_sandbox(d, link_local):
    print("2. from inside the sandbox")
    gateway = d.address(d.gateway, d.network)
    gateway_up = d.address(d.gateway, d.up)
    mongo = d.address(d.mongo, d.project)
    gateways = [g for g in (d.network_gateway(n) for n in (d.network, d.up, d.project, "bridge")) if g]
    check(d.network_gateway(d.network) is None, "the sandbox's network has no gateway address to try; the other three networks' gateway addresses are tried")
    addresses = host_addresses()
    # The controls: from an ordinary bridge (sdp-chat-up) the internet is reached, so the sandbox's refusals of it come from its network; whether the published port is reached from a container depends on this machine's own firewall, which is said and not checked.
    controls = [["1.1.1.1", 443]] + [[a, d.port] for a in gateways + addresses]
    seen = json.loads(d.docker("run", "--rm", "--network", d.up, ALPINE, "python", "-c", REACH, json.dumps(controls)).stdout or "[]")
    check(["1.1.1.1", 443] in seen, "control: a container on sdp-chat-up (an ordinary bridge) reaches an internet address")
    if not any(pair[1] == d.port for pair in seen):
        print("  note: no container on sdp-chat-up reaches the published port either: this machine's own firewall drops what containers send to it, so the published-port refusals below are not shown to come from the chat's network here")
    targets = [["1.1.1.1", 443], ["8.8.8.8", 53], [mongo, 3000], [gateway_up, 8080], [gateway_up, 8002]] + [[a, p] for a in gateways + addresses for p in (d.port, 22)]
    spec = {"targets": targets, "names": ["api.anthropic.com", "openrouter.ai", host.DATABASES[1].alias, d.mongo, d.postgres], "paths": ["/run/sdp", "/var/run/docker.sock", "/run/docker.sock", "/data", "/run/sdp-credential", "/run/sdp-gateway", str(d.root)], "token_name": host.TOKEN_NAME, "token": TOKEN, "link_local": link_local, "link_local_ports": [22, d.port]}
    result = d.docker("exec", "-i", d.sandbox, "python", "-c", PROBE, stdin=json.dumps(spec), timeout=600)
    check(result.returncode == 0, "the probe ran in the sandbox" + ("" if result.returncode == 0 else f": {result.stderr.strip()[-2000:]}"))
    out = json.loads(result.stdout.strip().splitlines()[-1])
    check(out["gateway_env"] == gateway, "SDP_CHAT_GATEWAY is the gateway's address on sdp-chat")
    check(out["wrong_path"] == [404, '{"error": "not_in_table"}'], f"the model port refuses a wrong path with the gateway's own error: {out['wrong_path']}")
    logs = d.docker("logs", d.gateway)
    # The probe's bare connections to the ports are logged too, without a method.
    model_lines = [line for line in (logs.stdout + logs.stderr).splitlines() if " port=8080 " in line and " method=-" not in line]
    check(len(model_lines) == 1 and "method=GET path=- status=404 in=0 out=0" in model_lines[0] and model_lines[0].endswith("reason=not_in_table"), "the gateway's log says it refused the request itself, with nothing sent on (nothing was forwarded)" + ("" if len(model_lines) == 1 else f": {model_lines}"))
    check(out["mcp"] == [200, '{"stand-in": "mongo-mcp"}'], f"port 8002 /mcp reaches the mongo stand-in through the gateway: {out['mcp']}")
    check(out["gateway_open"] == {"8080": True, "8001": False, "8002": True, "8003": False}, f"8001 (postgres, left out) and 8003 (starrocks, not running) are closed: {out['gateway_open']}")
    check(out["reached"] == [], f"none of {len(targets)} targets is reached: an internet address, the stand-in by its own address, the gateway's sdp-chat-up address, the published port and port 22 through {len(gateways)} network gateways and {len(addresses)} addresses of this machine" + (f" (reached: {len(out['reached'])})" if out["reached"] else ""))
    check(out["resolved"] == [], f"no name resolves: two on the internet, the stand-in's alias and container names ({out['resolved']})")
    check(out["inet6_interfaces"] in ([], ["lo"]), f"no interface of the sandbox but loopback has an IPv6 address (/proc/net/if_inet6: {out['inet6_interfaces']})")
    if link_local:
        check(out["link_local"] and all(result != "connected" for result in out["link_local"].values()), f"the bridge's link-local IPv6 address is not reached on port 22 nor on the published port: {out['link_local']}")
    else:
        print("  note: the bridge has no link-local IPv6 address (or `ip` is missing): no connection to one was tried")
    check(out["present"] == [], f"absent: the broker's socket folder, Docker's socket, the data folder, the gateway's two folders, the deployment folder ({out['present']})")
    deployment = [line for line in out["mounts"] if str(d.root) in line]
    check(sorted(line.split()[1] for line in deployment) == ["/run/sdp-chat", "/work"] and all(line.split()[0].endswith(("/.sdp/chat/work", "/.sdp/chat/socket")) for line in deployment), "the deployment is mounted only at /work and /run/sdp-chat, from the work and socket folders")
    check(out["token_files"] == [], "no file named model.token anywhere in the sandbox")
    check(out["token_in_environ"] == 0, "the token's text is in no process's environment")
    check(out["ids"] == [os.getuid(), os.getgid()], f"it runs as this user {out['ids']}")
    check(out["root_write"] is False, "a write to the root fails (read-only)")
    check(int(out["cap_eff"], 16) == 0, f"no capabilities (CapEff {out['cap_eff']})")
    for folder in ("work", "socket"):
        path = d.root / ".sdp" / "chat" / folder / "isolation-probe.txt"
        check(path.read_text() == "written in the sandbox\n" and path.stat().st_uid == os.getuid(), f"a write to {'/work' if folder == 'work' else '/run/sdp-chat'} lands in .sdp/chat/{folder}, owned by this user")
    return gateway_up


def check_outside(d, gateway_up):
    print("3. from sdp-chat-up and from this machine")
    pairs = [[gateway_up, p] for p in (8080, 8001, 8002, 8003)]
    seen = d.docker("run", "--rm", "--network", d.up, ALPINE, "python", "-c", REACH, json.dumps(pairs))
    check(seen.returncode == 0 and json.loads(seen.stdout) == [], "from a container on sdp-chat-up, the gateway's ports are closed on its address there (it listens on sdp-chat only)")
    inspected = d.docker("inspect", d.gateway, d.sandbox)
    check(inspected.returncode == 0 and TOKEN not in inspected.stdout, "`docker inspect` of both containers holds no token (no environment entry, argument or label)")
    published = json.loads(d.docker("inspect", "--format", "{{json .HostConfig.PortBindings}}", d.gateway).stdout or "null")
    check(not published, "the gateway publishes no port")


def check_lifecycle(d):
    print("4. status, a replaced database, unsetup, stop")
    code, text = d.cli("chat", "status")
    check(code == 0 and "Gateway: running" in text and "Sandbox: running" in text and "Databases the chat can read now: mongo" in text, "`chat status` returns 0 and names mongo as attached")
    d.docker("rm", "--force", d.mongo)
    d.standin(d.mongo, "mongo-mcp", ["MCP_MONGODB_USER=reader", "MCP_PORT=3000"], 3000)
    code, text = d.cli("chat", "status")
    check("Databases the chat can read now: none" in text and "mongo: dropped since the start" in text, "after the stand-in is replaced by a new container, `status` shows mongo as dropped")
    code, text = d.cli("chat", "unsetup", stdin="y\ny\n")
    check(code == 1 and "stop it first: python sdp.py chat stop" in text and (d.root / ".sdp" / "chat" / "credential" / "model.token").exists(), "`chat unsetup` refuses while the chat runs and removes nothing")
    code, text = d.cli("chat", "stop")
    check(code == 0 and "The chat has stopped." in text, "`chat stop` returned 0")
    check(d.leftovers() == ([], []), "no container and no network of the chat is left")
    check(d.networks_of(d.mongo) == [d.project] and d.networks_of(d.postgres) == [d.project], "the stand-ins are on their own network only")
    running = [d.docker("inspect", "--format", "{{.State.Status}}", n).stdout.strip() for n in (d.mongo, d.postgres)]
    check(running == ["running", "running"], "the stand-ins run on")
    code, text = d.cli("chat", "stop")
    check(code == 1 and "The chat is not running." in text, "a second `chat stop` returns 1")


def main():
    d = Deployment()
    failed = None
    try:
        try:
            print(f"deployment {d.root.name}, chat names *-check-{d.names[0].rsplit('-', 1)[1]}, Docker {d.docker('version', '--format', '{{.Server.Version}}').stdout.strip()}")
            link_local = check_start(d)
            gateway_up = check_sandbox(d, link_local)
            check_outside(d, gateway_up)
            check_lifecycle(d)
        except Exception as error:
            failed = error
    finally:
        left = d.clean_up()
    if left:
        print(f"FAILED: left behind: {left}")
        return 1
    print("  ok: no container, network or image of the check is left, and the temp folder is deleted (the base image python:3.13-slim and the build cache stay)")
    if failed is not None:
        print(f"FAILED: {type(failed).__name__}: {failed}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
