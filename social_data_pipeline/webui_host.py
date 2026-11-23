"""The host side of the web UI: `sdp webui setup`, `start`, `stop`, `rebuild`, `status` and `unsetup` (the `cmd_*` functions; `sdp.py` only passes the deployment folder).

What it keeps, in `<root>/.sdp/webui/` (0700): `config.json` (0600: the port, the listen addresses besides 127.0.0.1 (`0.0.0.0` first for every address), the names, how the certificate came, its SHA-256 fingerprint and end date) and `tls/` (0700: `cert.pem` and `key.pem`, 0600), written by the broker's file rules (`control.files`). The container, the network and the image are all called `sdp-webui`.

The container (`create_arguments`, one pure function) runs as the operator's uid and gid, read-only, without capabilities, without new privileges, with 256 processes, 512 MiB and a 16 MiB `/tmp` at most, on a network of its own without masquerading (so it reaches nothing outside the machine), with two read-only mounts: the broker's socket folder at `/run/sdp` and the certificate folder at `/tls`. Two more read-only mounts are added for the Queue window when their folders pass the checks of `_queue_mounts`: the query scheduler's control folder (`data/jobs/control`, made 0700 when missing) at `/run/sdp-jobs`, and its results folder (`JOBS_RESULT_ROOT` of `.env`, by default `./data/jobs/results`) at `/run/sdp-results`; a folder that does not pass is left out with one line saying why. One more read-only mount is added for the Chat window when `_chat_folder` passes it: the chat's socket folder (`.sdp/chat/socket`, a real folder of the operator without permission for others) at `/run/sdp-chat`, with the setting of the runner's socket; otherwise one line says why the Chat window will not be available. It listens on two ports: 127.0.0.1 of the host is published to the local port (8444) and each other listen address the operator named to 8443, so that the web UI tells a login from the machine itself by the port it arrived on and never by the client's address. When the operator answers `all`, the page's port is published once on every address instead (`-p <port>:8443`, which Docker binds on every IPv4 and IPv6 address), nothing is published to the local port (so every login counts as one from elsewhere), and the container gets `SDP_WEBUI_ANY_HOST=1`: the page answers under any `Host`. Docker without user-id mapping is required: rootless Docker and user-namespace remapping are refused (the broker checks the peer's uid, which must be the operator's).

Every Docker call goes through `_docker` (a test replaces it), with an argument list and never a shell; no secret is ever in an argument list (the key is a file in the mounted folder). Standard library and this package only; the setup prompts come from `social_data_pipeline.setup.utils`, imported when `setup` runs.
"""

import base64
import contextlib
import hashlib
import ipaddress
import json
import os
import shutil
import stat
import subprocess
from dataclasses import dataclass

from social_data_pipeline.control import files
from social_data_pipeline.webui import config as webui_config

IMAGE = "sdp-webui"
CONTAINER = "sdp-webui"
NETWORK = "sdp-webui"
NETWORK_OPTION = "com.docker.network.bridge.enable_ip_masquerade=false"
ROOT_LABEL = "sdp.webui.root"

STATE_FOLDER = ".sdp"
WEBUI_FOLDER = os.path.join(STATE_FOLDER, "webui")
TLS_FOLDER = os.path.join(WEBUI_FOLDER, "tls")
SOCKET_FOLDER = os.path.join(STATE_FOLDER, "socket")
CONFIG_NAME = "config.json"
CERT_NAME = "cert.pem"
KEY_NAME = "key.pem"
NEW_CERT_NAME = "new-cert.pem"
NEW_KEY_NAME = "new-key.pem"
DOCKERFILE = "Dockerfile.webui"

LISTEN_PORT = webui_config.LISTEN_PORT
LOCAL_PORT = webui_config.LOCAL_PORT
LOG_TAIL_LINES = 20
# The query scheduler's folders, for the Queue window.
JOBS_FOLDER = os.path.join("data", "jobs")
CONTROL_NAME = "control"
CONTROL_FOLDER = os.path.join(JOBS_FOLDER, CONTROL_NAME)
RESULTS_KEY = "JOBS_RESULT_ROOT"
RESULTS_DEFAULT = "./data/jobs/results"
QUEUE_MOUNT = webui_config.QUEUE_MOUNT
RESULTS_MOUNT = webui_config.RESULTS_MOUNT
# The chat's socket folder, for the Chat window.
CHAT_SOCKET_FOLDER = os.path.join(STATE_FOLDER, "chat", "socket")
CHAT_MOUNT = webui_config.CHAT_MOUNT
SOCKET_MOUNT = "/run/sdp"
TLS_MOUNT = "/tls"
SOCKET_IN_CONTAINER = SOCKET_MOUNT + "/broker.sock"
DEFAULT_PORT = 8443
LOCAL_ADDRESS = "127.0.0.1"
LOCAL_NAME = "localhost"
CERT_DAYS = 825
PEM_MAX_BYTES = 64 * 1024
CONFIG_MAX_BYTES = 64 * 1024
# Words an operator may type for "every address", beside the addresses 0.0.0.0 and :: (in any of their forms).
ALL_ADDRESS_WORDS = ("*", "all", "any")
# How the saved settings hold "every address": the first of their listen addresses. `::` there (an earlier version wrote it) means the same.
EVERY_ADDRESS = "0.0.0.0"
WILDCARDS = (EVERY_ADDRESS, "::")
EVERY_WARNING = "Warning: every address includes networks you did not choose (a VPN, an interface added later, Docker's own networks). The page needs the management password everywhere."

SETUP_FIRST = "  Set it up first: python sdp.py webui setup"
LOOPBACK_REFUSED = "a loopback address is not a listen address: this machine itself is always served on 127.0.0.1"
PROBE_LINES = {
    "answered": "The broker answered from inside the container.",
    "no_socket": "The broker is not running (no socket in .sdp/socket): start it with `python sdp.py broker start`.",
    "refused": "The broker is not running (nobody listens on its socket): start it with `python sdp.py broker start`.",
    "permission_denied": "The container may not open the broker's socket folder: the container does not run as the broker's user.",
    "no_reply": "The broker refused the container's connection without a reply: it does not run as the user the container runs as.",
    "not_reached": "The broker could not be reached from inside the container.",
    "refused_request": "The broker answered with an error.",
    "no_setting": "The container has no socket setting.",
    "not_serving": "The web UI's server did not start serving.",
}


class Refused(Exception):
    """A command or an answer was refused. `message` is one line for the operator."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


@dataclass(frozen=True)
class WebuiConfig:
    """The operator's answers: the port, the listen addresses besides 127.0.0.1 and the names, all checked; `every` when the page listens on every address and answers under any `Host` (`addresses` and `names` then only go into the certificate and the `Host` values)."""

    port: int
    addresses: tuple = ()
    names: tuple = ()
    every: bool = False


# ── the answers ─────────────────────────────────────────────────────────────


def parse_port(text):
    """The port from the operator's answer, 1024 to 65535. Raises Refused."""
    text = str(text).strip()
    if not text.isascii() or not text.isdigit() or not 1024 <= int(text) <= 65535:
        raise Refused("the port is a whole number from 1024 to 65535")
    return int(text)


def parse_addresses(text):
    """The listen addresses besides 127.0.0.1 from a comma-separated answer (empty for none), each in its short form. Raises Refused for anything but an IP address, for every address (0.0.0.0, ::), multicast, a link-local address with a zone, any loopback address (127.0.0.1, which is always there, `::1`, `127.0.0.2` and the like: the machine itself is served on 127.0.0.1 only) and an address named twice."""
    found = []
    for item in (part.strip() for part in str(text).split(",")):
        if not item:
            continue
        if "%" in item:
            raise Refused(f"{item!r} has a zone (the part after %): a link-local address with a zone cannot be published; give an address without one")
        try:
            address = ipaddress.ip_address(item.removeprefix("[").removesuffix("]") if item.startswith("[") else item)
        except ValueError:
            raise Refused(f"{item!r} is not an IP address") from None
        if address.is_unspecified:
            raise Refused(f"{item!r} stands for every address: type all")
        if address.is_multicast:
            raise Refused(f"{item!r} is a multicast address, which nothing can listen on")
        canonical = str(address)
        if _is_loopback(canonical):
            raise Refused(LOOPBACK_REFUSED)
        if canonical in found:
            raise Refused(f"{item!r} is named twice")
        found.append(canonical)
    return tuple(found)


def parse_names(text):
    """The names the page is reached by from a comma-separated answer (empty for none). Raises Refused for a name outside the rule of `webui.config.host_name_problem`, for `localhost` (always there) and for a name given twice."""
    found = []
    for item in (part.strip() for part in str(text).split(",")):
        if not item:
            continue
        problem = webui_config.host_name_problem(item)
        if problem is not None:
            raise Refused(f"{item!r}: {problem}")
        if item == LOCAL_NAME:
            raise Refused("localhost is always there: leave it out")
        if item in found:
            raise Refused(f"{item!r} is named twice")
        found.append(item)
    return tuple(found)


def _as_address(item):
    """The IP address `item` stands for (brackets allowed), or None when it is not one."""
    try:
        return ipaddress.ip_address(item.removeprefix("[").removesuffix("]") if item.startswith("[") else item)
    except ValueError:
        return None


def parse_reach(text, port):
    """The answer to the one question (IP addresses and names, comma-separated, mixed; empty for none) as a WebuiConfig of `port`: `all`, `any`, `*`, 0.0.0.0 and :: (in any form) stand for every address, alone or beside other entries; each other entry is an address when it reads as one (or holds a zone), else a name, and passes the checks of `parse_addresses` or `parse_names`. Raises Refused naming the entry."""
    every, addresses, names = False, [], []
    for item in (part.strip() for part in str(text).split(",")):
        if not item:
            continue
        address = _as_address(item)
        if item.lower() in ALL_ADDRESS_WORDS or (address is not None and address.is_unspecified):
            every = True
        elif address is not None or "%" in item:
            addresses.append(item)
        else:
            names.append(item)
    return WebuiConfig(port=port, addresses=parse_addresses(",".join(addresses)), names=parse_names(",".join(names)), every=every)


def reach_answer(cfg):
    """The answer to the one question that gives `cfg` again: `all` first when it listens on every address, then the addresses and the names."""
    return ", ".join([*(["all"] if cfg.every else []), *cfg.addresses, *cfg.names])


def every_lines(cfg):
    """Where the page answers when it listens on every address, and the warning."""
    return [f"The page answers on every address of this machine, port {cfg.port}: https://<address or name>:{cfg.port}/", EVERY_WARNING]


def host_values(cfg):
    """The names and addresses the page answers to, in their `Host` form: localhost, 127.0.0.1, the listen addresses, the names."""
    return [LOCAL_NAME, LOCAL_ADDRESS, *(webui_config.host_value(address) for address in cfg.addresses), *cfg.names]


def page_addresses(cfg):
    """The addresses a browser opens the page at."""
    return [f"https://{host}:{cfg.port}/" for host in host_values(cfg)]


def address_lines(cfg, heading):
    """The lines that say where the page answers: `heading` (when there is one) and one indented line per address, or with every address the line of `every_lines` and the warning."""
    if cfg.every:
        return [f"  {line}" for line in every_lines(cfg)]
    return [*([heading] if heading else []), *(f"    {address}" for address in page_addresses(cfg))]


def subject_alt_names(cfg):
    """The certificate's subject alternative names, in openssl's form."""
    return ",".join([f"DNS:{LOCAL_NAME}", f"IP:{LOCAL_ADDRESS}", *(f"IP:{address}" for address in cfg.addresses), *(f"DNS:{name}" for name in cfg.names)])


# ── argument lists ──────────────────────────────────────────────────────────


def _mount(source, target):
    return f"type=bind,source={source},target={target},readonly"


def _publish(address, port, container_port):
    host = f"[{address}]" if ipaddress.ip_address(address).version == 6 else address
    return f"{host}:{port}:{container_port}"


def create_arguments(root, uid, gid, cfg, gateway, queue_folder=None, results_folder=None, chat_folder=None):
    """The `docker create` argument list of the web UI's container. `root` is the deployment folder (an absolute path without a comma), `gateway` the IPv4 address of the network's gateway; `queue_folder` and `results_folder` are the scheduler's control folder and results folder (absolute paths without a comma) to mount for the Queue window, `chat_folder` the chat's socket folder for the Chat window, each None when it is left out. With `cfg.every` the page's port is published once on every address and nothing to the local port. 127.0.0.1 is published to the local port, every other listen address to the other one."""
    arguments = [
        "docker", "create",
        "--name", CONTAINER,
        "--label", f"{ROOT_LABEL}={root}",
        "--restart", "no",
        "--user", f"{uid}:{gid}",
        "--read-only",
        "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges",
        "--pids-limit", "256",
        "--memory", "512m",
        "--tmpfs", "/tmp:rw,noexec,nosuid,size=16m",
        "--network", NETWORK,
        "--mount", _mount(os.path.join(root, SOCKET_FOLDER), SOCKET_MOUNT),
        "--mount", _mount(os.path.join(root, TLS_FOLDER), TLS_MOUNT),
    ]
    if queue_folder is not None:
        arguments += ["--mount", _mount(queue_folder, QUEUE_MOUNT)]
    if results_folder is not None:
        arguments += ["--mount", _mount(results_folder, RESULTS_MOUNT)]
    if chat_folder is not None:
        arguments += ["--mount", _mount(chat_folder, CHAT_MOUNT)]
    if cfg.every:
        arguments += ["-p", f"{cfg.port}:{LISTEN_PORT}"]
    else:
        arguments += ["-p", _publish(LOCAL_ADDRESS, cfg.port, LOCAL_PORT)]
        for address in cfg.addresses:
            arguments += ["-p", _publish(address, cfg.port, LISTEN_PORT)]
    arguments += [
        "-e", f"{webui_config.ENV_HOSTS}={','.join(host_values(cfg))}",
        "-e", f"{webui_config.ENV_PORT}={cfg.port}",
        "-e", f"{webui_config.ENV_SOCKET}={SOCKET_IN_CONTAINER}",
        "-e", f"{webui_config.ENV_GATEWAY}={gateway}",
    ]
    if cfg.every:
        arguments += ["-e", f"{webui_config.ENV_ANY_HOST}=1"]
    if queue_folder is not None:
        arguments += ["-e", f"{webui_config.ENV_QUEUE_SOCKET}={webui_config.QUEUE_SOCKET_IN_CONTAINER}"]
    if results_folder is not None:
        arguments += ["-e", f"{webui_config.ENV_RESULTS}={RESULTS_MOUNT}"]
    if chat_folder is not None:
        arguments += ["-e", f"{webui_config.ENV_CHAT_SOCKET}={webui_config.CHAT_SOCKET_IN_CONTAINER}"]
    arguments.append(IMAGE)
    return arguments


def _one_off(root, uid, gid, *, writable):
    """The start of a `docker run` of a one-off container of the image with the certificate folder mounted: no network, the operator's uid and gid, read-only, no capabilities."""
    mount = f"type=bind,source={os.path.join(root, TLS_FOLDER)},target={TLS_MOUNT}" + ("" if writable else ",readonly")
    return ["docker", "run", "--rm", "--network", "none", "--user", f"{uid}:{gid}", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--mount", mount, IMAGE]


def certificate_arguments(root, uid, gid, cfg):
    """The `docker run` argument list that makes the self-signed certificate: an EC P-256 key, 825 days, for serving only, with the names and addresses as subject alternative names, written as `new-cert.pem` and `new-key.pem` into the certificate folder. Every value is its own argument: no shell."""
    return [
        *_one_off(root, uid, gid, writable=True),
        "openssl", "req", "-x509",
        "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
        "-noenc", "-keyout", f"{TLS_MOUNT}/{NEW_KEY_NAME}", "-out", f"{TLS_MOUNT}/{NEW_CERT_NAME}",
        "-days", str(CERT_DAYS), "-subj", "/CN=sdp-webui",
        "-addext", f"subjectAltName={subject_alt_names(cfg)}",
        "-addext", "extendedKeyUsage=serverAuth",
        "-addext", "keyUsage=critical,digitalSignature",
        "-addext", "basicConstraints=critical,CA:FALSE",
    ]


def end_date_arguments(root, uid, gid, name):
    """The `docker run` argument list that prints the end date of a certificate in the certificate folder."""
    return [*_one_off(root, uid, gid, writable=False), "openssl", "x509", "-in", f"{TLS_MOUNT}/{name}", "-noout", "-enddate"]


def public_key_arguments(root, uid, gid, name, *, of_key):
    """The `docker run` argument list that prints the public key of a certificate, or of a private key, in the certificate folder."""
    command = ["openssl", "pkey", "-in", f"{TLS_MOUNT}/{name}", "-pubout"] if of_key else ["openssl", "x509", "-in", f"{TLS_MOUNT}/{name}", "-noout", "-pubkey"]
    return [*_one_off(root, uid, gid, writable=False), *command]


# ── Docker ──────────────────────────────────────────────────────────────────


def _docker(arguments, *, capture=True, timeout=600):
    """Run a Docker command: a CompletedProcess (text). Raises Refused when Docker cannot be run at all."""
    try:
        return subprocess.run(arguments, capture_output=capture, text=True, timeout=timeout, stdin=subprocess.DEVNULL)
    except (OSError, subprocess.TimeoutExpired):
        raise Refused("Docker cannot be run, or did not answer in time") from None


def _last_line(result):
    lines = [line.strip() for line in ((result.stderr or "") + "\n" + (result.stdout or "")).splitlines() if line.strip()]
    errors = [line.strip() for line in (result.stderr or "").splitlines() if line.strip()]
    return (errors or lines or ["(Docker printed nothing)"])[-1]


def user_mapping(security_options):
    """`rootless` or `userns` when Docker's security options (as `docker info` lists them) say so, else None."""
    for option in security_options:
        fields = dict(part.split("=", 1) for part in str(option).split(",") if "=" in part)
        if fields.get("name") in ("rootless", "userns"):
            return fields["name"]
    return None


def _check_docker_mapping():
    result = _docker(["docker", "info", "--format", "{{json .SecurityOptions}}"])
    if result.returncode != 0:
        raise Refused(f"Docker cannot be asked about itself: {_last_line(result)}")
    try:
        options = json.loads(result.stdout)
    except ValueError:
        raise Refused("Docker's answer about its security options cannot be read") from None
    if not isinstance(options, list):
        options = []
    mapping = user_mapping(options)
    if mapping == "rootless":
        raise Refused("Docker runs rootless: the broker would see the web UI's connections as another user. The web UI needs Docker without user-id mapping")
    if mapping == "userns":
        raise Refused("Docker remaps user ids (userns-remap): the broker would see the web UI's connections as another user. The web UI needs Docker without user-id mapping")


def _container_state():
    """(status, root label) of the container `sdp-webui`, or None when there is none."""
    result = _docker(["docker", "container", "inspect", "--format", "{{json .State.Status}}\n{{json .Config.Labels}}", CONTAINER])
    if result.returncode != 0:
        listing = _docker(["docker", "ps", "-a", "--filter", f"name=^/{CONTAINER}$", "--format", "{{.ID}}"])
        if listing.returncode != 0:
            raise Refused(f"Docker cannot be asked about the container: {_last_line(listing)}")
        if listing.stdout.strip():
            raise Refused(f"Docker cannot say what the container {CONTAINER} is: {_last_line(result)}")
        return None
    status_text, _, labels_text = result.stdout.strip().partition("\n")
    try:
        status = json.loads(status_text)
        labels = json.loads(labels_text) or {}
    except ValueError:
        raise Refused(f"Docker's answer about the container {CONTAINER} cannot be read") from None
    return status, labels.get(ROOT_LABEL) if isinstance(labels, dict) else None


def _ours(state, root):
    return state is not None and state[1] == root


def _remove_container():
    _docker(["docker", "rm", "--force", CONTAINER])


# ── the state folder ────────────────────────────────────────────────────────


def _folders(root, *, create):
    """Open `.sdp`, `.sdp/webui` and `.sdp/webui/tls` one after the other by the broker's file rules; yields the descriptor of the last, or None when one is missing and `create` is false."""
    @contextlib.contextmanager
    def opened():
        with files.folder(os.path.join(root, STATE_FOLDER), create=create) as state_fd:
            if state_fd is None:
                yield None
                return
            with files.folder(os.path.join(root, WEBUI_FOLDER), create=create) as webui_fd:
                if webui_fd is None:
                    yield None
                    return
                with files.folder(os.path.join(root, TLS_FOLDER), create=create) as tls_fd:
                    yield (webui_fd, tls_fd) if tls_fd is not None else None
    return opened()


def _config_payload(cfg, certificate, fingerprint, not_after):
    return (json.dumps({"port": cfg.port, "addresses": [*([EVERY_ADDRESS] if cfg.every else []), *cfg.addresses], "names": list(cfg.names), "certificate": certificate, "fingerprint": fingerprint, "not_after": not_after}, indent=2) + "\n").encode("utf-8")


def read_config(root):
    """The saved settings as (WebuiConfig, the whole dict), or None when the web UI is not set up. Raises Refused when they cannot be read or do not pass the checks of the answers."""
    try:
        with files.folder(os.path.join(root, STATE_FOLDER)) as state_fd:
            if state_fd is None:
                return None
            with files.folder(os.path.join(root, WEBUI_FOLDER)) as webui_fd:
                if webui_fd is None:
                    return None
                raw = files.read_file(webui_fd, CONFIG_NAME, CONFIG_MAX_BYTES)
    except files.BrokerFileError as error:
        raise Refused(f"{WEBUI_FOLDER}: {error.message}") from None
    if raw is None:
        return None
    loopback = False
    try:
        saved = json.loads(raw)
        if not isinstance(saved, dict) or not isinstance(saved.get("port"), int) or isinstance(saved.get("port"), bool) or not all(isinstance(saved.get(key), list) and all(isinstance(item, str) for item in saved[key]) for key in ("addresses", "names")):
            raise ValueError()
        if any(_is_loopback(item) for item in saved["addresses"]):
            loopback = True
            raise ValueError()
        listed = [item for item in saved["addresses"] if item not in WILDCARDS]
        cfg = WebuiConfig(port=parse_port(str(saved["port"])), addresses=parse_addresses(",".join(listed)), names=parse_names(",".join(saved["names"])), every=len(listed) != len(saved["addresses"]))
        if list(cfg.addresses) != listed or list(cfg.names) != saved["names"]:
            raise ValueError()
    except (ValueError, Refused):
        if loopback:
            raise Refused(f"{LOOPBACK_REFUSED}.\n  {os.path.join(WEBUI_FOLDER, CONFIG_NAME)} holds one: run `python sdp.py webui setup` again") from None
        raise Refused(f"{os.path.join(WEBUI_FOLDER, CONFIG_NAME)} cannot be used: run `python sdp.py webui setup` again") from None
    return cfg, saved


def _is_loopback(text):
    try:
        address = ipaddress.ip_address(text.strip().removeprefix("[").removesuffix("]"))
    except ValueError:
        return False
    return address.is_loopback or (address.version == 6 and address.ipv4_mapped is not None and address.ipv4_mapped.is_loopback)


def certificate_fingerprint(pem_bytes):
    """The SHA-256 fingerprint of the first certificate of a PEM file, as colon-separated upper-case hex. Raises Refused when there is none."""
    text = pem_bytes.decode("ascii", "replace")
    begin, end = "-----BEGIN CERTIFICATE-----", "-----END CERTIFICATE-----"
    start = text.find(begin)
    stop = text.find(end, start + len(begin)) if start >= 0 else -1
    if start < 0 or stop < 0:
        raise Refused("the certificate file holds no PEM certificate")
    try:
        der = base64.b64decode("".join(text[start + len(begin):stop].split()), validate=True)
    except ValueError:
        raise Refused("the certificate file holds no readable PEM certificate") from None
    digest = hashlib.sha256(der).hexdigest().upper()
    return ":".join(digest[index:index + 2] for index in range(0, len(digest), 2))


def _read_tls_file(root, name):
    try:
        with _folders(root, create=False) as found:
            if found is None:
                return None
            return files.read_file(found[1], name, PEM_MAX_BYTES)
    except files.BrokerFileError as error:
        raise Refused(f"{TLS_FOLDER}: {error.message}") from None


# ── setup ───────────────────────────────────────────────────────────────────


def _read_operator_file(path, what):
    """The bytes of a certificate or key file the operator named: a regular file of at most 64 KiB, read without following a link in its last step and without waiting on a special file."""
    try:
        fd = os.open(os.path.expanduser(path), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise Refused(f"the {what} file cannot be opened (a missing file, a link or no permission)") from None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise Refused(f"the {what} file is not a regular file")
        data = os.read(fd, PEM_MAX_BYTES + 1)
    except OSError:
        raise Refused(f"the {what} file cannot be read") from None
    finally:
        os.close(fd)
    if len(data) > PEM_MAX_BYTES:
        raise Refused(f"the {what} file is larger than {PEM_MAX_BYTES // 1024} KiB")
    return data


def _ask_valid(ask, parse, *arguments, **keywords):
    """Ask until the answer passes `parse`; a refusal is printed and the question asked again."""
    while True:
        answer = ask(*arguments, **keywords)
        try:
            return parse(answer)
        except Refused as refusal:
            print(f"    {refusal.message}.")


def _management_password_set(root):
    """Whether the broker has a management password. Raises Refused when its file cannot be used."""
    from social_data_pipeline.control import broker, passwords

    try:
        return bool(passwords.read_password_file(broker.broker_folder(root)))
    except passwords.PasswordFileNotUsable:
        raise Refused("the broker's password file cannot be used: set the password again with `python sdp.py broker passwd`") from None


def _finish_certificate(tls_fd, from_names):
    """Give the new certificate and key (made by openssl, or copied) mode 0600, check that they belong to this user and are regular files, and give them their names, the key first."""
    for name in from_names:
        fd = files.open_file(tls_fd, name, os.O_RDONLY)
        if fd is None:
            raise Refused(f"{os.path.join(TLS_FOLDER, name)} was not made")
        try:
            os.fchmod(fd, files.FILE_MODE)
        finally:
            os.close(fd)
    os.replace(from_names[1], KEY_NAME, src_dir_fd=tls_fd, dst_dir_fd=tls_fd)
    os.replace(from_names[0], CERT_NAME, src_dir_fd=tls_fd, dst_dir_fd=tls_fd)


def _check_root(root):
    if "," in root or "\n" in root:
        raise Refused("the deployment folder's path holds a comma or a line break, which Docker's mount option cannot take: move the deployment")


def _build_image(root, after_failure=""):
    """Build the image `sdp-webui`, Docker's output printed as it comes. Raises Refused, ending with `after_failure`, when it could not be built."""
    print(f"\n  Building the image {IMAGE} ...", flush=True)
    built = _docker(["docker", "build", "-f", os.path.join(root, DOCKERFILE), "-t", IMAGE, root], capture=False, timeout=3600)
    if built.returncode != 0:
        raise Refused(f"the image {IMAGE} could not be built (see Docker's output above){after_failure}")


def _setup(root, ask, ask_choice, uid, gid):
    _check_root(root)
    if not _management_password_set(root):
        raise Refused("no management password is set. Set one first: python sdp.py broker passwd")
    state = _container_state()
    if state is not None:
        raise Refused("the web UI's container exists: stop it first with `python sdp.py webui stop`")
    try:
        previous = read_config(root)
    except Refused:
        previous = None
    old = previous[0] if previous else WebuiConfig(port=DEFAULT_PORT)

    print()
    port = _ask_valid(ask, parse_port, "Port of the web UI (1024-65535)", old.port, tag="webui_port")
    cfg = _ask_valid(ask, lambda text: parse_reach(text, port), "Addresses the page is opened at besides this machine (IP addresses or names, comma-separated; all for every address; empty for none)", reach_answer(old), tag="webui_addresses")
    choice = ask_choice("Certificate", ["generate", "own"], default="generate", tag="webui_certificate")
    own = None
    if choice == "own":
        certificate = _ask_valid(ask, lambda path: _read_operator_file(path, "certificate"), "Path of your certificate (PEM)", None, tag="webui_cert_path")
        certificate_fingerprint(certificate)
        key = _ask_valid(ask, lambda path: _read_operator_file(path, "key"), "Path of its private key (PEM, not encrypted)", None, tag="webui_key_path")
        if b"PRIVATE KEY-----" not in key or b"ENCRYPTED" in key:
            raise Refused("the key file holds no unencrypted PEM private key")
        own = (certificate, key)

    try:
        with _folders(root, create=True) as (webui_fd, tls_fd):
            for name in (NEW_CERT_NAME, NEW_KEY_NAME):
                with contextlib.suppress(FileNotFoundError):
                    os.unlink(name, dir_fd=tls_fd)
            _build_image(root)
            if own is None:
                made = _docker(certificate_arguments(root, uid, gid, cfg))
                if made.returncode != 0:
                    raise Refused(f"the certificate could not be made: {_last_line(made)}")
            else:
                files.replace_file(tls_fd, NEW_CERT_NAME, own[0])
                files.replace_file(tls_fd, NEW_KEY_NAME, own[1])
                keys = [_docker(public_key_arguments(root, uid, gid, name, of_key=of_key)) for name, of_key in ((NEW_CERT_NAME, False), (NEW_KEY_NAME, True))]
                if any(result.returncode != 0 for result in keys):
                    raise Refused("openssl cannot read the certificate or the key")
                if keys[0].stdout.strip() != keys[1].stdout.strip():
                    raise Refused("the key is not the key of the certificate")
            _finish_certificate(tls_fd, (NEW_CERT_NAME, NEW_KEY_NAME))
            ended = _docker(end_date_arguments(root, uid, gid, CERT_NAME))
            if ended.returncode != 0 or not ended.stdout.strip().startswith("notAfter="):
                raise Refused(f"openssl cannot read the certificate: {_last_line(ended)}")
            not_after = ended.stdout.strip().removeprefix("notAfter=")
            fingerprint = certificate_fingerprint(files.read_file(tls_fd, CERT_NAME, PEM_MAX_BYTES) or b"")
            files.replace_file(webui_fd, CONFIG_NAME, _config_payload(cfg, choice, fingerprint, not_after))
    except files.BrokerFileError as error:
        raise Refused(f"{WEBUI_FOLDER}: {error.message}") from None
    except OSError:
        raise Refused(f"{TLS_FOLDER} cannot be used") from None

    print("\n  The web UI is set up.")
    print(f"  Certificate SHA-256 fingerprint: {fingerprint}")
    print(f"  Valid until: {not_after}")
    if cfg.every:
        print("  Once started (python sdp.py webui start):")
    for line in address_lines(cfg, "  Once started (python sdp.py webui start), the page answers on:"):
        print(line)
    if cfg.every and choice == "generate":
        print(f"  The certificate is for {', '.join(host_values(cfg))}: opened under another address, the browser shows its certificate warning; accept it only when the fingerprint it shows is the one above.")
    return 0


def cmd_setup(root):
    """`sdp webui setup`: ask the port, the addresses and names the page is opened at, and the certificate; build the image; make or copy the certificate. Exit code 0, or 1 with one line."""
    from social_data_pipeline.setup.utils import ask, ask_choice

    try:
        return _setup(os.fspath(root), ask, ask_choice, os.getuid(), os.getgid())
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1


# ── start, stop, status, unsetup ────────────────────────────────────────────


def _network_gateway():
    """The IPv4 gateway address of the network `sdp-webui`, which is created (a bridge, without masquerading) when it is missing. Raises Refused for a network of another driver (a macvlan or ipvlan network's "gateway" is a router of the LAN, which could then reach the container's ports directly and log in as local), one that masquerades, or one without an IPv4 gateway."""
    def inspect():
        return _docker(["docker", "network", "inspect", "--format", "{{json .Driver}}\n{{json .Options}}\n{{json .IPAM.Config}}", NETWORK])

    found = inspect()
    if found.returncode != 0:
        made = _docker(["docker", "network", "create", "--driver", "bridge", "-o", NETWORK_OPTION, NETWORK])
        if made.returncode != 0:
            raise Refused(f"the network {NETWORK} could not be created: {_last_line(made)}")
        found = inspect()
        if found.returncode != 0:
            raise Refused(f"the network {NETWORK} cannot be looked at: {_last_line(found)}")
    lines = found.stdout.strip().split("\n")
    try:
        if len(lines) != 3:
            raise ValueError()
        driver = json.loads(lines[0])
        options = json.loads(lines[1] or "null") or {}
        ipam = json.loads(lines[2] or "null") or []
    except ValueError:
        raise Refused(f"Docker's answer about the network {NETWORK} cannot be read") from None
    if driver != "bridge":
        raise Refused(f"the network {NETWORK} is not a bridge network: remove it with `docker network rm {NETWORK}` and start again")
    key, _, value = NETWORK_OPTION.partition("=")
    if not isinstance(options, dict) or options.get(key) != value:
        raise Refused(f"the network {NETWORK} exists without masquerading switched off: remove it with `docker network rm {NETWORK}` and start again")
    for entry in ipam if isinstance(ipam, list) else []:
        try:
            gateway = ipaddress.IPv4Address(entry.get("Gateway") if isinstance(entry, dict) else None)
        except ValueError:
            continue
        if not gateway.is_unspecified and not gateway.is_loopback:
            return str(gateway)
    raise Refused(f"the network {NETWORK} has no IPv4 gateway address: remove it with `docker network rm {NETWORK}` and start again")


def _is_inside(path, folder):
    return path == folder or path.startswith(folder.rstrip("/") + "/")


def _env_of(root):
    """The `.env` of the deployment folder `root` as a dict, read by the rules of `setup.utils.load_env` (a line `KEY=value`, both stripped; empty lines and `#` lines skipped; the last of a key wins), which reads only the `.env` of the folder its module lies in and takes no path. A test holds the two to the same answers. Nothing of the file is printed."""
    path = os.path.join(root, ".env")
    if not os.path.exists(path):
        return {}
    env = {}
    with open(path) as handle:
        text = handle.read()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def _results_setting(root):
    """The value of `JOBS_RESULT_ROOT` in the `.env` of the deployment folder `root` (read by key; nothing of the file is printed), or the default."""
    value = _env_of(root).get(RESULTS_KEY)
    return value if value else RESULTS_DEFAULT


def _control_folder(root, uid):
    """The scheduler's control folder `data/jobs/control` for the mount, made 0700 when it is missing: (its path or None, the lines to print). `data/jobs` must be a real folder (not a link) of the operator, and so must the control folder; each is opened without following a link and looked at through its descriptor. The mode is set explicitly, so the umask does not matter."""
    queue_off = "Queue: the Queue window will not be available"
    try:
        jobs_fd = os.open(os.path.join(root, JOBS_FOLDER), os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return None, [f"{queue_off}: the scheduler's data folder {JOBS_FOLDER} is not there. Set up and start the scheduler (python sdp.py db setup-jobs, python sdp.py db start jobs), then stop and start the web UI."]
    except OSError:
        return None, [f"{queue_off}: {JOBS_FOLDER} is a link, not a folder, or cannot be opened."]
    try:
        info = os.fstat(jobs_fd)
        if info.st_uid != uid:
            return None, [f"{queue_off}: {JOBS_FOLDER} belongs to another user than you."]
        made = False
        try:
            os.mkdir(CONTROL_NAME, 0o700, dir_fd=jobs_fd)
            made = True
        except FileExistsError:
            pass
        except OSError:
            return None, [f"{queue_off}: {CONTROL_FOLDER} could not be made."]
        try:
            control_fd = os.open(CONTROL_NAME, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=jobs_fd)
        except OSError:
            return None, [f"{queue_off}: {CONTROL_FOLDER} is a link or not a folder; it was left as it is."]
        try:
            control = os.fstat(control_fd)
            if control.st_uid != uid:
                return None, [f"{queue_off}: {CONTROL_FOLDER} belongs to another user than you; it was left as it is."]
            lines = []
            if made:
                # mkdir's mode passes through the umask, which can only take bits away from 0700: this puts them back.
                os.fchmod(control_fd, 0o700)
            elif stat.S_IMODE(control.st_mode) & 0o077:
                lines.append(f"Queue: {CONTROL_FOLDER} is open to the group or to others: the scheduler keeps its control socket closed and the Queue window refuses the folder until you take that away (chmod 700 {CONTROL_FOLDER}).")
        finally:
            os.close(control_fd)
    except OSError:
        return None, [f"{queue_off}: {CONTROL_FOLDER} cannot be used."]
    finally:
        os.close(jobs_fd)
    return os.path.join(root, CONTROL_FOLDER), lines


def _results_folder(root):
    """The scheduler's results folder for the mount: (its resolved path or None, the lines to print). It must exist, have no comma in its path, and be neither `data/jobs` nor a folder above it nor inside the control folder (compared by resolved paths, so links count)."""
    downloads_off = "Queue: downloads will not be available"
    setting = _results_setting(root)
    path = os.path.normpath(setting if os.path.isabs(setting) else os.path.join(root, setting))
    if not os.path.isdir(path):
        return None, [f"{downloads_off}: the scheduler's results folder ({RESULTS_KEY}) is not there."]
    real = os.path.realpath(path)
    if "," in real or "\n" in real:
        return None, [f"{downloads_off}: the path of the scheduler's results folder holds a comma or a line break, which Docker's mount option cannot take."]
    jobs_real = os.path.realpath(os.path.join(root, JOBS_FOLDER))
    if _is_inside(jobs_real, real):
        return None, [f"{downloads_off}: the scheduler's results folder is {JOBS_FOLDER} or a folder above it, which would put the scheduler's own files in the web UI's reach."]
    if _is_inside(real, os.path.realpath(os.path.join(root, CONTROL_FOLDER))):
        return None, [f"{downloads_off}: the scheduler's results folder lies inside {CONTROL_FOLDER}."]
    return real, []


def _queue_mounts(root, uid):
    """(the control folder or None, the results folder or None, the lines to print) for the Queue window's two mounts: none without a usable control folder."""
    control, lines = _control_folder(root, uid)
    if control is None:
        return None, None, lines
    results, more = _results_folder(root)
    return control, results, lines + more


def _chat_folder(root, uid):
    """The chat's socket folder `.sdp/chat/socket` for the mount: (its path or None, the lines to print). Each of `.sdp`, `chat` and `socket` is opened relative to the one above without following a link; the socket folder must be of the operator with no permission for the group or others (the web UI refuses it otherwise). Nothing is made or changed."""
    chat_off = "Chat: the Chat window will not be available"
    fds = []
    try:
        try:
            for name in (os.path.join(root, STATE_FOLDER), "chat", "socket"):
                fds.append(os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fds[-1] if fds else None))
        except FileNotFoundError:
            return None, [f"{chat_off}: the chat's socket folder {CHAT_SOCKET_FOLDER} is not there. Set the chat up and start it (python sdp.py chat setup, python sdp.py chat start), then stop and start the web UI."]
        except OSError:
            return None, [f"{chat_off}: {CHAT_SOCKET_FOLDER} or a folder above it is a link, not a folder, or cannot be opened."]
        info = os.fstat(fds[-1])
        if info.st_uid != uid:
            return None, [f"{chat_off}: {CHAT_SOCKET_FOLDER} belongs to another user than you."]
        if stat.S_IMODE(info.st_mode) & 0o077:
            return None, [f"{chat_off}: {CHAT_SOCKET_FOLDER} is open to the group or to others; take that away (chmod 700 {CHAT_SOCKET_FOLDER}), then stop and start the web UI."]
    finally:
        for fd in fds:
            os.close(fd)
    return os.path.join(root, CHAT_SOCKET_FOLDER), []


def _set_up_or_refuse(root):
    """The saved settings as `read_config` gives them. Raises Refused when the web UI is not set up, its settings cannot be used, or the certificate or its key is missing."""
    _check_root(root)
    loaded = read_config(root)
    if loaded is None:
        raise Refused(f"the web UI is not set up.\n{SETUP_FIRST}")
    if _read_tls_file(root, CERT_NAME) is None or _read_tls_file(root, KEY_NAME) is None:
        raise Refused(f"the certificate or its key is missing.\n{SETUP_FIRST}")
    return loaded


def _start(root, uid, gid):
    loaded = _set_up_or_refuse(root)
    cfg = loaded[0]
    _check_docker_mapping()
    state = _container_state()
    if state is not None:
        if not _ours(state, root):
            raise Refused(f"a container named {CONTAINER} exists that this deployment did not start: remove it, or stop the web UI of the deployment it belongs to")
        if state[0] == "running":
            print("  The web UI is already running.")
            return 0
        _remove_container()
    try:
        with files.folder(os.path.join(root, STATE_FOLDER), create=True):
            pass
        with files.folder(os.path.join(root, SOCKET_FOLDER), create=True):
            pass
    except files.BrokerFileError as error:
        raise Refused(f"{SOCKET_FOLDER}: {error.message}") from None
    gateway = _network_gateway()
    queue_folder, results_folder, queue_lines = _queue_mounts(root, uid)
    chat_folder, chat_lines = _chat_folder(root, uid)
    for line in queue_lines + chat_lines:
        print(f"  {line}")
    for step in (create_arguments(root, uid, gid, cfg, gateway, queue_folder, results_folder, chat_folder), ["docker", "start", CONTAINER]):
        result = _docker(step)
        if result.returncode != 0:
            _remove_container()
            print(f"  Error: the web UI did not start. Docker said: {_last_line(result)}")
            print("  Check that every listen address is an address of this machine and that the port is not in use.")
            return 1
    probe = _docker(["docker", "exec", CONTAINER, "python", "-m", "social_data_pipeline.webui.probe"], timeout=60)
    word = probe.stdout.strip().splitlines()[-1] if probe.stdout.strip() else ""
    # `docker start` answers 0 for a server that ends while it starts (a key it cannot read, a setting it refuses); the probe waits until the server serves, and the state is looked at again.
    after = _container_state()
    if after is None or after[0] != "running" or word in ("", "not_serving"):
        logs = _docker(["docker", "logs", "--tail", str(LOG_TAIL_LINES), CONTAINER])
        _remove_container()
        print("  Error: the web UI's server ended right after its start, or did not start serving. Its last lines:")
        for line in ((logs.stdout or "") + (logs.stderr or "")).splitlines()[-LOG_TAIL_LINES:]:
            print(f"    {line}")
        print("  The container was removed. Check the certificate and key in .sdp/webui/tls (or run `python sdp.py webui setup` again).")
        return 1
    print("  The web UI is started.")
    print(f"  Broker: {PROBE_LINES.get(word, 'the broker could not be asked from inside the container.')}")
    print(f"  Certificate SHA-256 fingerprint: {loaded[1].get('fingerprint', '-')}")
    for line in address_lines(cfg, "  The page answers on:"):
        print(line)
    return 0


def cmd_start(root):
    """`sdp webui start`. Exit code 0 when it was started or already runs, 1 with one line otherwise."""
    try:
        return _start(os.fspath(root), os.getuid(), os.getgid())
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1


def cmd_stop(root):
    """`sdp webui stop`: stop and remove the container. Exit code 0 when it ran, 1 when it did not."""
    root = os.fspath(root)
    try:
        state = _container_state()
        if state is not None and not _ours(state, root):
            raise Refused(f"the container {CONTAINER} was not started by this deployment: nothing was stopped")
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1
    if state is None:
        print("  The web UI is not running.")
        return 1
    if state[0] != "running":
        _remove_container()
        print("  The web UI is not running (its stopped container was removed).")
        return 1
    stopped = _docker(["docker", "stop", "-t", "10", CONTAINER])
    removed = _docker(["docker", "rm", CONTAINER])
    if stopped.returncode != 0 or removed.returncode != 0:
        print(f"  Error: the container could not be stopped and removed: {_last_line(stopped if stopped.returncode != 0 else removed)}")
        return 1
    print("  The web UI has stopped.")
    return 0


def cmd_rebuild(root):
    """`sdp webui rebuild`: from the saved settings, with no question, build the image as setup does, then stop the web UI (when it runs) and start it; logged-in sessions end. A failed build stops nothing. Exit code: start's, or 1 with one line when the web UI is not set up, the image could not be built or the old container still runs."""
    root = os.fspath(root)
    try:
        _set_up_or_refuse(root)
        _build_image(root, ". The running web UI was not touched")
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        return 1
    print()
    print("  Restarting the web UI: logged-in sessions end (log in again once it has started).")
    if cmd_stop(root) != 0:
        # Stop's exit code 1 for a web UI that did not run is no failure here; a container that still runs is (start would take it for the web UI already running).
        try:
            state = _container_state()
        except Refused as refusal:
            print(f"  Error: {refusal.message}")
            return 1
        if state is not None and state[0] == "running":
            print(f"  Error: the container {CONTAINER} still runs (see the line above), so the web UI was not started again. The image is rebuilt: once that container is stopped, run `python sdp.py webui start`.")
            return 1
    return cmd_start(root)


def _queue_status():
    """What `status` says of the Queue window's two mounts and of the Chat window's in the running container."""
    found = _docker(["docker", "container", "inspect", "--format", "{{json .Mounts}}", CONTAINER])
    try:
        mounts = json.loads(found.stdout) if found.returncode == 0 else None
    except ValueError:
        mounts = None
    if not isinstance(mounts, list):
        return ["Queue, Chat: Docker cannot say which folders are mounted."]
    targets = {mount.get("Destination") for mount in mounts if isinstance(mount, dict)}
    lines = ["Queue: the scheduler's control folder is mounted" if QUEUE_MOUNT in targets else "Queue: the scheduler's control folder is not mounted, so the Queue window is not available (start the web UI after the scheduler's data folder exists)"]
    lines.append("Queue: the results folder is mounted (downloads)" if RESULTS_MOUNT in targets else "Queue: the results folder is not mounted, so there are no downloads")
    lines.append("Chat: the chat's socket folder is mounted" if CHAT_MOUNT in targets else "Chat: the chat's socket folder is not mounted, so the Chat window is not available (start the web UI after python sdp.py chat start)")
    return lines


def cmd_status(root):
    """`sdp webui status`: set up or not, running or not, where it answers, the certificate. Exit code 0 when it runs, 1 when not."""
    root = os.fspath(root)
    try:
        loaded = read_config(root)
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        loaded = None
    try:
        state = _container_state()
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        state = None
    running = state is not None and state[0] == "running" and _ours(state, root)
    print(f"  Web UI: {'running' if running else 'not running'}")
    if running:
        for line in _queue_status():
            print(f"  {line}")
    if state is not None and not _ours(state, root):
        print(f"  A container named {CONTAINER} belongs to another deployment.")
    if loaded is None:
        print("  Set up: no")
        print(SETUP_FIRST)
        return 0 if running else 1
    cfg, saved = loaded
    print("  Set up: yes")
    print(f"  Port: {cfg.port}")
    if cfg.every:
        print("  Listen addresses: all")
        if saved.get("certificate") == "generate":
            print(f"  In the certificate: {', '.join(host_values(cfg))}")
    else:
        print(f"  Listen addresses: {', '.join([LOCAL_ADDRESS, *cfg.addresses])}")
        print(f"  Names: {', '.join([LOCAL_NAME, *cfg.names])}")
    try:
        pem = _read_tls_file(root, CERT_NAME)
        fingerprint = certificate_fingerprint(pem) if pem is not None else "missing"
    except Refused as refusal:
        fingerprint = refusal.message
    print(f"  Certificate SHA-256 fingerprint: {fingerprint}")
    print(f"  Certificate valid until: {saved.get('not_after') if isinstance(saved.get('not_after'), str) else '-'}")
    for line in address_lines(cfg, None):
        print(line)
    return 0 if running else 1


def _remove_state_folder(root):
    """Remove `.sdp/webui` without following a link anywhere below the deployment folder: `.sdp` is opened by the broker's folder rule (a link or a folder of another user is refused), `webui` is looked at relative to it, and the tree is removed relative to that descriptor (`shutil.rmtree` with `dir_fd` follows no link inside). Returns True when it was removed, False when there was none."""
    try:
        with files.folder(os.path.join(root, STATE_FOLDER)) as state_fd:
            if state_fd is None:
                return False
            name = os.path.basename(WEBUI_FOLDER)
            try:
                info = os.stat(name, dir_fd=state_fd, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.geteuid():
                raise Refused(f"{WEBUI_FOLDER} is not a folder of this user: it was left as it is")
            shutil.rmtree(name, dir_fd=state_fd)
            return True
    except files.BrokerFileError:
        raise Refused(f"{STATE_FOLDER} is a link or not a folder of this user: {WEBUI_FOLDER} was left as it is") from None


def cmd_unsetup(root, confirm):
    """`sdp webui unsetup`: after `confirm()` says yes, remove the container, the network, the image and `.sdp/webui/`. Exit code 0, or 1 when something could not be removed."""
    root = os.fspath(root)
    if not confirm():
        print("  Nothing was removed.")
        return 0
    failed = False
    try:
        state = _container_state()
        if state is not None:
            if not _ours(state, root):
                raise Refused(f"the container {CONTAINER} was not started by this deployment: it was left, and so were the network and the image")
            _remove_container()
            print(f"  Removed the container {CONTAINER}.")
        for what, arguments, missing in (
            ("network", ["docker", "network", "rm", NETWORK], ("not found", "no such network")),
            ("image", ["docker", "image", "rm", IMAGE], ("no such image",)),
        ):
            result = _docker(arguments)
            if result.returncode == 0:
                print(f"  Removed the {what} {NETWORK if what == 'network' else IMAGE}.")
            elif not any(word in _last_line(result).lower() for word in missing):
                failed = True
                print(f"  Error: the {what} could not be removed: {_last_line(result)}")
    except Refused as refusal:
        print(f"  Error: {refusal.message}")
        failed = True
    else:
        try:
            if _remove_state_folder(root):
                print(f"  Removed {WEBUI_FOLDER}.")
            else:
                print(f"  There was no {WEBUI_FOLDER} to remove.")
        except (Refused, OSError) as error:
            failed = True
            print(f"  Error: {error.message if isinstance(error, Refused) else f'{WEBUI_FOLDER} could not be removed'}")
    return 1 if failed else 0
