"""The settings of the web UI, read from the environment `sdp webui start` gives the container, and the rules for the names and addresses the page is reached by.

Four variables, all required: `SDP_WEBUI_HOSTS` (the names and addresses the page answers to, comma-separated: host names, IPv4 addresses, IPv6 addresses in brackets), `SDP_WEBUI_PORT` (the port the browser uses, which is the published port, not one the container listens on), `SDP_WEBUI_SOCKET` (the broker's socket inside the container) and `SDP_WEBUI_GATEWAY` (the IPv4 address of the gateway of the container's network, which `sdp webui start` reads from Docker). Two more are optional, set by `sdp webui start` only when the scheduler's folders are there to mount: `SDP_WEBUI_QUEUE_SOCKET` (the scheduler's control socket inside the container, `/run/sdp-jobs/door.sock`) and `SDP_WEBUI_RESULTS` (the scheduler's results folder inside the container, `/run/sdp-results`); without them the Queue window says why it is not available, or offers no downloads. A third, `SDP_WEBUI_CHAT_SOCKET` (the chat's runner socket inside the container, `/run/sdp-chat/runner.sock`), is set only when the chat's socket folder was there to mount; without it the Chat window says why it is not available. A fourth, `SDP_WEBUI_ANY_HOST` (`1`), is set only when the operator chose every address: the page then answers under any `Host` (`Settings.any_host`).

The container listens on two ports: `LISTEN_PORT`, to which every listen address but 127.0.0.1 is published, and `LOCAL_PORT`, to which only 127.0.0.1 of the host is published (when the operator chose every address, nothing is published to it: the page's port is published on every address to `LISTEN_PORT`, and every login counts as one from elsewhere). A login is from the machine itself only with both signs: it arrived on `LOCAL_PORT`, and its client address is the gateway's (a client on 127.0.0.1 of the host is seen with that address, through Docker's proxy). Neither sign alone is enough: Docker's proxy can dial from the gateway for a client of any address on the other port, and a machine that routes to the container's network through the host may reach `LOCAL_PORT` directly with its own address. A missing or malformed one is refused with a `ConfigError` whose `message` is one line naming the variable, never its value. Standard library only.
"""

import ipaddress
import os
import re
from dataclasses import dataclass

ENV_HOSTS = "SDP_WEBUI_HOSTS"
ENV_PORT = "SDP_WEBUI_PORT"
ENV_SOCKET = "SDP_WEBUI_SOCKET"
ENV_GATEWAY = "SDP_WEBUI_GATEWAY"
ENV_QUEUE_SOCKET = "SDP_WEBUI_QUEUE_SOCKET"
ENV_RESULTS = "SDP_WEBUI_RESULTS"
ENV_CHAT_SOCKET = "SDP_WEBUI_CHAT_SOCKET"
ENV_ANY_HOST = "SDP_WEBUI_ANY_HOST"
# Where `sdp webui start` mounts the scheduler's control folder and its results folder, and the socket's path inside the first.
QUEUE_MOUNT = "/run/sdp-jobs"
QUEUE_SOCKET_IN_CONTAINER = QUEUE_MOUNT + "/door.sock"
RESULTS_MOUNT = "/run/sdp-results"
# Where `sdp webui start` mounts the chat's socket folder, and the runner's socket inside it.
CHAT_MOUNT = "/run/sdp-chat"
CHAT_SOCKET_IN_CONTAINER = CHAT_MOUNT + "/runner.sock"

# The two ports the container listens on.
LISTEN_PORT = 8443
LOCAL_PORT = 8444

DEFAULT_HTTPS_PORT = 443
# A unix socket address holds 108 bytes, the terminating zero byte included.
SOCKET_PATH_MAX_BYTES = 107
HOST_NAME_MAX_CHARS = 253
_LABEL = re.compile(r"[a-z0-9-]{1,63}")


class ConfigError(Exception):
    """A setting is missing or malformed. `message` is one line for the operator that names the variable and not its value."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def host_name_problem(name):
    """Why `name` is not a host name the page may be reached by, as one line, or None when it is one: lower-case letters, digits, hyphens and dots, labels of 1 to 63 characters, at most 253 in all, no wildcard, no trailing dot, and a last label that is not a number (that would be read as an IPv4 address)."""
    if not isinstance(name, str) or not name:
        return "a name is empty"
    if "*" in name:
        return "a wildcard is not a name: give each name the page is reached by"
    if len(name) > HOST_NAME_MAX_CHARS:
        return f"a name is longer than {HOST_NAME_MAX_CHARS} characters"
    if name.endswith("."):
        return "a name ends with a dot: leave the dot out"
    if not all(_LABEL.fullmatch(label) for label in name.split(".")):
        return "a name holds something other than lower-case letters, digits, hyphens and dots, or a part between two dots is empty or longer than 63 characters"
    if name.rsplit(".", 1)[-1].isdigit():
        return "a name whose last part is a number is read as an address, and this is not a valid IP address"
    return None


def host_value(name_or_address):
    """The form a name or an address takes in a `Host` header and a URL: an IPv6 address in brackets, anything else as it is."""
    try:
        address = ipaddress.ip_address(name_or_address)
    except ValueError:
        return name_or_address
    return f"[{address}]" if address.version == 6 else str(address)


def _host_entry(entry):
    """`entry` when it is a host name, an IPv4 address in its usual form or an IPv6 address in brackets in its short form; None otherwise."""
    if entry.startswith("[") and entry.endswith("]"):
        try:
            address = ipaddress.IPv6Address(entry[1:-1])
        except ValueError:
            return None
        return entry if f"[{address}]" == entry and address.scope_id is None else None
    try:
        address = ipaddress.IPv4Address(entry)
    except ValueError:
        return entry if host_name_problem(entry) is None else None
    return entry if str(address) == entry else None


@dataclass(frozen=True)
class Settings:
    """`hosts` the names and addresses in their `Host` form, `port` the port the browser uses, `socket_path` the broker's socket, `gateway` the IPv4 address of the network's gateway; `queue_socket` the scheduler's control socket and `results` its results folder, `chat_socket` the chat's runner socket, each None when it was not mounted; `any_host` true when the page answers under any `Host` (the operator chose every address)."""

    hosts: tuple
    port: int
    socket_path: str
    gateway: str
    queue_socket: str | None = None
    results: str | None = None
    chat_socket: str | None = None
    any_host: bool = False

    def allowed_hosts(self):
        """Every value a `Host` header may have: each name or address with the port, and also without it when the port is the default HTTPS port."""
        allowed = {f"{host}:{self.port}" for host in self.hosts}
        if self.port == DEFAULT_HTTPS_PORT:
            allowed |= set(self.hosts)
        return frozenset(allowed)


def load_settings(environ=None):
    """The settings from `environ` (the process environment when None). Raises ConfigError for a variable that is missing or malformed."""
    environ = os.environ if environ is None else environ

    def required(name):
        value = environ.get(name)
        if value is None or value == "":
            raise ConfigError(f"the setting {name} is missing: start the web UI with `python sdp.py webui start`")
        return value

    hosts = []
    for entry in required(ENV_HOSTS).split(","):
        if _host_entry(entry) is None or entry in hosts:
            raise ConfigError(f"the setting {ENV_HOSTS} holds an entry that is not a host name, an IPv4 address or an IPv6 address in brackets, or one entry twice")
        hosts.append(entry)
    port_text = required(ENV_PORT)
    if not port_text.isascii() or not port_text.isdigit() or not 1 <= int(port_text) <= 65535 or port_text != str(int(port_text)):
        raise ConfigError(f"the setting {ENV_PORT} is not a port number (1 to 65535)")
    socket_path = required(ENV_SOCKET)
    if not socket_path.startswith("/") or "\x00" in socket_path or len(os.fsencode(socket_path)) > SOCKET_PATH_MAX_BYTES:
        raise ConfigError(f"the setting {ENV_SOCKET} is not an absolute path of at most {SOCKET_PATH_MAX_BYTES} bytes")
    gateway_text = required(ENV_GATEWAY)
    try:
        gateway = ipaddress.IPv4Address(gateway_text)
    except ValueError:
        raise ConfigError(f"the setting {ENV_GATEWAY} is not an IPv4 address") from None
    if str(gateway) != gateway_text or gateway.is_unspecified or gateway.is_loopback:
        raise ConfigError(f"the setting {ENV_GATEWAY} is not the IPv4 address of a gateway in its short form")

    def optional_path(name, max_bytes):
        value = environ.get(name)
        if value is None or value == "":
            return None
        if not value.startswith("/") or "\x00" in value or len(os.fsencode(value)) > max_bytes:
            raise ConfigError(f"the setting {name} is not an absolute path of at most {max_bytes} bytes")
        return value

    queue_socket = optional_path(ENV_QUEUE_SOCKET, SOCKET_PATH_MAX_BYTES)
    results = optional_path(ENV_RESULTS, 4096)
    chat_socket = optional_path(ENV_CHAT_SOCKET, SOCKET_PATH_MAX_BYTES)
    any_host = environ.get(ENV_ANY_HOST)
    if any_host not in (None, "", "1"):
        raise ConfigError(f"the setting {ENV_ANY_HOST} is not 1")
    return Settings(hosts=tuple(hosts), port=int(port_text), socket_path=socket_path, gateway=str(gateway), queue_socket=queue_socket, results=results, chat_socket=chat_socket, any_host=any_host == "1")
