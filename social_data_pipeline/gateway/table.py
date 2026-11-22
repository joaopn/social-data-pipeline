"""The gateway's table: a typed, deny-by-default loader of its JSON config.

Anything unknown or malformed raises `ConfigError` whose text names the key (for example `listeners[1].upstream`) and never a value read from the credential file. The `model-anthropic` upstream is fixed here, not in the file.
"""

import ipaddress
import json
import os
import re
import stat
from dataclasses import dataclass, field, fields

MODEL_ANTHROPIC = "model-anthropic"
MODEL_OPENAI = "model-openai"
MCP = "mcp"
MODEL_KINDS = (MODEL_ANTHROPIC, MODEL_OPENAI)

_HOST = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?")
_BASE = re.compile(r"(?:/[A-Za-z0-9_~-][A-Za-z0-9._~-]*)*")
_PATH = re.compile(r"/[A-Za-z0-9._~/-]{0,255}")
_TOKEN = re.compile(r"[A-Za-z0-9._-]{1,128}")
_CREDENTIAL_MAX = 64 * 1024


class ConfigError(Exception):
    """The config or the credential file cannot be used; the text names the key, never a secret."""


@dataclass(frozen=True)
class Upstream:
    scheme: str
    host: str
    port: int
    base: str = ""

    @property
    def host_header(self):
        default = 443 if self.scheme == "https" else 80
        return self.host if self.port == default else f"{self.host}:{self.port}"


ANTHROPIC_UPSTREAM = Upstream("https", "api.anthropic.com", 443)


@dataclass(frozen=True)
class Limits:
    max_connections: int = 32
    listener_connections: int = 8
    head_timeout: float = 10.0
    body_timeout: float = 60.0
    model_body_cap: int = 32 * 1024 * 1024
    mcp_body_cap: int = 4 * 1024 * 1024
    requests_per_minute: int = 60
    concurrent_streams: int = 4
    model_idle_timeout: float = 600.0
    mcp_stream_idle_timeout: float = 3600.0
    mcp_idle_timeout: float = 120.0
    connect_timeout: float = 10.0


@dataclass(frozen=True)
class Listener:
    port: int
    kind: str
    upstream: Upstream
    beta_allowed: frozenset = frozenset()
    tool_types_allowed: frozenset = frozenset()
    model: str = ""
    paths: tuple = ()


@dataclass(frozen=True)
class Table:
    listen: str
    listeners: tuple
    credential_file: str = ""
    limits: Limits = field(default_factory=Limits)


_KIND_KEYS = {
    MODEL_ANTHROPIC: ({"port", "kind", "beta_allowed"}, {"tool_types_allowed"}),
    MODEL_OPENAI: ({"port", "kind", "upstream", "model"}, set()),
    MCP: ({"port", "kind", "upstream", "paths"}, set()),
}


def _keys(doc, where, required, optional):
    if not isinstance(doc, dict):
        raise ConfigError(f"{where or 'config'}: must be an object")
    for key in doc:
        if key not in required | optional:
            raise ConfigError(f"{where}{'.' if where else ''}{key}: unknown key")
    for key in sorted(required):
        if key not in doc:
            raise ConfigError(f"{where}{'.' if where else ''}{key}: missing")


def _int(value, key, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ConfigError(f"{key}: must be an integer from {low} to {high}")
    return value


def _tokens(value, key):
    if not isinstance(value, list) or not all(isinstance(v, str) and _TOKEN.fullmatch(v) for v in value):
        raise ConfigError(f"{key}: must be a list of names of letters, digits, '.', '_' or '-'")
    return frozenset(value)


def _upstream(value, key, scheme):
    m = re.fullmatch(r"([a-z]+)://([^/:?#@]+)(?::([0-9]{1,5}))?(/[^?#]*)?", value) if isinstance(value, str) else None
    if not m or m.group(1) != scheme or not _HOST.fullmatch(m.group(2)):
        raise ConfigError(f"{key}: must be an {scheme}://host[:port]{'/base/path' if scheme == 'https' else ''} address")
    base = (m.group(4) or "").rstrip("/")
    if scheme == "http" and base:
        raise ConfigError(f"{key}: must have no path")
    if not _BASE.fullmatch(base):
        raise ConfigError(f"{key}: the base path may hold only segments of letters, digits and '._~-' that do not start with '.'")
    port = _int(int(m.group(3)), key, 1, 65535) if m.group(3) else (443 if scheme == "https" else 80)
    return Upstream(scheme, m.group(2), port, base)


def _paths(value, key):
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{key}: must be a non-empty list of paths")
    for p in value:
        if not isinstance(p, str) or not _PATH.fullmatch(p) or "//" in p or any(s in (".", "..") for s in p.split("/")):
            raise ConfigError(f"{key}: each path must start with '/' and hold only letters, digits and '._~-/' without '//' or dot segments")
    return tuple(value)


def _listener(doc, i):
    where = f"listeners[{i}]"
    if not isinstance(doc, dict):
        raise ConfigError(f"{where}: must be an object")
    kind = doc.get("kind")
    if kind not in _KIND_KEYS:
        raise ConfigError(f"{where}.kind: must be one of {', '.join(_KIND_KEYS)}")
    _keys(doc, where, *_KIND_KEYS[kind])
    port = _int(doc["port"], f"{where}.port", 1, 65535)
    if kind == MODEL_ANTHROPIC:
        return Listener(port, kind, ANTHROPIC_UPSTREAM, beta_allowed=_tokens(doc["beta_allowed"], f"{where}.beta_allowed"), tool_types_allowed=_tokens(doc.get("tool_types_allowed", []), f"{where}.tool_types_allowed"))
    upstream = _upstream(doc["upstream"], f"{where}.upstream", "https" if kind == MODEL_OPENAI else "http")
    if kind == MODEL_OPENAI:
        model = doc["model"]
        if not isinstance(model, str) or not re.fullmatch(r"[\x21-\x7e]{1,256}", model):
            raise ConfigError(f"{where}.model: must be a model name of visible ASCII characters")
        return Listener(port, kind, upstream, model=model)
    return Listener(port, kind, upstream, paths=_paths(doc["paths"], f"{where}.paths"))


def _limits(doc):
    defaults = Limits()
    names = {f.name for f in fields(Limits)}
    _keys(doc, "limits", set(), names)
    values = {}
    for key, value in doc.items():
        if isinstance(getattr(defaults, key), int):
            values[key] = _int(value, f"limits.{key}", 1, 1 << 40)
        elif type(value) not in (int, float) or not 0 < value <= 86400:
            raise ConfigError(f"limits.{key}: must be a number of seconds above 0 and at most 86400")
        else:
            values[key] = float(value)
    return Limits(**values)


def parse(doc):
    """Return the `Table` of a decoded config document, or raise `ConfigError` naming the key."""
    _keys(doc, "", {"listen", "listeners"}, {"credential_file", "limits"})
    listen = doc["listen"]
    try:
        address = ipaddress.ip_address(listen) if isinstance(listen, str) else None
    except ValueError:
        address = None
    if address is None or address.is_unspecified:
        raise ConfigError("listen: must be one IP address, not a wildcard")
    listeners = doc["listeners"]
    if not isinstance(listeners, list) or not listeners:
        raise ConfigError("listeners: must be a non-empty list")
    parsed = tuple(_listener(item, i) for i, item in enumerate(listeners))
    ports = [lst.port for lst in parsed]
    if len(set(ports)) != len(ports):
        raise ConfigError("listeners: a port is used twice")
    credential_file = doc.get("credential_file", "")
    if not isinstance(credential_file, str):
        raise ConfigError("credential_file: must be a path")
    if not credential_file and any(lst.kind in MODEL_KINDS for lst in parsed):
        raise ConfigError("credential_file: required when a model listener exists")
    return Table(str(address), parsed, credential_file, _limits(doc.get("limits", {})))


def load(path):
    """Read and parse the config file at `path`."""
    try:
        with open(path, "rb") as f:
            doc = json.loads(f.read(1024 * 1024).decode("utf-8"))
    except (OSError, ValueError):
        raise ConfigError("config: cannot be read as a JSON file") from None
    return parse(doc)


def read_credential(path):
    """Return the stripped content of the credential file, which must be a regular file; errors never hold its content."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        raise ConfigError("credential_file: cannot be opened (missing, a link or not readable)") from None
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise ConfigError("credential_file: must be a regular file")
        data = os.read(fd, _CREDENTIAL_MAX + 1)
    finally:
        os.close(fd)
    try:
        value = data.decode("ascii").strip()
    except UnicodeDecodeError:
        value = ""
    if not value or len(data) > _CREDENTIAL_MAX or not re.fullmatch(r"[\x21-\x7e]+", value):
        raise ConfigError("credential_file: must hold one value of visible ASCII characters")
    return value
