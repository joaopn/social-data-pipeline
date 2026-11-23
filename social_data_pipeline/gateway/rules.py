"""Pure rules of the gateway: parse and check a client's request head, choose the upstream target, check a model body, build the upstream headers, filter the response headers.

Every refusal raises `Refused` with a fixed code and status; no refusal carries request data.
"""

import json
import re
from dataclasses import dataclass

from .table import MCP, MODEL_ANTHROPIC, MODEL_OPENAI

MAX_HEADER_LINES = 64
MAX_HEADER_BYTES = 16 * 1024
MAX_REQUEST_LINE = 8 * 1024
MAX_HEAD = MAX_REQUEST_LINE + MAX_HEADER_BYTES + 4
MAX_VALUE = 1024
MAX_DEPTH = 64
MAX_CONTAINERS = 200_000
MAX_ELEMENTS = 1_000_000  # commas outside strings

# The top-level keys of a model body. Anthropic: the keys measured from the requests of the pinned Claude Code, plus `tool_choice`, `temperature` and `stop_sequences`.
ANTHROPIC_KEYS = frozenset({"model", "messages", "system", "tools", "tool_choice", "max_tokens", "stream", "metadata", "thinking", "output_config", "context_management", "temperature", "stop_sequences"})
OPENAI_KEYS = frozenset({"model", "messages", "tools", "tool_choice", "parallel_tool_calls", "stream", "stream_options", "temperature", "top_p", "max_tokens", "max_completion_tokens", "stop", "seed", "presence_penalty", "frequency_penalty", "response_format", "reasoning_effort", "user"})
_SOURCE_TYPES = ("base64", "text", "content")

_METHOD = re.compile(rb"[A-Z]{1,16}")
_TARGET = re.compile(rb"/[\x21-\x7e]*")
_PATH_CHARS = re.compile(r"/[A-Za-z0-9._~/-]*")
_ESCAPE = re.compile(rb"\\.", re.DOTALL)
_STRING = re.compile(rb'"[^"]*"')
_NOT_BRACKET = re.compile(rb"[^\[\]{}]+")
_BAD_KEY = re.compile("[\x00-\x1f\ud800-\udfff]")
_NAME = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+")
_VALUE = re.compile(rb"[\t\x20-\x7e]*")
_DIGITS = re.compile(rb"[0-9]{1,12}")
_MCP_QUERY = re.compile(r"[A-Za-z0-9_.=&-]{0,256}")
_SINGLE = {"content-length", "host", "authorization", "transfer-encoding"}
_PASS_ALL = ("content-type", "accept")
_PASS_KIND = {MODEL_ANTHROPIC: ("anthropic-version",), MODEL_OPENAI: (), MCP: ("mcp-session-id", "mcp-protocol-version", "last-event-id")}
_RESPONSE = {"content-type", "cache-control", "mcp-session-id", "retry-after", "request-id"}
_ANTHROPIC_PATHS = ("/v1/messages", "/v1/messages/count_tokens")
# The header an `mcp` listener with a `secret_file` sends upstream; no client's header of this name is ever passed (it is not in the allow-lists).
SECRET_HEADER = "x-sdp-assistant"
LOGGED_METHODS = {"GET", "POST", "DELETE", "PUT", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"}


class Refused(Exception):
    def __init__(self, code, status=400):
        super().__init__(code)
        self.code = code
        self.status = status


@dataclass(frozen=True)
class Head:
    method: str
    path: str
    query: object  # None when the target has no '?'
    headers: tuple  # (lower-case name, value) pairs in order


@dataclass(frozen=True)
class Route:
    method: str
    target: str  # path and query sent upstream, below the upstream's base path
    table_path: str  # the table's entry, for the log
    stream: bool = False  # an MCP GET stream (its longer idle timeout)


def check_path(path):
    """Refuse a path that is not plain: a character outside letters, digits and '._~/-' (so no '%' encoding at all), '//', or a '.' or '..' segment."""
    if not _PATH_CHARS.fullmatch(path) or "//" in path:
        raise Refused("bad_path")
    if any(seg in (".", "..") for seg in path.split("/")):
        raise Refused("bad_path")


def parse_head(data):
    """Parse a request head (the bytes before the blank line, CRLF line ends only) into a `Head`."""
    if b"\n" in data.replace(b"\r\n", b"") or b"\r" in data.replace(b"\r\n", b""):
        raise Refused("bad_line_end")
    lines = data.split(b"\r\n")
    request_line, header_lines = lines[0], lines[1:]
    if len(request_line) > MAX_REQUEST_LINE:
        raise Refused("head_too_large", 431)
    if len(header_lines) > MAX_HEADER_LINES or len(data) - len(request_line) > MAX_HEADER_BYTES:
        raise Refused("head_too_large", 431)
    parts = request_line.split(b" ")
    if len(parts) != 3 or not _METHOD.fullmatch(parts[0]) or parts[2] != b"HTTP/1.1":
        raise Refused("bad_request_line")
    if parts[0] == b"CONNECT" or not _TARGET.fullmatch(parts[1]):
        raise Refused("bad_target")
    target = parts[1].decode("ascii")
    path, sep, query = target.partition("?")
    check_path(path)
    headers = []
    for line in header_lines:
        if line[:1] in (b" ", b"\t"):
            raise Refused("obs_fold")
        name, colon, value = line.partition(b":")
        if not colon or not _NAME.fullmatch(name) or not _VALUE.fullmatch(value):
            raise Refused("bad_header")
        headers.append((name.decode("ascii").lower(), value.strip(b" \t").decode("ascii")))
    names = [n for n, _ in headers]
    if any(names.count(n) > 1 for n in _SINGLE):
        raise Refused("repeated_header")
    if "transfer-encoding" in names:
        raise Refused("transfer_encoding")
    if "expect" in names:
        raise Refused("expect")
    return Head(parts[0].decode("ascii"), path, query if sep else None, tuple(headers))


def body_length(head, cap):
    """The declared body length: 0 without `Content-Length`, else its ASCII digits, at most `cap`."""
    values = [v for n, v in head.headers if n == "content-length"]
    if not values:
        return 0
    if not _DIGITS.fullmatch(values[0].encode("ascii")):
        raise Refused("bad_length")
    if int(values[0]) > cap:
        raise Refused("body_too_large", 413)
    return int(values[0])


def route(listener, head):
    """The upstream method and target for a request on `listener`, or `Refused` when the table does not allow it."""
    if listener.kind == MODEL_ANTHROPIC:
        if head.method != "POST" or head.path not in _ANTHROPIC_PATHS or head.query not in (None, "beta=true"):
            raise Refused("not_in_table", 404)
        return Route("POST", head.path + ("" if head.query is None else "?beta=true"), head.path)
    if listener.kind == MODEL_OPENAI:
        if head.method != "POST" or head.path != "/chat/completions" or head.query is not None:
            raise Refused("not_in_table", 404)
        return Route("POST", listener.upstream.base + head.path, head.path)
    if head.method not in ("GET", "POST", "DELETE"):
        raise Refused("not_in_table", 404)
    entry = next((p for p in listener.paths if head.path == p or (p.endswith("/") and head.path.startswith(p))), None)
    if entry is None:
        raise Refused("not_in_table", 404)
    if head.query is not None and not _MCP_QUERY.fullmatch(head.query):
        raise Refused("bad_query")
    return Route(head.method, head.path + (f"?{head.query}" if head.query else ""), entry, stream=head.method == "GET")


def _shape(data):
    """Check the shape outside strings before parsing, in linear time: escape pairs removed, then strings; the brackets (at most twice the container cap) and the commas counted, then the brackets walked. The refusal code, or None."""
    outside = _STRING.sub(b"", _ESCAPE.sub(b"", data))
    brackets = _NOT_BRACKET.sub(b"", outside)
    if len(brackets) > 2 * MAX_CONTAINERS or brackets.count(b"[") + brackets.count(b"{") > MAX_CONTAINERS:
        return "body_too_many_containers"
    if outside.count(b",") > MAX_ELEMENTS:
        return "body_too_many_elements"
    depth = 0
    for byte in brackets:
        depth += 1 if byte in (0x5B, 0x7B) else -1
        if depth > MAX_DEPTH:
            return "body_too_deep"
        if depth < 0:
            return "bad_json"
    return None


def _pairs(pairs):
    """The object hook: a key repeated or holding a control character or a surrogate, at any depth, refuses the body."""
    obj = dict(pairs)
    if len(obj) != len(pairs):
        raise Refused("repeated_key", 403)
    for key in obj:
        if _BAD_KEY.search(key):
            raise Refused("bad_key", 403)
    return obj


def _load_object(body):
    code = _shape(body)
    if code:
        raise Refused(code, 400 if code in ("body_too_deep", "bad_json") else 413)
    try:
        doc = json.loads(body.decode("utf-8"), object_pairs_hook=_pairs)
    except (ValueError, RecursionError):
        raise Refused("bad_json") from None
    if not isinstance(doc, dict):
        raise Refused("bad_json")
    return doc


def check_body(listener, body):
    """Refuse a model body that the table does not allow; the bytes themselves are forwarded unchanged by the caller."""
    if listener.kind == MCP:
        return
    doc = _load_object(body)
    anthropic = listener.kind == MODEL_ANTHROPIC
    if not doc.keys() <= (ANTHROPIC_KEYS if anthropic else OPENAI_KEYS):
        raise Refused("top_level_key", 403)
    model = doc.get("model")
    if not isinstance(model, str) or model not in (listener.models if anthropic else (listener.model,)):
        raise Refused("model", 403)
    tools = doc.get("tools")
    allowed = _anthropic_tool if anthropic else _openai_tool
    if "tools" in doc and (not isinstance(tools, list) or not all(allowed(listener, t) for t in tools)):
        raise Refused("tool_type", 403)
    if not (_anthropic_content_ok(doc) if anthropic else _openai_content_ok(doc)):
        raise Refused("content_source", 403)


def _anthropic_tool(listener, tool):
    kind = tool.get("type", "custom") if isinstance(tool, dict) else None
    return isinstance(kind, str) and (kind == "custom" or kind in listener.tool_types_allowed)


def _openai_tool(listener, tool):
    return isinstance(tool, dict) and tool.get("type") == "function"


def _anthropic_content_ok(doc):
    """Every dict and list under `system` and `messages`, walked without recursion (but not the `input` of a `tool_use` block, opaque tool arguments): each `source` key at any depth must be inline (an object of type `base64`, `text` or `content` without `url`), never a URL or a file."""
    stack = [doc.get("system"), doc.get("messages")]
    while stack:
        node = stack.pop()
        if isinstance(node, list):
            stack.extend(node)
        elif isinstance(node, dict):
            if "source" in node:
                source = node["source"]
                if not isinstance(source, dict) or source.get("type") not in _SOURCE_TYPES or "url" in source:
                    return False
            stack.extend(v for k, v in node.items() if not (k == "input" and node.get("type") == "tool_use"))
    return True


def _openai_content_ok(doc):
    """Each message's content: a string, null, or parts that are `text`, or `image_url` with a `data:` URL; nothing the provider would fetch."""
    messages = doc.get("messages")
    for message in messages if isinstance(messages, list) else ():
        content = message.get("content") if isinstance(message, dict) else None
        if content is None or isinstance(content, str):
            continue
        if not isinstance(content, list):
            return False
        for part in content:
            kind = part.get("type") if isinstance(part, dict) else None
            image = part.get("image_url") if kind == "image_url" else None
            if kind != "text" and not (isinstance(image, dict) and isinstance(image.get("url"), str) and image["url"].startswith("data:")):
                return False
    return True


def _clean(value):
    return value if len(value) <= MAX_VALUE and value.isascii() and not any(c in value for c in "\r\n\0") else None


def upstream_headers(listener, head, credential, length, secret=""):
    """The headers of the upstream request, built from the table: `Host`, the gateway's `Authorization` for a model kind, the listener's secret as `x-sdp-assistant` for an `mcp` listener that has one, a computed `Content-Length` and the allow-listed client headers."""
    out = [("Host", listener.upstream.host_header)]
    if listener.kind != MCP:
        out.append(("Authorization", f"Bearer {credential}"))
    elif secret:
        out.append((SECRET_HEADER, secret))
    names = _PASS_ALL + _PASS_KIND[listener.kind]
    seen = set()
    for name, value in head.headers:
        if name in names and name not in seen and _clean(value) is not None:
            seen.add(name)
            out.append((name, value))
    if listener.kind == MODEL_ANTHROPIC:
        betas = [b.strip() for n, v in head.headers if n == "anthropic-beta" for b in v.split(",")]
        kept = [b for b in dict.fromkeys(betas) if b in listener.beta_allowed]
        if kept:
            out.append(("anthropic-beta", ",".join(kept)))
    out.append(("Content-Length", str(length)))
    out.append(("Connection", "close"))
    return out


def response_headers(headers):
    """The upstream's response headers that may reach the client."""
    return [(n, v) for n, v in headers if (n.lower() in _RESPONSE or n.lower().startswith("anthropic-ratelimit-")) and _clean(v) is not None and _NAME.fullmatch(n.encode("ascii", "replace"))]
