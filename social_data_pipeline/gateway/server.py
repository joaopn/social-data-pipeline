"""The gateway's listeners and its forwarding, on threads.

Every client is treated as hostile. Request smuggling is removed by construction: each client connection carries exactly one request, read by the gateway's own strict parser (no `Transfer-Encoding`, exactly one `Content-Length`, the body cut at the declared length); the client connection is closed after the answer and nothing after the first request is ever parsed; each forwarded request gets a fresh upstream connection that is never reused or pooled, with `Connection: close` and a `Content-Length` computed from the bytes read. So no byte of one request can be taken as part of another, on either side.

The upstream request is built from the table, not copied: method and path from the rules, `Host` from the table, the gateway's own `Authorization` for a model listener, and only allow-listed client headers. Responses pass back with their status and allow-listed headers, a 3xx turned into a 502, the body streamed chunk by chunk as it arrives.
"""

import dataclasses
import datetime
import http
import http.client
import json
import math
import socket
import ssl
import sys
import threading
import time
from collections import deque

from . import rules
from .table import MCP, MODEL_ANTHROPIC, MODEL_KINDS, ConfigError

_LOOPBACK = ("127.0.0.1", "::1", "localhost")
BODY_CHECKS = 2  # model bodies parsed and checked at the same time
ACCEPT_PAUSE = 0.1


class _Closed(Exception):
    """End the exchange without an answer (a deadline passed, the client went away)."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


class _Limited(rules.Refused):
    def __init__(self, retry_after):
        super().__init__("rate_limited", 429)
        self.retry_after = retry_after


class Window:
    """At most `limit` events in any `span` seconds of `clock`, a sliding window."""

    def __init__(self, limit, span, clock):
        self._limit, self._span, self._clock = limit, span, clock
        self._times = deque()
        self._lock = threading.Lock()

    def take(self):
        """Count one event and return 0, or return the whole seconds to wait when the window is full."""
        with self._lock:
            now = self._clock()
            while self._times and self._times[0] <= now - self._span:
                self._times.popleft()
            if len(self._times) < self._limit:
                self._times.append(now)
                return 0
            return max(1, math.ceil(self._times[0] + self._span - now))


class _Slots:
    def __init__(self, limit):
        self._free = limit
        self._lock = threading.Lock()

    def take(self):
        with self._lock:
            if self._free <= 0:
                return False
            self._free -= 1
            return True

    def give(self):
        with self._lock:
            self._free += 1


def _stdout(line):
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


class Gateway:
    """Serve every listener of `table` on `table.listen`.

    `test_upstream` (tests only, never reached from the config file or the command line) is a loopback `http://` `Upstream` that replaces the fixed `model-anthropic` upstream and allows `http://` for `model-openai`.
    """

    def __init__(self, table, credential, *, log=_stdout, clock=time.monotonic, sleep=time.sleep, test_upstream=None):
        if test_upstream is not None and (test_upstream.scheme != "http" or test_upstream.host not in _LOOPBACK):
            raise ValueError("test_upstream must be a loopback http:// address")
        listeners = []
        for i, lst in enumerate(table.listeners):
            if lst.kind == MODEL_ANTHROPIC and test_upstream is not None:
                lst = dataclasses.replace(lst, upstream=test_upstream)
            if lst.kind in MODEL_KINDS and lst.upstream.scheme != "https" and test_upstream is None:
                raise ConfigError(f"listeners[{i}].upstream: must be https://")
            listeners.append(lst)
        if any(lst.kind in MODEL_KINDS for lst in listeners) and not credential:
            raise ConfigError("credential_file: required when a model listener exists")
        self._listeners = tuple(listeners)
        self._listen = table.listen
        self._limits = table.limits
        self._credential = credential
        self._log = log
        self._sleep = sleep
        self._conns = threading.BoundedSemaphore(table.limits.max_connections)
        self._listener_conns = [threading.BoundedSemaphore(table.limits.listener_connections) for _ in listeners]
        self._checks = threading.BoundedSemaphore(BODY_CHECKS)
        self._windows = [Window(table.limits.requests_per_minute, 60.0, clock) for _ in listeners]
        self._streams = [_Slots(table.limits.concurrent_streams) for _ in listeners]
        self._tls = ssl.create_default_context()
        self._lock = threading.Lock()
        self._live_socks = set()
        self._threads = set()
        self._servers = []
        self._stopping = threading.Event()
        self.ports = []

    def start(self):
        """Bind every listener (a failure names the port and closes what was bound) and start serving."""
        family = socket.AF_INET6 if ":" in self._listen else socket.AF_INET
        for i, lst in enumerate(self._listeners):
            sock = socket.socket(family, socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((self._listen, lst.port))
                sock.listen(64)
            except OSError:
                sock.close()
                self.stop()
                raise ConfigError(f"listeners[{i}].port: cannot bind port {lst.port} on the listen address") from None
            self._servers.append(sock)
            self.ports.append(sock.getsockname()[1])
        for i, sock in enumerate(self._servers):
            self._spawn(self._accept, sock, i)

    def stop(self):
        """Close the listeners and every open connection, then wait for the threads."""
        self._stopping.set()
        for sock in self._servers:
            _shutdown(sock)
            sock.close()
        with self._lock:
            for sock in list(self._live_socks):
                _shutdown(sock)
            threads = list(self._threads)
        for thread in threads:
            if thread is not threading.current_thread():
                thread.join(timeout=5)

    def _spawn(self, target, *args):
        thread = threading.Thread(target=self._run, args=(target, args), daemon=True)
        with self._lock:
            self._threads.add(thread)
        try:
            thread.start()
        except RuntimeError:
            with self._lock:
                self._threads.discard(thread)
            raise

    def _run(self, target, args):
        try:
            target(*args)
        finally:
            with self._lock:
                self._threads.discard(threading.current_thread())

    def _track(self, sock, add=True):
        with self._lock:
            (self._live_socks.add if add else self._live_socks.discard)(sock)

    def _accept(self, server, index):
        while not self._stopping.is_set():
            try:
                client, _ = server.accept()
            except OSError:
                if self._stopping.is_set():
                    return
                self._sleep(ACCEPT_PAUSE)
                continue
            if self._stopping.is_set() or not self._conns.acquire(blocking=False):
                client.close()
                continue
            if not self._listener_conns[index].acquire(blocking=False):
                self._conns.release()
                client.close()
                continue
            self._track(client)
            try:
                self._spawn(self._handle, client, index)
            except RuntimeError:
                self._track(client, add=False)
                client.close()
                self._listener_conns[index].release()
                self._conns.release()

    def _handle(self, client, index):
        lst = self._listeners[index]
        started = time.monotonic()
        rec = {"method": "-", "path": "-", "status": 0, "in": 0, "out": 0, "reason": "-"}
        try:
            self._exchange(client, index, rec)
        except rules.Refused as refused:
            rec["status"], rec["reason"] = refused.status, refused.code
            extra = f"Retry-After: {refused.retry_after}\r\n" if isinstance(refused, _Limited) else ""
            _send_error(client, refused.status, refused.code, extra)
        except _Closed as closed:
            rec["reason"] = closed.code
        except OSError:
            rec["reason"] = "client_gone"
        except Exception:
            rec["reason"] = "internal"
            _send_error(client, 500, "internal")
        finally:
            _linger_close(client)
            self._track(client, add=False)
            self._listener_conns[index].release()
            self._conns.release()
            stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            took = round((time.monotonic() - started) * 1000)
            self._log(f"{stamp} port={self.ports[index]} kind={lst.kind} method={rec['method']} path={rec['path']} status={rec['status']} in={rec['in']} out={rec['out']} ms={took} reason={rec['reason']}")

    def _exchange(self, client, index, rec):
        lst, limits = self._listeners[index], self._limits
        head_bytes, rest = _read_head(client, time.monotonic() + limits.head_timeout)
        head = rules.parse_head(head_bytes)
        rec["method"] = head.method if head.method in rules.LOGGED_METHODS else "-"
        route = rules.route(lst, head)
        rec["path"] = route.table_path
        length = rules.body_length(head, limits.mcp_body_cap if lst.kind == MCP else limits.model_body_cap)
        if lst.kind != MCP:
            wait = self._windows[index].take()  # before the body is read, so refused bodies count too
            if wait:
                raise _Limited(wait)
        body_deadline = time.monotonic() + limits.body_timeout
        body = _read_body(client, rest, length, body_deadline)
        rec["in"] = len(body)
        if lst.kind != MCP:
            if not self._checks.acquire(timeout=max(0.0, body_deadline - time.monotonic())):
                raise _Limited(1)
            try:
                rules.check_body(lst, body)
            finally:
                self._checks.release()
        headers = rules.upstream_headers(lst, head, self._credential, len(body))
        if lst.kind == MCP:
            idle = limits.mcp_stream_idle_timeout if route.stream else limits.mcp_idle_timeout
            return self._forward(client, lst, route, headers, body, idle, rec)
        if not self._streams[index].take():
            raise _Limited(1)
        try:
            self._forward(client, lst, route, headers, body, limits.model_idle_timeout, rec)
        finally:
            self._streams[index].give()

    def _forward(self, client, lst, route, headers, body, idle, rec):
        up = lst.upstream
        timeout = self._limits.connect_timeout
        if up.scheme == "https":
            conn = http.client.HTTPSConnection(up.host, up.port, timeout=timeout, context=self._tls)
        else:
            conn = http.client.HTTPConnection(up.host, up.port, timeout=timeout)
        up_sock = None
        try:
            try:
                conn.connect()
                up_sock = conn.sock
                self._track(up_sock)
                up_sock.settimeout(idle)
                conn.putrequest(route.method, route.target, skip_host=True, skip_accept_encoding=True)
                for name, value in headers:
                    conn.putheader(name, value)
                conn.endheaders(body)
                resp = conn.getresponse()
            except (OSError, http.client.HTTPException, ValueError):
                raise rules.Refused("upstream_unreachable", 502) from None
            if 300 <= resp.status < 400:
                raise rules.Refused("upstream_redirect", 502)
            if not (200 <= resp.status < 300 or 400 <= resp.status < 600):
                raise rules.Refused("upstream_status", 502)
            lines = [f"HTTP/1.1 {resp.status} {_phrase(resp.status)}\r\n"]
            lines += [f"{name}: {value}\r\n" for name, value in rules.response_headers(resp.getheaders())]
            no_body = resp.status in (204, 205)
            lines.append(("" if no_body else "Transfer-Encoding: chunked\r\n") + "Connection: close\r\n\r\n")
            client.settimeout(idle)
            client.sendall("".join(lines).encode("ascii"))
            rec["status"] = resp.status
            if no_body:
                return
            while True:
                try:
                    chunk = resp.read1(65536)
                except (OSError, http.client.HTTPException):
                    raise _Closed("upstream_cut") from None
                if not chunk:
                    break
                client.sendall(b"%x\r\n%s\r\n" % (len(chunk), chunk))
                rec["out"] += len(chunk)
            if resp.length:
                raise _Closed("upstream_cut")
            client.sendall(b"0\r\n\r\n")
        finally:
            if up_sock is not None:
                self._track(up_sock, add=False)
            conn.close()


def _phrase(status):
    try:
        return http.HTTPStatus(status).phrase
    except ValueError:
        return "Status"


def _read_head(client, deadline):
    buf = b""
    while True:
        end = buf.find(b"\r\n\r\n")
        if end >= 0:
            return buf[:end], buf[end + 4:]
        if len(buf) > rules.MAX_HEAD:
            raise rules.Refused("head_too_large", 431)
        if b"\n" in buf.replace(b"\r\n", b""):
            raise rules.Refused("bad_line_end")
        data = _recv(client, deadline, "head_timeout")
        if not data:
            raise _Closed("client_gone")
        buf += data


def _read_body(client, rest, length, deadline):
    """Exactly `length` bytes into one preallocated buffer; bytes after them are never read as a request."""
    body = bytearray(length)
    got = min(len(rest), length)
    body[:got] = rest[:got]
    with memoryview(body) as view:
        while got < length:
            _wait(client, deadline, "body_timeout")
            try:
                n = client.recv_into(view[got:], min(length - got, 1 << 20))
            except TimeoutError:
                raise _Closed("body_timeout") from None
            if not n:
                raise rules.Refused("short_body")
            got += n
    return body


def _wait(client, deadline, code):
    left = deadline - time.monotonic()
    if left <= 0:
        raise _Closed(code)
    client.settimeout(left)


def _recv(client, deadline, code):
    _wait(client, deadline, code)
    try:
        return client.recv(65536)
    except TimeoutError:
        raise _Closed(code) from None


def _send_error(client, status, code, extra=""):
    body = json.dumps({"error": code}).encode("ascii")
    head = f"HTTP/1.1 {status} {_phrase(status)}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n{extra}Connection: close\r\n\r\n"
    try:
        client.settimeout(5)
        client.sendall(head.encode("ascii") + body)
    except OSError:
        pass


def _shutdown(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def _linger_close(client):
    """Half-close and drain what the client still sends (briefly), so unread bytes do not turn the close into a reset that loses the answer."""
    try:
        client.shutdown(socket.SHUT_WR)
        client.settimeout(1)
        drained, end = 0, time.monotonic() + 1
        while drained < (1 << 20) and time.monotonic() < end:
            data = client.recv(65536)
            if not data:
                break
            drained += len(data)
    except OSError:
        pass
    finally:
        client.close()

