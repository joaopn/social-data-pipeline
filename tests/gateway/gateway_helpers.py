"""Stand-in upstreams on loopback that record exactly what they received, and raw-socket helpers."""

import socket
import threading
import time
from types import SimpleNamespace

CRED = "sk-gateway-credential-0123456789"
WAIT = 10


def ok_json(conn, rec):
    conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}")


class StandIn:
    """A loopback HTTP server that records (request line, header pairs, body) of each request and answers through `respond(conn, record)`."""

    def __init__(self, respond=ok_json):
        self.respond = respond
        self.records = []
        self.arrived = threading.Semaphore(0)
        self.sock = socket.create_server(("127.0.0.1", 0))
        self.port = self.sock.getsockname()[1]
        self.threads = [threading.Thread(target=self._accept, daemon=True)]
        self.threads[0].start()

    def _accept(self):
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            thread = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            self.threads.append(thread)
            thread.start()

    def _serve(self, conn):
        with conn:
            conn.settimeout(WAIT)
            buf = b""
            while b"\r\n\r\n" not in buf:
                data = conn.recv(65536)
                if not data:
                    return
                buf += data
            head, body = buf.split(b"\r\n\r\n", 1)
            line, *lines = head.decode("latin-1").split("\r\n")
            headers = [(n.strip().lower(), v.strip()) for n, v in (x.split(":", 1) for x in lines)]
            length = int(dict(headers).get("content-length", "0"))
            while len(body) < length:
                body += conn.recv(65536)
            rec = SimpleNamespace(line=line, headers=headers, body=body, raw=buf)
            self.records.append(rec)
            self.arrived.release()
            self.respond(conn, rec)

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()
        for thread in self.threads:
            thread.join(timeout=WAIT)


def request(target="/v1/messages", method="POST", headers=(), body=b'{"model":"x","max_tokens":1}', length=True):
    """Raw request bytes; `length=True` adds a correct `Content-Length`."""
    lines = [f"{method} {target} HTTP/1.1", "Host: gw"] + [f"{n}: {v}" for n, v in headers]
    if length and body:
        lines.append(f"Content-Length: {len(body)}")
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + body


def exchange(port, raw, half_close=True):
    """Send raw bytes, read the answer until the gateway closes; the gateway's credential is never in it."""
    with socket.create_connection(("127.0.0.1", port), timeout=WAIT) as sock:
        sock.sendall(raw)
        if half_close:
            sock.shutdown(socket.SHUT_WR)
        data = b""
        while chunk := sock.recv(65536):
            data += chunk
    assert CRED.encode() not in data
    return data


def parse(raw):
    """(status, {lower name: value}, body with chunked framing removed)."""
    head, _, body = raw.partition(b"\r\n\r\n")
    line, *lines = head.decode("latin-1").split("\r\n")
    headers = {n.strip().lower(): v.strip() for n, v in (x.split(":", 1) for x in lines)}
    if headers.get("transfer-encoding") == "chunked":
        out = b""
        while body:
            size, _, body = body.partition(b"\r\n")
            out, body = out + body[: int(size, 16)], body[int(size, 16) + 2:]
        body = out
    return int(line.split(" ")[1]), headers, body


def wait_for(predicate):
    deadline = time.monotonic() + WAIT
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.01)
