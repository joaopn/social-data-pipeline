"""Tests for `control.client`: why a request got no reply, told apart in `BrokerUnreachable.reason`.

The contracts pinned here:

- No socket file (and no folder for it) is `no_socket`; a socket file nobody listens on is `refused`; a socket that cannot be opened by this user is `permission_denied`; a connection the other end closes without a reply (what the broker does to another user) is `no_reply`, whether it closes before or after the request is in.
- Each reason has one constant text, which is the error's `message`; no text names the path or anything sent.
- `login` sends the channel it is given, `remote` by default.

Real unix sockets in a temp folder; no broker runs. Linux only.
"""

from __future__ import annotations

import json
import os
import socket
import struct
import sys
import threading

import pytest

from social_data_pipeline.control import client

pytestmark = pytest.mark.skipif(not sys.platform.startswith("linux"), reason="unix sockets")


def reason_of(path):
    with pytest.raises(client.BrokerUnreachable) as caught:
        client.Client(str(path), timeout=5).hello()
    return caught.value


def serve_once(path, behave):
    """A listening socket at `path` whose one connection is handed to `behave` in a thread."""
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    listener.listen(1)

    def run():
        connection, _ = listener.accept()
        try:
            behave(connection)
        finally:
            connection.close()
            listener.close()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


def test_no_socket_file_and_no_folder_are_no_socket(tmp_path):
    """Bug class: a broker that was never started reported like one that refuses or misbehaves, so the operator looks for the wrong fault."""
    assert reason_of(tmp_path / "broker.sock").reason == "no_socket"
    assert reason_of(tmp_path / "missing" / "broker.sock").reason == "no_socket"


def test_a_socket_file_nobody_listens_on_is_refused(tmp_path):
    """Bug class: a socket left by a broker that is gone read as "no socket" or as a broker that answers nothing."""
    path = tmp_path / "broker.sock"
    stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    stale.bind(str(path))
    stale.close()

    assert reason_of(path).reason == "refused"


@pytest.mark.skipif(os.geteuid() == 0, reason="root opens every folder")
def test_a_socket_this_user_cannot_open_is_permission_denied(tmp_path):
    """Bug class: a socket folder of the wrong mode or owner (the web UI container run as another user) reported as "the broker is not running"."""
    folder = tmp_path / "socket"
    folder.mkdir()
    path = folder / "broker.sock"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(path))
    listener.listen(1)
    os.chmod(folder, 0)
    try:
        assert reason_of(path).reason == "permission_denied"
    finally:
        os.chmod(folder, 0o700)
        listener.close()


@pytest.mark.parametrize("read_first", [True, False])
def test_a_connection_closed_without_a_reply_is_no_reply(tmp_path, read_first):
    """Bug class: the broker's answer to another user (the connection closed at once, without a reply) reported as "nobody listens" or "no socket", which sends the operator to start a broker that runs."""
    path = tmp_path / "broker.sock"

    def close(connection):
        if read_first:
            connection.recv(65536)

    thread = serve_once(path, close)
    assert reason_of(path).reason == "no_reply"
    thread.join(5)


def test_a_connection_closed_before_the_request_is_sent_is_no_reply(tmp_path, monkeypatch):
    """Bug class: a broker that closes the connection before the request is in (the send fails) reported as "cannot be reached" instead of "closed without a reply"."""
    path = tmp_path / "broker.sock"
    closed = threading.Event()
    thread = serve_once(path, lambda connection: None)
    real_encode = client.framing.encode

    def encode_after_the_close(document, *, max_bytes):
        thread.join(5)
        closed.set()
        return real_encode(document, max_bytes=max_bytes)

    monkeypatch.setattr(client.framing, "encode", encode_after_the_close)
    assert reason_of(path).reason == "no_reply"
    assert closed.is_set()


def test_each_reason_has_its_constant_text_and_no_path(tmp_path):
    """Bug class: a reason without a text (a KeyError where an error was due), or a text that carries the path of the socket."""
    assert set(client.REASONS) == {"no_socket", "refused", "permission_denied", "no_reply", "not_reached"}
    error = reason_of(tmp_path / "broker.sock")
    assert error.code == "unreachable" and error.message == client.REASONS["no_socket"]
    assert str(tmp_path) not in str(error) and str(tmp_path) not in error.message
    assert all(isinstance(text, str) and text for text in client.REASONS.values())


@pytest.mark.parametrize("channel", [None, "local", "remote"])
def test_login_sends_its_channel(tmp_path, channel):
    """Bug class: a client that drops the channel, so every login of the web UI draws on the remote budget whatever the web UI decided."""
    path = tmp_path / "broker.sock"
    received = []

    def answer(connection):
        header = connection.recv(4)
        (length,) = struct.unpack(">I", header)
        payload = b""
        while len(payload) < length:
            payload += connection.recv(length - len(payload))
        received.append(json.loads(payload))
        reply = json.dumps({"ok": True, "result": {"token": "made-up-token-1", "session": 1, "expires_in": 60}}).encode()
        connection.sendall(struct.pack(">I", len(reply)) + reply)

    thread = serve_once(path, answer)
    session = client.Client(str(path), timeout=5)
    session.login("made-up-password-1", **({} if channel is None else {"channel": channel}))
    thread.join(5)

    assert received == [{"op": "login", "password": "made-up-password-1", "channel": channel or "remote"}]
    assert session.token == "made-up-token-1"
