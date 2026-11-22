"""The web UI's server, what the container runs: `python -m social_data_pipeline.webui.serve`.

One uvicorn server in one process, with one application (the guarded one `app.from_environment` builds, once, before any socket is bound: a setting it refuses ends the process with its line) on two sockets: `config.LISTEN_PORT` (8443), to which every listen address but 127.0.0.1 is published, and `config.LOCAL_PORT` (8444), to which only 127.0.0.1 of the host is published (in the every-address mode one publication goes to `config.LISTEN_PORT` and nothing is published to `config.LOCAL_PORT`). The application tells a local login by two signs together: the port it arrived on (the ASGI scope's `server` is `config.LOCAL_PORT`) AND its client address being the network's gateway (see `config`).

HTTPS with the certificate and key of the mounted folder; one process (the sessions live in its memory); HTTP/1.1 through h11 only; no WebSocket; no `Server` header; forwarded headers not believed; uvicorn's own access log off (the guard logs method, path and status); at most 256 connections at a time over both ports, which is also the only bound on clients that send their request headers slowly; idle connections closed after 5 seconds.
"""

import socket

import uvicorn

from social_data_pipeline.webui import app as webapp
from social_data_pipeline.webui import config

CERT_FILE = "/tls/cert.pem"
KEY_FILE = "/tls/key.pem"
CONNECTIONS_MAX = 256
KEEP_ALIVE_SECONDS = 5


def server_config(application):
    """The uvicorn settings of the web UI, serving `application`."""
    return uvicorn.Config(
        application,
        ssl_certfile=CERT_FILE,
        ssl_keyfile=KEY_FILE,
        http="h11",
        ws="none",
        workers=1,
        server_header=False,
        proxy_headers=False,
        access_log=False,
        limit_concurrency=CONNECTIONS_MAX,
        timeout_keep_alive=KEEP_ALIVE_SECONDS,
    )


def listening_sockets():
    """One TCP socket on every address of the container for each of the two ports, bound and not yet listening (the server starts listening)."""
    made = []
    for port in (config.LISTEN_PORT, config.LOCAL_PORT):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", port))
        made.append(sock)
    return made


def main():
    application = webapp.from_environment()
    uvicorn.Server(server_config(application)).run(sockets=listening_sockets())


if __name__ == "__main__":
    main()
