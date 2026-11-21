"""Ask the broker for `hello` from inside the web UI's container: `python -m social_data_pipeline.webui.probe`, which `sdp webui start` runs with `docker exec`.

It first waits, at most 20 seconds, until the web UI's server accepts connections on both of its ports (so that a server that ends while it starts, on a key it cannot read for one, is not taken for a running one), then asks the broker. It prints one word and exits: `answered` (exit code 0), `not_serving` when the server did not accept connections in time, or why the broker gave no answer, a reason of `BrokerUnreachable` (`no_socket`, `refused`, `permission_denied`, `no_reply`, `not_reached`), `refused_request` for an error the broker answered, or `no_setting` when the container has no socket setting (exit code 1). Standard library and the broker's client only.
"""

import os
import socket
import sys
import time

from social_data_pipeline.control import client
from social_data_pipeline.webui import config

PROBE_SECONDS = 5
SERVING_SECONDS = 20


def serving(seconds=SERVING_SECONDS):
    """Whether both ports of the server accept a connection within `seconds`."""
    deadline = time.monotonic() + seconds
    waiting = [config.LISTEN_PORT, config.LOCAL_PORT]
    while waiting and time.monotonic() < deadline:
        for port in list(waiting):
            with socket.socket() as attempt:
                attempt.settimeout(1)
                if attempt.connect_ex(("127.0.0.1", port)) == 0:
                    waiting.remove(port)
        if waiting:
            time.sleep(0.2)
    return not waiting


def main():
    if not serving():
        print("not_serving")
        return 1
    socket_path = os.environ.get(config.ENV_SOCKET)
    if not socket_path:
        print("no_setting")
        return 1
    try:
        client.Client(socket_path, timeout=PROBE_SECONDS).hello()
    except client.BrokerUnreachable as error:
        print(error.reason)
        return 1
    except client.BrokerError:
        print("refused_request")
        return 1
    print("answered")
    return 0


if __name__ == "__main__":
    sys.exit(main())
