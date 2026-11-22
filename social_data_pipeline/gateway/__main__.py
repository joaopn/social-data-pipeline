"""Run the gateway: `python -m social_data_pipeline.gateway <config.json>`; serves until SIGTERM or SIGINT, then returns 0."""

import signal
import sys
import threading

from .server import Gateway
from .table import ConfigError, load, read_credential


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: python -m social_data_pipeline.gateway <config.json>", file=sys.stderr)
        return 2
    try:
        table = load(args[0])
        credential = read_credential(table.credential_file) if table.credential_file else ""
        gateway = Gateway(table, credential)
        gateway.start()
    except ConfigError as err:
        print(f"gateway: refusing to start: {err}", file=sys.stderr)
        return 2
    for lst, port in zip(table.listeners, gateway.ports):
        print(f"gateway: {lst.kind} listening on port {port}", flush=True)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    stop.wait()
    gateway.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
