"""The environment the broker keeps for itself and for everything it starts: a short list of names, and nothing else.

A process that runs for a long time and starts the CLI on behalf of a browser should not carry whatever the shell it was started from happened to hold (other tokens, a preloaded library, a Python path). What stays is what finding programs, a home folder, the locale and the time zone, reaching Docker and the network, and running the CLI as the terminal does need: the compose project and file, which decide which deployment a compose command addresses, and the two Hugging Face variables that the compose file passes on and the CLI reads. The module uses the standard library only and changes nothing when it is loaded.
"""

import os

ALLOWED = (
    "PATH", "HOME", "USER", "LOGNAME", "LANG", "LANGUAGE", "TZ", "TMPDIR", "XDG_RUNTIME_DIR",
    "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "SSH_AUTH_SOCK",
    "COMPOSE_PROJECT_NAME", "COMPOSE_FILE",
    "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy",
    "HF_TOKEN", "HF_HOME",
)
# Every locale category: LC_ALL, LC_CTYPE, LC_MESSAGES, ...
ALLOWED_PREFIX = "LC_"
# The allowed variables whose value is a secret: what the broker must overwrite in text it hands out.
SECRET_NAMES = ("HF_TOKEN",)


def is_allowed(name):
    """Whether a variable of this name stays."""
    return isinstance(name, str) and (name in ALLOWED or name.startswith(ALLOWED_PREFIX))


def clean(environ):
    """A new dict with the allowed variables of `environ` and their values. Nothing is added."""
    return {name: value for name, value in environ.items() if is_allowed(name)}


def known_secrets(environ):
    """The values of the allowed variables of `environ` that are secrets, in the order of SECRET_NAMES, each once, empty ones left out: for the list of what redaction overwrites."""
    found = []
    for name in SECRET_NAMES:
        value = environ.get(name)
        if isinstance(value, str) and value and value not in found:
            found.append(value)
    return found


def reduce_process_environment():
    """Remove every variable that is not allowed from this process's environment, in place, and return the names that were removed, sorted. Only names: a value can be a secret. Call it before any thread is started: a change of the environment while another thread reads it (any library that asks for a variable does) is not safe."""
    removed = sorted(name for name in os.environ if not is_allowed(name))
    for name in removed:
        del os.environ[name]
    return removed
