"""The OpenAI-compatible back end of the chat's runner: one process of the project's own agent (`chat.agent`) per turn, which prints its progress as the stream-json lines of Claude Code, so the turn is run and read by `claude.Process` and `claude.Translator` unchanged.

The process gets an environment built from scratch (`environment`): the turn's fresh `HOME`, `TMPDIR` and `MPLCONFIGDIR`, `PATH`, the `PYTHONPATH` that holds this package, the gateway's address, the model and the attached databases; nothing of the runner's own. Python runs it with `-P` (the working folder, the chat's `files/`, which the assistant writes, is not put on the module path) and `-s` (no user site folder).
"""

import os
import sys

from . import claude

MODULE = "social_data_pipeline.chat.agent"
# The folder that holds the package (`/app` in the sandbox image).
PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def command(python, config, files):
    """The argument list of one turn: `config` is the chat's `home/` (where the history is kept), `files` its working folder."""
    return [python, "-P", "-s", "-m", MODULE, "--home", config, "--files", files]


def environment(home, tmp, gateway, model, databases):
    """The whole environment of the process; `home` is the turn's fresh `HOME`, `tmp` its scratch folder."""
    return {
        "HOME": home, "TMPDIR": tmp, "MPLCONFIGDIR": os.path.join(tmp, "matplotlib"),
        "PATH": claude.PATH, "PYTHONPATH": PACKAGE_ROOT,
        "SDP_CHAT_GATEWAY": gateway, "SDP_CHAT_MODEL": model, "SDP_CHAT_DATABASES": ",".join(databases),
    }


class OpenAICompatible:
    """The back end: `gateway` is the gateway's address, `model` the configured model, `databases` the names of the attached databases."""

    name = "openai-compatible"

    def __init__(self, gateway, model, databases, *, python=sys.executable, line_max=claude.LINE_MAX_BYTES, grace=claude.STOP_GRACE_SECONDS, stderr_max=claude.STDERR_MAX_BYTES, join=claude.JOIN_SECONDS):
        self.gateway, self.model, self.databases, self.python = gateway, model, tuple(databases), python
        self.line_max, self.grace, self.stderr_max, self.join = line_max, grace, stderr_max, join

    def turn(self, chat_fd, config, home, files, tmp, text, session, resume):
        """A `claude.Process` for one turn (not started) and its `claude.Translator`; `session` and `resume` are not used (the history is the agent's file in `config`)."""
        argv = command(self.python, config, files)
        env = environment(home, tmp, self.gateway, self.model, self.databases)
        process = claude.Process(argv, env, files, claude.user_line(text), chat_fd, line_max=self.line_max, grace=self.grace, stderr_max=self.stderr_max, join=self.join)
        return process, claude.Translator()
