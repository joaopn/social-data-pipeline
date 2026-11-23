"""The OpenAI-compatible back end of the chat's runner: one process of the project's own agent (`chat.agent`) per turn, which prints its progress as the stream-json lines of Claude Code, so the turn is run and read by `claude.Process` and `claude.Translator` unchanged.

A generated title (`title`) is one request to the gateway's model port (`/chat/completions`, the configured model, `claude.TITLE_INSTRUCTION` as the system message, the question, at most 30 tokens, no streaming), within 30 seconds in all, its answer read up to 64 KiB.

The process gets an environment built from scratch (`environment`): the turn's fresh `HOME`, `TMPDIR` and `MPLCONFIGDIR`, `PATH`, the `PYTHONPATH` that holds this package, the gateway's address, the model, the attached databases, with the query queue `SDP_CHAT_QUEUE=1` and with the query time limit `SDP_CHAT_QUERY_SECONDS`; nothing of the runner's own. Python runs it with `-P` (the working folder, the chat's `files/`, which the assistant writes, is not put on the module path) and `-s` (no user site folder).
"""

import http.client
import json
import os
import sys
import time

from . import claude

MODULE = "social_data_pipeline.chat.agent"
# The folder that holds the package (`/app` in the sandbox image).
PACKAGE_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# The gateway's path of the model (the agent's `MODEL_PATH`) and the most tokens a title may take.
MODEL_PATH = "/chat/completions"
TITLE_MAX_TOKENS = 30
TitleFailed = claude.TitleFailed


def command(python, config, files):
    """The argument list of one turn: `config` is the chat's `home/` (where the history is kept), `files` its working folder."""
    return [python, "-P", "-s", "-m", MODULE, "--home", config, "--files", files]


def environment(home, tmp, gateway, model, databases, queue=False, query_seconds=None):
    """The whole environment of the process; `home` is the turn's fresh `HOME`, `tmp` its scratch folder; `SDP_CHAT_QUEUE=1` with the query queue; `SDP_CHAT_QUERY_SECONDS` with the query time limit."""
    return {
        "HOME": home, "TMPDIR": tmp, "MPLCONFIGDIR": os.path.join(tmp, "matplotlib"),
        "PATH": claude.PATH, "PYTHONPATH": PACKAGE_ROOT,
        "SDP_CHAT_GATEWAY": gateway, "SDP_CHAT_MODEL": model, "SDP_CHAT_DATABASES": ",".join(databases),
        **({"SDP_CHAT_QUEUE": "1"} if queue else {}),
        **({claude.QUERY_ENV: str(query_seconds)} if query_seconds is not None else {}),
    }


class OpenAICompatible:
    """The back end: `gateway` is the gateway's address, `model` the configured model (the one model a chat may be given, and the model of a chat without one), `databases` the names of the attached databases, `queue` whether the query queue is attached, `query_seconds` the query time limit (None: none was given)."""

    name = "openai-compatible"

    def __init__(self, gateway, model, databases, *, queue=False, query_seconds=None, python=sys.executable, line_max=claude.LINE_MAX_BYTES, grace=claude.STOP_GRACE_SECONDS, stderr_max=claude.STDERR_MAX_BYTES, join=claude.JOIN_SECONDS):
        self.gateway, self.model, self.databases, self.queue, self.python = gateway, model, tuple(databases), queue, python
        self.query_seconds = query_seconds
        self.models, self.model_default = (model,), model
        self.line_max, self.grace, self.stderr_max, self.join = line_max, grace, stderr_max, join

    def turn(self, chat_fd, config, home, files, tmp, text, session, resume, model=None):
        """A `claude.Process` for one turn (not started) and its `claude.Translator`; `session` and `resume` are not used (the history is the agent's file in `config`), nor `model` (the runner takes only the configured model, which the agent always gets)."""
        argv = command(self.python, config, files)
        env = environment(home, tmp, self.gateway, self.model, self.databases, self.queue, self.query_seconds)
        process = claude.Process(argv, env, files, claude.user_line(text), chat_fd, line_max=self.line_max, grace=self.grace, stderr_max=self.stderr_max, join=self.join)
        return process, claude.Translator()

    def title(self, text, seconds=claude.TITLE_SECONDS):
        """A title for a chat that starts with `text`: one request to the gateway's model port with the configured model, within `seconds` in all (every read waits only for what is left), the answer at most `claude.TITLE_OUTPUT_MAX` bytes. Returns (the first choice's message text, None: the endpoint reports no cost); raises TitleFailed (any status but 200 is refused without its body read)."""
        body = json.dumps({"model": self.model, "messages": [{"role": "system", "content": claude.TITLE_INSTRUCTION}, {"role": "user", "content": text}], "max_tokens": TITLE_MAX_TOKENS, "stream": False}, ensure_ascii=False).encode("utf-8")
        deadline = time.monotonic() + seconds

        def left():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TitleFailed("the title request took longer than its limit")
            return remaining

        connection = http.client.HTTPConnection(self.gateway, claude.MODEL_PORT, timeout=left())
        try:
            connection.request("POST", MODEL_PATH, body=body, headers={"Content-Type": "application/json", "Accept": "application/json"})
            # Kept: the connection lets go of its socket when the answer says it closes, and the reads still go through it.
            sock = connection.sock
            sock.settimeout(left())
            response = connection.getresponse()
            if response.status != 200:
                raise TitleFailed(f"the endpoint answered {int(response.status)}")
            data = bytearray()
            while True:
                sock.settimeout(left())
                chunk = response.read1(65536)
                if not chunk:
                    break
                data += chunk
                if len(data) > claude.TITLE_OUTPUT_MAX:
                    raise TitleFailed("the title answer is larger than its limit")
        except (OSError, http.client.HTTPException, ValueError):
            raise TitleFailed("the title request failed") from None
        finally:
            connection.close()
        try:
            doc = json.loads(bytes(data).decode("utf-8"))
            content = doc["choices"][0]["message"]["content"]
        except (UnicodeDecodeError, ValueError, RecursionError, LookupError, TypeError):
            raise TitleFailed("the title answer is not a chat completion") from None
        if not isinstance(content, str):
            raise TitleFailed("the title answer holds no text")
        return content, None

