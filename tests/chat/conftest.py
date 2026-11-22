"""A runner in `tmp_path` with a stand-in `claude` executable (a Python script that follows a plan file), served on a socket; everything it started is ended at teardown."""

import json
import sys
import time
from types import SimpleNamespace

import pytest
from chat_helpers import INIT, STANDIN, exchange, result

from social_data_pipeline.chat import claude, runner


@pytest.fixture
def chat_runner(tmp_path):
    """`chat_runner(limits=..., backend=..., databases=..., cap=..., queue=...)`: a started runner (with the stand-in `claude` unless another back end object is given; `queue` attaches the query queue); `.call(op, **fields)`, `.plan(steps)`, `.records()`, `.wait(chat)`."""
    made = []
    script = tmp_path / "claude"
    plan_path = tmp_path / "plan.json"
    record = tmp_path / "record.jsonl"
    script.write_text(f"#!{sys.executable}\nPLAN = {str(plan_path)!r}\n" + STANDIN)
    script.chmod(0o755)

    def make(limits=None, backend=None, databases=("mongo",), cap=1 << 30, queue=False):
        work, tmp = tmp_path / "work", tmp_path / "tmp"
        work.mkdir(exist_ok=True)
        tmp.mkdir(exist_ok=True)
        back = backend or claude.ClaudeCode("127.0.0.1", databases, queue=queue, executable=str(script), grace=0.5, join=0.5)
        chats = runner.Runner(str(work), back, back.name, list(databases), cap, queue=queue, tmp=str(tmp), limits=limits or runner.Limits())
        socket_path = str(tmp_path / "runner.sock")
        server = runner.Server(chats, socket_path)
        server.start()
        made.append((server, chats))

        def call(op, **fields):
            return exchange(socket_path, {"op": op, **fields})

        def plan(steps):
            plan_path.write_text(json.dumps({"record": str(record), "steps": steps}))

        def records():
            return [json.loads(line) for line in record.read_text().splitlines()] if record.exists() else []

        def wait(chat, seconds=20):
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline:
                reply = call("chat.events", chat=chat, offset=0, limit=200)["result"]
                if not reply["running"]:
                    return reply["events"]
                time.sleep(0.05)
            raise AssertionError("the turn did not end in time")

        def new():
            return call("chat.new")["result"]["chat"]

        plan([INIT, result()])
        return SimpleNamespace(runner=chats, server=server, socket=socket_path, work=work, tmp=tmp, call=call, plan=plan, records=records, wait=wait, new=new)

    yield make
    for server, chats in made:
        server.close()
        chats.close()
