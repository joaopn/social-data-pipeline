import json
import pytest
from pathlib import Path


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: tests that take >5s (decompression, DB)")
    config.addinivalue_line("markers", "postgres: requires PostgreSQL container")
    config.addinivalue_line("markers", "mongo: requires MongoDB container")


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent / "fixtures"

@pytest.fixture
def reddit_fixtures_dir(fixtures_dir):
    return fixtures_dir / "reddit"

@pytest.fixture
def custom_fixtures_dir(fixtures_dir):
    return fixtures_dir / "custom"

@pytest.fixture
def config_fixtures_dir(fixtures_dir):
    return fixtures_dir / "config"

@pytest.fixture
def state_fixtures_dir(fixtures_dir):
    return fixtures_dir / "state"

def load_ndjson(path):
    """Load NDJSON file into list of dicts."""
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records

def count_ndjson_lines(path):
    """Count non-empty lines in an NDJSON file."""
    count = 0
    with open(path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


class FakeRunDocker:
    """Stands in for `runstore._docker`, the one function through which the run store calls Docker. It answers `ps`, `inspect`, `logs` and `stop` from a list of made-up containers the way Docker does (the label filter of `ps` is honoured; a stopped container is gone) and keeps every call, so a test can assert what was asked and what was never asked.

    `calls` holds the argument list of every call in order, `options` the keyword arguments of the same calls. `reachable = False` makes every call fail the way a missing binary, a stopped daemon or a timeout does; `unreachable_from` is the number of calls that are answered before that happens. `inspect_answer` replaces the answer of every `inspect`, `extra_ps_lines` are added to the output of every `ps`, and `on_ps` is called with the list of labels of each `ps` before it is answered. `stop_fails` makes `stop` fail and leave the container running; `ends_before_stop` makes `stop` fail the way it does for a container that ended by itself a moment earlier (it is gone afterwards). `ends_at_inspect` holds the ids of containers that end just before they are inspected, and `inspect_fails` makes every `inspect` fail while the containers stay; `ends_at_logs` and `logs_fail` do the same for `logs`. `daemon_down_from` is the number of calls that are answered before the daemon stops: from then on every command exits with an error and prints nothing, which is what the Docker client does when the daemon is gone.
    """

    def __init__(self, unavailable):
        self._unavailable = unavailable
        self.calls = []
        self.options = []
        self.containers = []
        self.reachable = True
        self.inspect_answer = None
        self.extra_ps_lines = []
        self.stop_fails = False
        self.ends_before_stop = False
        self.ends_at_inspect = set()
        self.inspect_fails = False
        self.ends_at_logs = set()
        self.logs_fail = False
        self.daemon_down_from = None
        self.unreachable_from = None
        self.on_ps = None

    def add(self, container, working_dir, *, service="parse", run="", started="2026-11-21T10:30:00.123456789Z", source="reddit", log="", oneoff=True):
        """Add a running container. `run` is its `sdp.run` label ("" for none), `source` its SOURCE variable (None when it has none)."""
        self.containers.append({"id": container, "working_dir": str(working_dir), "service": service, "run": run, "started": started, "source": source, "log": log, "oneoff": oneoff})

    def named(self, command):
        """The argument lists of the calls of one Docker command."""
        return [call for call in self.calls if call[0] == command]

    def _labels(self, container):
        labels = {"com.docker.compose.project.working_dir": container["working_dir"], "com.docker.compose.service": container["service"]}
        if container["oneoff"]:
            labels["com.docker.compose.oneoff"] = "True"
        if container["run"]:
            labels["sdp.run"] = container["run"]
        return labels

    def _find(self, container_id):
        return next((container for container in self.containers if container["id"] == container_id), None)

    def __call__(self, args, *, timeout, merge_stderr=False, attached=False):
        args = list(args)
        self.calls.append(args)
        self.options.append({"timeout": timeout, "merge_stderr": merge_stderr, "attached": attached})
        if not self.reachable or (self.unreachable_from is not None and len(self.calls) > self.unreachable_from):
            raise self._unavailable("Docker cannot be reached")
        if self.daemon_down_from is not None and len(self.calls) > self.daemon_down_from:
            return 1, ""
        command = args[0]
        if command == "ps":
            labels = [args[index + 1].removeprefix("label=") for index, argument in enumerate(args) if argument == "--filter"]
            if self.on_ps is not None:
                self.on_ps(labels)
            listed = list(self.containers)
            for label in labels:
                name, has_value, value = label.partition("=")
                listed = [container for container in listed if name in self._labels(container) and (not has_value or self._labels(container)[name] == value)]
            lines = [json.dumps([container["id"], container["working_dir"], container["service"], container["run"]]) for container in listed]
            return 0, "".join(line + "\n" for line in lines + self.extra_ps_lines)
        container = self._find(args[-1])
        if command == "inspect":
            if self.inspect_answer is not None:
                return 0, self.inspect_answer
            if container is not None and container["id"] in self.ends_at_inspect:
                self.containers.remove(container)
                return 1, ""
            if container is None or self.inspect_fails:
                return 1, ""
            return 0, f"{container['started']} " + ("" if container["source"] is None else f"SOURCE={container['source']}") + "\n"
        if command == "logs":
            if container is not None and container["id"] in self.ends_at_logs:
                self.containers.remove(container)
                return 1, "Error response from daemon: No such container\n"
            if container is None or self.logs_fail:
                return 1, "Error response from daemon: No such container\n"
            return 0, container["log"]
        if command == "stop":
            if container is not None and self.ends_before_stop:
                self.containers.remove(container)
                return 1, ""
            if container is None or self.stop_fails:
                return 1, ""
            self.containers.remove(container)
            return 0, container["id"] + "\n"
        raise AssertionError(f"the run store ran a Docker command no test expects: {args}")


@pytest.fixture
def run_docker_guard(monkeypatch):
    """Keep a test of the run commands away from the real Docker: the run store's Docker function is replaced by one that records the call and answers "Docker cannot be reached", and the test fails at its end if that function was ever reached. A test replaces it in turn, through `fake_run_docker` or, to test the real function with `subprocess.run` replaced, with `.real`. The two test files of the run commands use this fixture for every test."""
    from types import SimpleNamespace

    from social_data_pipeline import runstore

    guard = SimpleNamespace(real=runstore._docker, reached=[])

    def refuse(args, **kwargs):
        guard.reached.append(list(args))
        raise runstore.DockerUnavailable(runstore.DOCKER_NOT_REACHABLE)

    monkeypatch.setattr(runstore, "_docker", refuse)
    yield guard
    assert guard.reached == [], f"the test reached Docker without a fake in its place: {guard.reached}"


@pytest.fixture
def fake_run_docker(monkeypatch, run_docker_guard):
    """Replace the run store's Docker function with a FakeRunDocker and return it."""
    from social_data_pipeline import runstore

    fake = FakeRunDocker(runstore.DockerUnavailable)
    monkeypatch.setattr(runstore, "_docker", fake)
    return fake
