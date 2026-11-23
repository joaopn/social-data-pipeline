"""The allow-list of tasks: the CLI actions that are not a pipeline profile and run as recorded runs (`control.runs.start_task`), with their log, their cancel and their end in the run store like a pipeline run.

A task is a verb of the registry (`control.verbs`) that is switched on for the socket and names its action, a keyword-only `do_*` function of the CLI module that asks nothing and returns an exit code. What its entry here adds:

- `key`: the names of the fields that tell two tasks of the verb apart. One task of the verb with the same values of these fields can be alive at a time; a second one is refused. Each must be a required socket input of the verb whose type is not a secret.
- `label`: how the run is named where a pipeline run shows its profile, a `str.format` text over the key fields, such as "download \u00b7 {source}".

Adding a task is one line in `TASKS` (and the verb's name in the pinned set of its test), once the verb has typed inputs, `socket_enabled=True` and its `action`:

    Task("source.download", key=("source",), label="download \u00b7 {source}"),

The action runs in `control.child`, in a process group of its own under the run's supervisor. What it prints, on standard output and standard error, is the run's log, as it is printed: it reports its progress by printing lines (flushed; the child runs Python unbuffered). It returns its exit code, or raises `ActionError`. A cancel sends its process group TERM and, after the grace time, KILL: what the action has done until then stays done (files it wrote, work a database was handed through `docker compose exec`).

The module imports nothing of the CLI and nothing that starts a process, so the broker can read the list at load time.
"""

from dataclasses import dataclass
from types import MappingProxyType


@dataclass(frozen=True)
class Task:
    """One verb that runs as a recorded run. `key` is a tuple of field names, `label` a format text over them."""

    verb: str
    key: tuple
    label: str

    def __post_init__(self):
        object.__setattr__(self, "key", tuple(self.key))


def _allow_list(*tasks):
    by_verb = {}
    for task in tasks:
        if task.verb in by_verb:
            raise ValueError(f"task {task.verb} is listed twice")
        by_verb[task.verb] = task
    return MappingProxyType(by_verb)


TASKS = _allow_list(
    # One index build per database at a time: a second one would queue behind the first's schema change (StarRocks) or compete for its maintenance memory (PostgreSQL).
    Task("db.create_indexes", key=("database",), label="create indexes \u00b7 {database}"),
)


def key_values(task, validated):
    """The values of the task's key fields in a validated request, as a dict. Raises KeyError when the request lacks one (a key field that is not a required input)."""
    return {name: validated[name] for name in task.key}


def label_of(task, key):
    """The label of the run, from the key values."""
    return task.label.format(**key)
