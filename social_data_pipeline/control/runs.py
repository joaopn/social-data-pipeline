"""Starting a recorded pipeline run: `start_run` checks the request, refuses a second run of the same profile for the same source, creates the run in the run store and starts the process that supervises it (`control.supervise`), which lives on by itself.

Nothing in the terminal starts a recorded run: `sdp run` records nothing. A run started here shows in `sdp runs list` as kind `web`, its output is read with `sdp runs logs` and it is stopped with `sdp runs cancel`.

What a caller (the host process that serves a browser session) must know:

- `fields` is the decoded JSON of a `run.start` request, the admin password in plain text included. The password goes to the supervisor over standard input and from there to the child; it is in no file, no argument list and no environment variable of these two processes. An error raised here holds no value of the request, but the frames of its traceback do: never log a traceback from `start_run`.
- The supervisor and, through it, the child and `docker compose` inherit the environment of the process that calls `start_run`, unchanged: keeping that environment free of anything a run must not see is the caller's job.
- The supervisor's standard output and standard error are the run's log, so a supervisor that cannot even be loaded leaves the interpreter's error text there, and the run then reads as `lost`.
- The supervisor is started in a session of its own and is not waited for. It ends when the run has ended; a caller that lives long calls `reap_supervisors` now and then (every `start_run` does), so that ended supervisors do not stay in the process table. A service manager that kills every process of the caller's control group when the caller stops ends the supervisors with it; their runs then read as `detached` or `lost`.
- What stays open: the pipeline's `mongo_ingest` step asks a question inside its container when it meets a truncated file, which a recorded run cannot answer (the question gets end-of-file). A run started with `sdp run` in a terminal that is still in its checks or its image build has no container yet, so the refusal of a second run cannot see it. A supervisor that dies before it has recorded anything (killed, or unable to start in a damaged installation) leaves the run as the store then reports it, `lost` or `detached`.

`start_task` starts a task the same way: a verb of the allow-list `control.tasks.TASKS` (a CLI action that is not a pipeline profile, such as a download), recorded with kind `task` and the task's label in place of a profile. Its action runs in the supervisor's child; the supervisor never asks Docker about it. What the caller must know is the same as for `start_run`, with every secret field of the request in place of the admin password.

Linux is assumed, as in the run store: the run's lock is a `flock` lock that the supervisor inherits with the descriptor.
"""

import contextlib
import json
import os
import subprocess
import sys
import threading

from social_data_pipeline import runstore
from social_data_pipeline.control import errors, tasks, verbs
from social_data_pipeline.control.types import Name, reveal

RUN_VERB = "run.start"
SUPERVISOR_ARGV = (sys.executable, "-m", "social_data_pipeline.control.supervise")

# Why a run was not started. None of them holds a value of the request.
RUN_ACTIVE = "a run of this profile for this source is already active"
DOCKER_NOT_REACHABLE = "Docker cannot be reached, so it cannot be told whether a run of this profile for this source is already active"
NOT_THE_DEPLOYMENT = "the folder is not the deployment folder of the CLI"
CLI_NOT_LOADABLE = "the CLI module cannot be loaded"
RECORD_NOT_POSSIBLE = "the fields of the run are not what a record can hold"
START_FAILED = "the process that supervises the run could not be started"
STARTED_BY_NOT_VALID = "started_by must be a short label of letters, digits, '_', '.' and '-'"
NOT_A_TASK = "the verb is not one that runs as a task"
TASK_ACTIVE = "a task of this verb for the same target is already active"
TASK_NOT_USABLE = "the task's entry does not fit its verb"
SOURCE_TASK_ACTIVE = "a download of this source is running, which writes its files: start the run when it has ended"
SOURCE_RUN_ACTIVE = "a pipeline run of this source is alive, which reads its files: start the download when it has ended"
DOCKER_NOT_REACHABLE_FOR_TASK = "Docker cannot be reached, so it cannot be told whether a pipeline run of this source is alive"

START_FAILED_LINE = b"[run start] the process that supervises the run could not be started\n"

# The states in which a recorded run counts as alive: its supervisor lives, or its container does, or Docker could not be asked.
_ALIVE_STATES = (runstore.STATE_STARTING, runstore.STATE_RUNNING, runstore.STATE_DETACHED, runstore.STATE_UNKNOWN)
# Every recorded run is looked at, not the newest few.
_EVERY_RUN = 2 ** 62

# The supervisors this process started and has not yet seen end.
_supervisors = []
_supervisors_lock = threading.Lock()


def reap_supervisors():
    """Collect the supervisors this process started that have ended, so that they do not stay in the process table. Returns how many are still running."""
    with _supervisors_lock:
        _supervisors[:] = [process for process in _supervisors if process.poll() is None]
        return len(_supervisors)


def _refuse_a_second_run(root, service_profiles, profile, source):
    """Raise RunNotStarted when a run of the profile for the source is alive: a recorded run that has not ended and is not lost, or one started in a terminal whose container lives. A terminal run of the profile whose source Docker did not hand over counts: it cannot be told apart. With Docker out of reach nothing can be told, and the run is refused."""
    listing = runstore.list_runs(root, service_profiles, _EVERY_RUN)
    if not listing["docker_available"]:
        raise errors.RunNotStarted(DOCKER_NOT_REACHABLE, "docker_not_reachable")
    for run in listing["runs"]:
        if run["kind"] == runstore.KIND_TASK and run.get("task") in tasks.SOURCE_FILE_TASKS and run["source"] == source and run["state"] in _ALIVE_STATES:
            raise errors.RunNotStarted(SOURCE_TASK_ACTIVE, "source_task_active")
        if run["profile"] != profile:
            continue
        if run["kind"] == runstore.KIND_TERMINAL:
            active = run["source"] in (source, None)
        else:
            active = run["source"] == source and run["state"] in _ALIVE_STATES
        if active:
            raise errors.RunNotStarted(RUN_ACTIVE, "run_active")


def _spawn_supervisor(root, run_id, lock_fd, request):
    """Start the supervisor of a run in a session of its own, with the run's log as its standard output and standard error, hand it the lock descriptor and write the request into its standard input."""
    log_fd = runstore.open_run_log(root, run_id)
    try:
        process = subprocess.Popen([*SUPERVISOR_ARGV, run_id, str(lock_fd)], cwd=root, stdin=subprocess.PIPE, stdout=log_fd, stderr=log_fd, start_new_session=True, pass_fds=(lock_fd,))
    finally:
        os.close(log_fd)
    try:
        try:
            process.stdin.write(request)
            process.stdin.flush()
        finally:
            # Closed on every way out: a write that fails (the supervisor is gone already) must not leave the pipe open in this process.
            with contextlib.suppress(OSError):
                process.stdin.close()
    except BaseException:
        process.kill()
        process.wait()
        raise
    with _supervisors_lock:
        _supervisors.append(process)


def _record_failed_start(root, run_id):
    """Say in the run's log that it never started, and record it as finished with exit code 1. The caller still holds the run's lock."""
    with contextlib.suppress(runstore.StoreError, OSError):
        log_fd = runstore.open_run_log(root, run_id)
        try:
            os.write(log_fd, START_FAILED_LINE)
        finally:
            os.close(log_fd)
    with contextlib.suppress(runstore.StoreError, ValueError, OSError):
        runstore.end_run(root, run_id, runstore.STATE_FINISHED, 1)


def start_run(root, fields, *, started_by):
    """Start a pipeline run for the request `fields` in the deployment `root` and return the id of the recorded run. `started_by` is a short label for who asked (letters, digits, `_`, `.`, `-`; at most 64 characters), kept in the record.

    The fields are checked with `validate_request("run.start", fields)` before anything exists. Then, holding the lock of the whole store, so that of two calls at the same time one sees the other's run: a run of the same profile for the same source that is alive is refused, and the run is created with its lock taken before its record is written. The record's `flags` hold the request's fields whose type is not a secret, without `profile` and `source`. The supervisor gets the lock descriptor and the request; this process closes its own copy of the descriptor.

    Raises the registry's errors for a request it refuses; ValueError for a `started_by` that is not such a label; RunNotStarted with a constant message and a `code` for everything else, a CLI module that cannot be imported and fields the store cannot record included. When the supervisor could not be started, the run exists and is recorded as `finished` with exit code 1 and one line in its log.
    """
    validated = verbs.validate_request(RUN_VERB, fields)
    root, sdp = _checked_starter_and_root(root, started_by)
    service_profiles = sdp._run_service_profiles()

    inputs = verbs.VERBS[RUN_VERB].socket_inputs
    flags = {name: value for name, value in validated.items() if not inputs[name].type.is_secret and name not in ("profile", "source")}
    plain = reveal(validated)

    record = {"profile": validated["profile"], "source": validated["source"], "flags": flags, "started_by": started_by}
    return _create_and_spawn(root, record, lambda: _refuse_a_second_run(root, service_profiles, validated["profile"], validated["source"]), lambda run_id: {"verb": RUN_VERB, "fields": plain, "run_id": run_id})


def _checked_starter_and_root(root, started_by):
    """Refuse a starter label that is no label, a CLI module that cannot be loaded and a folder that is not the CLI's deployment; (the root as a string, the CLI module)."""
    try:
        Name().validate(started_by)
    except ValueError:
        raise ValueError(STARTED_BY_NOT_VALID) from None
    root = os.fspath(root)

    # The CLI is a top-level script module: imported here, at use, and never when this module is loaded.
    try:
        import sdp
    except Exception:
        # Whatever a damaged CLI module raises while it loads.
        raise errors.RunNotStarted(CLI_NOT_LOADABLE, "cli_not_loadable") from None

    # The child acts on the deployment of the CLI module it imports, which is the one in its working directory: a run recorded in another folder would be a record of something else.
    if os.path.realpath(sdp.ROOT) != os.path.realpath(root):
        raise errors.RunNotStarted(NOT_THE_DEPLOYMENT, "not_the_deployment")
    return root, sdp


def _create_and_spawn(root, record, refuse_a_second, request_of):
    """Under the lock of the whole store, look for a second run (`refuse_a_second()` raises) and create the run with its lock held; then start its supervisor with the request `request_of(run_id)`. Returns the run id."""
    reap_supervisors()
    try:
        with runstore.store_lock(root):
            refuse_a_second()
            run_id, lock_fd = runstore.create_run_locked(root, record)
    except runstore.StoreError as error:
        raise errors.RunNotStarted(error.message, "store_not_usable") from None
    except ValueError:
        raise errors.RunNotStarted(RECORD_NOT_POSSIBLE, "record_not_possible") from None
    try:
        request = json.dumps(request_of(run_id)).encode("utf-8")
        _spawn_supervisor(root, run_id, lock_fd, request)
    except Exception:
        _record_failed_start(root, run_id)
        raise errors.RunNotStarted(START_FAILED, "start_failed") from None
    finally:
        os.close(lock_fd)
    return run_id


def _refuse_a_second_task(root, verb_name, key):
    """Raise RunNotStarted when a task of the verb with the same key values is alive (its supervisor holds its lock). The recorded runs are read without Docker: a task's state never needs it."""
    for run in runstore.list_recorded_runs(root, _EVERY_RUN):
        if run["kind"] != runstore.KIND_TASK or run["task"] != verb_name or run["state"] not in _ALIVE_STATES:
            continue
        recorded = {**run["flags"], "source": run["source"]}
        if all(recorded.get(name) == value for name, value in key.items()):
            raise errors.RunNotStarted(TASK_ACTIVE, "task_active")


def _refuse_beside_a_run_of_the_source(root, service_profiles, source):
    """Raise RunNotStarted when a pipeline run of the source is alive: a recorded run of a profile (not a task) that has not ended and is not lost, or one started in a terminal whose container lives, its source the same or not handed over by Docker. With Docker out of reach nothing can be told, and the task is refused."""
    listing = runstore.list_runs(root, service_profiles, _EVERY_RUN)
    if not listing["docker_available"]:
        raise errors.RunNotStarted(DOCKER_NOT_REACHABLE_FOR_TASK, "docker_not_reachable")
    for run in listing["runs"]:
        if run["kind"] == runstore.KIND_TASK:
            continue
        if run["kind"] == runstore.KIND_TERMINAL:
            active = run["source"] in (source, None)
        else:
            active = run["source"] == source and run["state"] in _ALIVE_STATES
        if active:
            raise errors.RunNotStarted(SOURCE_RUN_ACTIVE, "source_run_active")


def start_task(root, verb_name, fields, *, started_by):
    """Start the task `verb_name` (a verb of `control.tasks.TASKS`) for the request `fields` in the deployment `root` and return the id of the recorded run. `started_by` is as for `start_run`.

    A verb that is not on the allow-list is refused before its fields are looked at; then the fields are checked with `validate_request(verb_name, fields)`, and the CLI module must have the verb's action. Under the lock of the whole store, a task of the same verb whose key fields have the same values that is alive is refused, and the run is created with its lock taken: kind `task`, the verb, the label made from the key values, the `source` field (when the request has one) as its source, and as `flags` the request's other fields whose type is not a secret. The supervisor gets the request (`{"verb", "fields"}`, every secret in plain text) on its standard input; its child calls the action with the fields.

    A task of `tasks.SOURCE_FILE_TASKS` (a download) is also refused while a pipeline run of its source is alive (`source_run_active`; Docker out of reach: `docker_not_reachable`), and `start_run` refuses a pipeline run of a source while such a task of it is alive (`source_task_active`).

    Raises the registry's errors for a request it refuses; ValueError for a `started_by` that is not a label; RunNotStarted with a constant message and a `code` for everything else (`not_a_task`, `task_not_usable` for an entry whose key fields are not required plain inputs of the verb or whose verb has no action in the CLI, `task_active`, and the codes of `start_run`).
    """
    task = tasks.TASKS.get(verb_name) if isinstance(verb_name, str) else None
    if task is None:
        raise errors.RunNotStarted(NOT_A_TASK, "not_a_task")
    validated = verbs.validate_request(verb_name, fields)
    root, sdp = _checked_starter_and_root(root, started_by)

    verb = verbs.VERBS[verb_name]
    inputs = verb.socket_inputs
    if verb.action is None or not callable(getattr(sdp, verb.action, None)) or not all(name in inputs and not inputs[name].type.is_secret and name in validated for name in task.key):
        raise errors.RunNotStarted(TASK_NOT_USABLE, "task_not_usable")
    key = tasks.key_values(task, validated)
    flags = {name: value for name, value in validated.items() if not inputs[name].type.is_secret and name != "source"}
    plain = reveal(validated)
    record = {"profile": None, "task": verb_name, "label": tasks.label_of(task, key), "source": validated.get("source"), "flags": flags, "started_by": started_by}
    if verb_name in tasks.SOURCE_FILE_TASKS:
        service_profiles = sdp._run_service_profiles()

        def refuse_a_second():
            _refuse_a_second_task(root, verb_name, key)
            _refuse_beside_a_run_of_the_source(root, service_profiles, validated.get("source"))
    else:
        def refuse_a_second():
            _refuse_a_second_task(root, verb_name, key)
    return _create_and_spawn(root, record, refuse_a_second, lambda run_id: {"verb": verb_name, "fields": plain})
