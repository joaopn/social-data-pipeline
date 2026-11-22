"""One verb, one process: `python -m social_data_pipeline.control.child`, started in the deployment folder, runs the action of one verb and ends.

It reads ONE JSON object from standard input to end-of-file: `{"verb": <name>, "fields": {...}}` and, only with `run.start`, an optional `"run_id"` (the id of the recorded run the call belongs to). The fields pass `validate_request`; the verb must have an action; the CLI module is imported and the action is called with the validated fields as keyword arguments, a secret unwrapped to plain text, `_confirm=sdp.already_confirmed` where the action has that parameter and `_run_id` for `run.start` when a run id was sent. A field the action's signature does not have is an error, never dropped.

What comes back:

- The integer a `do_*` action returns is the exit code; what the action prints is this process's output. A result that is no exit code (not a whole number from 0 to 255: nothing at all, a boolean, the negative number of a signal that ended `docker compose`) is exit code 1 with one line that says so, since the operating system would cut it down to a number that can read as success.
- What a `get_*` action returns is printed as one JSON document on standard output, with exit code 0.
- An `ActionError` prints its message on standard error and its code is the exit code.
- A request that is not valid JSON, has not that shape or is refused by the registry prints one line on standard error and the exit code is 2. Nothing was called.
- Anything else that goes wrong prints one line that names the kind of error only, with exit code 1.

The request is never printed or logged, and no line this module prints holds a field's value: the messages are constants or the registry's own, which name the verb and the field only, and no traceback is ever printed (its frames hold the request). The action's own output is the terminal command's and is not filtered here.

What the caller must do: start one process per request (an action writes the admin password and a run's paths into its process environment and never clears them), in the deployment folder (the CLI module that is imported is the one found from there), with the request on standard input and never in the argument list or the environment. Every confirmation is answered yes, so the caller is the one that took the operator's confirmation. A caller that must not act on a source folder reached through a symbolic link checks that itself: the registry's check holds at validation time only. The pipeline's `mongo_ingest` step asks a question inside its container when it meets a truncated file; a run started from here cannot answer it, the question gets end-of-file.
"""

import inspect
import json
import sys

from social_data_pipeline.control import REQUEST_MAX_BYTES, errors, verbs
from social_data_pipeline.control.types import reveal

EXIT_FAILED = 1
EXIT_REFUSED = 2

# The one verb that can carry the id of a recorded run.
RUN_VERB = "run.start"

# Why a request was refused before the registry saw it. None of them holds a value of the request.
REQUEST_TOO_LARGE = "the request is larger than a request may be."
REQUEST_NOT_JSON = "the request is not valid JSON."
REQUEST_NOT_AN_OBJECT = "the request must be an object with 'verb', 'fields' and nothing else but 'run_id'."
RUN_ID_NOT_ACCEPTED = "a run id is accepted only with run.start."
RUN_ID_NOT_VALID = "the run id is not the id of a recorded run."


class _Refused(Exception):
    """The request was refused before anything was called. `message` is one line that holds no value of the request."""

    def __init__(self, message):
        super().__init__(message)
        self.message = message


def _read_request(stream):
    """The request read from a binary stream to its end: a dict with `verb` (a string), `fields` (an object) and maybe `run_id`."""
    raw = stream.read(REQUEST_MAX_BYTES + 1)
    if len(raw) > REQUEST_MAX_BYTES:
        raise _Refused(REQUEST_TOO_LARGE)
    try:
        request = json.loads(raw)
    except (ValueError, RecursionError):
        # ValueError: not JSON, not UTF-8. RecursionError: nested hundreds of levels deep.
        raise _Refused(REQUEST_NOT_JSON) from None
    if not isinstance(request, dict) or not {"verb", "fields"} <= set(request) <= {"verb", "fields", "run_id"}:
        raise _Refused(REQUEST_NOT_AN_OBJECT)
    if not isinstance(request["verb"], str) or not isinstance(request["fields"], dict):
        raise _Refused(REQUEST_NOT_AN_OBJECT)
    return request


def _checked_run_id(request):
    """The run id of a request, or None when it has none. Refused on any verb but `run.start`, and when it is not the id of a recorded run; `null` is a refused value, not an absent one."""
    if "run_id" not in request:
        return None
    if request["verb"] != RUN_VERB:
        raise _Refused(RUN_ID_NOT_ACCEPTED)
    from social_data_pipeline import runstore

    try:
        recorded = runstore.run_id_kind(request["run_id"]) == runstore.KIND_RECORDED
    except runstore.StoreError:
        recorded = False
    if not recorded:
        raise _Refused(RUN_ID_NOT_VALID)
    return request["run_id"]


def _is_exit_code(value):
    """Whether a value is a number a process can exit with: a whole number from 0 to 255, and no boolean."""
    return isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 255


def _arguments(sdp, verb, action, validated, run_id):
    """The keyword arguments the action is called with."""
    parameters = inspect.signature(action).parameters
    arguments = reveal(validated)
    for name in arguments:
        if name not in parameters:
            # The name is one of the registry's own field names.
            raise _Refused(f"{verb.name}: the action does not take the field '{name}'.")
    if "_confirm" in parameters:
        arguments["_confirm"] = sdp.already_confirmed
    if run_id is not None:
        if "_run_id" not in parameters:
            raise _Refused(f"{verb.name}: the action does not take a run id.")
        arguments["_run_id"] = run_id
    return arguments


def _run(stream):
    request = _read_request(stream)
    try:
        validated = verbs.validate_request(request["verb"], request["fields"])
    except errors.ControlError as error:
        # The registry's message names the verb and the field and never holds a value.
        raise _Refused(error.message) from None
    verb = verbs.VERBS[request["verb"]]
    if verb.action is None:
        raise _Refused(f"{verb.name} has no action that runs without a terminal.")
    run_id = _checked_run_id(request)

    # The CLI is a top-level script module: imported here, at use, and never by the registry.
    import sdp

    action = getattr(sdp, verb.action, None)
    if not callable(action):
        raise _Refused(f"{verb.name}: the CLI has no function for this verb.")
    arguments = _arguments(sdp, verb, action, validated, run_id)

    try:
        result = action(**arguments)
    except sdp.ActionError as error:
        print(f"Error: {error.message}", file=sys.stderr)
        return error.exit_code if _is_exit_code(error.exit_code) else EXIT_FAILED
    if verb.action.startswith("get_"):
        document = json.dumps(result)
        sys.stdout.write(document + "\n")
        return 0
    if not _is_exit_code(result):
        # A whole number is safe to print; anything else is only named as missing.
        returned = f"{result}, which is no exit code" if isinstance(result, int) and not isinstance(result, bool) else "no exit code"
        print(f"Error: {verb.name}: the action returned {returned}.", file=sys.stderr)
        return EXIT_FAILED
    return result


def main(stdin=None):
    """Run the request read from `stdin` (a binary stream; this process's standard input when None) and return the exit code."""
    try:
        return _run(sys.stdin.buffer if stdin is None else stdin)
    except _Refused as refusal:
        print(f"Error: {refusal.message}", file=sys.stderr)
        return EXIT_REFUSED
    except Exception as error:
        # Only the kind of error is named: its text and its traceback can hold a value of the request.
        print(f"Error: the request failed ({type(error).__name__}).", file=sys.stderr)
        return EXIT_FAILED


if __name__ == "__main__":
    sys.exit(main())
