"""The broker's answers as sentences for the operator: one sentence for every error code the broker can answer, one for every reason `BrokerUnreachable` gives, and a fallback for a code this table does not know.

The sentences are constants. Nothing the broker sent with an error is put into one: the text that comes with `failed` is the end of a command's error output, which is shown apart, as text, by whoever renders it. Standard library and the broker's client only.
"""

from social_data_pipeline.control import client

_START_BROKER = "Start it in a terminal with: python sdp.py broker start"

CODE_SENTENCES = {
    # Requests and replies.
    "bad_request": "The broker did not take the request: the web UI and the broker may be of different versions.",
    "request_too_large": "The request was larger than the broker takes.",
    "bad_reply": "The broker sent an answer the web UI cannot read.",
    "internal": "The broker could not answer this request.",
    "stopping": "The broker is stopping and takes no new request.",
    "too_large": "The answer was too large to send.",
    # Logins and sessions.
    "unauthorized": "You are not logged in, or the password is wrong.",
    "throttled": "There were too many failed logins: the broker takes the next attempt in at most 30 seconds.",
    "no_password_set": "No management password is set. Set one in a terminal with: python sdp.py broker passwd",
    "password_file_not_usable": "The broker's password file cannot be used. Set the password again in a terminal with: python sdp.py broker passwd",
    "step_up_failed": "The management password given again was wrong.",
    "session_ended": "The management password was given wrong too often: the session has ended.",
    # The verbs.
    "unknown_verb": "The broker does not know this action.",
    "not_available": "This action is not available through the web UI.",
    "invalid_input": "A field of the request was refused.",
    "step_up_required": "This action needs the management password again.",
    "password_not_allowed_here": "This start cannot take a database password that holds a quote, a dollar sign, a backtick or a backslash.",
    # What a verb met.
    "run_active": "A pipeline run is alive: this action waits until it has ended.",
    "docker_unreachable": "Docker cannot be asked whether a pipeline run is alive, so this action was not run.",
    "docker_not_reachable": "Docker cannot be reached, so it cannot be told whether such a run is already active.",
    "not_the_deployment": "The broker's folder is not the deployment the CLI acts on.",
    "store_not_usable": "The run state folder cannot be used.",
    "cli_not_loadable": "The broker could not load the CLI.",
    "record_not_possible": "The run's fields cannot be recorded.",
    "audit_not_writable": "The broker's audit log cannot be written, so it takes no action that changes something.",
    "broker_folder_not_usable": "The broker's folder is gone or cannot be used, so it takes no action that changes something.",
    "busy": "The broker is busy with another request of this kind: try again in a moment.",
    "timeout": "The command took longer than a read may take and was ended.",
    "failed": "The command failed.",
    "start_failed": "The command could not be started.",
    # No reply at all, when the reason is not known.
    "unreachable": "The broker cannot be reached.",
}

REASON_SENTENCES = {
    client.NO_SOCKET: f"The broker is not running: its socket is not there. {_START_BROKER}",
    client.REFUSED: f"The broker is not running: nobody listens on its socket. {_START_BROKER}",
    client.PERMISSION_DENIED: "The web UI may not open the broker's socket: it does not run as the user the broker runs as.",
    client.NO_REPLY: "The broker closed the connection without answering: the web UI may run as another user than the broker.",
    client.NOT_REACHED: "The broker cannot be reached.",
}

FALLBACK = "The broker refused the request for a reason the web UI does not know."


def sentence(error):
    """The sentence for a `BrokerError`: by its reason for `BrokerUnreachable`, by its code otherwise, the fallback for anything else."""
    if isinstance(error, client.BrokerUnreachable):
        return REASON_SENTENCES.get(getattr(error, "reason", None), CODE_SENTENCES["unreachable"])
    code = getattr(error, "code", None)
    return CODE_SENTENCES.get(code, FALLBACK) if isinstance(code, str) else FALLBACK


def code_sentence(code):
    """The sentence for an error code, the fallback for one the table does not know."""
    return CODE_SENTENCES.get(code, FALLBACK) if isinstance(code, str) else FALLBACK
