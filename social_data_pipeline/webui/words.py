"""The broker's answers as sentences for the operator: one sentence for every error code the broker can answer, one for every reason `BrokerUnreachable` gives, and a fallback for a code this table does not know; and the same for the query scheduler's control socket (`DOOR_CODE_SENTENCES`, `DOOR_REASON_SENTENCES`, `door_sentence`), whose reasons also include the web UI's own checks before it asks the socket (no settings, the control folder's check).

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
    "task_active": "The same task for the same source is already running: see Runs.",
    "source_task_active": "A download of this source is running, which writes its files: start the run when it has ended.",
    "source_run_active": "A pipeline run of this source is alive, which reads its files: start the download when it has ended (Runs).",
    "source_run_alive": "A run of this source is alive, which reads the files this rewrites: change the source's configuration when the run has ended.",
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


# ── the scheduler's control socket ──

_START_SCHEDULER = "Start it in a terminal with: python sdp.py db start jobs (which also rebuilds its image)"
_RESTART_WEBUI = "python sdp.py webui stop, then python sdp.py webui start"

# One sentence for every code the scheduler's control socket answers with (`jobs/door.py`; a test parses them out of it).
DOOR_CODE_SENTENCES = {
    "bad_request": "The scheduler did not take the request: the web UI and the scheduler may be of different versions.",
    "request_too_large": "The request was larger than the scheduler takes.",
    "unknown_target": "The target is not one of the scheduler's configured targets.",
    "not_found": "The scheduler has no job with this id.",
    "not_pending": "The job is not pending any more, so nothing was done.",
    "changed": "The job changed at the scheduler since this page read it, so nothing was done: open the job again and look at it anew.",
    "too_large": "The job's record is too large for the web UI: use the scheduler's own page for it.",
    "explain_failed": "The database could not explain the query. Its error text:",
    "explain_timeout": "The database did not answer the EXPLAIN in time: it may still be running there.",
    "busy": "The scheduler is busy with other requests: try again in a moment.",
    "not_done": "The job has no finished result to preview.",
    "preview_not_available": "This result cannot be previewed here (a format or a size the preview does not read).",
    "preview_failed": "The result could not be previewed.",
    "timeout": "The scheduler did not take up the request in time: nothing was done.",
    "outcome_unknown": "The scheduler did not finish the request in time: it may still happen.",
    "unavailable": "The scheduler is stopping and takes no new request.",
    "internal": "The scheduler could not answer this request.",
    "bad_reply": "The scheduler sent an answer the web UI cannot read.",
    # The web UI's own, at the confirmation of a change of auto-accept.
    "settings_not_read": "The target's auto-accept settings could not be read again before the change, so nothing was sent: open the auto-accept page again and choose again.",
    "settings_changed": "The target's auto-accept settings changed since this page read them, so the change would not end as the page said: nothing was sent. Choose again.",
}

# Why the scheduler's control socket cannot be asked: the settings, the check of its folder, and each reason the client gives.
DOOR_REASON_SENTENCES = {
    "no_settings": f"The scheduler's data folder (data/jobs) was not there when the web UI was started, so the Queue window has no way to the scheduler. Set the scheduler up (python sdp.py db setup-jobs) and start it (python sdp.py db start jobs), then restart the web UI ({_RESTART_WEBUI}).",
    "folder_missing": f"The scheduler's control folder is not there in the web UI's container, so nothing was sent: restart the web UI ({_RESTART_WEBUI}).",
    "folder_not_a_folder": "The scheduler's control folder is not a real folder (a link or a file is in its place), so nothing was sent to it.",
    "folder_other_owner": "The scheduler's control folder belongs to another user than the one the web UI runs as, so nothing was sent to it.",
    "folder_open": "The scheduler's control folder is open to the group or to others, so nothing was sent to it: take those permissions away (chmod 700 data/jobs/control).",
    "socket_not_a_socket": "Something other than a socket is in the place of the scheduler's control socket (a file or a link), so nothing was sent to it: remove what is at data/jobs/control/door.sock, then restart the scheduler (python sdp.py db start jobs).",
    "socket_other_owner": "The scheduler's control socket belongs to another user than the one the web UI runs as, so nothing was sent to it: restart the scheduler (python sdp.py db start jobs), which gives the socket to the owner of its data folder.",
    "socket_open": "The scheduler's control socket is open to the group or to others, so nothing was sent to it: restart the scheduler (python sdp.py db start jobs), which makes it 0600.",
    client.NO_SOCKET: f"The scheduler's control socket is not there: the scheduler is not running, or its image was built before it had one. {_START_SCHEDULER}",
    client.REFUSED: f"Nobody listens on the scheduler's control socket: the scheduler is not running. {_START_SCHEDULER}",
    client.PERMISSION_DENIED: "The web UI may not open the scheduler's control socket: it does not run as the user the scheduler's data folder belongs to.",
    client.NO_REPLY: "The scheduler closed the connection without answering: it refuses this user (the web UI runs as another user than the one the scheduler's data folder belongs to), or it did not answer in time.",
    client.NOT_REACHED: "The scheduler's control socket cannot be reached.",
}

DOOR_FALLBACK = "The scheduler refused the request for a reason the web UI does not know."

# A change that was sent and got no reply: it may have been made.
DOOR_NO_ANSWER = "No answer came from the scheduler after the request was sent: it may have been done."
DOOR_NO_ANSWER_LOOK = {"queue.auto_accept": "Open the auto-accept page again to see."}
DOOR_NO_ANSWER_LOOK_JOB = "Open the job again to see."


def door_no_answer_sentence(name):
    """The sentence for a change of the Queue (the table's entry `name`) whose request was sent and got no reply."""
    return f"{DOOR_NO_ANSWER} {DOOR_NO_ANSWER_LOOK.get(name, DOOR_NO_ANSWER_LOOK_JOB)}"
DOOR_UNREACHABLE = "The scheduler cannot be reached."


def door_sentence(error):
    """The sentence for an error of the scheduler's control socket: by its reason when it could not be asked (`BrokerUnreachable`, or the web UI's own `DoorUnavailable` with a `reason`), by its code otherwise, the fallback for anything else."""
    reason = getattr(error, "reason", None)
    if isinstance(error, client.BrokerUnreachable) or getattr(error, "code", None) == "unreachable":
        return DOOR_REASON_SENTENCES.get(reason, DOOR_UNREACHABLE) if isinstance(reason, str) else DOOR_UNREACHABLE
    code = getattr(error, "code", None)
    return DOOR_CODE_SENTENCES.get(code, DOOR_FALLBACK) if isinstance(code, str) else DOOR_FALLBACK


# ── the chat's runner (the Chat window) ──

_START_CHAT = "Start the chat in a terminal with: python sdp.py chat start"

# One sentence for every code the chat's runner answers with (`chat/runner.py`; a test parses them out of it), and the web UI's own for an answer of another shape.
CHAT_CODE_SENTENCES = {
    "bad_request": "The chat's sandbox did not take the request: the web UI and the sandbox may be of different versions (python sdp.py chat setup builds the sandbox again).",
    "request_too_large": "The request was larger than the chat's sandbox takes.",
    "not_found": "There is no chat with this id: it may have been deleted.",
    "no_such_file": "There is no such file in the chat's folder.",
    "busy": "A turn of this chat is running: wait until it has ended, or stop it.",
    "too_many": "Two turns are running in the sandbox already: try again when one has ended.",
    "over_cap": "The chat's work folder is over its soft cap: delete chats to make room.",
    "chat_full": "This chat's log is full: start a new chat.",
    "too_many_chats": "There are 200 chats already: delete one first.",
    "too_large": "The sandbox's answer was too large to send.",
    "not_usable": "The chat's folder in the sandbox cannot be used.",
    "model_not_allowed": "The chat's sandbox does not offer this model: choose one of the models the page offers.",
    "unavailable": "The chat's sandbox is stopping and takes no new request.",
    "internal": "The chat's sandbox could not answer this request.",
    "not_understood": "The chat's sandbox answered something this page does not understand, so nothing of it is shown.",
}

# Why the chat's runner cannot be asked: the setting, the check of its folder and socket, and each reason the client gives.
CHAT_REASON_SENTENCES = {
    "no_settings": f"The chat is not set up, or its socket folder (.sdp/chat/socket) was not there when the web UI was started, so the Chat window has no way to the chat. Set it up (python sdp.py chat setup) and start it (python sdp.py chat start), then restart the web UI ({_RESTART_WEBUI}).",
    "folder_missing": f"The chat's socket folder is not there in the web UI's container, so nothing was sent: restart the web UI ({_RESTART_WEBUI}).",
    "folder_not_a_folder": "The chat's socket folder is not a real folder (a link or a file is in its place), so nothing was sent to it.",
    "folder_other_owner": "The chat's socket folder belongs to another user than the one the web UI runs as, so nothing was sent to it.",
    "folder_open": "The chat's socket folder is open to the group or to others, so nothing was sent to it: take those permissions away (chmod 700 .sdp/chat/socket).",
    "socket_not_a_socket": f"Something other than a socket is in the place of the chat's socket (a file or a link), so nothing was sent to it: stop the chat (python sdp.py chat stop), remove what is at .sdp/chat/socket/runner.sock, then start it again. {_START_CHAT}",
    "socket_other_owner": "The chat's socket belongs to another user than the one the web UI runs as, so nothing was sent to it.",
    "socket_open": "The chat's socket is open to the group or to others, so nothing was sent to it: restart the chat (python sdp.py chat stop, then python sdp.py chat start).",
    client.NO_SOCKET: f"The chat's socket is not there: the chat is not running. {_START_CHAT}",
    client.REFUSED: f"Nobody listens on the chat's socket: the chat is not running. {_START_CHAT}",
    client.PERMISSION_DENIED: "The web UI may not open the chat's socket: it does not run as the user the chat runs as.",
    client.NO_REPLY: "The chat's sandbox closed the connection without an answer this page can read (none in time, or not one frame of JSON), so nothing of it is shown.",
    client.NOT_REACHED: "The chat's socket cannot be reached.",
}

CHAT_FALLBACK = "The chat's sandbox refused the request for a reason the web UI does not know."
CHAT_UNREACHABLE = "The chat's sandbox cannot be reached."


def chat_sentence(error):
    """The sentence for an error of the chat's runner: by its reason when it could not be asked (`BrokerUnreachable`, or the web UI's own `assistant.AssistantUnavailable`), by its code otherwise (only a text code is looked up), the fallback for anything else. Nothing the runner sent besides its code is used."""
    reason = getattr(error, "reason", None)
    if isinstance(error, client.BrokerUnreachable) or getattr(error, "code", None) == "unreachable":
        return CHAT_REASON_SENTENCES.get(reason, CHAT_UNREACHABLE) if isinstance(reason, str) else CHAT_UNREACHABLE
    code = getattr(error, "code", None)
    return CHAT_CODE_SENTENCES.get(code, CHAT_FALLBACK) if isinstance(code, str) else CHAT_FALLBACK


def sentence(error):
    """The sentence for a `BrokerError`: by its reason for `BrokerUnreachable`, by its code otherwise, the fallback for anything else."""
    if isinstance(error, client.BrokerUnreachable):
        return REASON_SENTENCES.get(getattr(error, "reason", None), CODE_SENTENCES["unreachable"])
    code = getattr(error, "code", None)
    return CODE_SENTENCES.get(code, FALLBACK) if isinstance(code, str) else FALLBACK


def code_sentence(code):
    """The sentence for an error code, the fallback for one the table does not know."""
    return CODE_SENTENCES.get(code, FALLBACK) if isinstance(code, str) else FALLBACK
