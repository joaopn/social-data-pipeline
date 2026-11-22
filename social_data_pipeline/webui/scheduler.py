"""How the web UI reaches the query scheduler's control socket (`jobs/door.py` in the scheduler), for the Queue window.

`Scheduler(socket_path, results)` holds the two settings `sdp webui start` gives the container when it mounted the scheduler's folders (`config.Settings.queue_socket`, `.results`), each None when it did not. `call(name, fields, timeout)` asks the socket for one entry of the web UI's table (`actions.call`, a `queue.*` entry), after checks that send nothing when they fail: the setting is there (`no_settings`); the control folder (the socket's folder) is a real folder, not a link, of this process's own user, with no permission for the group or others (`folder_missing`, `folder_not_a_folder`, `folder_other_owner`, `folder_open`); and the socket itself is there and is a socket of this user with no permission for the group or others (`no_socket`, `socket_not_a_socket`, `socket_other_owner`, `socket_open`). Those refusals are a `SchedulerUnavailable` with the reason; a socket that cannot be reached is the client's `BrokerUnreachable` (`no_socket`, `refused`, `permission_denied`, `no_reply`, `not_reached`); a refusal of the socket is a `BrokerError` with the socket's code. `words.door_sentence` words each.

The time limits: a read 30 seconds; EXPLAIN 90 seconds (the socket gives the database 60); any other change 75 seconds (the socket answers `outcome_unknown` after twice 30 seconds on the scheduler's loop). The calls block: the routes make them in worker threads, never on the event loop. Standard library, the broker's client (which speaks the socket's framing unchanged) and this package only.
"""

import os
import stat

from social_data_pipeline.control import client
from social_data_pipeline.webui import actions

READ_SECONDS = 30
EXPLAIN_SECONDS = 90
CHANGE_SECONDS = 75
FOLDER_REASONS = ("folder_missing", "folder_not_a_folder", "folder_other_owner", "folder_open")
# The socket itself: what the scheduler makes is a socket, 0600, of the folder's owner. A missing one is `client.NO_SOCKET`.
SOCKET_REASONS = ("socket_not_a_socket", "socket_other_owner", "socket_open")


class SchedulerUnavailable(client.BrokerError):
    """The control socket is not asked: `reason` is `no_settings`, one of `FOLDER_REASONS` or `SOCKET_REASONS`, or `no_socket`."""

    def __init__(self, reason):
        super().__init__("unreachable", "the scheduler's control socket is not asked")
        self.reason = reason


def _connect(socket_path, timeout):
    return client.Client(socket_path, timeout=timeout)


def timeout_of(name):
    """The time limit of a call of the table's entry `name`."""
    if name == "queue.explain":
        return EXPLAIN_SECONDS
    return CHANGE_SECONDS if actions.TABLE[name].state_changing else READ_SECONDS


class Scheduler:
    """The way to the scheduler's control socket. `connect(socket_path, timeout)` makes a client (a stand-in in tests), `lstat` and `uid` are the folder check's (injected in tests)."""

    def __init__(self, socket_path, results, *, connect=_connect, lstat=os.lstat, uid=os.getuid):
        self.socket_path = socket_path
        self.results = results
        self._connect = connect
        self._lstat = lstat
        self._uid = uid

    @property
    def available(self):
        return self.socket_path is not None

    def folder_problem(self):
        """Why the control folder or the socket in it must not be used, or None. The folder: missing, not a real folder (a link included), of another user, open to the group or others. The socket (looked at without following a link): missing (`no_socket`), not a socket (a file, a link), of another user, open to the group or others; this also refuses a socket someone planted while the folder was open."""
        folder = os.path.dirname(self.socket_path)
        try:
            info = self._lstat(folder)
        except (OSError, ValueError):
            return "folder_missing"
        if not stat.S_ISDIR(info.st_mode):
            return "folder_not_a_folder"
        if info.st_uid != self._uid():
            return "folder_other_owner"
        if info.st_mode & 0o077:
            return "folder_open"
        try:
            found = self._lstat(self.socket_path)
        except FileNotFoundError:
            return client.NO_SOCKET
        except (OSError, ValueError):
            return "socket_not_a_socket"
        if not stat.S_ISSOCK(found.st_mode):
            return "socket_not_a_socket"
        if found.st_uid != self._uid():
            return "socket_other_owner"
        if found.st_mode & 0o077:
            return "socket_open"
        return None

    def call(self, name, fields=None, timeout=None):
        """Ask the socket for the table's entry `name` with `fields`; returns the reply's result. Raises SchedulerUnavailable, `client.BrokerError` (BrokerUnreachable included) or `actions.NotInTable`."""
        action = actions.TABLE.get(name) if isinstance(name, str) else None
        if action is None or action.door is None:
            raise actions.NotInTable("the page asked the scheduler for something the table does not hold")
        if self.socket_path is None:
            raise SchedulerUnavailable("no_settings")
        problem = self.folder_problem()
        if problem is not None:
            raise SchedulerUnavailable(problem)
        return actions.call(self._connect(self.socket_path, timeout_of(name) if timeout is None else timeout), name, fields or {})
