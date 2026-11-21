"""The supervisor of one recorded pipeline run: `python -m social_data_pipeline.control.supervise <run_id> <lock_fd>`, started in the deployment folder by `control.runs.start_run`, which created the run and hands over the descriptor that holds the run's lock.

The supervisor holds that lock for as long as it lives, which is what makes the run read as `starting` or `running` and not as `lost`. It reads the request (the JSON `control.child` takes, the admin password included) from standard input to end-of-file, keeps it in memory only, and starts ONE child, `python -u -m social_data_pipeline.control.child`, in the deployment folder and in a session and process group of its own: the request goes into the child's standard input, which is then closed, and the child's output goes into the run's log. The request is never written to a file, an argument list or an environment variable.

While the child lives, the supervisor looks about twice a second for a cancel request (the `cancel` file of the run, or the signal TERM or INT sent to the supervisor itself; HUP is ignored). What it does then, and why it is done this way:

- Killing the `docker compose run` process does not stop its container, so a cancel stops the containers that carry the run's label, through the run store, with the interrupt signal. The compose process then ends by itself with the container's exit code.
- When Docker answers that there is no such container (an image is being built, or the checks before the container starts are running), the child's process group is sent TERM, and KILL once the grace time is over. An answer that is missing is not that answer: the group is ended on it only after the limited number of tries.
- After either, the containers are looked for again, until the child's processes have ended AND a listing made after that shows no container with the run's label (a compose process can start the container up to its last moment). The run is then recorded as `cancelled` with the child's exit code.
- When the child ends by itself, the supervisor first waits until everything the child started has ended too (the child's process group; a cancel is still honoured): a compose process that outlives the child can still start the container. Then the run is `finished` with the child's exit code, unless a container of the run is still alive (the compose process died and the container did not). The run then stays `running`: what the container prints from that moment on is appended to the log from Docker (a few lines around that moment can be missing or repeated), its exit code comes from `docker wait`, and a cancel request is still honoured.

Before an end is written, the run's containers are listed once more in every state. Measured on Docker 29.5.3 with Compose v5.7.0: a compose process that gets TERM in the first half second after it was started can leave a container that exists and never started, one that is started a moment later by the compose plugin (which outlives the Docker client by that moment), or one that shows up in the listing only after both processes are gone. A container that was created and never started is removed (without force, so one that has started meanwhile stays and is stopped as a running one), and the end is written only when no container with the run's label is listed at all; after a cancel that ended the child's processes by a signal, only when that was so twice, one pause apart. What stays open: a container that the daemon lists later than that pause is left behind as created; it never runs unless someone starts it by hand.

An end is recorded only when it is established: when Docker does not answer, or a container cannot be stopped or does not go away, after a limited number of tries, a line is written into the log and the record is left as it is, so the run store goes on to report the run as `detached`, `lost` or `unknown` from what Docker says later. A cancel request that arrives after the run ended by itself changes nothing: the run is `finished`.

The supervisor's own problems (the child cannot be started, the record cannot be written) are plain lines in the run's log that begin with `[run supervisor]`; no traceback is written, since its frames hold the request. `start_run` also gives the supervisor the run's log as its standard output and standard error, so a supervisor that cannot even be loaded (a damaged installation) leaves the interpreter's own error there; at that point nothing of the request has been read. Every Docker call is made by the run store. A supervisor that is killed with KILL records nothing: the run store then reports the run as `detached` while its container lives and as `lost` afterwards.

Linux is assumed: the lock is a `flock` lock that travels with the inherited descriptor, the child is ended through its process group, and `/proc/<pid>/stat` is read to tell whether a process of that group is still running.
"""

import contextlib
import errno
import os
import signal
import subprocess
import sys
import threading
import time

from social_data_pipeline import runstore
from social_data_pipeline.control import REQUEST_MAX_BYTES

CHILD_ARGV = (sys.executable, "-u", "-m", "social_data_pipeline.control.child")
# How often the supervisor looks for a cancel request.
POLL_SECONDS = 0.5
# How long a process is given to end after TERM before it is sent KILL, and how long the compose process is given to end by itself once its container is gone.
GRACE_SECONDS = 10
# How many times in a row Docker may fail to answer, or to stop a container, before the supervisor gives up on establishing the end of the run.
DOCKER_ATTEMPTS = 10
# How long the following of a container's output is given to write its last lines once the container has ended.
OUTPUT_DRAIN_SECONDS = 5

LINE_PREFIX = "[run supervisor] "

EXIT_END_RECORDED = 0
EXIT_PROBLEM = 1
EXIT_NO_RUN = 2


class _Run:
    """What the steps of one supervision share: where the run is, its log, and whether a cancel was asked."""

    def __init__(self, root, run_id, log_fd, interrupted, poll_seconds, grace_seconds):
        self.root = root
        self.run_id = run_id
        self.log_fd = log_fd
        self.interrupted = interrupted
        self.poll_seconds = poll_seconds
        self.grace_seconds = grace_seconds
        self.cancelled = False

    def say(self, text):
        """Write one line of the supervisor's own into the run's log. `text` is a constant, with at most the name of a kind of error or a number in it."""
        with contextlib.suppress(OSError):
            os.write(self.log_fd, f"{LINE_PREFIX}{text}\n".encode("utf-8"))

    def cancel_seen(self):
        """Whether a cancel was asked, by a signal to this process or through the run's cancel file. Once true it stays true."""
        if not self.cancelled:
            try:
                self.cancelled = bool(self.interrupted()) or runstore.cancel_requested(self.root, self.run_id)
            except runstore.StoreError:
                # The run's folder cannot be looked into right now: no request can be seen there.
                pass
        return self.cancelled

    def containers(self):
        """How many containers carry the run's label, asked again a limited number of times when Docker does not answer; None when it never did."""
        for _ in range(DOCKER_ATTEMPTS):
            try:
                return runstore.count_run_containers(self.root, self.run_id)
            except runstore.DockerUnavailable:
                time.sleep(self.poll_seconds)
        return None

    def cleared(self):
        """The last look before an end is written: how many containers with the run's label are still listed, in any state, after one that was created and never started was removed. None when Docker did not answer, which is not "none is left"."""
        try:
            return runstore.clear_run_containers(self.root, self.run_id)
        except runstore.DockerUnavailable:
            return None


class _Feed:
    """The request on its way into the child's standard input. The pipe is written without waiting, so a child that does not read never holds the supervisor; what did not fit is written at the next look."""

    def __init__(self, fd, request):
        self.fd = fd
        self.rest = memoryview(request)

    def push(self):
        if self.fd is None:
            return
        try:
            while self.rest:
                self.rest = self.rest[os.write(self.fd, self.rest):]
        except BlockingIOError:
            return
        except OSError:
            # The child ended or closed its input: nobody is left to read the rest.
            pass
        self.close()

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def _group_has_a_running_process(group):
    """Whether a process of the process group is running, read from `/proc`. A process that has ended and only waits to be collected (an orphan stays in the table until the system's first process collects it) is not running."""
    try:
        os.killpg(group, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        # The group exists, and this user may signal none of its processes: whether one of them runs is read below.
        pass
    wanted = str(group).encode("ascii")
    try:
        names = os.listdir("/proc")
    except OSError:
        return True
    for name in names:
        if not name.isdigit():
            continue
        try:
            with open(f"/proc/{name}/stat", "rb") as handle:
                # After the name in brackets: the state, the parent, the process group.
                fields = handle.read().rsplit(b")", 1)[1].split()
        except (OSError, IndexError):
            continue
        if len(fields) > 2 and fields[2] == wanted and fields[0] != b"Z":
            return True
    return False


class _Processes:
    """The child and what it started: its process group, of which the child is the first process. The child is waited for and collected only through this object, because the moment it is collected its number is free for other processes: the group is looked at there and then, and once it was seen empty it is never looked at or signalled again."""

    def __init__(self, child):
        self.child = child
        self.gone = False

    def _look(self):
        """Count the group as gone when two looks in a row find no running process in it. One look can miss a process that hands over to a successor and ends between the listing of `/proc` and the reading of its entry."""
        if not _group_has_a_running_process(self.child.pid) and not _group_has_a_running_process(self.child.pid):
            self.gone = True

    def child_running(self):
        """Whether the child itself is running."""
        if self.gone:
            return False
        if self.child.poll() is None:
            return True
        self._look()
        return False

    def alive(self):
        """Whether the child, or a process it left behind in its process group, is running."""
        return self.child_running() or not self.gone

    def wait(self, seconds):
        """Wait until the child ends, at most `seconds`; when it has ended already, just that long. The group is looked at before this returns, so that a group that is empty is known to be so before anything that takes time (a call of Docker) is done."""
        if self.child_running():
            with contextlib.suppress(subprocess.TimeoutExpired):
                self.child.wait(timeout=seconds)
        else:
            time.sleep(seconds)
        self.child_running()

    def signal(self, signum):
        """Send a signal to every process of the group, unless the group was seen empty."""
        if not self.gone:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(self.child.pid, signum)

    def give_up(self):
        """Count the group as gone: what is left of it did not end on KILL."""
        self.gone = True


def _stop_run(run, processes):
    """A cancel was asked: stop the run, and return its end as (state, exit code), or None when the end could not be established. `processes` is None when the child and what it started have ended already and only a container is left."""
    term_at = None
    kill_at = None
    failures = 0
    unsettled = 0
    empty_looks = 0
    ended = processes is None
    while True:
        try:
            found, left = runstore.stop_run_containers(run.root, run.run_id)
        except runstore.DockerUnavailable:
            found, left = None, None
        failures = failures + 1 if found is None or left else 0
        if not ended:
            if found and not left and processes.child_running():
                # The container is gone and the compose process that was attached to it ends by itself, with the container's exit code: it is given the grace time before it counts as a process that has to be ended.
                processes.wait(run.grace_seconds)
                continue
            if processes.alive():
                if found == 0 or failures >= DOCKER_ATTEMPTS:
                    # Docker answered that there is no container to stop (an image build, the checks before the container starts), or it has not answered, or not stopped the container, for the limited number of tries: the child's process group is ended. An answer that is missing once is not "no container": ending a compose process whose container lives would cut the run off from its log and lose the container's exit code.
                    now = time.monotonic()
                    if term_at is None:
                        processes.signal(signal.SIGTERM)
                        term_at = now
                    elif kill_at is None and now - term_at >= run.grace_seconds:
                        processes.signal(signal.SIGKILL)
                        kill_at = now
                    elif kill_at is not None and now - kill_at >= run.grace_seconds:
                        processes.give_up()
                processes.wait(run.poll_seconds)
                continue
            # The processes have ended. Only a listing made after this point is the last word on the containers: until now a compose process could still have started one.
            ended = True
            continue
        if found is not None and not left:
            # No running container is left. A compose process that was ended while it created the container can have left one that never started, which the last look removes; one that has started after all is found running at the next round. When the processes were ended by a signal from here, one empty look is not enough: measured, the container that such a compose process was creating can show up in the listing only after the process is gone, so a second look is made one pause later.
            if run.cleared() == 0:
                empty_looks += 1
                if empty_looks >= (1 if term_at is None else 2):
                    return runstore.STATE_CANCELLED, None if processes is None else processes.child.returncode
            else:
                unsettled += 1
        if failures >= DOCKER_ATTEMPTS or unsettled >= DOCKER_ATTEMPTS:
            return None
        time.sleep(run.poll_seconds)


def _follow_output(run, since, done):
    """Append the container's output to the run's log until the container ends or `done` is set. Runs in a thread of its own, next to the wait for the container's exit code."""
    try:
        runstore.follow_run_output(run.root, run.run_id, run.log_fd, since, done.is_set)
    except Exception as error:
        run.say(f"the container's output could not be read from Docker ({type(error).__name__})")


def _watch_container(run, since):
    """The child has ended and a container of the run is alive: follow the container's output and wait for its exit code. Returns the end as (state, exit code), or None when it could not be established."""
    run.say("the compose process ended while the container of the run is alive: from here on the output is read from Docker")
    done = threading.Event()
    follower = threading.Thread(target=_follow_output, args=(run, since, done), daemon=True)
    follower.start()
    try:
        code = None
        for _ in range(DOCKER_ATTEMPTS):
            try:
                waited = runstore.wait_run_container(run.root, run.run_id, run.cancel_seen)
            except runstore.DockerUnavailable:
                waited = None
            if waited is not None:
                code = waited
            if code is None and run.cancel_seen():
                return _stop_run(run, None)
            left = run.containers()
            if left is None:
                return None
            if left == 0 and run.cleared() == 0:
                # Without an exit code, the container ended before Docker could be asked to wait for it.
                return runstore.STATE_FINISHED, code
            time.sleep(run.poll_seconds)
        return None
    finally:
        # The following ends by itself when the container does; it is given a moment for the last lines, and is ended here when the container lives on.
        follower.join(timeout=OUTPUT_DRAIN_SECONDS)
        done.set()
        follower.join(timeout=OUTPUT_DRAIN_SECONDS)


def _watch(run, processes, feed):
    """Wait for the run to end. Returns the end as (state, exit code), or None when it could not be established."""
    while processes.child_running():
        feed.push()
        if run.cancel_seen():
            feed.close()
            return _stop_run(run, processes)
        processes.wait(run.poll_seconds)
    feed.close()
    # The child has ended; what it started may not have. A compose process that outlives the child (the child was killed, or crashed, during the image build) can still start the run's container, so the containers are counted only once the whole process group is gone.
    while processes.alive():
        if run.cancel_seen():
            return _stop_run(run, processes)
        time.sleep(run.poll_seconds)
    ended_at = time.time()
    for _ in range(DOCKER_ATTEMPTS):
        left = run.containers()
        if left is None:
            return None
        if left:
            return _watch_container(run, ended_at)
        # No running container. The end is written only when none is listed in any other state either: one that was created and never started is removed by this look, and one that has started after all is found running at the next round.
        if run.cleared() == 0:
            return runstore.STATE_FINISHED, processes.child.returncode
        time.sleep(run.poll_seconds)
    return None


def _end(run, state, exit_code):
    """Write the end of the run into its record; the supervisor's exit code."""
    try:
        runstore.end_run(run.root, run.run_id, state, exit_code)
    except runstore.RunAlreadyEnded:
        run.say("the record of the run already holds an end: it is left as it is")
        return EXIT_PROBLEM
    except (runstore.StoreError, ValueError, OSError):
        run.say(f"the run ended ({state}, exit code {'not known' if exit_code is None else exit_code}), but its record could not be written")
        return EXIT_PROBLEM
    return EXIT_END_RECORDED


def _supervise(run, lock_fd, request, child_argv):
    if not runstore.holds_run_lock(run.root, run.run_id, lock_fd):
        run.say("refused: this process does not hold the lock of the run")
        return EXIT_PROBLEM
    record = runstore.read_record(run.root, run.run_id)
    if record is None or record["state"] != runstore.STATE_STARTING:
        run.say("refused: the run is not one that waits to be started")
        return EXIT_PROBLEM
    if not isinstance(request, bytes) or len(request) > REQUEST_MAX_BYTES:
        run.say("the request is larger than a request may be")
        return _end(run, runstore.STATE_FINISHED, 1)

    # The lock stays with this process: a child that held a copy would keep the run looking supervised after the supervisor is gone.
    os.set_inheritable(lock_fd, False)
    read_fd, write_fd = os.pipe()
    try:
        os.set_blocking(write_fd, False)
        child = subprocess.Popen(list(child_argv), cwd=run.root, stdin=read_fd, stdout=run.log_fd, stderr=run.log_fd, start_new_session=True)
    except OSError as error:
        os.close(write_fd)
        run.say(f"the run could not be started ({errno.errorcode.get(error.errno, 'OSError')})")
        return _end(run, runstore.STATE_FINISHED, 1)
    except BaseException:
        os.close(write_fd)
        raise
    finally:
        os.close(read_fd)
    feed = _Feed(write_fd, request)
    try:
        feed.push()
        try:
            runstore.update_record(run.root, run.run_id, state=runstore.STATE_RUNNING)
        except (runstore.StoreError, ValueError, OSError):
            run.say("the record of the run could not be written: it still says starting")
        end = _watch(run, _Processes(child), feed)
    finally:
        feed.close()
    if end is None:
        run.say("the end of the run could not be established (Docker does not answer, or a container of the run is not stopped or does not go away): its record is left as it is")
        return EXIT_PROBLEM
    return _end(run, *end)


def supervise(root, run_id, lock_fd, request, *, child_argv=None, interrupted=None, poll_seconds=None, grace_seconds=None):
    """Supervise the recorded run `run_id` of the deployment in `root` until it has ended, and return the exit code of the supervisor: 0 when the end of the run is in its record, 1 when something kept it from that (the line that says what is in the run's log), 2 when the run has no log to write to.

    `lock_fd` is the inherited descriptor that holds the run's lock; `request` the bytes of the JSON for the child. `child_argv` replaces the command of the child (CHILD_ARGV), `interrupted` is a function that answers whether this process was told to end (a cancel request), `poll_seconds` and `grace_seconds` replace POLL_SECONDS and GRACE_SECONDS.
    """
    root = os.fspath(root)
    try:
        log_fd = runstore.open_run_log(root, run_id)
    except runstore.StoreError:
        return EXIT_NO_RUN
    run = _Run(root, run_id, log_fd, interrupted or (lambda: False), POLL_SECONDS if poll_seconds is None else poll_seconds, GRACE_SECONDS if grace_seconds is None else grace_seconds)
    try:
        return _supervise(run, lock_fd, request, CHILD_ARGV if child_argv is None else child_argv)
    except Exception as error:
        # Only the kind of error is named: its text and its traceback can hold the request.
        run.say(f"the supervisor stopped on an error ({type(error).__name__})")
        return EXIT_PROBLEM
    finally:
        os.close(log_fd)


def main(argv=None):
    """The entry point: `<run_id> <lock_fd>` as arguments, the request on standard input, the deployment folder as the working directory."""
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 2 or not (argv[1].isascii() and argv[1].isdigit() and len(argv[1]) <= 9):
        print("usage: python -m social_data_pipeline.control.supervise <run_id> <lock_fd>, with the request on standard input", file=sys.stderr)
        return EXIT_NO_RUN
    told_to_end = []
    signal.signal(signal.SIGTERM, lambda signum, frame: told_to_end.append(signum))
    signal.signal(signal.SIGINT, lambda signum, frame: told_to_end.append(signum))
    signal.signal(signal.SIGHUP, signal.SIG_IGN)
    try:
        request = sys.stdin.buffer.read(REQUEST_MAX_BYTES + 1)
        return supervise(os.getcwd(), argv[0], int(argv[1]), request, interrupted=lambda: bool(told_to_end))
    except Exception as error:
        # This process's standard error is the run's log. Only the kind of error is named: its text and its traceback can hold the request.
        print(f"{LINE_PREFIX}the supervisor stopped on an error ({type(error).__name__})", file=sys.stderr)
        return EXIT_PROBLEM


if __name__ == "__main__":
    sys.exit(main())
