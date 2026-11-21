"""How the broker's own folder and the files in it are opened: the rules of the run store, for the modules of this package that keep a file (the password file, the audit log).

The folder is opened by its path without following a link in its last step, must be a real folder and must belong to the user this process acts as; when it is created it gets mode 0700. A file is opened by its name relative to the folder's descriptor, never through a link and never waiting (a named pipe planted under the name would otherwise hold the caller until someone writes to it), must be a regular file and must belong to the same user; when it is created it gets mode 0600, whatever the umask. A file is replaced in one step: the new content goes to a temporary file in the same folder, which then takes the file's name.

Every refusal is a `BrokerFileError` with one constant line that names no path, a failure of the file system included (a mount that refuses a change of mode, for one). The module uses the standard library and this package's errors only, keeps no state and creates nothing when it is loaded.

A temporary file of a replacement that a kill interrupted (between its creation and the rename) stays in the folder under a name of the form `<name>.<8 hex>.tmp`, mode 0600. Nothing sweeps such files; they hold what was being written and are safe to delete.
"""

import contextlib
import errno
import os
import secrets
import stat

from social_data_pipeline.control.errors import ControlError

FOLDER_MODE = 0o700
FILE_MODE = 0o600

FOLDER_NOT_A_FOLDER = "the broker folder is not a real folder (a symbolic link or a file is in its place)"
FOLDER_NOT_USABLE = "the broker folder cannot be used"
NOT_A_REGULAR_FILE = "a file of the broker folder is not a regular file"
NOT_OUR_FILE = "a file of the broker folder belongs to another user"
FILE_NOT_USABLE = "a file of the broker folder cannot be used"


class BrokerFileError(ControlError):
    """The broker folder or a file in it was refused. `message` is one constant line that holds no path."""

    code = "broker_file_not_usable"


def _checked_folder(fd):
    """Refuse a descriptor that is not a folder of the user this process acts as. Asked of the opened folder itself, so the answer is about the folder that is then used."""
    info = os.fstat(fd)
    if not stat.S_ISDIR(info.st_mode):
        raise BrokerFileError(FOLDER_NOT_A_FOLDER)
    if info.st_uid != os.geteuid():
        raise BrokerFileError(FOLDER_NOT_USABLE)


def _open_folder(path, create):
    """Open the folder at `path`: a descriptor, or None for a folder that is missing and is not to be created. Only the last step is created; a missing parent is a refusal."""
    created = False
    if create:
        try:
            os.mkdir(path, FOLDER_MODE)
            created = True
        except FileExistsError:
            pass
        except OSError:
            raise BrokerFileError(FOLDER_NOT_USABLE) from None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except FileNotFoundError:
        if create:
            raise BrokerFileError(FOLDER_NOT_USABLE) from None
        return None
    except OSError as error:
        # A link in the folder's place is reported as "not a directory", like a file.
        if error.errno in (errno.ENOTDIR, errno.ELOOP):
            raise BrokerFileError(FOLDER_NOT_A_FOLDER) from None
        raise BrokerFileError(FOLDER_NOT_USABLE) from None
    try:
        _checked_folder(fd)
        if created:
            # The umask can only have taken bits away from 0700; this puts them back.
            os.fchmod(fd, FOLDER_MODE)
    except BaseException as error:
        os.close(fd)
        # Asking about the folder or setting its mode failed (some mounts refuse a change of mode): a refusal like any other.
        if isinstance(error, OSError):
            raise BrokerFileError(FOLDER_NOT_USABLE) from None
        raise
    return fd


@contextlib.contextmanager
def folder(where, *, create=False):
    """Yield a descriptor of the broker folder for the length of a `with` block, or None when the folder is missing and `create` is false. `where` is the folder's path, or a descriptor the caller already holds (it is checked like an opened path and left open). Raises BrokerFileError for a link, a file, a folder of another user and any file-system error."""
    if isinstance(where, int) and not isinstance(where, bool):
        try:
            _checked_folder(where)
        except OSError:
            raise BrokerFileError(FOLDER_NOT_USABLE) from None
        yield where
        return
    try:
        path = os.fspath(where)
    except TypeError:
        raise BrokerFileError(FOLDER_NOT_USABLE) from None
    fd = _open_folder(path, create)
    try:
        yield fd
    finally:
        if fd is not None:
            os.close(fd)


def _checked_file(fd):
    """Close and refuse a descriptor that is not a regular file of the user this process acts as."""
    try:
        info = os.fstat(fd)
    except BaseException:
        os.close(fd)
        raise
    if not stat.S_ISREG(info.st_mode):
        os.close(fd)
        raise BrokerFileError(NOT_A_REGULAR_FILE)
    if info.st_uid != os.geteuid():
        os.close(fd)
        raise BrokerFileError(NOT_OUR_FILE)
    return fd


def open_file(dir_fd, name, flags, *, create=False):
    """Open the file `name` of the folder `dir_fd` with `flags` (the access mode, O_APPEND): a descriptor, or None when the file is missing and `create` is false. With `create` a missing file is made with mode 0600. Raises BrokerFileError for a link, for anything but a regular file, for a file of another user and for any other file-system error. The descriptor is opened without waiting; on a regular file that changes nothing."""
    flags |= os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        if create:
            try:
                fd = os.open(name, flags | os.O_CREAT | os.O_EXCL, FILE_MODE, dir_fd=dir_fd)
            except FileExistsError:
                fd = os.open(name, flags, dir_fd=dir_fd)
            else:
                try:
                    os.fchmod(fd, FILE_MODE)
                except BaseException:
                    os.close(fd)
                    raise
        else:
            fd = os.open(name, flags, dir_fd=dir_fd)
    except FileNotFoundError:
        if create:
            raise BrokerFileError(FILE_NOT_USABLE) from None
        return None
    except OSError as error:
        # A link under the name: ELOOP. A named pipe opened for writing with nobody reading: ENXIO.
        if error.errno in (errno.ELOOP, errno.ENXIO):
            raise BrokerFileError(NOT_A_REGULAR_FILE) from None
        raise BrokerFileError(FILE_NOT_USABLE) from None
    try:
        return _checked_file(fd)
    except OSError:
        raise BrokerFileError(FILE_NOT_USABLE) from None


def read_file(dir_fd, name, max_bytes):
    """The bytes of the file `name` of the folder `dir_fd`, read with one open and one read; None when there is no such file. Raises BrokerFileError for a link, anything but a regular file, a file of another user, a file larger than `max_bytes` and any file-system error."""
    fd = open_file(dir_fd, name, os.O_RDONLY)
    if fd is None:
        return None
    try:
        data = os.read(fd, max_bytes + 1)
    except OSError:
        raise BrokerFileError(FILE_NOT_USABLE) from None
    finally:
        os.close(fd)
    if len(data) > max_bytes:
        raise BrokerFileError(FILE_NOT_USABLE)
    return data


def replace_file(dir_fd, name, payload):
    """Replace the file `name` of the folder `dir_fd` in one step: the content is written to a temporary file in the same folder (mode 0600), which then takes the name, so a reader sees the old file or the new one and never a part of either. When anything fails the old file is left as it was and the temporary file is removed. Raises BrokerFileError when something other than a regular file of this user is under the name (it is not replaced: whoever put it there is to take it away) and for any file-system error."""
    try:
        try:
            info = os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
        except FileNotFoundError:
            info = None
        if info is not None:
            if not stat.S_ISREG(info.st_mode):
                raise BrokerFileError(NOT_A_REGULAR_FILE)
            if info.st_uid != os.geteuid():
                raise BrokerFileError(NOT_OUR_FILE)
        temporary = f"{name}.{secrets.token_hex(4)}.tmp"
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, FILE_MODE, dir_fd=dir_fd)
        try:
            try:
                os.fchmod(fd, FILE_MODE)
                view = memoryview(payload)
                while view:
                    view = view[os.write(fd, view):]
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(temporary, name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        except BaseException:
            with contextlib.suppress(OSError):
                os.unlink(temporary, dir_fd=dir_fd)
            raise
        # The new name is made durable too; a folder that cannot be synced does not undo the replacement.
        with contextlib.suppress(OSError):
            os.fsync(dir_fd)
    except OSError:
        raise BrokerFileError(FILE_NOT_USABLE) from None
