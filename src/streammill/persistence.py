"""Optional local state-file persistence for StreamMill.

When ``STREAMMILL_STATE_FILE`` names a non-empty path, the service
treats every successful state-changing request as one durable commit:
the full post-commit snapshot document is written to the state file
before the success response is released. The file therefore only ever
shows the complete snapshot of one successful commit — writes go to a
temporary file in the same directory, are fsynced and then atomically
renamed over the target, and the directory is fsynced afterwards.

Startup validates the configured path strictly: an empty path, a
directory, a missing or unwritable parent directory, or an existing
file that is not valid UTF-8 JSON all raise StateStoreError so the
server can exit non-zero without listening. An existing document is
handed to the regular snapshot-restore contract, which validates it
strictly and never repairs it.
"""

from __future__ import annotations

import json
import os
import tempfile


class StateStoreError(Exception):
    """Raised when the state file cannot be loaded, validated or written."""


class StateStore:
    """Durable full-snapshot state file behind atomic renames."""

    def __init__(self, path: str) -> None:
        self._path = path

    @property
    def path(self) -> str:
        return self._path

    @classmethod
    def open(cls, path: str) -> tuple["StateStore", object | None]:
        """Validate the configured path and load any existing document.

        Returns the store plus the parsed JSON document, or None when
        the file does not exist yet (the instance then starts empty and
        the first commit creates the file). Raises StateStoreError when
        the path is empty, a directory, its parent directory is missing
        or not writable, or the existing file cannot be read or is not
        valid UTF-8 JSON. The document itself is not interpreted here;
        the snapshot-restore contract validates it.
        """
        if not path:
            raise StateStoreError("state file path is empty")
        if os.path.isdir(path):
            raise StateStoreError(f"state file path is a directory: {path}")
        document = None
        if os.path.exists(path):
            try:
                with open(path, "rb") as handle:
                    raw = handle.read()
            except OSError as exc:
                raise StateStoreError(
                    f"state file cannot be read: {exc}"
                ) from exc
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise StateStoreError(
                    f"state file is not valid UTF-8: {exc}"
                ) from exc
            try:
                document = json.loads(text)
            except ValueError as exc:
                raise StateStoreError(
                    f"state file is not valid JSON: {exc}"
                ) from exc
        else:
            directory = os.path.dirname(os.path.abspath(path))
            if not os.path.isdir(directory):
                raise StateStoreError(
                    f"state file directory does not exist: {directory}"
                )
            # Probe writability the same way commits write: create and
            # remove a temporary file in the target directory.
            try:
                fd, probe = tempfile.mkstemp(
                    dir=directory, prefix=".streammill-", suffix=".probe"
                )
                os.close(fd)
                os.unlink(probe)
            except OSError as exc:
                raise StateStoreError(
                    f"state file directory is not writable: {exc}"
                ) from exc
        return cls(path), document

    def persist(self, document: object) -> None:
        """Durably replace the state file with one complete snapshot.

        The document is serialized to a temporary file in the same
        directory, fsynced and atomically renamed over the target, so
        the state file only ever presents a complete snapshot; the
        directory is fsynced afterwards so the rename itself survives a
        crash. Any failure raises StateStoreError and leaves the
        previously persisted file untouched.
        """
        data = json.dumps(document, ensure_ascii=False, sort_keys=True).encode(
            "utf-8"
        )
        directory = os.path.dirname(os.path.abspath(self._path))
        temporary = None
        try:
            fd, temporary = tempfile.mkstemp(
                dir=directory, prefix=".streammill-", suffix=".tmp"
            )
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._path)
            temporary = None
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError as exc:
            raise StateStoreError(
                f"state file cannot be written: {exc}"
            ) from exc
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
