from pathlib import Path


class StorageError(Exception):
    def __init__(self, message="Storage Exception!"):
        super().__init__(message)


class StorageCannotAcquireLock(StorageError):
    def __init__(self, message="Cannot acquire file lock!", file: Path | str = None):
        if file is not None:
            file = str(file)
            message = message + f" Lock file: {file}"

        super().__init__(message)
