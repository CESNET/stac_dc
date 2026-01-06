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


class StorageFileNotFoundError(StorageError):
    def __init__(self, message="File not found!", file: Path | str = None):
        if file is not None:
            message = f"File {file} not found!"

        super().__init__(message)


class S3Error(Exception):
    def __init__(self, message="S3 Storage Exception!"):
        super().__init__(message)


class S3BucketNotSpecified(S3Error):
    def __init__(self, message="S3 Bucket not specified!"):
        super().__init__(message)
