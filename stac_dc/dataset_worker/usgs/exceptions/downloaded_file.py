class DownloadedFileError(Exception):
    def __init__(self, message="Downloaded File General Error!", display_id=None):
        if display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        self.message = message + f" displayId:{display_id}"
        super().__init__(self.message)


class DownloadedFileWrongConstructorArgumentsPassed(DownloadedFileError):
    def __init__(
            self,
            message="Wrong arguments passed to DownloadedFile constructor!",
            display_id=None
    ):
        super().__init__(message=message, display_id=display_id)


class DownloadedFileWorkdirNotSpecified(DownloadedFileError):
    def __init__(self, message="Workdir not specified!", display_id=None):
        super().__init__(message=message, display_id=display_id)


class DownloadedFileS3ConnectorNotSpecified(DownloadedFileError):
    def __init__(self, message="S3Connector not specified!", display_id=None):
        super().__init__(message=message, display_id=display_id)


class DownloadedFileSTACConnectorNotSpecified(DownloadedFileError):
    def __init__(self, message="STACConnector not specified!", display_id=None):
        if display_id is None:
            raise Exception(f"Exception [{message}] raised from an uninitialized instance!")

        super().__init__(message=message, display_id=display_id)


class DownloadedFileDownloadedFileHasDifferentSize(DownloadedFileError):
    def __init__(
            self, message="Downloaded file size not matching expected file size!",
            expected_size=None, real_size=None,
            display_id=None
    ):
        self.message = message + f" Expected size: {str(expected_size)}, real size: {str(real_size)}."
        super().__init__(self.message, display_id=display_id)


class DownloadedFileUrlDoesNotContainFilename(DownloadedFileError):
    def __init__(self, message="URL does not contain filename!", url=None, display_id=None):
        if url is not None:
            self.message = message + f" {str(url)}."
        else:
            self.message = message

        super().__init__(message=self.message, display_id=display_id)


class DownloadedFileDoesNotContainMetadata(DownloadedFileError):
    def __init__(self, message="Downloaded file does not contain metadata!", display_id=None):
        super().__init__(message=message, display_id=display_id)


class DownloadedFileThreadLockNotSet(DownloadedFileError):
    def __init__(self, message="Thread lock is not set!", display_id=None):
        super().__init__(message=message, display_id=display_id)


class DownloadedFileCannotCreateStacItem(DownloadedFileError):
    def __init__(self, message="Unable to create STAC item!", display_id=None):
        super().__init__(message=message, display_id=display_id)


class DownloadedFileFilenameToUntarNotSpecified(DownloadedFileError):
    def __init__(self, message="Filename to be extracted from tar archive not specified!", display_id=None):
        super().__init__(message=message, display_id=display_id)
