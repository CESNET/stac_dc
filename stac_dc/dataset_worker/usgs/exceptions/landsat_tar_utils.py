from pathlib import Path


class LandsatTarUtilsException(Exception):
    def __init__(self, message="TarUtilsException"):
        self.message = message


class TarObjectNotSpecifiedException(LandsatTarUtilsException):
    def __init__(self, message="TAR file is not specified!"):
        super().__init__(message)


class TarFileNotExistsException(LandsatTarUtilsException):
    def __init__(self, message="TAR file does not exists!", path: Path = None):
        if path:
            message = message + " Given path: " + str(path)
        super().__init__(message)

class TarFilePathTraversalRisk(LandsatTarUtilsException):
    def __init__(self, message="Unsecure path inside TAR!", path: Path = None):
        if path is not None:
            message = f"{message} Path inside TAR: {str(path)}"
        super().__init__(message)