from pathlib import Path


class LandsatProcessorException(Exception):
    def __init__(self, message="LandsatProcessorException"):
        self.message = message


class LandsatTarFileNotSpecifiedException(LandsatProcessorException):
    def __init__(self, message="Landsat TAR file is not specified!"):
        super().__init__(message)


class LandsatDatasetNotSpecified(LandsatProcessorException):
    def __init__(self, message="Landsat dataset is not specified!"):
        super().__init__(message)


class LandsatTarFileUnexpectedContents(LandsatProcessorException):
    def __init__(self, path: Path = None, message="Unexpected contents in Landsat TAR file!", additional_info=None):
        if path is not None:
            message = f"{message} Path: {path}"

        if additional_info is not None:
            message = f"{message} Additional info: {additional_info}"

        super().__init__(message)


class LandsatStacJsonDictNotAvailable(LandsatProcessorException):
    def __init__(self, message="Landsat STAC JSON dictionary is not available!"):
        super().__init__(message)


class LandsatFileAlreadyProcessed(LandsatProcessorException):
    def __init__(self, path: Path):
        message = f"Landsat file {path.name} already processed!"
        super().__init__(message)


class LandsatTarDoesNotContainStacFile(LandsatProcessorException):
    def __init__(self, path: Path):
        message = f"Landsat file {path.name} does not contain STAC JSON!"
        super().__init__(message)
