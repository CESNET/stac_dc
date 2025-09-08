class EnvException(Exception):
    def __init__(self, message="Environment exception!"):
        super().__init__(message)


class ProjectRootNotSet(EnvException):
    def __init__(self, message="Project root directory must be set!"):
        super().__init__(message)


class ERA5NotLoaded(EnvException):
    def __init__(self, message="Environment variables for ERA5 not loaded!"):
        super().__init__(message)


class LandsatNotLoaded(EnvException):
    def __init__(self, message="Environment variables for Landsat not loaded!"):
        super().__init__(message)
