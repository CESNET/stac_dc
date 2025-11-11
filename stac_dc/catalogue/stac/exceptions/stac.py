class STACError(Exception):
    """Base class for all STAC-related exceptions."""
    def __init__(self, message="STAC Catalogue General Error!", **kwargs):
        super().__init__(message)
        self.message = message
        self.details = kwargs or {}

    def __str__(self):
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class STACCredentialsNotProvided(STACError):
    """Raised when STAC credentials are missing."""
    def __init__(self, message="STAC credentials were not provided!"):
        super().__init__(message)


class STACTokenNotObtainedError(STACError):
    """Raised when a STAC authentication token cannot be obtained."""
    def __init__(self, message="STAC token not obtained!"):
        super().__init__(message)


class STACRequestTimeout(STACError):
    """Raised when a STAC request exceeds timeout or retry limit."""
    def __init__(self, retry=None, max_retries=None, message=None):
        if retry is not None:
            msg = f"STAC request timed out after {retry} retries."
            if max_retries is not None:
                msg += f" Max retries: {max_retries}."
        else:
            msg = message or "STAC request timed out."
        super().__init__(msg, retry=retry, max_retries=max_retries)


class STACRequestNotOK(STACError):
    """Raised when a STAC HTTP request returns a non-OK status code."""
    def __init__(self, status_code=None, message=None, **kwargs):
        msg = message or (
            f"STAC request failed with status code {status_code}!"
            if status_code is not None
            else "STAC request status code not 200/OK!"
        )
        super().__init__(msg, status_code=status_code, **kwargs)


class STACUnsupportedMethod(STACError):
    """Raised when an unsupported HTTP method is used."""
    def __init__(self, method=None, message=None):
        msg = message or (
            f"HTTP method '{method}' is not supported by this script."
            if method is not None
            else "Selected HTTP method is not supported by this script."
        )
        super().__init__(msg, method=method)


class STACHostNotSpecified(STACError):
    """Raised when no STAC host URL is configured."""
    def __init__(self, message="STAC host server URL not specified!"):
        super().__init__(message)
