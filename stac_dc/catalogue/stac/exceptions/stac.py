class STACError(Exception):
    def __init__(self, message="STAC Catalogue General Error!"):
        self.message = message
        super().__init__(self.message)


class STACCredentialsNotProvided(STACError):
    def __init__(self, message="STAC Credentials were not provided!"):
        self.message = message
        super().__init__(self.message)


class STACTokenNotObtainedError(STACError):
    def __init__(self, message="STAC Token not obtained!"):
        self.message = message
        super().__init__(self.message)


class STACRequestTimeout(STACError):
    def __init__(self, message="STAC Request Timeouted", retry=None, max_retries=None):
        if retry is not None:
            self.message = f"STAC Request Timeouted after {retry} retries."

            if max_retries is not None:
                self.message = f"{self.message} Max retries: {max_retries}."
        else:
            self.message = message

        super().__init__(self.message)


class STACRequestNotOK(STACError):
    def __init__(self, message="STAC Request status code not 200/OK!", status_code=None):
        if status_code is not None:
            self.message = f"STAC Request status code is {status_code}!"
        else:
            self.message = message

        super().__init__(self.message)


class STACUnsupportedMethod(STACError):
    def __init__(self, message="Selected HTTP method is not supported by script.", method=None):
        if method is not None:
            self.message = f"HTTP method {method} is not supported by script."
        else:
            self.message = message

        super().__init__(self.message)

class STACHostNotSpecified(STACError):
    def __init__(self, message="STAC host servere url not specified!"):
        super().__init__(message)