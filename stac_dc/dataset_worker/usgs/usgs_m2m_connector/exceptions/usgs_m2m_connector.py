class USGSM2MConnectorException(Exception):
    def __init__(self, message: str = "USGS M2M API Connector General Error!"):
        self.message = message
        super().__init__(self.message)


class USGSM2MTokenNotObtainedException(USGSM2MConnectorException):
    def __init__(self, message: str = "USGS M2M API Token not obtained!"):
        self.message = message
        super().__init__(self.message)


class USGSM2MCredentialsNotProvided(USGSM2MConnectorException):
    def __init__(self, message: str = "USGS M2M API Credentials were not provided!"):
        self.message = message
        super().__init__(self.message)


class USGSM2MRequestTimeout(USGSM2MConnectorException):
    def __init__(self, message: str = "USGS M2M API Request timed out.", retry: int = None, max_retries: int = None):

        if retry is not None and max_retries is not None:
            self.message = f"USGS M2M API Request timed out after {retry} retries. Max retries: {max_retries}."
        else:
            self.message = message

        super().__init__(self.message)


class USGSM2MRequestNotOK(USGSM2MConnectorException):
    def __init__(self, status_code: int = None, response_text: str = None):
        message = "USGS M2M API Request status code not 200/OK!"

        if status_code is not None:
            message = f"USGS M2M API Request status code is {status_code}!"

        if response_text is not None:
            message += f" Response text: {response_text}"

        super().__init__(message)


class USGSM2MDownloadRequestReturnedFewerURLs(USGSM2MConnectorException):
    def __init__(self, entity_ids_count: int = None, urls_count: int = None):
        message = "USGS M2M API download-request endpoint returned fewer URLs!"

        if entity_ids_count is not None and urls_count is not None:
            message += f" Entity IDs count: {entity_ids_count}, URLs count: {urls_count}."

        super().__init__(message)


class USGSM2MDownloadRequestFailed(USGSM2MConnectorException):
    def __init__(self, url: str = None):
        message = "File download failed after all retries!"

        if url:
            message += f" Failed URL: {url}"

        super().__init__(message)
