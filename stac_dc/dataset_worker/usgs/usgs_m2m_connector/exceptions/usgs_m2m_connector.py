class USGSM2MConnectorException(Exception):
    def __init__(self, message="USGS M2M API Connector General Error!"):
        self.message = message
        super().__init__(self.message)


class USGSM2MTokenNotObtainedException(USGSM2MConnectorException):
    def __init__(self, message="USGS M2M API Token not obtained!"):
        self.message = message
        super().__init__(self.message)


class USGSM2MCredentialsNotProvided(USGSM2MConnectorException):
    def __init__(self, message="USGS M2M API Credentials were not provided!"):
        self.message = message
        super().__init__(self.message)


class USGSM2MRequestTimeout(USGSM2MConnectorException):
    def __init__(self, message="USGS M2M API Request Timeouted.", retry=None, max_retries=None):
        if retry is not None:
            self.message = "USGS M2M API Request Timeouted after {} retries.".format(retry)

            if max_retries is not None:
                self.message = self.message + " Max retries: {}.".format(max_retries)
        else:
            self.message = message

        super().__init__(self.message)


class USGSM2MRequestNotOK(USGSM2MConnectorException):
    def __init__(self, message="USGS M2M API Request status code not 200/OK!", status_code=None):
        if status_code is not None:
            self.message = "USGS M2M API Request status code is {}!".format(status_code)
        else:
            self.message = message

        super().__init__(self.message)


class USGSM2MDownloadRequestReturnedFewerURLs(USGSM2MConnectorException):
    def __init__(
            self,
            message="USGS M2M API download-request endpoint returned fewer URLs! entityIds count: {}, URLs count: {}.",
            entity_ids_count=None, urls_count=None
    ):
        if entity_ids_count and urls_count:
            self.message = message.format(entity_ids_count, urls_count)
        else:
            self.message = message


class USGSM2MDownloadableUrlsNotObtained(USGSM2MConnectorException):
    def __init__(self, message="Downloadable URLs not obtained!", downloadable_urls=None):
        self.message = message
        for url in downloadable_urls:
            self.message = self.message + '\n' + str(url)

        super().__init__(self.message)