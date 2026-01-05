class CDSWorkerError(Exception):
    def __init__(self, message="CDS Worker General Error!"):
        self.message = message
        super().__init__(self.message)


class CDSWorkerDataNotAvailable(CDSWorkerError):
    def __init__(self, message="Requested data is not available!", yet=False):
        """
        CDSWorkerDataNotAvailable exception

        :param message:
        :param yet: If requested day is too recent, data may not be available to public yet. Then this flag should be True
        """
        self._yet = yet
        self.message = message
        super().__init__(self.message)

    def not_available_yet(self):
        return self._yet