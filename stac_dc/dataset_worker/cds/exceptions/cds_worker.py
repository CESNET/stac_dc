class CDSWorkerError(Exception):
    def __init__(self, message="CDS Worker General Error!"):
        self.message = message
        super().__init__(self.message)


class CDSWorkerDataNotAvailableYet(CDSWorkerError):
    def __init__(self, message="Requested data is not available yet!"):
        self.message = message
        super().__init__(self.message)