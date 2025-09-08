from pathlib import Path

class DatasetWorkerError(Exception):
    def __init__(self, message="Dataset Worker General Error!"):
        self.message = message
        super().__init__(self.message)


class DatasetWorkerStorageNotSpecified(DatasetWorkerError):
    def __init__(self, message="Storage not specified!"):
        self.message = message
        super().__init__(self.message)

class DatasetWorkerCatalogueNotSpecified(DatasetWorkerError):
    def __init__(self, message="Catalogue not specified!"):
        self.message = message
        super().__init__(self.message)
