class S3Error(Exception):
    def __init__(self, message="S3 Storage Exception!"):
        super().__init__(message)


class S3BucketNotSpecified(S3Error):
    def __init__(self, message="S3 Bucket not specified!"):
        super().__init__(message)