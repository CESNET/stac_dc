import boto3
import botocore.exceptions

from . import Storage
from .exceptions import *


class S3(Storage):
    def __init__(
            self,
            s3_host: str,
            access_key: str,
            secret_key: str,
            host_bucket: str,
            collection: str,
            service_name: str = "s3",
            **kwargs,
    ):
        super().__init__(collection=collection, **kwargs)
        if not host_bucket:
            raise S3BucketNotSpecified()
        self._bucket = host_bucket
        self._s3_client = boto3.client(
            service_name=service_name,
            endpoint_url=s3_host,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    # ---------------- LOW-LEVEL IMPLEMENTATION ----------------
    def _upload(self, remote_file_path: str, local_file_path: Path | str):
        local_file_path = str(local_file_path)
        self._logger.info(f"Uploading '{local_file_path}' to S3 key '{remote_file_path}'")
        self._s3_client.upload_file(local_file_path, self._bucket, remote_file_path)

    def _download(self, remote_file_path: str, local_file_path: Path | str):
        local_file_path = str(local_file_path)
        self._logger.info(f"Downloading S3 key '{remote_file_path}' to '{local_file_path}'")
        if not self._exists(remote_file_path):
            raise StorageFileNotFoundError(file=remote_file_path)
        with open(local_file_path, "wb") as f:
            self._s3_client.download_fileobj(self._bucket, remote_file_path, f)

    def _delete(self, remote_file_path: str):
        self._logger.info(f"Deleting S3 key '{remote_file_path}'")
        self._s3_client.delete_object(Bucket=self._bucket, Key=remote_file_path)

    def _exists(self, remote_file_path: str, expected_length: int | None = None) -> bool:
        try:
            head = self._s3_client.head_object(Bucket=self._bucket, Key=remote_file_path)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

        if expected_length is None:
            return True

        try:
            expected_length = int(expected_length)
        except (TypeError, ValueError):
            self._logger.warning(f"Invalid expected_length: {expected_length!r}")
            return False

        actual_length = int(head["ContentLength"])
        if actual_length != expected_length:
            self._logger.warning(
                f"S3 key '{remote_file_path}' length mismatch ({actual_length} != {expected_length})"
            )
            return False

        return True
