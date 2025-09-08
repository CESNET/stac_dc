import boto3
import logging

import botocore.exceptions

from pathlib import Path

from stac_dc.storage import Storage

from env import env

from .exceptions.s3 import *


class S3(Storage):
    def __init__(
        self,
        s3_host,
        access_key,
        secret_key,
        host_bucket,
        service_name='s3',
        logger=logging.getLogger(env.get_app__name()),
    ):
        """
        Initialize the S3 storage connector

        :param s3_host: Endpoint URL of the S3 service
        :param access_key: Access key
        :param secret_key: Secret key
        :param host_bucket: Name of the bucket used as the root for all operations
        :param service_name: AWS service name (default: "s3")
        :param logger: Logger
        :raises S3BucketNotSpecified: If no bucket is provided
        """

        self._s3_client = boto3.client(
            service_name=service_name,
            endpoint_url=s3_host,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        if host_bucket is None:
            raise S3BucketNotSpecified()

        self._bucket = host_bucket

        super().__init__(logger=logger)

    def upload(self, remote_file_path: str, local_file_path: Path | str):
        """
        Upload a local file to S3

        :param remote_file_path: Target S3 key (path in the bucket)
        :param local_file_path: Local path to the file that will be uploaded
        """

        local_file_path = str(local_file_path)
        bucket_key = remote_file_path

        self._logger.info(f"Uploading local file '{local_file_path}' to S3 as key '{bucket_key}'")
        self._s3_client.upload_file(local_file_path, self._bucket, bucket_key)

    def download(self, remote_file_path: str, local_file_path: Path | str):
        """
        Download a file from S3 to the local filesystem

        :param remote_file_path: S3 key of the file to download
        :param local_file_path: Local path where the file will be saved
        :raises botocore.exceptions.ClientError: If the download fails (key not found etc.)
        """

        local_file_path = str(local_file_path)
        bucket_key = remote_file_path

        self._logger.info(f"Downloading S3 key '{bucket_key}' into local file '{local_file_path}'")

        try:
            with open(local_file_path, 'wb') as downloaded_file:
                self._s3_client.download_fileobj(self._bucket, bucket_key, downloaded_file)

        except botocore.exceptions.ClientError as e:
            raise e

    def delete(self, remote_file_path: str):
        """
        Delete a file from the S3 bucket

        :param remote_file_path: S3 key of the file to delete
        """

        bucket_key = remote_file_path
        self._logger.info(f"Deleting S3 key '{bucket_key}'")
        self._s3_client.delete_object(Bucket=self._bucket, Key=bucket_key)

    def exists(self, remote_file_path: str, expected_length=None) -> bool:
        """
        Check whether a file exists in S3, optionally verifying its size

        :param remote_file_path: S3 key of the file to check
        :param expected_length: Expected file size in bytes (int); None to skip size validation
        :return: True if the file exists (and matches size if expected_length is given), otherwise False
        :raises botocore.exceptions.ClientError: For errors other than a 404
        """

        bucket_key = str(remote_file_path)

        try:
            key_head = self._s3_client.head_object(Bucket=self._bucket, Key=bucket_key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # File/key does not exist
                return False
            else:
                # Could be 403 etc
                raise e

        # File exists now

        if expected_length is not None:
            # Size check required
            if str(key_head['ContentLength']) == expected_length:
                # File size checks
                return True

            else:
                # File size mismatch
                self._logger.warning(
                    f"S3 key '{bucket_key}' length ({key_head['ContentLength']} b) does not match expected length "
                    f"({expected_length} b)!"
                )

                return False
        else:
            # No size check required
            return True
