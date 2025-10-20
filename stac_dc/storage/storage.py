import json
import logging
import random
import tempfile
import time
import uuid

from abc import ABC, abstractmethod
from contextlib import contextmanager

from .exceptions import *


class Storage(ABC):
    def __init__(self, logger: logging.Logger):
        self._logger = logger

    @abstractmethod
    def download(self, remote_file_path: str, local_file_path: Path | str):
        pass

    @abstractmethod
    def upload(self, remote_file_path: str, local_file_path: Path | str):
        pass

    @abstractmethod
    def delete(self, remote_file_path: str):
        pass

    @abstractmethod
    def exists(self, remote_file_path: str, expected_length=None) -> bool:
        pass

    @staticmethod
    def _get_lock_file_name(remote_file_path: str) -> str:
        return f"{remote_file_path}.lock"

    #########
    # LOCKS
    #########

    @contextmanager
    def locked(self, remote_file_path: str, max_retries: int = 10, ttl: int = 120):
        lock_id = None
        try:
            lock_id = self.acquire_lock(remote_file_path=remote_file_path, max_retries=max_retries, ttl=ttl)
            yield
        finally:
            if lock_id:
                try:
                    self.release_lock(remote_file_path=remote_file_path, lock_id=lock_id)
                except Exception as e:
                    self._logger.warning(f"Could not release lock for {remote_file_path}: {e}")
                    raise e

    def acquire_lock(self, remote_file_path: str, max_retries: int = 10, ttl: int = 120) -> str:
        lock_file_name = self._get_lock_file_name(remote_file_path)
        assigned_lock_id = str(uuid.uuid4())

        for attempt in range(max_retries):
            if not self.exists(remote_file_path=lock_file_name):
                self._logger.info(f"Creating lock for {remote_file_path}.")
                tmp_lock = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8")
                try:
                    json.dump(
                        {
                            "uuid": assigned_lock_id,
                            "timestamp": time.time(),
                            "ttl": ttl,
                        },
                        tmp_lock,
                        indent=2
                    )
                finally:
                    tmp_lock.close()

                self.upload(remote_file_path=lock_file_name, local_file_path=tmp_lock.name)

                verify_tmp = tempfile.NamedTemporaryFile(mode="w+b", delete=False)
                try:
                    self.download(remote_file_path=lock_file_name, local_file_path=verify_tmp.name)
                    with open(verify_tmp.name, "r", encoding="utf-8") as f:
                        content = json.load(f)
                    if content.get("uuid") == assigned_lock_id:
                        return assigned_lock_id
                finally:
                    Path(tmp_lock.name).unlink(missing_ok=True)
                    verify_tmp.close()
                    Path(verify_tmp.name).unlink(missing_ok=True)

            else:
                verify_tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".json", delete=False)
                try:
                    self.download(remote_file_path=lock_file_name, local_file_path=verify_tmp.name)
                    with open(verify_tmp.name, "r", encoding="utf-8") as f:
                        content = json.load(f)

                    lock_ttl = content["ttl"]
                    lock_timestamp = content["timestamp"]

                    if (time.time() - lock_timestamp) > lock_ttl:
                        self._logger.info(
                            f"Lock file '{lock_file_name}' expired after {lock_ttl} s, deleting it."
                        )
                        self.delete(remote_file_path=lock_file_name)

                finally:
                    verify_tmp.close()
                    Path(verify_tmp.name).unlink(missing_ok=True)

            time.sleep(0.5 + random.random())

        raise StorageCannotAcquireLock(file=lock_file_name)

    def release_lock(self, remote_file_path: str, lock_id: str):
        lock_file_name = self._get_lock_file_name(remote_file_path)

        verify_tmp = tempfile.NamedTemporaryFile(mode="w+b", suffix=".json", delete=False)
        try:
            self.download(remote_file_path=lock_file_name, local_file_path=verify_tmp.name)
            with open(verify_tmp.name, "r", encoding="utf-8") as f:
                content = json.load(f)
            if content.get("uuid") == lock_id:
                self.delete(remote_file_path=lock_file_name)
        finally:
            verify_tmp.close()
            Path(verify_tmp.name).unlink(missing_ok=True)

    ############
    # END LOCKS
    ############
