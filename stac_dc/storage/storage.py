import json
import logging
import random
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager

from env import env
from .exceptions import *


# ==============================================
# BASE STORAGE CLASS
# ==============================================
class Storage(ABC):
    def __init__(self, collection: str, logger: logging.Logger | None = None):
        if not collection:
            raise ValueError("Collection must be specified at construction")
        self._collection = collection.strip("/")
        self._logger = logger or logging.getLogger(env.get_app__name())

    # ---------------- PATH HANDLING ----------------
    def _full_path(self, relative_path: str) -> str:
        relative_path = relative_path.lstrip("/")
        return f"{self._collection}/{relative_path}" if self._collection else relative_path

    # ---------------- PUBLIC API ----------------
    def upload(self, remote_file_path: str, local_file_path: Path | str):
        return self._upload(self._full_path(remote_file_path), local_file_path)

    def download(self, remote_file_path: str, local_file_path: Path | str):
        return self._download(self._full_path(remote_file_path), local_file_path)

    def delete(self, remote_file_path: str):
        return self._delete(self._full_path(remote_file_path))

    def exists(self, remote_file_path: str, expected_length: int | None = None) -> bool:
        return self._exists(self._full_path(remote_file_path), expected_length)

    # ---------------- LOW-LEVEL IMPLEMENTATION ----------------
    @abstractmethod
    def _upload(self, remote_file_path: str, local_file_path: Path | str):
        ...

    @abstractmethod
    def _download(self, remote_file_path: str, local_file_path: Path | str):
        ...

    @abstractmethod
    def _delete(self, remote_file_path: str):
        ...

    @abstractmethod
    def _exists(self, remote_file_path: str, expected_length: int | None = None) -> bool:
        ...

    # ---------------- LOCKS ----------------
    @staticmethod
    def _get_lock_file_name(remote_file_path: str) -> str:
        return f"{remote_file_path}.lock"

    @contextmanager
    def locked(self, remote_file_path: str, max_retries: int = 10, ttl: int = 120):
        lock_id = None
        try:
            lock_id = self.acquire_lock(remote_file_path, max_retries, ttl)
            yield
        finally:
            if lock_id:
                try:
                    self.release_lock(remote_file_path, lock_id)
                except Exception as e:
                    self._logger.warning(f"Could not release lock for {remote_file_path}: {e}")
                    raise

    # ---------------- TEMP FILE CONTEXT MANAGER ----------------
    @contextmanager
    def _temp_path(self, suffix: str = ""):
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        path = Path(tmp.name)
        tmp.close()
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)

    # ---------------- ACQUIRE / RELEASE LOCK ----------------
    def acquire_lock(self, remote_file_path: str, max_retries: int = 10, ttl: int = 120) -> str:
        lock_file_name = self._get_lock_file_name(remote_file_path)
        lock_id = str(uuid.uuid4())

        for _ in range(max_retries):
            if not self.exists(lock_file_name):
                self._logger.info(f"Creating lock for {remote_file_path}")
                with self._temp_path(".json") as tmp_lock_path:
                    with open(tmp_lock_path, "w", encoding="utf-8") as f:
                        json.dump({"uuid": lock_id, "timestamp": time.time(), "ttl": ttl}, f, indent=2)
                    self.upload(lock_file_name, tmp_lock_path)

                # verify lock
                with self._temp_path(".json") as verify_path:
                    self.download(lock_file_name, verify_path)
                    with open(verify_path, encoding="utf-8") as f:
                        content = json.load(f)
                    if content.get("uuid") == lock_id:
                        return lock_id
            else:
                # check for expired lock
                with self._temp_path(".json") as verify_path:
                    self.download(lock_file_name, verify_path)
                    with open(verify_path, encoding="utf-8") as f:
                        content = json.load(f)
                    if (time.time() - content.get("timestamp", 0)) > content.get("ttl", ttl):
                        self._logger.info(f"Lock '{lock_file_name}' expired, deleting")
                        self.delete(lock_file_name)

            time.sleep(0.5 + random.random())

        raise StorageCannotAcquireLock(file=lock_file_name)

    def release_lock(self, remote_file_path: str, lock_id: str):
        lock_file_name = self._get_lock_file_name(remote_file_path)
        with self._temp_path(".json") as verify_path:
            self.download(lock_file_name, verify_path)
            with open(verify_path, encoding="utf-8") as f:
                content = json.load(f)
            if content.get("uuid") == lock_id:
                self.delete(lock_file_name)