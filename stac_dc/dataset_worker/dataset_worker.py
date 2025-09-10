from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stac_dc.catalogue import Catalogue
    from stac_dc.storage import Storage
    from stac_dc.dataset_worker.aoi import AOI

import json
import logging
import tempfile
import time
import random
import uuid

from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta, timezone

from .exceptions import *

from env import env


class DatasetWorker(ABC):
    _last_downloaded_day_filename: str = "last_downloaded_day.json"

    _dataset: str
    _aoi: AOI

    _storage: Storage

    def __init__(
            self,
            dataset: str,
            aoi: AOI,
            storage: Storage,
            catalogue: Catalogue,
            logger: logging.Logger = logging.getLogger(env.get_app__name()),
    ):
        self._dataset: str = dataset
        self._aoi: AOI = aoi

        if not storage:
            raise DatasetWorkerStorageNotSpecified()
        self._storage: Storage = storage

        if not storage:
            raise DatasetWorkerCatalogueNotSpecified()
        self._catalogue: Catalogue = catalogue

        self._logger: logging.Logger = logger

    def get_dataset(self) -> str:
        return self._dataset

    def get_aoi(self) -> AOI:
        return self._aoi

    @abstractmethod
    def get_id(self, day: date) -> str:
        pass

    @abstractmethod
    def get_catalogue_download_host(self):
        pass

    @abstractmethod
    def run(self, **kwargs) -> None:
        """
        Run pipeline
        1. get days to download
        2. API fetch
        3. download from api
        4. upload to storage
        5. create & register STAC item
        """
        pass

    def _get_last_downloaded_day(self):
        """
        Method reads date of last downloaded day from storage for the current AOI
        :return: last_downloaded_day: datetime.date of last downloaded day
        """

        # delete=False - do not delete tempfile instantly; but finally block needed
        last_downloaded_day_file = tempfile.NamedTemporaryFile(mode='w+b', suffix='.json', delete=False)

        try:
            self._storage.download(
                remote_file_path=f"{self._dataset}/{self._last_downloaded_day_filename}",
                local_file_path=last_downloaded_day_file.name
            )

            last_downloaded_day = datetime.strptime(
                json.load(last_downloaded_day_file)[self._aoi.get_name()],
                "%Y-%m-%d"
            ).date()

        finally:
            # Delete tempfile
            last_downloaded_day_file.close()
            Path(last_downloaded_day_file.name).unlink(missing_ok=True)

        self._logger.info(
            f"Last downloaded day for AOI {self._aoi.get_name()} "
            f"is {last_downloaded_day.strftime('%Y-%m-%d')}"
        )

        return last_downloaded_day

    def _get_days_to_download(
            self, redownload_threshold: int, recent_days: int = 10
    ) -> list[tuple[date, bool]]:
        today = datetime.now(timezone.utc).date()
        redownload_day = today - timedelta(weeks=redownload_threshold)

        last_downloaded_day = self._get_last_downloaded_day()

        date_from = min(redownload_day, last_downloaded_day)
        date_to = today

        days_to_download: list[tuple[date, bool]] = []

        for i in range(1, (date_to - date_from).days + 1):
            d = date_from + timedelta(days=i)

            force_redownload = (
                    d >= today - timedelta(days=recent_days)  # last n recent_days
                    or d == redownload_day  # ...or threshold day (for example 12 weeks back)
            )

            days_to_download.append((d, force_redownload))

        return days_to_download

    def _set_last_downloaded_day(self, last_downloaded_day: date):
        """
        Update date of last downloaded day in storage for the current AOI.
        """
        remote_file_path = f"{self._dataset}/{self._last_downloaded_day_filename}"

        lock_id = self._storage.acquire_lock(remote_file_path=remote_file_path)

        last_downloaded_day_file = tempfile.NamedTemporaryFile(mode="w+b", suffix=".json", delete=False)
        try:
            try:
                self._storage.download(remote_file_path=remote_file_path, local_file_path=last_downloaded_day_file.name)
                with open(last_downloaded_day_file.name, "r", encoding="utf-8") as f:
                    last_downloaded_day_file_contents = json.load(f)
            except Exception:
                last_downloaded_day_file_contents = {}

            last_downloaded_day_file_contents[self._aoi.get_name()] = last_downloaded_day.strftime("%Y-%m-%d")

            with open(last_downloaded_day_file.name, "w", encoding="utf-8") as f:
                json.dump(
                    last_downloaded_day_file_contents, f,
                    indent=2
                )

            self._storage.upload(local_file_path=last_downloaded_day_file.name, remote_file_path=remote_file_path)

        finally:
            last_downloaded_day_file.close()
            Path(last_downloaded_day_file.name).unlink(missing_ok=True)

            try:
                self._storage.release_lock(remote_file_path=remote_file_path, lock_id=lock_id)
            except Exception as e:
                self._logger.warning(f"Could not release lock for {remote_file_path}: {e}")

        self._logger.info(
            f"Last downloaded day for AOI {self._aoi.get_name()} "
            f"updated to {last_downloaded_day.strftime('%Y-%m-%d')}"
        )
