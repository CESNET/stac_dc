from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from stac_dc.catalogue import Catalogue
    from stac_dc.storage import Storage
    from stac_dc.dataset_worker.aoi import AOI

from typing import Any, List, Tuple

import logging

from abc import ABC, abstractmethod
from datetime import date

from .exceptions import *

from env import env


class DatasetWorker(ABC):
    _run_attempt: int

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
        self._run_attempt: int = 0

        self._dataset: str = dataset
        self._aoi: AOI = aoi

        if not storage:
            raise DatasetWorkerStorageNotSpecified()
        self._storage: Storage = storage

        if not storage:
            raise DatasetWorkerCatalogueNotSpecified()
        self._catalogue: Catalogue = catalogue

        self._logger: logging.Logger = logger

    def get_run_attempt(self) -> int:
        return self._run_attempt

    def increase_run_attempt(self) -> None:
        self._run_attempt = self.get_run_attempt() + 1

    def reset_run_attempt(self) -> None:
        self._run_attempt = 0

    def get_dataset(self) -> str:
        return self._dataset

    def get_aoi(self) -> AOI:
        return self._aoi

    @abstractmethod
    def get_id(self, *args: Any, **kwargs: Any) -> str:
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

    @abstractmethod
    def _get_days_to_download(self, *args: Any, **kwargs: Any) -> List[Tuple[date, bool]]:
        pass

    @abstractmethod
    def _get_last_downloaded_day(self, *args: Any, **kwargs: Any) -> date:
        pass

    @abstractmethod
    def _set_last_downloaded_day(self, *args: Any, **kwargs: Any) -> None:
        pass
