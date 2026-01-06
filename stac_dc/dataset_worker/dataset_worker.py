from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Tuple

if TYPE_CHECKING:
    from stac_dc.catalogue.catalogue import Catalogue
    from stac_dc.catalogue.catalogue_item import CatalogueItem
    from stac_dc.storage import Storage
    from stac_dc.dataset_worker.aoi import AOI

import json
import logging

from abc import ABC, abstractmethod
from datetime import date, datetime
from tempfile import NamedTemporaryFile

from .exceptions import *

from env import env


class DatasetWorker(ABC):
    def __init__(
            self,
            dataset: str,
            catalogue_collection: str | None,
            aoi: AOI,
            storage: Storage,
            catalogue: Catalogue,
            logger: logging.Logger | None = None,
    ):
        if not dataset:
            raise ValueError("Dataset must be specified")
        self._dataset = dataset
        self._catalogue_collection = catalogue_collection or dataset

        if not aoi:
            raise ValueError("Area of interest must be specified")
        self._aoi = aoi

        if not storage:
            raise ValueError("Storage must be specified")
        self._storage = storage

        if not catalogue:
            raise ValueError("Catalogue must be specified")
        self._catalogue = catalogue

        self._logger = logger or logging.getLogger(env.get_app__name())

        self._run_attempt = 0
        self._last_downloaded_day_filename = "last_downloaded_day.json"

    # ------------------------
    # Run attempt management
    # ------------------------
    def get_run_attempt(self) -> int:
        return self._run_attempt

    def increase_run_attempt(self) -> None:
        self._run_attempt += 1

    def reset_run_attempt(self) -> None:
        self._run_attempt = 0

    # ------------------------
    # Dataset & AOI getters
    # ------------------------
    def get_dataset(self) -> str:
        return self._dataset

    def get_aoi(self) -> AOI:
        return self._aoi

    # ------------------------
    # Abstract methods
    # ------------------------
    @abstractmethod
    def get_catalogue_download_host(self):
        pass

    @abstractmethod
    def run(self, **kwargs) -> None:
        """
        Run pipeline:
        1. get days to download
        2. API fetch
        3. download from API
        4. upload to storage
        5. create & register catalogue item
        """
        pass

    @abstractmethod
    def _get_days_to_download(self, *args: Any, **kwargs: Any) -> List[Tuple[date, bool]]:
        pass

    # ------------------------
    # Catalogue registration
    # ------------------------
    def _register_catalogue_item(self, item: CatalogueItem):
        self._catalogue.register_item(item)

    @abstractmethod
    def _get_path_to_catalogue_file(self, day: date) -> str:
        pass

    def _save_catalogue_item(self, day: date, item: CatalogueItem):
        path_to_file = self._get_path_to_catalogue_file(day)

        tmp_file = NamedTemporaryFile(
            mode="w+b",
            suffix=".json",
            delete=False,
        )

        try:
            with open(tmp_file.name, "w+", encoding="utf-8") as f:
                json.dump(item.to_stac(), f, indent=2)

            with self._storage.locked(path_to_file):
                self._storage.upload(
                    remote_file_path=path_to_file,
                    local_file_path=tmp_file.name
                )

        finally:
            tmp_file.close()
            Path(tmp_file.name).unlink(missing_ok=True)

    # ------------------------
    # Last downloaded day
    # ------------------------
    def _get_last_downloaded_day(self) -> date:
        tmp_path = None

        with self._storage.locked(self._last_downloaded_day_filename):
            try:
                tmp_file = NamedTemporaryFile(
                    mode="w+b",
                    suffix=".json",
                    delete=False,
                )
                tmp_path = Path(tmp_file.name)
                tmp_file.close()

                self._storage.download(
                    remote_file_path=self._last_downloaded_day_filename,
                    local_file_path=tmp_path,
                )

                with open(tmp_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

            finally:
                if tmp_path:
                    tmp_path.unlink(missing_ok=True)

        try:
            last_day = datetime.strptime(
                data[self._aoi.get_name()],
                "%Y-%m-%d",
            ).date()

            self._logger.info(
                f"Last downloaded day for AOI {self._aoi.get_name()}: {last_day}"
            )

            return last_day

        except KeyError:
            self._logger.warning(
                f"Last downloaded day for AOI {self._aoi.get_name()} is not specified! Returning today."
            )

            return date.today()

    def _set_last_downloaded_day(self, last_downloaded_day: date) -> None:
        with self._storage.locked(self._last_downloaded_day_filename):
            try:
                tmp_file = NamedTemporaryFile(mode="w+b", suffix=".json", delete=False)
                try:
                    self._storage.download(
                        remote_file_path=self._last_downloaded_day_filename,
                        local_file_path=tmp_file.name
                    )
                    with open(tmp_file.name, "r", encoding="utf-8") as f:
                        contents = json.load(f)
                except Exception:
                    contents = {}

                contents[self._aoi.get_name()] = last_downloaded_day.strftime("%Y-%m-%d")

                with open(tmp_file.name, "w", encoding="utf-8") as f:
                    json.dump(contents, f, indent=2)

                self._storage.upload(
                    remote_file_path=self._last_downloaded_day_filename,
                    local_file_path=tmp_file.name
                )
            finally:
                tmp_file.close()
                Path(tmp_file.name).unlink(missing_ok=True)

        self._logger.info(
            f"Updated last downloaded day for dataset {self._dataset}, AOI {self._aoi.get_name()} to {last_downloaded_day}"
        )

    @abstractmethod
    def _build_catalogue_item(self, day: date, assets: List[dict]) -> CatalogueItem:
        """
        Return a prepared CatalogueItem for the given day and assets.
        Implementace je dataset-specific, ale logika vytvoření itemu je
        jednotná a patří do DatasetWorker vrstvy.
        """
        pass

    # ------------------------
    # Save arbitrary file to storage
    # ------------------------
    def _save_to_storage(self, file_to_save: Path, remote_path: str) -> None:
        with self._storage.locked(remote_path):
            self._storage.upload(remote_file_path=remote_path, local_file_path=file_to_save)
            self._logger.info(f"Saved {file_to_save.name} to storage as {remote_path}")
