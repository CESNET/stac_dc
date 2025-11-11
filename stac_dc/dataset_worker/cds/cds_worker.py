import logging
import tempfile

from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

import cdsapi
import requests

from stac_dc.dataset_worker.dataset_worker import DatasetWorker
from .exceptions import CDSWorkerDataNotAvailableYet
from env import env


class CDSWorker(DatasetWorker, ABC):
    _product_types: list[str]
    _variables: list[str]
    _available_hours: list[str]
    _formats: list[str]

    def __init__(
            self,
            formats: Optional[list[str]] = None,
            logger: logging.Logger = logging.getLogger(env.get_app__name()),
            **kwargs,
    ):
        self._formats = formats or []
        super().__init__(logger=logger, **kwargs)

    # ------------------------
    # Abstract methods for workers
    # ------------------------

    @abstractmethod
    def _prepare_cdsapi_call_dict(self, day: date, product_type: str, data_format: str) -> dict:
        """Prepare the request dictionary for cdsapi.Client().retrieve()."""
        pass

    @abstractmethod
    def _get_redownload_threshold(self) -> int:
        """Return number of days to consider for redownload in case of missing data."""
        pass

    @abstractmethod
    def get_catalogue_download_host(self) -> str:
        """Return host of the catalogue service (e.g., CDS)."""
        pass

    # ------------------------
    # Helpers
    # ------------------------

    def _get_file_parent_dir(self, day: date) -> str:
        return f"{day:%Y/%m/%d}/{self._aoi.get_name()}"

    def _get_file_path(self, day: date, product_type: str, data_format: str) -> str:
        return f"{self._get_file_parent_dir(day)}/{product_type}.{data_format}"

    def get_id(self, day: date) -> str:
        return f"{self._dataset}_{day:%Y_%m_%d}_{self._aoi.get_name()}"

    # ------------------------
    # Main worker
    # ------------------------

    def run(self, **kwargs) -> None:
        """Main pipeline: download missing assets and register them into catalogue."""
        self._logger.debug("CDS pipeline started")

        days_to_download: List[Tuple[date, bool]] = self._get_days_to_download(
            redownload_threshold=self._get_redownload_threshold()
        )

        try:
            for day_to_download in days_to_download:
                day, force_redownload = day_to_download

                self._logger.info(f"[{day:%Y-%m-%d}] Start processing")

                assets = self._process_day(day, force_redownload)

                if assets:
                    self._register_catalogue_item(day, assets)
                else:
                    self._logger.info(f"[{day:%Y-%m-%d}] Skipping catalogue item (no assets)")

                self._set_last_downloaded_day(day)

                self._logger.info(f"[{day:%Y-%m-%d}] Finished processing")

                self.reset_run_attempt()

        except CDSWorkerDataNotAvailableYet:
            self._logger.info("All downloaded, no more data available.")

    # ---------------------------------------------------------------------
    # Helpers for run()
    # ---------------------------------------------------------------------

    def _process_day(self, day: date, force_redownload: bool) -> list[dict[str, str]]:
        """Download all required assets for one day and return their metadata."""
        assets: list[dict[str, str]] = []

        for product_type in self._product_types:
            for data_format in self._formats:
                storage_path = f"{self._dataset}/{self._get_file_path(day, product_type, data_format)}"
                tmp_file: Optional[Path] = None

                try:
                    if not force_redownload and self._storage.exists(storage_path):
                        self._logger.info(f"[{day:%Y-%m-%d}] Already exists: {storage_path}")
                        assets.append(self._make_asset(product_type, data_format, storage_path))
                        continue

                    tmp_file = None

                    try:
                        tmp_file = self._download_from_api(day, product_type, data_format)
                    except CDSWorkerDataNotAvailableYet as e:
                        self._logger.info(f"[{day:%Y-%m-%d}] {e.message}")
                        raise e

                    if not tmp_file:
                        continue

                    self._save_to_storage(tmp_file, storage_path)
                    assets.append(self._make_asset(product_type, data_format, storage_path))

                except Exception as e:
                    self._logger.error(
                        f"[{day:%Y-%m-%d}] Error downloading {product_type}.{data_format}: {e}",
                        exc_info=True,
                    )
                    raise

                finally:
                    if tmp_file:
                        tmp_file.unlink(missing_ok=True)

        return assets

    @abstractmethod
    def _prepare_stac_feature_json(self, day: date, assets: list[dict]) -> str:
        pass

    def _register_catalogue_item(self, day: date, assets: list[dict[str, str]]) -> None:
        feature_json = self._prepare_stac_feature_json(day, assets)

        tmp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".json", encoding="utf-8", delete=False
            ) as tmp_file:
                tmp_file.write(feature_json)
                tmp_file_path = Path(tmp_file.name)


            remote_path = f"{self._dataset}/{self._get_file_parent_dir(day)}.json"
            self._save_to_storage(file_to_save=tmp_file_path, remote_path=remote_path)

            feature_id = self._catalogue.register_item(dataset=self._dataset, json_data=feature_json)
            self._logger.info(f"[{day:%Y-%m-%d}] Registered STAC item ({feature_id}) and uploaded JSON to storage.")

        except Exception as e:
            self._logger.error(f"[{day:%Y-%m-%d}] Failed to register STAC item: {e}", exc_info=True)
            raise
        finally:
            if tmp_file_path and tmp_file_path.exists():
                tmp_file_path.unlink(missing_ok=True)

    @staticmethod
    def _make_asset(product_type: str, data_format: str, href: str) -> dict[str, str]:
        return {
            "product_type": product_type,
            "data_format": data_format,
            "href": href,
        }

    @abstractmethod
    def _check_dataset_not_available(self, cds_exception: requests.exceptions.HTTPError) -> bool:
        pass

    # ------------------------
    # CDS API connection
    # ------------------------

    def _call_cdsapi(self, request: dict) -> Path:
        """Perform CDS API request and return path to the downloaded file."""
        downloaded_file = tempfile.NamedTemporaryFile(
            mode="w+b",
            suffix=f".{request['data_format']}",
            delete=False,
        )
        downloaded_file.close()

        try:
            cdsapi.Client().retrieve(
                self._dataset,
                request,
                downloaded_file.name,
            )
        except requests.exceptions.HTTPError as http_error:
            if self._check_dataset_not_available(cds_exception=http_error):
                raise CDSWorkerDataNotAvailableYet(f"Requested data not available yet")
            else:
                raise http_error

        return Path(downloaded_file.name)

    def _download_from_api(self, day: date, product_type: str, data_format: str) -> Optional[Path]:
        """Download one product for a given day from CDS API."""
        self._logger.info(f"[{day:%Y-%m-%d}] Downloading {product_type}.{data_format}")
        file_path = self._call_cdsapi(
            request=self._prepare_cdsapi_call_dict(day, product_type, data_format)
        )
        self._logger.info(f"[{day:%Y-%m-%d}] Downloaded {product_type}.{data_format} into {file_path.name}")
        return file_path
