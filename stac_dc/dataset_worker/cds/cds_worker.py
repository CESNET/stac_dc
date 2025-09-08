import json
import logging
import tempfile
from abc import ABC, abstractmethod
from datetime import date
from pathlib import Path
from typing import Optional

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

        days_to_download = self._get_days_to_download(
            redownload_threshold=self._get_redownload_threshold()
        )

        for day_to_download in days_to_download:
            day = day_to_download[0]
            force_redownload = day_to_download[1]

            self._logger.info(f"[{day:%Y-%m-%d}] Start processing")

            assets = self._process_day_assets(day, force_redownload)

            if assets:
                self._register_catalogue_item(day, assets)
            else:
                self._logger.info(f"[{day:%Y-%m-%d}] Skipping catalogue item (no assets)")

            self._set_last_downloaded_day(day)

            self._logger.info(f"[{day:%Y-%m-%d}] Finished processing")

    # ---------------------------------------------------------------------
    # Helpers for run()
    # ---------------------------------------------------------------------

    def _process_day_assets(self, day: date, force_redownload: bool) -> list[dict[str, str]]:
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

                    tmp_file = self._download_from_api(day, product_type, data_format)
                    if not tmp_file:
                        continue

                    self._save_to_storage(tmp_file, storage_path)
                    assets.append(self._make_asset(product_type, data_format, storage_path))

                except CDSWorkerDataNotAvailableYet as e:
                    self._logger.warning(
                        f"[{day:%Y-%m-%d}] Not yet available: {product_type}.{data_format} ({e})"
                    )

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

    def _register_catalogue_item(self, day: date, assets: list[dict[str, str]]) -> None:
        _, catalogue_item, catalogue_item_format = self._catalogue.register_item(
            worker=self,
            dataset=self._dataset,
            day=day,
            aoi=self._aoi,
            assets=assets,
        )
        self._logger.info(f"[{day:%Y-%m-%d}] Registered catalogue item with {len(assets)} assets")

        tmp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(
                    mode="w", suffix=f".{catalogue_item_format}", encoding="utf-8", delete=False
            ) as tmp_file:
                tmp_file.write(catalogue_item)
                tmp_file_path = Path(tmp_file.name)

            self._save_to_storage(
                file_to_save=tmp_file_path,
                remote_path=f"{self._dataset}/{self._get_file_parent_dir(day)}.{catalogue_item_format}",
            )

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
            http_error_content = json.loads(http_error.response.content.decode())
            if (
                http_error.response.status_code == 400
                and "None of the data you have requested is available yet"
                in http_error_content.get("detail", "")
            ):
                raise CDSWorkerDataNotAvailableYet(f"Requested data not available yet") from http_error
            raise

        return Path(downloaded_file.name)

    def _download_from_api(self, day: date, product_type: str, data_format: str) -> Optional[Path]:
        """Download one product for a given day from CDS API."""
        self._logger.info(f"[{day:%Y-%m-%d}] Downloading {product_type}.{data_format}")
        file_path = self._call_cdsapi(
            request=self._prepare_cdsapi_call_dict(day, product_type, data_format)
        )
        self._logger.info(f"[{day:%Y-%m-%d}] Downloaded {product_type}.{data_format} into {file_path.name}")
        return file_path

    def _save_to_storage(self, file_to_save: Path, remote_path: str) -> None:
        """Upload file into remote storage."""
        self._storage.upload(remote_file_path=remote_path, local_file_path=file_to_save)
        self._logger.info(f"Saved {file_to_save.name} to storage as {remote_path}")
