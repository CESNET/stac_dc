import json
import logging
import tempfile

import cdsapi
import requests

from abc import ABC, abstractmethod
from datetime import date
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urljoin

from stac_dc.catalogue import Catalogue, CDSItem
from stac_dc.catalogue.asset import Asset
from stac_dc.dataset_worker.aoi import AOI
from stac_dc.dataset_worker.dataset_worker import DatasetWorker
from stac_dc.storage import Storage
from .exceptions import CDSWorkerDataNotAvailableYet


class CDSWorker(DatasetWorker, ABC):
    def __init__(
            self,
            dataset: str,
            catalogue_collection: str | None,
            aoi: AOI,
            storage: Storage,
            catalogue: Catalogue,
            logger: logging.Logger | None = None,
    ):
        super().__init__(
            dataset=dataset,
            catalogue_collection=catalogue_collection,
            aoi=aoi,
            storage=storage,
            catalogue=catalogue,
            logger=logger,
        )

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
        """Return host of the catalogue service (like download.stac.cesnet.cz)."""
        pass

    def _check_dataset_not_available(self, cds_exception: requests.exceptions.HTTPError) -> bool:
        exception_content = json.loads(cds_exception.response.content.decode())

        return (
                cds_exception.response.status_code == 400
                and
                (
                        "None of the data you have requested is available yet" in exception_content.get("traceback", "")
                        or
                        "None of the data you have requested is available yet" in exception_content.get("detail", "")
                )
        )

    # ------------------------
    # Helpers for file paths
    # ------------------------
    def _get_file_parent_dir(self, day: date) -> str:
        return f"{day:%Y/%m/%d}/{self._aoi.get_name()}"

    def _get_file_path(self, day: date, product_type: str, data_format: str) -> str:
        return f"{self._get_file_parent_dir(day)}/{product_type}.{data_format}"

    def get_id(self, day: date) -> str:
        return f"{self._catalogue_collection}_{day:%Y%m%d}_{self._aoi.get_name()}"

    # ------------------------
    # Main pipeline
    # ------------------------
    def run(self, **kwargs) -> None:
        """Main pipeline: download missing assets and register them into catalogue."""
        self._logger.debug("CDS pipeline started")

        days_to_download: List[Tuple[date, bool]] = self._get_days_to_download(
            redownload_threshold=self._get_redownload_threshold()
        )

        try:
            for day, force_redownload in days_to_download:
                self._logger.info(f"[{day:%Y-%m-%d}] Start processing")

                assets: List[Asset] = self._process_day(day, force_redownload)

                if assets:
                    catalogue_item = self._build_catalogue_item(day, assets)
                    self._register_catalogue_item(catalogue_item)
                    self._save_catalogue_item(day, catalogue_item)
                else:
                    self._logger.info(f"[{day:%Y-%m-%d}] Skipping catalogue item (no assets)")

                self._set_last_downloaded_day(day)
                self._logger.info(f"[{day:%Y-%m-%d}] Finished processing")

                self.reset_run_attempt()

        except CDSWorkerDataNotAvailableYet:
            self._logger.info("All downloaded, no more data available.")

    # ------------------------
    # Process one day
    # ------------------------
    def _process_day(self, day: date, force_redownload: bool) -> List[Asset]:
        """Download all required assets for one day and return their metadata."""
        assets: List[Asset] = []

        for product_type in self._product_types:
            for data_format in self._formats:
                storage_path = self._get_file_path(day, product_type, data_format)
                tmp_file: Optional[Path] = None

                try:
                    if not force_redownload and self._storage.exists(storage_path):
                        self._logger.info(f"[{day:%Y-%m-%d}] Already exists: {storage_path}")
                        assets.append(
                            self._make_asset(
                                product_type,
                                data_format,
                                self._storage.get_storage_full_path(storage_path)
                            )
                        )
                        continue

                    tmp_file = self._download_from_api(day, product_type, data_format)

                    if tmp_file:
                        self._save_to_storage(tmp_file, storage_path)
                        assets.append(
                            self._make_asset(
                                product_type,
                                data_format,
                                self._storage.get_storage_full_path(storage_path)
                            )
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

    # ------------------------
    # Helpers
    # ------------------------
    def _get_path_to_catalogue_file(self, day: date) -> str:
        return f"{self._get_file_parent_dir(day)}.json"

    def _make_asset(self, product_type: str, data_format: str, storage_path: str) -> Asset:
        product_types_map = {
            "reanalysis": "Reanalysis",
            "ensemble_members": "Ensemble members",
            "ensemble_mean": "Ensemble mean",
            "ensemble_spread": "Ensemble spread",
        }

        mimetypes_map = {
            "grib": "application/grib",
            "nc": "application/netcdf",
        }

        formats_name_map = {
            "grib": "GRIB",
            "nc": "NetCDF",
        }

        key = f"{product_type}-{data_format}"
        href = urljoin(self.get_catalogue_download_host(), storage_path)
        title = f"{product_types_map[product_type]} product type in {formats_name_map[data_format]} format"
        return Asset(key=key, href=href, type=mimetypes_map[data_format], title=title)

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
                raise CDSWorkerDataNotAvailableYet("Requested data not available yet")
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

    def _build_catalogue_item(self, day: date, assets: List[Asset]) -> CDSItem:
        return CDSItem(
            id=self.get_id(day),
            start_datetime=datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc),
            end_datetime=datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc),
            aoi=self._aoi,
            assets=assets,
            dataset=self._dataset,
            collection=self._catalogue_collection,
        )
