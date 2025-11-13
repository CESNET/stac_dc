import json
import logging

from datetime import date, datetime, timedelta, timezone
from tempfile import NamedTemporaryFile
from typing import Any, List, Tuple

from . import USGSWorker

from stac_dc.storage import S3
from stac_dc.storage.exceptions import *

from stac_dc.catalogue import STAC

from .landsat_processor import LandsatProcessor

from env import env


class LandsatWorker(USGSWorker):
    def __init__(self, logger=logging.getLogger(env.get_app__name()), **kwargs):
        super().__init__(
            logger=logger,
            storage=S3(
                s3_host=env.get_landsat()["s3_host"],
                access_key=env.get_landsat()["s3_access_key"],
                secret_key=env.get_landsat()["s3_secret_key"],
                host_bucket=env.get_landsat()["s3_host_bucket"],
            ),
            catalogue=STAC(
                username=env.get_landsat()["stac_username"],
                password=env.get_landsat()["stac_password"],
                stac_host=env.get_landsat()["stac_host"],
            ),
            **kwargs
        )

        self._items_missing_usgs_stac_filename: str = f"{self.get_dataset()}/items_missing_usgs_stac.json"

    def get_catalogue_download_host(self) -> str:
        return env.get_landsat()["stac_asset_download_root"]

    def _get_days_to_download(self, *args: Any, **kwargs: Any) -> List[Tuple[date, bool]]:
        today = datetime.now(timezone.utc).date()
        redownload_threshold = env.get_landsat().get("redownload_threshold")

        last_downloaded_day = self._get_last_downloaded_day()
        if last_downloaded_day:
            from_day = max(last_downloaded_day, today) - timedelta(days=redownload_threshold)
        else:
            from_day = today - timedelta(days=redownload_threshold)

        if from_day > today:
            from_day = today

        days_to_download: List[Tuple[date, bool]] = []
        for i in range((today - from_day).days + 1):  # +1 (today inclusive)
            day = from_day + timedelta(days=i)
            days_to_download.append((day, False))

        return days_to_download

    def _get_items_missing_usgs_stac(self) -> List[str]:
        data: List[str] = []

        with self._storage.locked(self._items_missing_usgs_stac_filename):
            try:
                with NamedTemporaryFile(mode='w+b', suffix='.json', delete=False) as tmp_file:
                    try:
                        self._storage.download(remote_file_path=self._items_missing_usgs_stac_filename,
                                               local_file_path=tmp_file.name)
                        tmp_file.seek(0)
                        data = json.load(tmp_file)

                        if not isinstance(data, list):
                            raise ValueError(
                                f"File {self._items_missing_usgs_stac_filename} does not contain valid list!")

                    except FileNotFoundError:
                        pass

            finally:
                tmp_file.close()
                Path(tmp_file.name).unlink(missing_ok=True)

        self._logger.info(
            f"Missing USGS generated STAC for {len(data)} files."
        )

        return data

    def _save_item_missing_usgs_stac(self, display_id: str):
        data: List[str] = []

        with self._storage.locked(self._items_missing_usgs_stac_filename):
            try:
                with NamedTemporaryFile(mode='w+b', suffix='.json', delete=False) as tmp_file:
                    try:
                        self._storage.download(
                            remote_file_path=self._items_missing_usgs_stac_filename,
                            local_file_path=tmp_file.name
                        )
                        tmp_file.seek(0)
                        data = json.load(tmp_file)

                        if not isinstance(data, list):
                            raise ValueError(
                                f"File {self._items_missing_usgs_stac_filename} does not contain valid list!"
                            )

                    except FileNotFoundError:
                        data = []

            finally:
                tmp_file.close()
                Path(tmp_file.name).unlink(missing_ok=True)

        if display_id not in data:
            data.append(display_id)

        try:
            with NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as tmp_file:
                json.dump(data, tmp_file)
                tmp_file.flush()

                with self._storage.locked(self._items_missing_usgs_stac_filename):
                    self._storage.upload(
                        local_file_path=tmp_file.name,
                        remote_file_path=self._items_missing_usgs_stac_filename
                    )

        finally:
            tmp_file.close()
            Path(tmp_file.name).unlink(missing_ok=True)

        self._logger.info(f"Added {display_id} to missing USGS STAC list ({len(data)} total).")

    def run(self, **kwargs):
        self._logger.debug(f"{self._dataset} pipeline started")

        days_to_download: List[Tuple[date, bool]] = self._get_days_to_download(
            redownload_threshold=env.get_landsat()["redownload_threshold"]
        )

        items_missing_usgs_stac: List[str] = self._get_items_missing_usgs_stac()

        for day_to_download in days_to_download:
            day, force_redownload = day_to_download

            self._logger.info(f"[{day:%Y-%m-%d}] Start processing")

            self._process_day(day, force_redownload)

            self._set_last_downloaded_day(day)

            self._logger.info(f"[{day:%Y-%m-%d}] Finished processing")

            self.reset_run_attempt()

        for item_missing_usgs_stac in items_missing_usgs_stac:
            self._logger.info(f"[{item_missing_usgs_stac}] Started processing]")

            self._process_item(item_missing_usgs_stac)

        self._logger.info("All downloaded, no more data available.")

    def _process_landsat_tar(self, landsat_tar_path: Path):
        landsat_processor = LandsatProcessor(
            dataset=self.get_dataset(),
            landsat_tar_path=landsat_tar_path,
        )

        path_to_stac_file, pregenerated_stac = landsat_processor.process_landsat_tar()

        if not pregenerated_stac:
            self._save_item_missing_usgs_stac(display_id=landsat_tar_path.stem)

        with open(path_to_stac_file, "r") as stac_file:
            json_data = json.load(stac_file)
            self._catalogue.register_item(dataset=self._dataset, json_data=json_data)

        self._save_to_storage(
            file_to_save=path_to_stac_file,
            remote_path=f"{self.get_dataset()}/{path_to_stac_file.name}"
        )

        self._save_to_storage(
            file_to_save=landsat_tar_path,
            remote_path=f"{self.get_dataset()}/{landsat_tar_path.name}"
        )
