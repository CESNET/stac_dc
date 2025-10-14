import json
import logging
import tempfile

from abc import abstractmethod
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

from stac_dc.dataset_worker.cds import CDSWorker

from stac_dc.storage import S3
from stac_dc.catalogue import STAC

from env import env


class ERA5Worker(CDSWorker):
    def __init__(
            self,
            logger=logging.getLogger(env.get_app__name()),
            **kwargs,
    ):
        self._available_hours = [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ]

        super().__init__(
            logger=logger,
            formats=env.get_era5()["data_formats"],
            storage=S3(
                s3_host=env.get_era5()["s3_host"],
                access_key=env.get_era5()["s3_access_key"],
                secret_key=env.get_era5()["s3_secret_key"],
                host_bucket=env.get_era5()["s3_host_bucket"],
            ),
            catalogue=STAC(
                username=env.get_era5()["stac_username"],
                password=env.get_era5()["stac_password"],
                stac_host=env.get_era5()["stac_host"],
            ),
            **kwargs,
        )

    def _get_redownload_threshold(self) -> int:
        return env.get_era5()["redownload_threshold"]

    def get_catalogue_download_host(self) -> str:
        return env.get_era5()["stac_asset_download_root"]

    @abstractmethod
    def _prepare_cdsapi_call_dict(self, day: date, product_type: str, data_format: str) -> dict:
        pass

    def _get_days_to_download(
            self,
            redownload_threshold: int,  # weeks
            recent_days: int = 10,  # days
            threshold_window: int = 2  # days
    ) -> List[Tuple[date, bool]]:
        today = datetime.now(timezone.utc).date()
        last_downloaded = self._get_last_downloaded_day() or today

        def daterange(start: date, end: date):
            while start <= end:
                yield start
                start += timedelta(days=1)

        # intervals
        gap_days = max(0, (today - last_downloaded).days)
        redownload_anchor = today - timedelta(weeks=redownload_threshold)

        intervals = [
            # redownload: Force download == True
            (
                redownload_anchor - timedelta(days=gap_days + threshold_window),
                redownload_anchor + timedelta(days=threshold_window),
                True,
            ),
            # middle: between last_downloaded and recent_start, Force download == False
            (
                last_downloaded + timedelta(days=1),
                today - timedelta(days=recent_days),
                False,
            ),
            # recent: last n recent_days, Force == True
            (
                today - timedelta(days=recent_days - 1),
                today,
                True,
            ),
        ]

        days_map: dict[date, bool] = {}
        for start, end, force in intervals:
            for d in daterange(start, end):
                if d <= today:
                    days_map[d] = False if env.get_era5()["recatalogize_only"] else (days_map.get(d, False) or force)

        days_list: List[Tuple[date, bool]] = sorted(days_map.items())
        return days_list

    def _get_last_downloaded_day(self) -> date:
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

    def _set_last_downloaded_day(self, last_downloaded_day: date) -> None:
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
