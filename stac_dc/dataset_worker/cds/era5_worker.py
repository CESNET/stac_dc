import logging

from abc import abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple

from env import env as env
from stac_dc.catalogue import STAC
from stac_dc.dataset_worker.aoi import AOI
from stac_dc.dataset_worker.cds import CDSWorker
from stac_dc.storage import S3


class ERA5Worker(CDSWorker):
    def __init__(
            self,
            dataset: str,
            aoi: AOI,
            catalogue_collection: str | None = None,
            logger: logging.Logger | None = None,
    ):
        collection = catalogue_collection or dataset

        storage = S3(
            s3_host=env.get_era5()["s3_host"],
            access_key=env.get_era5()["s3_access_key"],
            secret_key=env.get_era5()["s3_secret_key"],
            host_bucket=env.get_era5()["s3_host_bucket"],
            collection=collection,
        )

        catalogue = STAC(
            stac_host=env.get_era5()["stac_host"],
            username=env.get_era5()["stac_username"],
            password=env.get_era5()["stac_password"],
            collection=collection,
        )

        super().__init__(
            dataset=dataset,
            catalogue_collection=collection,
            aoi=aoi,
            storage=storage,
            catalogue=catalogue,
            logger=logger,
        )

        self._formats = env.get_era5()["data_formats"]

    def _get_redownload_threshold(self) -> int:
        return env.get_era5()["redownload_threshold"]

    def get_catalogue_download_host(self) -> str:
        return env.get_era5()["stac_asset_download_root"]

    @abstractmethod
    def _prepare_cdsapi_call_dict(self, day: date, product_type: str, data_format: str) -> dict:
        pass

    def _get_days_to_download(
            self,
            redownload_threshold: int,  # days
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
        redownload_anchor = today - timedelta(days=redownload_threshold)

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
