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
        self._stac_template_path = None
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

    def _prepare_stac_feature_json(self, day: date, assets: list[dict]) -> str:
        with open(self._stac_template_path) as f:
            feature_dict = json.load(f)

        feature = feature_dict['features'][0]
        feature['id'] = self.get_id(day)
        feature['bbox'] = self._aoi.get_bbox()
        feature['geometry']['coordinates'] = self._aoi.get_polygon()
        feature['properties'].update({
            'start_datetime': f"{day}T00:00:00Z",
            'end_datetime': f"{day}T23:59:59Z",
            'datetime': f"{day}T00:00:00Z"
        })

        for asset in assets:
            url = f"{self.get_catalogue_download_host()}/{asset['href']}"
            key = f"{asset['product_type'].replace('_', '-')}-{asset['data_format']}"
            feature['assets'][key]['href'] = url

        feature['assets'] = {k: v for k, v in feature['assets'].items() if v.get('href')}

        return json.dumps(feature_dict, indent=2)