import logging

from abc import abstractmethod
from datetime import date

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
