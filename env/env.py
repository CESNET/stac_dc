import os, json

from dotenv import load_dotenv
from pathlib import Path

from .exceptions import *


class Env:
    _instance = None  # Singleton instance

    _app__project_root: Path = None

    _era5: dict = {}
    _landsat: dict = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Env, cls).__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self):
        self._load_app()
        self._load_era5()
        self._load_landsat()

    ##################################################################
    # app.env
    ##################################################################

    def _load_app(self):
        load_dotenv("app.env")
        self._app__name = os.getenv("APP__NAME", "STAC_DC")
        self._app__log_level = os.getenv("APP__LOG_LEVEL", "INFO").upper()

    def get_app__name(self) -> str:
        return self._app__name

    def get_app__log_level(self) -> str:
        return self._app__log_level

    def get_app__project_root(self) -> Path:
        if self._app__project_root is None:
            raise ProjectRootNotSet()
        return self._app__project_root

    def set_app__project_root(self, path: Path):
        self._app__project_root = path

    ##################################################################

    ##################################################################
    # era5.env
    ##################################################################

    def _load_era5(self):
        load_dotenv("era5.env")

        self._era5['datasets_aios'] = json.loads(os.getenv("ERA5__DATASET_AIO", "[]"))

        self._era5['s3_host'] = os.getenv("ERA5__S3_HOST", default=None)
        self._era5['s3_host_bucket'] = os.getenv("ERA5__S3_HOST_BUCKET", default=None)
        self._era5['s3_access_key'] = os.getenv("ERA5__S3_ACCESS_KEY", default=None)
        self._era5['s3_secret_key'] = os.getenv("ERA5__S3_SECRET_KEY", default=None)

        self._era5['stac_host'] = os.getenv("ERA5__STAC_HOST", default=None)
        self._era5['stac_username'] = os.getenv("ERA5__STAC_USERNAME", default=None)
        self._era5['stac_password'] = os.getenv("ERA5__STAC_PASSWORD", default=None)
        self._era5['stac_asset_download_root'] = os.getenv("ERA5__STAC_ASSET_DOWNLOAD_ROOT", default=None)

        self._era5['data_formats'] = [
            data_format.strip()
            for data_format in os.getenv("ERA5__DATA_FORMATS", default="grib").split(",") if data_format.strip()
        ]
        self._era5['redownload_threshold'] = int(os.getenv("ERA5__REDOWNLOAD_THRESHOLD", "13"))

    def get_era5(self):
        if not self._era5:
            raise ERA5NotLoaded()

        return self._era5

    ##################################################################

    ##################################################################
    # landsat.env
    ##################################################################

    def _load_landsat(self):
        load_dotenv("landsat.env")

        self._landsat['demanded_datasets'] = os.getenv("LANDSAT__DEMANDED_DATASETS", default=None)

        self._landsat['s3_host'] = os.getenv("LANDSAT__S3_HOST", default=None)
        self._landsat['s3_host_bucket'] = os.getenv("LANDSAT__S3_HOST_BUCKET", default=None)
        self._landsat['s3_access_key'] = os.getenv("LANDSAT__S3_ACCESS_KEY", default=None)
        self._landsat['s3_secret_key'] = os.getenv("LANDSAT__S3_SECRET_KEY", default=None)

        self._landsat['stac_host'] = os.getenv("LANDSAT__STAC_HOST", default=None)
        self._landsat['stac_username'] = os.getenv("LANDSAT__STAC_USERNAME", default=None)
        self._landsat['stac_password'] = os.getenv("LANDSAT__STAC_PASSWORD", default=None)
        self._landsat['stac_asset_download_root'] = os.getenv("LANDSAT__STAC_ASSET_DOWNLOAD_ROOT", default=None)

        self._landsat['m2m_api_url'] = os.getenv("LANDSAT__M2M_API_URL", default=None)
        self._landsat['m2m_username'] = os.getenv("LANDSAT__M2M_USERNAME", default=None)
        self._landsat['m2m_token'] = os.getenv("LANDSAT__M2M_TOKEN", default=None)
        self._landsat['m2m_scene_label'] = os.getenv("LANDSAT__M2M_SCENE_LABEL", default=None)

    def get_landsat(self):
        if not self._landsat:
            raise LandsatNotLoaded()

        return self._landsat

    ##################################################################

    def get_all_datasets_aios(self) -> list[list[str]]:
        datasets_aios = []

        datasets_aios.extend(self.get_era5()['datasets_aios'])
        # datasets_aios.extend(self.get_landsat()['datasets_aios'])

        return datasets_aios


env = Env()
