import logging
from datetime import date
from typing import Dict

from stac_dc.dataset_worker.dataset_worker import DatasetWorker
from env import env


class LandsatWorker(DatasetWorker):
    def __init__(self, logger=logging.getLogger(env.get_app__name()), **kwargs):
        super().__init__(dataset="landsat", logger=logger, **kwargs)
        self._m2m_api_connector = USGSM2MConnector(logger=logger)

    def get_id(self, day: date) -> str:
        raise NotImplementedError()

    def get_catalogue_download_host(self) -> str:
        return env.get_landsat()["stac_asset_download_root"]

    def run(self, **kwargs):
        self._logger.debug(f"{self._dataset_name} pipeline started")

        days_to_download = self._get_days_to_download(
            redownload_threshold=env.get_landsat()["redownload_threshold"]
        )

        for day, force in days_to_download:
            self._logger.info(f"[{day}] Start {self._dataset_name} processing")

            assets = self._process_day_assets(day, force)

            if assets:
                self._register_catalogue_item(day, assets)

            self._set_last_downloaded_day(day)
            self._logger.info(f"[{day}] Finished {self._dataset_name} processing")

    def _process_day_assets(self, day: date, force_redownload: bool) -> list[Dict]:
        assets = []
        downloadable_files = self._m2m_api_connector.get_downloadable_files(
            dataset=self._dataset_name,
            geojson=self._aoi.to_geojson(),
            time_start=day,
            time_end=day,
        )
        for file_attributes in downloadable_files:
            asset = self._download_and_save(file_attributes)
            if asset:
                assets.append(asset)
        return assets

    def _download_and_save(self, file_attributes: Dict) -> Dict:
        """
        Stáhne soubor a uloží ho do storage (např. S3).
        Vrací asset dict připravený pro STAC.
        """
        url = file_attributes["url"]
        display_id = file_attributes["displayId"]
        filename = f"{display_id}.tar"

        tmp_path = self._storage.get_tmp_path(self._dataset, filename)
        self._logger.info(f"Downloading {url} → {tmp_path}")
        self._m2m_api_connector.download_file(url, tmp_path)

        # nahrát na storage (např. S3)
        asset_url = self._storage.upload(tmp_path, dataset=self._dataset)

        return {
            "href": asset_url,
            "title": display_id,
            "type": "application/x-tar",
            "roles": ["data"],
            "file:metadata": file_attributes,
        }
