import logging
import os
import tempfile

from abc import ABC, abstractmethod
from datetime import date, datetime, time
from pathlib import Path
from typing import Dict, List, Tuple

from .usgs_m2m_connector import USGSM2MConnector
from .. import DatasetWorker


class USGSWorker(DatasetWorker, ABC):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)

        self._m2m_api_connector = USGSM2MConnector(dataset=self.get_dataset())

    @abstractmethod
    def _process_landsat_tar(self, path_to_tar: Path):
        pass

    def _process_day(self, day: date, force_redownload: bool):
        downloadable_files_days = self.search_by_daterange(start=day, end=day)

        for file_attributes in downloadable_files_days:
            with tempfile.TemporaryDirectory() as tmpdirname:
                downloaded_file_path: Path = self.download(
                    display_id=file_attributes["displayId"],
                    download_url=file_attributes["url"],
                    output_dir=tmpdirname,
                    force_redownload=force_redownload
                )

                if downloaded_file_path is None:
                    continue

                self._process_landsat_tar(downloaded_file_path)

    def search_by_daterange(self, start: date, end: date) -> List[Dict]:
        start_datetime = datetime.combine(start, time.min)  # 00:00:00
        end_datetime = datetime.combine(end, time.max)  # 23:59:59.999999

        return self._m2m_api_connector.get_files_by_date_range(
            geojson=self._aoi.get_geojson_polygon(),
            time_start=start_datetime,
            time_end=end_datetime,
        )

    def search_by_id(self, id: str) -> List[Dict]:
        pass

    def download(self, download_url: str, output_dir: str, display_id: str = "", force_redownload=False) -> Path | None:
        if not force_redownload:
            usgs_filesize = self._m2m_api_connector.get_file_size(download_url=download_url)
            if self._storage.exists(
                    remote_file_path=f"{self.get_dataset()}/{display_id}.tar",
                    expected_length=usgs_filesize
            ):
                self._logger.info(
                    f"Already downloaded filesize of product {display_id} matches remote filesize ({usgs_filesize} B), "
                    f"force_redownload={force_redownload} -> skipping."
                )
                return None

        output_file_path, proper_filename = self._m2m_api_connector.download_file(
            download_url=download_url,
            output_dir=output_dir
        )

        if not proper_filename:
            if display_id != "":
                output_file_path.rename(output_file_path.with_name(f"{display_id}.tar"))

        return output_file_path
