import os
import logging

from datetime import date, datetime
from typing import List, Tuple

from .usgs_m2m_connector import USGSM2MConnector


class USGSWorker:
    def __init__(self, dataset: str, aoi, start: datetime, end: datetime, **kwargs):
        self.dataset = dataset
        self.aoi = aoi
        self.start = start
        self.end = end
        self.kwargs = kwargs

        self.connector = USGSM2MConnector()
        self.logger = logging.getLogger(self.__class__.__name__)

    def _get_days_to_download(self, redownload_threshold: int) -> List[Tuple[date, bool]]:
        pass
        #TODO start from here!

    def search(self):
        return self.connector.get_downloadable_files(
            dataset=self.dataset,
            geojson=self.aoi.to_geojson(),
            time_start=self.start,
            time_end=self.end,
        )

    def download(self, output_dir: str):
        results = self.search()
        downloaded = []
        for item in results:
            filename = f"{item['displayId']}.tar"
            output_path = os.path.join(output_dir, filename)
            self.connector.download_file(item["url"], output_path)
            downloaded.append({**item, "path": output_path})
        return downloaded
