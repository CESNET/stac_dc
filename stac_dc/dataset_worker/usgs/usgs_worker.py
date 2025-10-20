import os
import logging

from abc import ABC
from datetime import date, datetime, time
from typing import Dict, List

from .usgs_m2m_connector import USGSM2MConnector
from .. import DatasetWorker


class USGSWorker(DatasetWorker, ABC):
    def __init__(self, *args, **kwargs):
        self._m2m_api_connector = USGSM2MConnector()

        super().__init__(**kwargs)

    def search_by_daterange(self, start: date, end: date) -> List[Dict]:
        start_datetime = datetime.combine(start, time.min)  # 00:00:00
        end_datetime = datetime.combine(end, time.max)  # 23:59:59.999999

        return self.connector.get_downloadable_files(
            dataset=self.dataset,
            geojson=self.aoi.to_geojson(),
            time_start=start_datetime,
            time_end=end_datetime,
        )

    def download(self, output_dir: str):
        results = self.search_by_daterange()
        downloaded = []
        for item in results:
            filename = f"{item['displayId']}.tar"
            output_path = os.path.join(output_dir, filename)
            self.connector.download_file(item["url"], output_path)
            downloaded.append({**item, "path": output_path})
        return downloaded
