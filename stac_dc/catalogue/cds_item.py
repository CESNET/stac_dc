from datetime import datetime
from typing import List

from . import CatalogueItem
from .asset import Asset
from stac_dc.dataset_worker.aoi import AOI


class CDSItem(CatalogueItem):
    def __init__(
            self,
            id: str,
            start_datetime: datetime,
            end_datetime: datetime,
            aoi: AOI,
            assets: List[Asset],
            dataset: str,
            collection: str,
    ) -> None:
        super().__init__(
            id=id,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            aoi=aoi,
            dataset=dataset,
            collection=collection,
            assets=assets,
            via_url=f"https://cds.climate.copernicus.eu/cdsapp#!/dataset/{dataset}"
        )
