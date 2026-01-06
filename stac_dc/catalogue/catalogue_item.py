from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any

from stac_dc.dataset_worker.aoi import AOI
from .asset import Asset

@dataclass
class CatalogueItem:
    id: str
    start_datetime: datetime
    end_datetime: datetime
    aoi: AOI
    dataset: str
    collection: str
    assets: List[Asset] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    via_url: str | None = None

    def to_stac(self) -> Dict[str, Any]:
        links = []
        if self.via_url:
            links.append({"href": self.via_url, "rel": "via"})

        return {
            "type": "Feature",
            "id": self.id,
            "geometry": self.aoi.get_geojson_polygon(),
            "bbox": self.aoi.get_bbox(),
            "properties": {
                "start_datetime": self.start_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_datetime": self.end_datetime.strftime("%Y-%m-%dT%H:%M:%SZ"),
                **self.properties
            },
            "assets": {
                a.key: {
                    "href": a.href,
                    "type": a.type,
                    "title": a.title,
                } for a in self.assets
            },
            #"collection": self.collection,
            "links": links
        }
