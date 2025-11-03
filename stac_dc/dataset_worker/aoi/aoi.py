# AOI as Area of Interest

from abc import ABC
from typing import Any, Dict

from shapely.geometry import box

class AOI(ABC):
    _name: str = ""
    _bbox_coordinates: list[float] = []
    _polygon_coordinates: list[list[float]] = []

    @classmethod
    def get_name(cls) -> str:
        return cls._name

    @classmethod
    def get_bbox(cls) -> list[float]:
        return cls._bbox_coordinates

    @classmethod
    def get_polygon(cls) -> list[list[list]]:
        bbox_polygon = box(cls.get_bbox()[1], cls.get_bbox()[0], cls.get_bbox()[3], cls.get_bbox()[2])
        return [[list(coord) for coord in bbox_polygon.exterior.coords]]

    @classmethod
    def get_geojson_polygon(cls) -> Dict[str, Any]:
        polygon = {
            "type": "Polygon",
            "coordinates": cls.get_polygon(),
        }

        return polygon
