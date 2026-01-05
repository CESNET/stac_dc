from .aoi import AOI
from .czech_republic_aoi import CzechRepublicAOI
from .finland_aoi import FinlandAOI
from .spain_aoi import SpainAOI

aois_map: dict[str, type[AOI]] = {
    "czech-republic": CzechRepublicAOI,
    "finland": FinlandAOI,
    "spain": SpainAOI,
}
