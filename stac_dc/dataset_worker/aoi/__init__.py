from .aoi import AOI
from .czech_republic_aoi import CzechRepublicAOI

aois_map: dict[str, type[AOI]] = {
    "czech_republic": CzechRepublicAOI
}
