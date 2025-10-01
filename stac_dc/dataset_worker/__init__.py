from .dataset_worker import DatasetWorker

from .cds import CDSWorker
from .cds import ReanalysisERA5SingleLevelsWorker
from .cds import ReanalysisERA5PressureLevelsWorker
from .cds import ReanalysisERA5LandWorker
#from .usgs import LandsatOTC2L1Worker
#from .usgs import LandsatOTC2L2Worker

workers_map: dict[str, type[DatasetWorker]] = {
    "reanalysis-era5-single-levels": ReanalysisERA5SingleLevelsWorker,
    "reanalysis-era5-pressure-levels": ReanalysisERA5PressureLevelsWorker,
    "reanalysis-era5-land": ReanalysisERA5LandWorker,
#    "landsat_ot_c2_l1": LandsatOTC2L1Worker,
#    "landsat_ot_c2_l2": LandsatOTC2L2Worker
}
