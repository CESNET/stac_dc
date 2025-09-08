from .dataset_worker import DatasetWorker

from .cds import CDSWorker
from .cds import ReanalysisERA5SingleLevelsWorker
from .cds import ReanalysisERA5PressureLevelsWorker
from .cds import ReanalysisERA5LandWorker

workers_map: dict[str, type[DatasetWorker]] = {
    "reanalysis-era5-single-levels": ReanalysisERA5SingleLevelsWorker,
    "reanalysis-era5-pressure-levels": ReanalysisERA5PressureLevelsWorker,
    "reanalysis-era5-land": ReanalysisERA5LandWorker,
}
