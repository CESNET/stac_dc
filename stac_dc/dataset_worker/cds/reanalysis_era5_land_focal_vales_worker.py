import logging

from env import env as env
from stac_dc.dataset_worker.aoi import AOI
from stac_dc.dataset_worker.cds import ERA5Worker


class ReanalysisERA5LandFocalValesWorker(ERA5Worker):
    def __init__(
            self,
            aoi: AOI,
            logger: logging.Logger | None = None,
    ):
        super().__init__(
            dataset="reanalysis-era5-land",
            catalogue_collection="focal-reanalysis-era5-land",
            aoi=aoi,
            logger=logger,
        )

        self._product_types = ['reanalysis']

        self._variables = [
            "2m_dewpoint_temperature",
            "2m_temperature",
            "volumetric_soil_water_layer_1",
            "volumetric_soil_water_layer_2",
            "volumetric_soil_water_layer_3",
            "volumetric_soil_water_layer_4",
            "surface_solar_radiation_downwards",
            "surface_thermal_radiation_downwards",
            "runoff",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "total_precipitation",
            "total_evaporation"
        ]

        self._available_hours = [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ]

    def _prepare_cdsapi_call_dict(self, day, product_type, data_format):
        return {
            'variable': self._variables,
            'year': day.year,
            'month': day.month,
            'day': day.day,
            'time': self._available_hours,
            'data_format': data_format,
            'download_format': 'unarchived',
            'area': [
                self._aoi.get_bbox()[2],  # North
                self._aoi.get_bbox()[1],  # West
                self._aoi.get_bbox()[0],  # South
                self._aoi.get_bbox()[3],  # East
            ],
        }
