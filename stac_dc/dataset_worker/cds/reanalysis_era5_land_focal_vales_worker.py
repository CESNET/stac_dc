import json
import logging
import requests

from pathlib import Path

from stac_dc.dataset_worker.cds import ERA5Worker

from env import env as env


class ReanalysisERA5LandFocalValesWorker(ERA5Worker):
    def __init__(
            self,
            logger=logging.getLogger(env.get_app__name()),
            **kwargs
    ):
        stac_template_path: Path = (
                Path(__file__).resolve().parent / "stac_templates" / "[feature]reanalysis-era5-land.json"
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

        super().__init__(
            logger=logger,
            dataset="reanalysis-era5-land",
            catalogue_collection="focal-reanalysis-era5-land",
            stac_template_path=stac_template_path,
            **kwargs
        )

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

    def _check_dataset_not_available(self, cds_exception: requests.exceptions.HTTPError) -> bool:
        exception_content = json.loads(cds_exception.response.content.decode())

        return (
                cds_exception.response.status_code == 400
                and
                "None of the data you have requested is available yet" in exception_content.get("traceback", "")
        )
