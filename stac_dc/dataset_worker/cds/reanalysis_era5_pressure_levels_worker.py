import json
import logging
import requests

from pathlib import Path

from stac_dc.dataset_worker.cds import ERA5Worker

from env import env as env


class ReanalysisERA5PressureLevelsWorker(ERA5Worker):
    def __init__(
            self,
            logger=logging.getLogger(env.get_app__name()),
            **kwargs
    ):
        stac_template_path: Path = (
                Path(__file__).resolve().parent / "stac_templates" / "[feature]reanalysis-era5-pressure-levels.json"
        )
        self._product_types = ['reanalysis', 'ensemble_members', 'ensemble_mean', 'ensemble_spread']
        self._variables = [
            'divergence',
            'fraction_of_cloud_cover',
            'geopotential',
            'ozone_mass_mixing_ratio',
            'potential_vorticity',
            'relative_humidity',
            'specific_cloud_ice_water_content',
            'specific_cloud_liquid_water_content',
            'specific_humidity',
            'specific_rain_water_content',
            'specific_snow_water_content',
            'temperature',
            'u_component_of_wind',
            'v_component_of_wind',
            'vertical_velocity',
            'vorticity',
        ]
        self._pressure_levels = [
            '1', '2', '3', '5', '7', '10', '20', '30', '50', '70',
            '100', '125', '150', '175', '200', '225', '250', '300', '350', '400',
            '450', '500', '550', '600', '650', '700', '750', '775', '800', '825',
            '850', '875', '900', '925', '950', '975', '1000',
        ]

        super().__init__(
            logger=logger,
            dataset="reanalysis-era5-pressure-levels",
            stac_template_path=stac_template_path,
            **kwargs
        )

    def _prepare_cdsapi_call_dict(self, day, product_type, data_format):
        return {
            'product_type': product_type,
            'variable': self._variables,
            'pressure_level': self._pressure_levels,
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
                "None of the data you have requested is available yet" in exception_content.get("detail", "")
        )
