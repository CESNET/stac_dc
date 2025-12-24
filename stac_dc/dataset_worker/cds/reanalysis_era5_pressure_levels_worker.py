import logging

from stac_dc.dataset_worker.aoi import AOI
from stac_dc.dataset_worker.cds import ERA5Worker


class ReanalysisERA5PressureLevelsWorker(ERA5Worker):
    def __init__(self, aoi: AOI, logger: logging.Logger | None = None):
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

        super().__init__(
            dataset="reanalysis-era5-pressure-levels",
            aoi=aoi,
            logger=logger,
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
                self._aoi.get_bbox()[2],
                self._aoi.get_bbox()[1],
                self._aoi.get_bbox()[0],
                self._aoi.get_bbox()[3],
            ],
        }
