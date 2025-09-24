import logging

from stac_dc.dataset_worker.cds import ERA5Worker

from env import env


class ReanalysisERA5LandWorker(ERA5Worker):
    def __init__(
            self,
            logger=logging.getLogger(env.get_app__name()),
            **kwargs
    ):
        self._product_types = ['reanalysis']
        self._variables = [
            "2m_dewpoint_temperature",
            "2m_temperature",
            "skin_temperature",
            "soil_temperature_level_1",
            "soil_temperature_level_2",
            "soil_temperature_level_3",
            "soil_temperature_level_4",
            "lake_bottom_temperature",
            "lake_ice_depth",
            "lake_ice_temperature",
            "lake_mix_layer_depth",
            "lake_mix_layer_temperature",
            "lake_shape_factor",
            "lake_total_layer_temperature",
            "snow_albedo",
            "snow_cover",
            "snow_density",
            "snow_depth",
            "snow_depth_water_equivalent",
            "snowfall",
            "snowmelt",
            "temperature_of_snow_layer",
            "skin_reservoir_content",
            "volumetric_soil_water_layer_1",
            "volumetric_soil_water_layer_2",
            "volumetric_soil_water_layer_3",
            "volumetric_soil_water_layer_4",
            "forecast_albedo",
            "surface_latent_heat_flux",
            "surface_net_solar_radiation",
            "surface_net_thermal_radiation",
            "surface_sensible_heat_flux",
            "surface_solar_radiation_downwards",
            "surface_thermal_radiation_downwards",
            "evaporation_from_bare_soil",
            "evaporation_from_open_water_surfaces_excluding_oceans",
            "evaporation_from_the_top_of_canopy",
            "evaporation_from_vegetation_transpiration",
            "potential_evaporation",
            "runoff",
            "snow_evaporation",
            "sub_surface_runoff",
            "surface_runoff",
            "total_evaporation",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "surface_pressure",
            "total_precipitation",
            "leaf_area_index_high_vegetation",
            "leaf_area_index_low_vegetation"
        ]

        super().__init__(
            logger=logger,
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
