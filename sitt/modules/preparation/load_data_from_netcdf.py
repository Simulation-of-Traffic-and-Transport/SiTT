# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Load generic data from a netcdf file and save it to a retrievable structure.

Example YAML could be:
  - class: LoadDataFromNETCDF
    module: modules.preparation
    args:
      name: temperature
      filename: ./data/era5_data.nc
      latitude: latitude
      longitude: longitude
      time: time
      variables:
        # Full table: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation
        temperature:
        # 2 metre temperature in °K, converted into °C
        # 2m_temperature
        # https://codes.ecmwf.int/grib/param-db/167
          variable: t2m
          offset: -273.15
        # Convective rain rate in kg/(m²s)
        # convective_rain_rate
        # https://codes.ecmwf.int/grib/param-db/228218
        rainfall:
          variable: crr
        # Convective snowfall rate water equivalent in kg/(m²s)
        # convective_snowfall_rate_water_equivalent
        # https://codes.ecmwf.int/grib/param-db/228220
        snowfall:
          variable: csfr
        # Snow depth in m of water equivalent
        # snow_depth
        # https://codes.ecmwf.int/grib/param-db/141
        snow_depth:
          variable: sd
        # Precipitation type in integer type (0: no precipitation, 1: rain, 5: snow, ...)
        # precipitation_type
        # https://codes.ecmwf.int/grib/param-db/260015
        precipitation_type:
          variable: ptype
        # 10 metre V (northward) wind component in m/s
        # 10m_v_component_of_wind
        # https://codes.ecmwf.int/grib/param-db/166
        v10:
          variable: v10
        # 10 metre U (eastward) wind component in m/s
        # 10m_u_component_of_wind
        # https://codes.ecmwf.int/grib/param-db/165
        u10:
          variable: u10
        # K index (potential for a thunderstorm) in K (see table)
        # k_index
        # https://codes.ecmwf.int/grib/param-db/260121
        k_index:
          variable: kx

See information on this dataset: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels
and documentation: https://confluence.ecmwf.int/display/CKB/ERA5%3A+data+documentation
"""

import datetime as dt
import logging

from netCDF4 import Dataset

from sitt import Configuration, Context, PreparationInterface, SpaceTimeData

logger = logging.getLogger()


class LoadDataFromNETCDF(PreparationInterface):
    """
    Load generic data from a netcdf file and save it to a retrievable structure.
    """

    def __init__(self, name: str = 'temperature', filename: str = 'weather.nc', file_format: str = 'NETCDF4',
                 latitude: str = 'latitude', longitude: str = 'longitude', time: str = 'time',
                 start_date: dt.date | None = None, variables: dict[str, dict[str, any]] = {}):
        super().__init__()
        self.name: str = name
        """Key in context to find space time data again."""
        self.filename: str = filename
        """filename to load data from"""
        self.file_format: str = file_format
        """File format of nc file, default is NETCDF4"""
        self.latitude: str = latitude
        """Name of latitude in dataset"""
        self.longitude: str = longitude
        """Name of longitude in dataset"""
        self.time: str = time
        """Name of time in dataset"""
        self.start_date: dt.date | None = start_date
        """Start date different from global one."""
        self.variables: dict[str, dict[str, any]] = variables
        """Variables to map values on"""

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info("Loading NETCDF file: " + self.filename)

        dataset = Dataset(self.filename, 'r', format=self.file_format)

        # show data available in the netcdf file
        if logger.level <= logging.DEBUG:
            logger.debug("Variables in dataset: " + ', '.join(dataset.variables.keys()))

        # create space_time_data object
        context.space_time_data[self.name] = SpaceTimeData(dataset, self.variables, self.latitude, self.longitude,
                                                           self.time, self.start_date)

        return context
