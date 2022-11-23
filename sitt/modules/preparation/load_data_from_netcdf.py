# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Load generic data from a netcdf file and save it to a retrievable structure."""

import logging

from netCDF4 import Dataset

from sitt import Configuration, Context, PreparationInterface, SpaceTimeData
import datetime as dt
logger = logging.getLogger()


class LoadDataFromNETCDF(PreparationInterface):
    """Load generic data from a netcdf file and save it to a retrievable structure."""

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

        context.space_time_data[self.name] = SpaceTimeData(Dataset(self.filename, 'r', format=self.file_format),
                                                           self.variables, self.latitude, self.longitude, self.time,
                                                           self.start_date)

        return context
