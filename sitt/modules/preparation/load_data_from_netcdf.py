# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Load generic data from a netcdf file and save it to a retrievable structure.
"""

import datetime as dt
import logging

import xarray as xr

from sitt import Configuration, Context, PreparationInterface, XArrayNetCDFData

logger = logging.getLogger()


class LoadDataFromNETCDF(PreparationInterface):
    """
    Load generic data from a netcdf file and save it to a retrievable structure.
    """

    def __init__(self, name: str = 'temperature', filename: str = 'sitt_data.nc',
                 latitude: str = 'latitude', longitude: str = 'longitude', time: str = 'time'):
        super().__init__()
        self.name: str = name
        """Key in context to find space time data again."""
        self.filename: str = filename
        """filename to load data from"""
        self.latitude: str = latitude
        """Name of latitude in dataset"""
        self.longitude: str = longitude
        """Name of longitude in dataset"""
        self.time: str = time
        """Name of time in dataset"""


    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info("Loading NETCDF file: " + self.filename)

        ds = xr.open_dataset(self.filename)

        # show data available in the netcdf file
        if logger.level <= logging.DEBUG:
            logger.debug("Variables in dataset: " + ', '.join(ds.variables.keys()))

        context.space_time_data[self.name] = XArrayNetCDFData(ds, self.latitude, self.longitude, self.time)

        return context
