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
    Loads data from a NETCDF file and stores it in the context for further
    processing. This class is designed to read space-time data, providing
    specific support for latitude, longitude, and time dimensions.

    This class is initialized with the necessary dataset configuration,
    such as the filename to load, and the identifiers for latitude, longitude,
    and time within the dataset.

    :ivar name: Key in the context to identify the space-time data.
    :type name: str
    :ivar filename: File path to the NETCDF dataset.
    :type filename: str
    :ivar latitude: Name of the latitude variable in the dataset.
    :type latitude: str
    :ivar longitude: Name of the longitude variable in the dataset.
    :type longitude: str
    :ivar time: Name of the time variable in the dataset.
    :type time: str
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
        """
        Run the process to load a NETCDF file and store the processed data in the given context.

        This method reads a NETCDF file using its filename, processes it with XArray, and then saves
        the processed data into the `space_time_data` attribute of the provided context.

        :param config: Configuration object containing settings and parameters for the operation.
        :type config: Configuration
        :param context: Context object where the processed space-time data will be stored after processing.
        :type context: Context
        :return: The updated context that includes the stored space-time data.
        :rtype: Context
        """
        if logger.level <= logging.INFO:
            logger.info("Loading NETCDF file: " + self.filename)

        ds = xr.open_dataset(self.filename)

        # show data available in the netcdf file
        if logger.level <= logging.DEBUG:
            logger.debug("Variables in dataset: " + ', '.join(ds.variables.keys()))

        context.space_time_data[self.name] = XArrayNetCDFData(ds, self.latitude, self.longitude, self.time)

        return context
