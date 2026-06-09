# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Spatio-temporal classes to make working with spatial and temporal data easier"""

import abc
import datetime as dt
import logging

import numpy as np
import xarray as xr

logger = logging.getLogger()

__all__ = [
    "SpatioTemporalInterface",
    "XArrayNetCDFData",
]


class SpatioTemporalInterface(abc.ABC):
    """
    Interface for handling spatiotemporal data.

    This abstract base class provides a blueprint for working with specific
    data associated with geographic locations and timestamps. It defines methods
    for retrieving general data or specific fields of data, ensuring consistent
    access patterns for derived implementations.
    """

    @abc.abstractmethod
    def get(self, lat: float, lon: float, date: dt.datetime) -> any:
        """
        This abstract method is designed to retrieve data based on the provided geographic
        coordinates and a specific date. Implementations of this method should define how
        the data is fetched and returned. The method is expected to process the input
        parameters to generate the desired output.

        :param lat: Latitude of the location to retrieve data for.
        :type lat: float
        :param lon: Longitude of the location to retrieve data for.
        :type lon: float
        :param date: Date for which data should be retrieved.
        :type date: datetime.datetime
        :return: Data corresponding to the provided geographic location and date.
        :rtype: Any
        """
        pass

    def get_field(self, lat: float, lon: float, date: dt.datetime, field: str) -> any:
        """
        Retrieve a specific field value from the data for a given latitude, longitude, and date.

        This method extracts the value of the specified field from the data corresponding
        to the provided geographical and temporal parameters.

        :param lat: Latitude coordinate of the desired location.
        :type lat: float
        :param lon: Longitude coordinate of the desired location.
        :type lon: float
        :param date: Date and time for which the field value is desired.
        :type date: dt.datetime
        :param field: The name of the field to be retrieved from the data.
        :type field: str
        :return: The value of the specified field for the provided location and date.
        :rtype: any
        """
        return self.get(lat, lon, date)[field]


class XArrayNetCDFData(SpatioTemporalInterface):
    """
    Representation of spatial and temporal data encapsulated within an xarray Dataset.

    This class is designed for handling NetCDF data stored in xarray Datasets, providing functionalities
    for retrieving geospatial and temporal data based on specified latitude, longitude, and time inputs.
    It features caching mechanisms for frequently accessed data and rounding to the nearest grid cell
    for improved performance and precision.

    :ivar data: The xarray Dataset containing NetCDF data.
    :type data: xr.Dataset
    :ivar latitude: The name of the latitude coordinate in the Dataset.
    :type latitude: str
    :ivar longitude: The name of the longitude coordinate in the Dataset.
    :type longitude: str
    :ivar time: The name of the time coordinate in the Dataset.
    :type time: str
    """

    def __init__(self, data: xr.Dataset, latitude: str = 'latitude', longitude: str = 'longitude', time: str = 'time'):
        self.data: xr.Dataset = data
        """Data from xarray netcdf file."""
        self.latitude: str = latitude
        """Latitude variable name."""
        self.longitude: str = longitude
        """Longitude variable name."""
        self.time: str = time
        """Time variable name."""

        # calculate deltas
        self.lat_delta = np.abs(self.data.coords[self.latitude].values[1] - self.data.coords[self.latitude].values[0])
        self.lon_delta = np.abs(self.data.coords[self.longitude].values[1] - self.data.coords[self.longitude].values[0])

        # set up cache
        self._last_day: None | int = None
        self._cache: dict[tuple[dt.datetime, np.float64, np.float64], any] = {}

    def get(self, lat: float, lon: float, date: dt.datetime) -> any:
        """
        Retrieve data for a specific latitude, longitude, and datetime. Adjusts values
        to the nearest grid cell and hourly timestamp. Caches the result to optimize
        repeated requests for the same key within the same day.

        :param lat: Latitude in decimal degrees. Positive values for North,
                    negative for South.
        :type lat: float
        :param lon: Longitude in decimal degrees. Positive values for East,
                    negative for West.
        :type lon: float
        :param date: Datetime object representing the desired timestamp. The
                     minute, second, and microsecond components are truncated
                     to the nearest hour.
        :type date: datetime.datetime
        :return: Retrieved data corresponding to the adjusted latitude,
                 longitude, and datetime. Returns None if no data is found.
        :rtype: any
        """
        # round to nearest grid cell
        lat_adj = np.round(lat / self.lat_delta) * self.lat_delta
        lon_adj = np.round(lon / self.lon_delta) * self.lon_delta
        date_adj = date.replace(minute=0, second=0, microsecond=0)

        # reset cache if day has changed
        if self._last_day != date.day:
            self._cache = {}
            self._last_day = date.day

        # check cache
        key = (date_adj, lat_adj, lon_adj)
        if key in self._cache:
            return self._cache[key]

        # get data from file
        try:
            vals = self.data.sel({self.latitude: lat_adj, self.longitude: lon_adj, self.time: date_adj})
        except KeyError:
            # logging.getLogger().error(f"No data found for lat={lat_adj}, lon={lon_adj}, time={date_adj}")
            vals = None

        # set cache
        self._cache[key] = vals

        return vals
