# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Spatio temporal classes"""
import abc
import datetime as dt
import logging

import netCDF4 as nc
import numpy as np

logger = logging.getLogger()


__all__ = [
    "SpatioTemporalInterface",
    "SpaceTimeData",
    "SpaceData"
]

class SpatioTemporalInterface(abc.ABC):
    def __init__(self, data: nc.Dataset, variables: dict[str, dict[str, any]], latitude: str = 'latitude',
                 longitude: str = 'longitude'):
        # """Variables to map values on"""

        # create aggregated data
        self.lat: np.ma.core.MaskedArray = data.variables[latitude][:]
        """latitude array"""
        self.lon: np.ma.core.MaskedArray = data.variables[longitude][:]
        """longitude array"""

        # add variables
        self.variables: dict[str, nc.Variable] = {}
        self.offsets: dict[str, float] = {}
        for key in variables:
            var_name = key
            if 'variable' in variables[key]:
                var_name = variables[key]['variable']
            if var_name in data.variables:
                self.variables[key] = data.variables[var_name]
                if 'offset' in variables[key]:
                    self.offsets[key] = variables[key]['offset']
            else:
                logging.getLogger().error(data.variables)
                raise Exception('Variable does not exist in dataset: ' + var_name)

        # set min/max values for quicker tests below
        self.min_lat = self.lat.min()
        self.max_lat = self.lat.max()
        self.min_lon = self.lon.min()
        self.max_lon = self.lon.max()

    @abc.abstractmethod
    def get(self, lat: float, lon: float, date: dt.datetime, fields: list[str] | None = None) -> dict[str, any] | None:
        """Get data for at given location and date"""
        pass


class SpaceTimeData(SpatioTemporalInterface):
    def __init__(self, data: nc.Dataset, variables: dict[str, dict[str, any]], latitude: str = 'latitude',
                 longitude: str = 'longitude', time: str = 'time', start_date: dt.date | None = None):
        super().__init__(data, variables, latitude, longitude)
        # """Variables to map values on"""
        self.start_date: dt.date | None = start_date
        """Start date different from global one."""

        self.times: nc.Variable = data.variables[time]
        """time dataset"""
        self._cache: dict[tuple[int, int, int], dict[str, any]] = {}
        """Cached data for quicker access"""

        # set min/max values for quicker tests below
        times = self.times[:]
        self.min_times = times.min()
        self.max_times = times.max()

    def _in_bounds(self, lat: float, lon: float, date_number: float) -> bool:
        """
        Tests if lat, lon and time are within the bounds of the dataset
        :param lat: latitude
        :param lon: longitude
        :param date_number: date number
        :return: true if in bounds, false otherwise
        """
        if self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon and self.min_times <= date_number <= self.max_times:
            return True

        return False

    def _get_date_number(self, date: dt.datetime | None) -> float | None:
        """
        Returns date number for given date - returns none if datetimes have not been set

        :param date: date

        :return: date number or None
        """
        if date is None:
            return None

        return nc.date2num(date, self.times.units, calendar=self.times.calendar, has_year_zero=False)

    def get(self, lat: float, lon: float, date: dt.datetime, fields: list[str] | None = None) -> dict[str, any] | None:

        # convert to date number
        date_num = self._get_date_number(date)
        if date_num is None:
            return None

        # check bounds
        if not self._in_bounds(lat, lon, date_num):
            return None

        # add all fields, if none have been set
        if fields is None or len(fields) == 0:
            fields = list(self.variables.keys())

        # find the closest indexes
        lat_idx = (np.abs(self.lat - lat)).argmin()
        lon_idx = (np.abs(self.lon - lon)).argmin()
        time_idx = (np.abs(self.times[:] - date_num)).argmin()

        # we use a cache to store previously calculated values, because accessing indexes in the NETCDF file is quite
        # slow
        return self.get_variables_by_index(lat_idx, lon_idx, time_idx, fields)

    def get_variables_by_index(self, lat_idx: int, lon_idx: int, time_idx: int, fields: list[str]) -> dict[str, any]:
        key = (lat_idx, lon_idx, time_idx)
        if key not in self._cache:
            # aggregate variables
            variables: dict[str, any] = {}

            for field in fields:
                if field in self.variables:
                    value = self.variables[field][time_idx][lat_idx][lon_idx]

                    # apply offset, if it exists
                    if field in self.offsets:
                        value += self.offsets[field]

                    variables[field] = value

            self._cache[key] = variables

            return variables
        else:
            return self._cache[key]


class SpaceData(SpatioTemporalInterface):
    def __init__(self, data: nc.Dataset, variables: dict[str, dict[str, any]], latitude: str = 'latitude',
                 longitude: str = 'longitude', time: str = 'time', start_date: dt.date | None = None):
        super().__init__(data, variables, latitude, longitude)
        self._cache: dict[tuple[int, int], dict[str, any]] = {}
        """Cached data for quicker access"""

    def _in_bounds(self, lat: float, lon: float) -> bool:
        """
        Tests if lat, lon and time are within the bounds of the dataset
        :param lat: latitude
        :param lon: longitude
        :param date_number: date number
        :return: true if in bounds, false otherwise
        """
        if self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon:
            return True

        return False

    def get(self, lat: float, lon: float, date: dt.datetime, fields: list[str] | None = None) -> dict[str, any] | None:
        # check bounds
        if not self._in_bounds(lat, lon):
            return None

        # add all fields, if none have been set
        if fields is None or len(fields) == 0:
            fields = list(self.variables.keys())

        # find the closest indexes
        lat_idx = (np.abs(self.lat - lat)).argmin()
        lon_idx = (np.abs(self.lon - lon)).argmin()

        # we use a cache to store previously calculated values, because accessing indexes in the NETCDF file is quite
        # slow
        return self.get_variables_by_index(lat_idx, lon_idx, fields)

    def get_variables_by_index(self, lat_idx: int, lon_idx: int, fields: list[str]) -> dict[str, any]:
        key = (lat_idx, lon_idx)
        if key not in self._cache:
            # aggregate variables
            variables: dict[str, any] = {}

            for field in fields:
                if field in self.variables:
                    value = self.variables[field][lat_idx][lon_idx]

                    # apply offset, if it exists
                    if field in self.offsets:
                        value += self.offsets[field]

                    variables[field] = value

            self._cache[key] = variables

            return variables
        else:
            return self._cache[key]