# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
import datetime as dt

import netCDF4 as nc
import numpy as np

from sitt.base import SpaceTimeData, Configuration

test_data = nc.Dataset('test.nc', 'r', format='NETCDF4')


def test_space_time_data_init():
    st_data = SpaceTimeData(test_data,  {'temperature': {}})

    assert st_data.variables
    assert 'temperature' in st_data.variables
    assert len(st_data.variables['temperature'])
    assert 'none' not in st_data.variables
    assert len(st_data.lat)
    assert len(st_data.lon)
    assert len(st_data.times)
    assert st_data.min_lat < st_data.max_lat
    assert st_data.min_lon < st_data.max_lon
    assert st_data.min_times < st_data.max_times


def test_space_time_data_init_error():
    try:
        SpaceTimeData(test_data, {'dummy': {}})
    except Exception:
        assert True


def test_space_time_data_get_date_number():
    # test none
    st_data = SpaceTimeData(test_data, {'temperature': {}})
    config = Configuration()

    assert st_data._get_date_number(1, 5, config) is None

    # test global
    config.start_date = dt.date(1995, 7, 1)
    assert st_data._get_date_number(1, 5, config) == 837101

    # test local
    st_data.start_date = dt.date(1990, 2, 15)
    assert st_data._get_date_number(1, 3, config) == 790011


def test_space_time_data_get():
    st_data = SpaceTimeData(test_data, {'temperature': {}})
    config = Configuration()
    config.start_date = dt.date(1995, 7, 1)

    idx_lat = 5
    idx_lon = 9

    lat = test_data.variables['latitude'][idx_lat]
    lon = test_data.variables['longitude'][idx_lon]
    day = 2
    hours = 14.4345
    date_num = st_data._get_date_number(day, hours, config)
    time_idx = (np.abs(st_data.times[:] - date_num)).argmin()

    result = st_data.get(lat, lon, day, hours, config)
    assert result is not None
    assert len(result) == 1
    assert 'temperature' in result and result['temperature'] == st_data.variables['temperature'][time_idx][idx_lat][idx_lon]


def test_space_time_data_in_bounds():
    st_data = SpaceTimeData(test_data, {'temperature': {}})

    latitudes, longitudes = test_data.variables['latitude'][:], test_data.variables['longitude'][:]
    times = test_data.variables['time']

    for time in times:
        for lat in latitudes:
            for lon in longitudes:
                assert st_data._in_bounds(lat, lon, time)

    lat_min = latitudes.min()
    lon_min = longitudes.min()
    time_min = times[:].min()
    lat_max = latitudes.max()
    lon_max = longitudes.max()
    time_max = times[:].max()
    assert not st_data._in_bounds(lat_min - 1, lon_min, time_min)
    assert not st_data._in_bounds(lat_min, lon_min - 1, time_min)
    assert not st_data._in_bounds(lat_min, lon_min, time_min - 1)
    assert not st_data._in_bounds(lat_max + 1, lon_max, time_max)
    assert not st_data._in_bounds(lat_max, lon_max + 1, time_max)
    assert not st_data._in_bounds(lat_max, lon_max, time_max + 1)
