# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
from sitt.base import SpaceTimeData
import netCDF4 as nc

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


def test_space_time_data_in_bounds():
    st_data = SpaceTimeData(test_data, {'temperature': {}})

    latitudes, longitudes = test_data.variables['latitude'][:], test_data.variables['longitude'][:]
    times = test_data.variables['time']

    for time in times:
        for lat in latitudes:
            for lon in longitudes:
                assert st_data.in_bounds(lat, lon, nc.num2date(time, times.units, times.calendar))

    lat_min = latitudes.min()
    lon_min = longitudes.min()
    time_min = times[:].min()
    lat_max = latitudes.max()
    lon_max = longitudes.max()
    time_max = times[:].max()
    assert not st_data.in_bounds(lat_min - 1, lon_min, nc.num2date(time_min, times.units, times.calendar))
    assert not st_data.in_bounds(lat_min, lon_min - 1, nc.num2date(time_min, times.units, times.calendar))
    assert not st_data.in_bounds(lat_min, lon_min, nc.num2date(time_min - 1, times.units, times.calendar))
    assert not st_data.in_bounds(lat_max + 1, lon_max, nc.num2date(time_max, times.units, times.calendar))
    assert not st_data.in_bounds(lat_max, lon_max + 1, nc.num2date(time_max, times.units, times.calendar))
    assert not st_data.in_bounds(lat_max, lon_max, nc.num2date(time_max + 1, times.units, times.calendar))


def test_space_time_data_get():
    st_data = SpaceTimeData(test_data, {'temperature': {}})

    latitudes, longitudes = test_data.variables['latitude'][:], test_data.variables['longitude'][:]
    times = test_data.variables['time']

    i = 0
    for time in times:
        j = 0
        for lat in latitudes:
            k = 0
            for lon in longitudes:
                assert test_data.variables['temperature'][i][j][k] ==\
                       st_data.get('temperature', lat, lon, nc.num2date(time, times.units, times.calendar))
                k += 1
            j += 1
        i += 1
