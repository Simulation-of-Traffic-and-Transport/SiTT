# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create test data"""
import netCDF4 as nc
import numpy as np
import datetime as dt

# create test data
rootgrp = nc.Dataset('test.nc', "w", format="NETCDF4")

longitude = rootgrp.createDimension("longitude", 15)
latitude = rootgrp.createDimension("latitude", 10)
time = rootgrp.createDimension("time", 5)

latitudes = rootgrp.createVariable("latitude", "f4", ("latitude",))
latitudes.units = 'degrees_north'
longitudes = rootgrp.createVariable("longitude", "f4", ("longitude",))
longitudes.units = 'degrees_east'
times = rootgrp.createVariable("time", "i4", ("time",))
times.units = 'hours since 1900-01-01 00:00:00.0'
times.calendar = 'gregorian'
temperatures = rootgrp.createVariable("temperature", "f4", ("time", "latitude", "longitude",))
temperatures.units = "C"

lons = np.arange(10, 17.5, 0.5)
lats = np.arange(10, 15, 0.5)
longitudes[:] = lons
latitudes[:] = lats

dates = [dt.datetime(1995, 7, 1, 6) + n * dt.timedelta(hours=12) for n in range(temperatures.shape[0])]
times[:] = nc.date2num(dates, units=times.units, calendar=times.calendar)
temperatures[:, :, :] = np.random.uniform(0, 30, size=(5, 10, 15))
