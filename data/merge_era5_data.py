#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Merge data retrieved by get_era5_data.py into one file. This speeds up data retrieval by a large amount.

import netCDF4 as nc

variables = ['2m_temperature', 'convective_rain_rate', 'convective_snowfall_rate_water_equivalent', 'snow_depth']

rootgrp = nc.Dataset("era5_data.nc", "w", format="NETCDF4")
first = True

for variable in variables:
    data = nc.Dataset("era5_data_" + variable + ".nc", "r", format="NETCDF4")

    # copy dimensions and base variables from first
    if first:
        for dimension in data.dimensions:
            rootgrp.createDimension(dimension, data.dimensions[dimension].size)

        latitudes = rootgrp.createVariable("latitude", "f4", ("latitude",))
        latitudes.units = 'degrees_north'
        longitudes = rootgrp.createVariable("longitude", "f4", ("longitude",))
        longitudes.units = 'degrees_east'
        times = rootgrp.createVariable("time", "i4", ("time",))
        times.units = 'hours since 1900-01-01 00:00:00.0'
        times.calendar = 'gregorian'

        # copy data
        longitudes[:] = data.variables["longitude"][:]
        latitudes[:] = data.variables["latitude"][:]
        times[:] = data.variables["time"][:]

        first = False

    # copy data
    for var in data.variables:
        if var in ['longitude', 'latitude', 'time']:
            continue

        my_var = rootgrp.createVariable(var, "i2", data.variables[var].dimensions, fill_value=data.variables[var].getncattr('_FillValue'))
        for attr in data.variables[var].ncattrs():
            if attr != '_FillValue':
                my_var.setncattr(attr, data.variables[var].getncattr(attr))
        my_var[:] = data.variables[var][:]

print(rootgrp)
