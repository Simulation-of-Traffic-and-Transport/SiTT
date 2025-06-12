#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Merge data retrieved by get_era5_data.py into one file. This speeds up data retrieval by a large amount.

import glob

import xarray as xr

# Load all data files and merge them into one xarray dataset.
# You need to install dask for this to work: pip install dask (already satisfied in requirements.txt)
data = xr.merge([xr.open_mfdataset(f, decode_times=False) for f in glob.glob('*[0-9][0-9][0-9][0-9].nc')])
# we also rename "valid_time" to "time" for consistency with other data sources - this has been changed in the ERA5 data
# in 2014, also see: https://forum.ecmwf.int/t/new-time-format-in-era5-netcdf-files/3796/5
data = (data.rename_dims({"valid_time": "time"})
        .rename_vars({"valid_time": "time"})
        .drop_vars(["expver", "number"], errors="ignore")
        )
print(data)
data.to_netcdf("era5_data.nc")

# xarray changes the internal formats of the data into 64/32bit equivalents. This creates far bigger files, which
# might be something we have to address in the future.
#
# The original data format in Copernicus data is int16 for values and float32 for coordinates. Saving values in these
# formats would save about half of the disk space, but I have experienced precision loss when converting values back to
# int16, so we leave it as such for now.
