#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Merge data retrieved by get_era5_data.py into one file. This speeds up data retrieval by a large amount.

import xarray as xr

# Load all data files and merge them into one xarray dataset.
# You need to install dask for this to work: pip install dask (already satisfied in requirements.txt)
data = xr.open_mfdataset('original/*[0-9][0-9][0-9][0-9].nc')
# We do not use xr.merge, because for some reason this will create huge files in the end.

# # we also rename "valid_time" to "time" for consistency with other data sources - this has been changed in the ERA5 data
# # in 2024, also see: https://forum.ecmwf.int/t/new-time-format-in-era5-netcdf-files/3796/5
data = (data.rename_dims({"valid_time": "time"})
        .rename_vars({"valid_time": "time"})
        .drop_vars(["expver", "number"], errors="ignore")
        )
print(data)
data.to_netcdf("era5_data.nc")
