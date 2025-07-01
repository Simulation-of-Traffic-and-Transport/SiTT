#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Merge data retrieved via Download from https://public.hub.geosphere.at/public/datahub.html?id=snowgrid_cl-v2-1d-1km/filelisting&prefix=/snow_depth/
#
# These are the snow heights of Austria, and we downloaded the files from the provided URL - years 1990 to 1999.

import glob

import xarray as xr

# Load all data files and merge them into one xarray dataset.
# You need to install dask for this to work: pip install dask (already satisfied in requirements.txt)
data = xr.open_mfdataset('original_geosphere_at/SNOWGRID-CL_snow_depth_*.nc')
# We remove and change some data so it is more conform with our other data sources.
data = (data.drop_vars(["lambert_conformal_conic", "lat", "lon"], errors="ignore"))
data = (data.rename_dims({"y": "lat", "x": "lon"})
        .rename_vars({"y": "lat", "x": "lon"})
        )
print(data)
data.to_netcdf("geosphere_at_data.nc")
