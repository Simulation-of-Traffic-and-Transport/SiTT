#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Create fresh snow of last x hours from 5era data

import xarray as xr

# Settings
delta_hours = 3
"""Number of past hours to consider for fresh snow calculation"""
snow_multiplier = 10.
"""
Multiplier for the snowfall heights to convert m of water equivalent to snow depth in m (we consider snow to be 10
times less dense than water, so about 100 kg per m³ - could also be 50 kg, then the multiplier would be 20)
see https://www.weltderphysik.de/thema/hinter-den-dingen/schneelast/
"""

# Open the ERA5 data
ds = xr.open_dataset('era5_data.nc')

snow_depth = ds['sd'] * snow_multiplier
snow_depth = snow_depth.transpose('latitude', 'longitude', 'time')

# convert to dataframe for easier manipulation
snow_depth_df = snow_depth.to_dataframe()

# get the difference from the last x hours and fill NaN with 0
snow_depth_df['sd_diff'] = snow_depth_df.diff(periods=delta_hours).fillna(0.)

# set negative values to 0
snow_depth_df['new_snow'] = snow_depth_df['sd_diff'].apply(lambda val: max(0., val))

# drop the original columns
snow_depth_df.drop(['sd', 'sd_diff'], axis=1, inplace=True)

# create a new xarray dataset
new_ds = snow_depth_df.to_xarray().transpose('time', 'latitude', 'longitude')

# Generate dynamic encoding for all variables
encoding = {}
for var in new_ds.data_vars:
    var_dims = new_ds[var].dims  # Get dimensions of the variable
    var_shape = new_ds[var].shape  # Get the shape of the variable
    var_chunks = tuple(min(size, 50) for size in var_shape)  # Adjust chunk sizes
    encoding[var] = {
        "zlib": True,  # Enable compression
        "complevel": 4,  # Compression level (1-9, 4 is a good balance)
        "chunksizes": var_chunks  # Chunk sizes
    }

# save
new_ds.to_netcdf('new_snow.nc', compute=True, engine="netcdf4", encoding=encoding)
