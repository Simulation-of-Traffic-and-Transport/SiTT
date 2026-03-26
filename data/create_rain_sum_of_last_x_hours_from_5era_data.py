#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Create the sum of rainfall for the last x hours from ERA5 data

import xarray as xr

# Settings
delta_hours = 24
"""Number of past hours to consider for rain calculation"""


# Open the ERA5 data
ds = xr.open_dataset('era5_data.nc')

# Unit is kg / (m² * s)
# As https://codes.ecmwf.int/grib/param-db/228218 states:
# 1 kg of water spread over 1 square metre of surface is 1 mm deep (neglecting the effects of temperature on the
# density of water), therefore the units are equivalent to mm per second.
# So, the unit can be read as mm/s - we have to get mm/h as our frame is per hour.
convective_rain_rate = ds['crr'] * 3600 # multiple with 60*60 to get mm/h
convective_rain_rate = convective_rain_rate.transpose('latitude', 'longitude', 'time')

# convert to dataframe for easier manipulation
convective_rain_rate_df = convective_rain_rate.to_dataframe()
# sum up last x hours to get the sum of rainfall of the last x hours
convective_rain_rate_df['rain_last_24h'] = convective_rain_rate_df['crr'].rolling(delta_hours, min_periods=1).sum()
# drop the original column
convective_rain_rate_df.drop(['crr'], axis=1, inplace=True)

# create a new xarray dataset with the sum of rainfall for the last x hours
new_ds = convective_rain_rate_df.to_xarray().transpose('time', 'latitude', 'longitude')

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
new_ds.to_netcdf('rain_sum_last_24h.nc', compute=True, engine="netcdf4", encoding=encoding)