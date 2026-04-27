#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xarray as xr

# Create interpolated weather data for SiTT from ERA5 data.
# We need to transform some data to make it more suitable for SiTT.

ds = xr.open_dataset('era5_data.nc')

# output dataset
xrds = xr.Dataset(coords=ds.coords)

########################################################################################################################
# TEMPERATURE
########################################################################################################################

#just copy the data, convert to Celsius
xrds['t'] = ds['t2m'] - 273.15  # Convert from Kelvin to Celsius

########################################################################################################################
ds.close()

xrds.to_netcdf('sitt_data.nc')
xrds.close()