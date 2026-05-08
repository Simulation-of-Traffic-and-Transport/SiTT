#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xarray as xr

# Create interpolated weather data for SiTT from ERA5 data.
# We need to transform some data to make it more suitable for SiTT.

ds = xr.open_dataset('era5_data.nc')

# output dataset
xrds = xr.Dataset(coords=ds.coords)

#####################################################################
# Settings
#####################################################################
min_wind_gust_kph = 50
"""minimal wind gust speed in kilometers per hour which creates a warning"""
delta_hours = 3
"""Number of past hours to consider for fresh snow calculation"""
snow_multiplier = 10.
"""
Multiplier for the snowfall heights to convert m of water equivalent to snow depth in m (we consider snow to be 10
times less dense than water, so about 100 kg per m³ - could also be 50 kg, then the multiplier would be 20)
see https://www.weltderphysik.de/thema/hinter-den-dingen/schneelast/
"""

########################################################################################################################
# TEMPERATURE
########################################################################################################################

#just copy the data, convert to Celsius
xrds['t'] = ds['t2m'] - 273.15  # Convert from Kelvin to Celsius


########################################################################################################################
# Fresh Snow Data
########################################################################################################################

# sum snowfall in the last x hours
xrds['snow'] = ds['csf'].rolling({'time': delta_hours}, min_periods=1).sum()

########################################################################################################################
# Wind Gusts
########################################################################################################################

xrds['wind'] = ds['i10fg'] / 3.6  # convert from meters per second to kilometers per hour


########################################################################################################################
# Rain
########################################################################################################################

xr_rain = xr.Dataset(coords=ds.coords)

# convert to rainfall rate from mm/s to mm/h
xr_rain['rain_mm_per_h']: xr.DataArray = ds['crr'] *  3600 # multiple with 60*60 to get mm/h

# We take the criteria of the DWS (Deutsche Wetterdienst):
# https://www.dwd.de/DE/wetter/warnungen_aktuell/kriterien/warnkriterien.html
# There are two factors in rain:
# - Heavy rain
# - Persistent rain
# We record both as booleans in the dataset for step 2 and 3

# Moreover, we need to record light rain, too. This is defined as more than 5mm in the last 24 hours. This is relevant
#  for carts.

xr_rain['rain_mm_per_6h']: xr.DataArray = xr_rain['rain_mm_per_h'].rolling({'time': 6}, min_periods=1).sum()
xr_rain['rain_mm_per_12h']: xr.DataArray = xr_rain['rain_mm_per_h'].rolling({'time': 12}, min_periods=1).sum()
xr_rain['rain_mm_per_24h']: xr.DataArray = xr_rain['rain_mm_per_h'].rolling({'time': 24}, min_periods=1).sum()
xr_rain['rain_mm_per_48h']: xr.DataArray = xr_rain['rain_mm_per_h'].rolling({'time': 48}, min_periods=1).sum()
xr_rain['rain_mm_per_72h']: xr.DataArray = xr_rain['rain_mm_per_h'].rolling({'time': 72}, min_periods=1).sum()

# Light Rain: more than 5 mm in the last 24 hours
xrds['light_rain'] = xr.where(xr_rain['rain_mm_per_24h'] >= 5., True, False)

# Heavy Rain step 2: more than 15 mm in the last hour or more than 20 mm in the last 6 hours
xrds['heavy_rain_2'] = xr.where(xr.ufuncs.logical_or(xr_rain['rain_mm_per_h'] >= 15., xr_rain['rain_mm_per_6h'] >= 20.), True, False)

# Heavy Rain step 3: more than 25 mm in the last 6 hours or more than 35 mm in the last 24 hours
xrds['heavy_rain_3'] = xr.where(xr.ufuncs.logical_or(xr_rain['rain_mm_per_h'] >= 25., xr_rain['rain_mm_per_6h'] >= 35.), True, False)

# Persistent Rain step 2: more than 25 mm in the last 12 hours, more than 30 mm in the last 24 hours, more than 40 mm in the last 48 hours, or more than 60 mm in the last 72 hours
xrds['persistent_rain_2'] = xr.where(xr.ufuncs.logical_or(xr_rain['rain_mm_per_12h'] >= 25., xr.ufuncs.logical_or(xr_rain['rain_mm_per_24h'] >= 30., xr.ufuncs.logical_or(xr_rain['rain_mm_per_48h'] >= 40., xr_rain['rain_mm_per_72h'] >= 60.))), True, False)

# Persistent Rain Step 3: more than 40 mm in the last 12 hours, more than 50 mm in the last 24 hours, more than 60 mm in the last 48 hours, or more than 90 mm in the last 72 hours
xrds['persistent_rain_3'] = xr.where(xr.ufuncs.logical_or(xr_rain['rain_mm_per_12h'] >= 40., xr.ufuncs.logical_or(xr_rain['rain_mm_per_24h'] >= 50., xr.ufuncs.logical_or(xr_rain['rain_mm_per_48h'] >= 60., xr_rain['rain_mm_per_72h'] >= 90.))), True, False)

########################################################################################################################
ds.close()

xrds.to_netcdf('sitt_data.nc')
xrds.close()