#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Create mean/median/mode heights from era5 data

import numpy as np
import rasterio
import rioxarray
import xarray as xr
from scipy import stats

# Open the heights raster TIFF file and reproject it to EPSG:4326, so it is compatible with the ERA5 data
raster = rioxarray.open_rasterio('DTM Austria 10m v2 by Sonny.tif', masked=True).squeeze()
raster_new = raster.rio.reproject(rasterio.CRS.from_string('EPSG:4326'))

# Open the ERA5 data
ds = xr.open_dataset('era5_data.nc')

# get lat and lon values of ERA5 data
lats = ds.latitude.values
lons = ds.longitude.values

diff_lat = np.abs(lats[1] - lats[0])/2
diff_lon = np.abs(lons[1] - lons[0])/2

# create a new dataset that will store the mean heights
mean_heights = np.zeros((len(lats), len(lons)))
median_heights = np.zeros((len(lats), len(lons)))
mode_heights = np.zeros((len(lats), len(lons)))

# now calculate median height for each pixel in the new dataset
for i in range(len(lats)):
    for j in range(len(lons)):
        # calculate bounding box for current pixel - this is a square that covers the current pixel
        x1, y1 = lons[j] - diff_lon, lats[i] - diff_lat
        x2, y2 = lons[j] + diff_lon, lats[i] + diff_lat

        # calculate min and max x,y for current pixel
        min_x, max_x = min(x1, x2), max(x1, x2)
        min_y, max_y = min(y1, y2), max(y1, y2)

        try:
            # clip raster to current pixel bounding box
            data = raster_new.rio.clip_box(minx=min_x, maxx=max_x, miny=min_y, maxy=max_y)
            # calculate and store mean height for current pixel - it is the mean of all non-NaN values in the clipped raster
            mh = float(data.mean().values)
            # calculate and store median height for current pixel
            md = float(data.median().values)
            # calculate and store mode height for current pixel - we flatten the values, find the lowest common height band (100 m resolution) and find the most common one within this raster. We add 50 m to get the middle value.
            result = stats.mode(np.floor(data.values.flatten() / 100) * 100, nan_policy='omit').mode
            if not np.isnan(result):
                mm = result + 50
            else:
                mm = np.nan
        except:
            # some of the pixels in the raster may not cover the current pixel bounding box, so we set the median height to NaN
            mh = np.nan
            md = np.nan
            mm = np.nan

        # store the median/mean/modus height in the new dataset if it is not NaN
        if not np.isnan(mh):
            mean_heights[i, j] = mh

        if not np.isnan(md):
            median_heights[i, j] = md

        if not np.isnan(mm):
            mode_heights[i, j] = mm

            #print(f"Height of pixel ({j}, {i}) is {h:.2f} m")

# save the new dataset as a netCDF file - we create an xarray with variables and coordinates
heights_data = xr.Dataset(
    data_vars=dict(
        mean_height=(["latitude", "longitude"], mean_heights),
        median_height=(["latitude", "longitude"], median_heights),
        mode_height=(["latitude", "longitude"], mode_heights),
    ),
    coords={'latitude': lats, 'longitude': lons}
)

heights_data.to_netcdf('mean_heights_data.nc')
