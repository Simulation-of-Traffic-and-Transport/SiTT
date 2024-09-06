# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Convert a xyz data file to a shapefile.
"""

import geopandas as gpd
import pandas as pd

crs = 31287  # EPSG code
input_file = "LavantHoehenraster.xyz"
output_file = "LavantHoehenraster.shp"

# read xyz file
df = pd.read_csv(input_file, sep=",", header=None, names=["x", "y", "z"])

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y, df.z, crs), crs=crs)

gdf.to_file(output_file)
