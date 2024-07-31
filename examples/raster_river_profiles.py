# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
River raster reader to text xyz files from GIS.
"""

import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point, Polygon

crs = 31287  # EPSG code
# height_raster = "gail1_reduced_5m.xyz"
#
#
# # read xyz file
# df = pd.read_csv(height_raster, sep=",", header=None, names=["x", "y", "z"])
#
# gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y, df.z, crs), crs=crs)
#
# gdf.to_file('gail1_reduced_5m.shp')

height_raster = "Gail1Hoehenraster.xyz"

# read xyz file
df = pd.read_csv(height_raster, sep=",", header=None, names=["x", "y", "z"])

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y, df.z, crs), crs=crs)

# test point
p = Point(388568.3, 303768.4)
closest_point = gdf.geometry.iloc[gdf.geometry.distance(p).idxmin()]

neighbor_indexes = sorted(gdf.geometry.distance(closest_point).sort_values().head(9).index)
# create 3x3 matrix with neighbor indexes
heights = np.zeros(9)

for x in range(3):
    for y in range(3):
        # set heights
        heights[x * 3 + y] = gdf.geometry.iloc[neighbor_indexes[x * 3 + y]].z

lowest_p = heights.argmin()
if lowest_p == 0:
    print("Flow direction: NW")
if lowest_p == 1:
    print("Flow direction: N")
if lowest_p == 2:
    print("Flow direction: NE")
if lowest_p == 3:
    print("Flow direction: W")
if lowest_p == 4:
    raise ValueError("Invalid point")
if lowest_p == 5:
    print("Flow direction: O")
if lowest_p == 6:
    print("Flow direction: SW")
if lowest_p == 7:
    print("Flow direction: S")
if lowest_p == 8:
    print("Flow direction: SE")


# TODO: more precise calculation of triangle and corner and the inclination might get more accurate results
# if lowest_p == 0:
#     check_p = (7, 5)
# if lowest_p == 1:
#     check_p = (0, 2)
# if lowest_p == 2:
#     check_p = (1, 5)
# if lowest_p == 3:
#     check_p = (0, 6)
# if lowest_p == 4:
#     raise ValueError("Invalid point")
# if lowest_p == 5:
#     check_p = (2, 8)
# if lowest_p == 6:
#     check_p = (3, 7)
# if lowest_p == 7:
#     check_p = (6, 8)
# if lowest_p == 8:
#     check_p = (7, 5)
#
# # get other corner
# if heights[check_p[0]] < heights[check_p[1]]:
#     corner = check_p[0]
# else:
#     corner = check_p[1]
#
# triangle = Polygon(gdf.geometry.iloc[[neighbor_indexes[4], neighbor_indexes[lowest_p], neighbor_indexes[corner]]])
# print(triangle)
# #print(gdf.geometry.iloc[gdf.geometry.distance(closest_point).sort_values().head(9).index])

