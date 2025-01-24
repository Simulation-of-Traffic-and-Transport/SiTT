# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Convert xyz data files to shapefile in a single directory.
"""

import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

for child in Path('heights').iterdir():
    if child.is_file():
        basename = os.path.splitext(child.name)[0]
        print(basename)

        crs = 31287  # EPSG code
        input_file = "heights/" + basename + ".xyz"
        output_file = "heights/" + basename + ".shp"

        # read xyz file
        df = pd.read_csv(input_file, sep=",", header=None, names=["x", "y", "z"])

        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y, df.z, crs), crs=crs)

        gdf.to_file(output_file)
