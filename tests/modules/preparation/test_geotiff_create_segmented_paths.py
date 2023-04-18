# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
import math
from os.path import exists, join
from tempfile import gettempdir

import geopandas as gpd
import rasterio
import requests
from pyproj import Transformer

from sitt.modules.preparation import GeoTIFFCreateSegmentedPaths


def prepare_test_data():
    filename = join(gettempdir(), "Envisat_ASAR_2003-08-04.tif")

    if not exists(filename):
        print("Downloading Envisat_ASAR_2003-08-04.tif...")
        myfile = requests.get("http://leoworks.terrasigna.com/files/Envisat_ASAR_2003-08-04.tif")
        open(filename, 'wb').write(myfile.content)
        print("Done!")

    return filename


# inspired by https://stackoverflow.com/questions/62283718/how-to-extract-a-profile-of-value-from-a-raster-along-a-given-line
def test_geotiff_create_segmented_paths_create_segments():
    # load raster io
    filename = prepare_test_data()
    rds: rasterio.io.DatasetReader = rasterio.open(filename)
    transformer = Transformer.from_crs("EPSG:4326", rds.crs, always_xy=True)

    # min resolution to split - half of the hypotenuse of the resolution triangle will render a very good minimum
    # resolution threshold
    min_resolution = math.sqrt(math.pow(rds.res[0], 2) + math.pow(rds.res[1], 2)) / 2

    s = gpd.GeoSeries.from_wkt(
        [
            "LINESTRING (12.28231320537815 45.436033527344705 61.0, 12.330296611165766 45.470975578297988 59.0, 12.330297611165766 45.470976578297988 59.0)"],
        crs="EPSG:4326")
    gdf = gpd.GeoDataFrame([{"geom": s[0]}])

    # create test entity
    entity = GeoTIFFCreateSegmentedPaths()

    output = entity.create_segments(rds, transformer, gdf)

    assert len(output) == 1
    line = output.values[0][0]
    assert len(line.coords) == 684
    assert line.coords[0][0] == 12.28231320537815
    assert line.coords[0][1] == 45.436033527344705
    assert line.coords[0][2] == 61.0

    assert line.coords[-1][0] == 12.330297611165766
    assert line.coords[-1][1] == 45.470976578297988
    assert line.coords[-1][2] == 59.0

    # TODO: more tests to see if out shape really is ok
