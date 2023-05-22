# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT

from os.path import exists, join
from tempfile import gettempdir

import geopandas as gpd
import rasterio
import requests
from pyproj import Transformer

from sitt.modules.preparation import GeoTIFFHeightForPathsAndHubs


# download test data from leoworks
def prepare_test_data():
    filename = join(gettempdir(), "Envisat_ASAR_2003-08-04.tif")

    if not exists(filename):
        print("Downloading Envisat_ASAR_2003-08-04.tif...")
        myfile = requests.get("http://leoworks.terrasigna.com/files/Envisat_ASAR_2003-08-04.tif")
        open(filename, 'wb').write(myfile.content)
        print("Done!")

    return filename


def test_geotiff_height_for_paths_and_hubs_calculate_init():
    entity = GeoTIFFHeightForPathsAndHubs()

    assert entity.file is None
    assert entity.crs_from ==  "EPSG:4326"
    assert entity.always_xy == True
    assert entity.overwrite == False
    assert entity.band == 1


def test_geotiff_height_for_paths_and_hubs_calculate_heights():
    # load raster io
    filename = prepare_test_data()
    rds: rasterio.io.DatasetReader = rasterio.open(filename)
    transformer = Transformer.from_crs("EPSG:4326", rds.crs, always_xy=True)

    # test data
    s = gpd.GeoSeries.from_wkt(["LINESTRING (12.30966981031923 45.44755534767208 0, 12.310878919955238 45.44772546652944 0, 12.310985215307854 45.44764856354787 0, 12.311496761692316 45.44756000846952 0, 12.311809004290625 45.447704492999414 0, 12.311503405151853 45.44690982350518 0, 12.3107859115217 45.44636217085676 0, 12.3107859115217 45.44636450130483 0)"], crs="EPSG:4326")
    gdf = gpd.GeoDataFrame([{"geom": s[0]}])

    # load test class
    subject = GeoTIFFHeightForPathsAndHubs()
    output = subject.calculate_heights(rds, transformer, gdf, "test")

    # expected values
    expected = [255., 58., 66., 61., 56., 154., 64., 64.]

    for _, row in output.iterrows():
        g = row.geom
        idx = 0
        for coord in g.coords:
            assert coord[2] == expected[idx]
            idx += 1
