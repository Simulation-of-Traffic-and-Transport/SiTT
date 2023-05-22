# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Set the height for paths and hubs using a GeoTIFF height map"""
import logging

import geopandas as gpd
import rasterio
import yaml
from pyproj import Transformer
from shapely.geometry import shape

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class GeoTIFFHeightForPathsAndHubs(PreparationInterface):
    """Set the height for paths and hubs using a GeoTIFF height map"""

    def __init__(self, file: str | None = None, crs_from: str = "EPSG:4326", always_xy: bool = True,
                 band: int = 1, overwrite: bool = False):
        super().__init__()
        self.file: str | None = file
        self.crs_from: str = crs_from
        self.always_xy: bool = always_xy
        self.overwrite: bool = overwrite
        self.band: int = band

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info("Setting heights for paths and hubs using GeoTIFF height map " + self.file)

        # load geo
        rds: rasterio.io.DatasetReader = rasterio.open(self.file)
        transformer = Transformer.from_crs(self.crs_from, rds.crs, always_xy=self.always_xy)

        # calculate hub heights
        context.raw_roads = self.calculate_heights(rds, transformer, context.raw_roads, 'roads')
        context.raw_rivers = self.calculate_heights(rds, transformer, context.raw_rivers, 'rivers')
        context.raw_hubs = self.calculate_heights(rds, transformer, context.raw_hubs, 'hubs')

        return context

    def calculate_heights(self, rds: rasterio.io.DatasetReader, transformer: Transformer,
                          raw: gpd.geodataframe.GeoDataFrame, label: str) -> gpd.geodataframe.GeoDataFrame:
        # get relevant band
        band = rds.read(self.band)

        if raw is not None and len(raw):
            counter = 0

            for idx, row in raw.iterrows():
                if 'geom' in row:
                    g = row.geom
                    changed = False
                    geom = []

                    if g and g.coords:
                        for coord in g.coords:
                            if self.overwrite is False and len(coord) > 2 and coord[2] > 0:
                                continue

                            lng = coord[0]
                            lat = coord[1]

                            xx, yy = transformer.transform(lng, lat)
                            x, y = rds.index(xx, yy)
                            height = band[x, y]
                            # TODO: for roads, do we want to have a different type of height calculation?

                            geom.append((lng, lat, height))
                            changed = True

                        if changed:
                            raw.at[idx, 'geom'] = shape({"type": row.geom.geom_type, "coordinates": geom})
                            counter += 1

            if logger.level <= logging.INFO:
                logger.info("Calculated heights for %s: %d/%d", label, counter, len(raw))

        return raw

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "GeoTIFFHeightForPathsAndHubs"
