# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Create subsegments for paths using a GeoTIFF image - this will make long paths a bit more fine-grained regarding
height profiles. If you really need to use this in your preparation phase is a matter of taste and the quality of your
input data.
"""
import logging
import math

import geopandas as gpd
import rasterio
import shapely.ops as sp_ops
import yaml
from pyproj import Transformer
from pyproj.enums import TransformDirection
from shapely.geometry import shape, LineString

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class GeoTIFFCreateSegmentedPaths(PreparationInterface):
    """
    Create subsegments for paths using a GeoTIFF image - this will make long paths a bit more fine-grained regarding
    height profiles. If you really need to use this in your preparation phase is a matter of taste and the quality of
    your input data.
    """

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
            logger.info("Creating segmented paths using GeoTIFF height map " + self.file)

        # load geo
        rds: rasterio.io.DatasetReader = rasterio.open(self.file)
        transformer = Transformer.from_crs(self.crs_from, rds.crs, always_xy=self.always_xy)

        # calculate new segments
        context.raw_roads = self.create_segments(rds, transformer, context.raw_roads)

        return context

    # inspired by https://stackoverflow.com/questions/62283718/how-to-extract-a-profile-of-value-from-a-raster-along-a-given-line
    def create_segments(self, rds: rasterio.io.DatasetReader, transformer: Transformer,
                        raw: gpd.geodataframe.GeoDataFrame) -> gpd.geodataframe.GeoDataFrame:
        # get relevant band
        band = rds.read(self.band)

        # min resolution to split - half of the hypotenuse of the resolution triangle will render a very good minimum
        # resolution threshold
        min_resolution = math.sqrt(math.pow(rds.res[0], 2) + math.pow(rds.res[1], 2)) / 2

        if raw is not None and len(raw):
            counter = 0

            for idx, row in raw.iterrows():
                if 'geom' in row:
                    g = row.geom
                    changed = False
                    geom = []

                    if g and g.coords:
                        last_coord = None
                        for coord in g.coords:
                            if last_coord is not None:
                                # guess resolution
                                line = LineString([last_coord, coord])
                                leg = sp_ops.transform(transformer.transform, line)
                                # too short for splitting? just add coordinate (and possibly the first one, too)
                                if leg.length < min_resolution:
                                    if len(geom) == 0:
                                        geom.append(last_coord)
                                    geom.append(coord)
                                    continue

                                # not too short: create segments
                                # how many points to create?
                                changed = True
                                points_to_create = math.ceil(leg.length / min_resolution)

                                for i in range(points_to_create):
                                    point = leg.interpolate(i / points_to_create - 1., normalized=True)
                                    # access the nearest pixel in the rds
                                    x, y = rds.index(point.x, point.y)
                                    t_x, t_y = transformer.transform(point.x, point.y,
                                                                     direction=TransformDirection.INVERSE)
                                    # added already? might happen in some cases, if our resolution is too dense - in
                                    # this case, we skip the point
                                    if len(geom) and t_x == geom[-1][0] and t_y == geom[-1][1]:
                                        continue

                                    # get height
                                    height = band[x, y]
                                    # transform back and add point
                                    geom.append((t_x, t_y, height))

                            last_coord = coord

                        if changed:
                            raw.at[idx, 'geom'] = shape({"type": row.geom.geom_type, "coordinates": geom})
                            counter += 1

            if logger.level <= logging.INFO:
                logger.info("Segmented %d/%d paths", counter, len(raw))

        return raw

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "GeoTIFFCreateSegmentedPaths"
