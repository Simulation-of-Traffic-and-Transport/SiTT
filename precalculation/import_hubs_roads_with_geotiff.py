# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import hubs and roads from another database."""
import math
import os
from urllib import parse

import rasterio
from pyproj import Transformer
from pyproj.enums import TransformDirection
from shapely import wkb, Point, LineString, ops
from sqlalchemy import create_engine, text

# This is an example file - we will interpret data created by archeologists and put it into out sitt schema.

# --------------- settings ---------------
db_user = 'postgres'
db_password = '12345'
db_host = 'localhost'
db_port = 5432
db_name = 'sitt'

geo_tiff_path = '../data/DTM Austria 10m v2 by Sonny.tif'
geo_tiff_band = 1  # band number to use from GeoTIFF
segment_paths = True  # set to false in order to leave paths as they are
crs_no = 4326  # projection CRS number

# ----------------------------------------


def get_height_for_coordinate(coord: tuple[float, float]) -> float:
    # get height for coordinate
    xx, yy = transformer.transform(coord[0], coord[1])
    x, y = rds.index(xx, yy)
    return band[x, y]


# inspired by https://stackoverflow.com/questions/62283718/how-to-extract-a-profile-of-value-from-a-raster-along-a-given-line
def create_segments(coords: list[tuple[float, float]]) -> list[tuple[float, float, float]]:
    # min resolution to split - half of the hypotenuse of the resolution triangle will render a very good minimum
    # resolution threshold
    min_resolution = math.sqrt(math.pow(rds.res[0], 2) + math.pow(rds.res[1], 2)) / 2

    ret_coords: list[tuple[float, float, float]] = []
    last_coord = None

    for coord in coords:
        if last_coord is not None:
            # guess resolution
            line = LineString([last_coord, coord])
            leg = ops.transform(transformer.transform, line)
            # too short for splitting? just add coordinate (and possibly the first one, too)
            if leg.length < min_resolution:
                if len(ret_coords) == 0:
                    ret_coords.append((last_coord[0], last_coord[1], get_height_for_coordinate(last_coord),))
                ret_coords.append((coord[0], coord[1], get_height_for_coordinate(coord),))
                continue

            # not too short: create segments
            # how many points to create?
            points_to_create = math.ceil(leg.length / min_resolution)

            for i in range(points_to_create):
                point = leg.interpolate(i / points_to_create - 1., normalized=True)
                # access the nearest pixel in the rds
                x, y = rds.index(point.x, point.y)
                t_x, t_y = transformer.transform(point.x, point.y,
                                                 direction=TransformDirection.INVERSE)
                # added already? might happen in some cases, if our resolution is too dense - in
                # this case, we skip the point
                if len(ret_coords) and t_x == ret_coords[-1][0] and t_y == ret_coords[-1][1]:
                    continue

                # get height
                height = band[x, y]
                # transform back and add point
                ret_coords.append((t_x, t_y, height))

        last_coord = coord

    return ret_coords


def parse_yes_no_entry(s: str) -> bool:
    """Parse a yes/no entry."""
    v = s.lower()
    if v == 'y' or v == 'yes' or v == 'p':  # p == probably, we take it as true
        return True
    return False


def clean_coords(coords: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Clean coordinates by weeding out duplicate values (zero length legs)."""
    new_coords = []
    last_coord = None
    for coord in coords:
        if last_coord is None or last_coord != coord:
            new_coords.append(coord)
        last_coord = coord
    return new_coords


# load GeoTIFF
rds: rasterio.io.DatasetReader = rasterio.open(os.path.abspath(geo_tiff_path))
transformer = Transformer.from_crs(crs_no, rds.crs, always_xy=True)
# get relevant band
band = rds.read(geo_tiff_band)

# connect to database
conn = create_engine('postgresql://' + db_user + ':' + parse.quote_plus(db_password) + '@' + db_host + ':' + str(
    db_port) + '/' + db_name).connect()

# --------------- get hub data from source ---------------
for result in conn.execute(text("SELECT rechubid, geom, harbor, overnight FROM topology.rechubs")):
    # get column data
    hub_id = result[0]
    geom = result[1]
    harbor = parse_yes_no_entry(result[2])
    overnight = parse_yes_no_entry(result[3])
    market = False  # just take fixed value for now
    # we have only one coordinate for hubs
    coord = wkb.loads(geom).coords[0]

    point = "SRID=" + str(crs_no) + ";" + str(Point(coord[0], coord[1], get_height_for_coordinate(coord)))

    conn.execute(text(
        f"INSERT INTO sitt.hubs (id, geom, overnight, harbor, market) VALUES ('{hub_id}', '{point}', {overnight}, {harbor}, {market});"))

conn.commit()

# -------------- get road data from source ---------------
for result in conn.execute(text("SELECT recroadid, hubaid, hubbid, geom FROM topology.recroads")):
    # get column data
    road_id = result[0]
    hubaid = result[1]
    hubbid = result[2]
    geom = result[3]
    coords = clean_coords(wkb.loads(geom).coords)

    if segment_paths:
        coords = create_segments(coords)
    else:
        # segment legs only
        new_coords = []
        for coord in coords:
            new_coords.append((coord[0], coord[1], get_height_for_coordinate(coord)))
        coords = new_coords

    new_geom = "SRID=" + str(crs_no) + ";" + str(LineString(coords))

    conn.execute(text(
        f"INSERT INTO sitt.roads (id, geom, hub_id_a, hub_id_b) VALUES ('{road_id}', '{new_geom}', '{hubaid}', '{hubbid}');"))

conn.commit()
