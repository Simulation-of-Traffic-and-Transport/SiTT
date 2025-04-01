# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Take a GeoTIFF and create heights for all coordinates in the database."""

import argparse
import math
import os
from urllib import parse

import requests
import geopandas as gpd
import rasterio
from pyproj import Transformer
from pyproj.enums import TransformDirection
from shapely import LineString, ops
from shapely.geometry import shape
from sqlalchemy import create_engine, Table, Column, MetaData, \
    String, select, text, update



def work_coordinates(coords) -> tuple[list, bool]:
    changed = False
    geom = []

    for coord in coords:
        # keep?
        if args.keep_existing and len(coord) > 2 and coord[2] > 0:
            continue

        lng = coord[0]
        lat = coord[1]

        height = get_height_for_coordinate((lng, lat))

        geom.append((lng, lat, height))
        changed = True

    return geom, changed


def get_height_for_coordinate(coord: tuple[float, float]) -> float:
    # get height for coordinate
    xx, yy = transformer.transform(coord[0], coord[1])
    x, y = rds.index(xx, yy)
    height = band[x, y]

    # Fallback?
    if height < -1000. and args.google_api_key:
        # TODO: combine more than one coordinate in one request to make this way cheaper
        response = requests.get(f"https://maps.googleapis.com/maps/api/elevation/json?locations={coord[1]}%2C{coord[0]}&key={args.google_api_key}")
        data = response.json()

        # print(data)
        return data['results'][0]['elevation']

    return height


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


if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="Take a GeoTIFF and create heights for all coordinates in the database.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection source')
    parser.add_argument('--xy', dest='always_xy', default=True, type=bool, help='use the traditional GIS order')
    parser.add_argument('-i', '--input-file', dest='file', required=True, type=str, help='input file (GeoTIFF)')
    parser.add_argument('-b', '--band', dest='band', default=1, type=int, help='band to use from GeoTIFF')
    parser.add_argument('-k', '--keep', dest='keep_existing', default=True, type=bool,
                        help='keep existing coordinates (overwrite otherwise)')
    parser.add_argument('-S', '--create-segments', dest='create_segments', default=False, type=bool,
                        help='create new input coordinates for roads by splitting the line into segments based on height tiles; improves the heights a bit, probably not needed if your input data is pretty good anyway')
    parser.add_argument('--google-api-key', dest='google_api_key', default='', type=str, help='Google API key for elevation data (if needed)')

    parser.add_argument('-t', '--tables', dest='tables', type=str, nargs='+', default='all', help='tables to update',
                        choices=['all', 'hubs', 'roads', 'water_bodies', 'water_lines'])

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # check file exists
    if not os.path.exists(args.file):
        print("File does not exist: " + args.file)
        parser.exit(1)

    # load geo
    rds: rasterio.io.DatasetReader = rasterio.open(os.path.abspath(args.file))
    transformer = Transformer.from_crs(args.crs_no, rds.crs, always_xy=args.always_xy)
    # get relevant band
    band = rds.read(args.band)

    # which tables should be updated
    tables = args.tables
    if tables == 'all' or 'all' in tables:
        tables = ['hubs', 'roads']  # we skip these for now, because 2D is easier for water: 'water_bodies', 'water_lines'

    # connect to database
    conn = create_engine('postgresql://' + args.user + ':' + parse.quote_plus(args.password) + '@' + args.server + ':' +
                         str(args.port) + '/' + args.database).connect()

    print("GeoTIFF loaded, band loaded, database connected - working...")

    for table in tables:
        print("Updating " + table)

        # get hubs - create statement via sql alchemy
        idCol = Column('id')
        geomCol = Column('geom')
        t = Table(table, MetaData(), idCol, geomCol, schema=args.schema)
        s = select(idCol, geomCol).select_from(t)
        data = gpd.GeoDataFrame.from_postgis(str(s.compile()), conn,
                                             geom_col='geom',
                                             index_col='id')

        counter = 0
        total = len(data)

        for idx, row in data.iterrows():
            if 'geom' in row:
                g = row.geom
                changed_any = False

                # complex shapes, like Polygons, MultiPolygons
                if g.geom_type == 'Polygon' or g.geom_type == 'MultiPolygon':
                    all_coords = []

                    # get coordinates for exterior ring
                    new_coords, changed = work_coordinates(g.exterior.coords)
                    all_coords.append(new_coords)
                    if changed:
                        changed_any = True

                    for interior in g.interiors:
                        new_coords, changed = work_coordinates(interior.coords)
                        all_coords.append(new_coords)
                        if changed:
                            changed_any = True

                    if changed_any:
                        # create SQL statement
                        new_shape = shape({"type": row.geom.geom_type, "coordinates": all_coords})

                # simple shapes, like Points, etc.
                elif g and g.coords:
                    if g.geom_type == 'LineString' and args.create_segments:
                        new_coords = create_segments(g.coords)
                        changed_any = True
                    else:
                        new_coords, changed_any = work_coordinates(g.coords)
                    new_shape = shape({"type": row.geom.geom_type, "coordinates": new_coords})

                # any change?
                if changed_any:
                    # create SQL statement
                    new_value = text(String().literal_processor(dialect=conn.dialect)(
                        value="SRID=" + str(args.crs_no) + ";" + str(new_shape)))
                    stmt = update(t).where(idCol == row.name).values(geom=new_value)
                    conn.execute(stmt)
                    counter += 1

        conn.commit()
        print(f"Updated {counter}/{total} coordinates in {table}.")

print("Done.")
