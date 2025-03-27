# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to calculate river slopes (inclines) using hub heights (z-heights). We use a simplified method to
calculate the slopes. We simply take the start and end points of each river path and calculate the slope for the whole
segment. Otherwise, we would need a much more accurate height model of the ancient topography.
"""
import argparse

from os.path import abspath
import numpy as np
import psycopg2
import rasterio
import requests
from pyproj import Transformer
from shapely import wkb, MultiPoint
from shapely.ops import transform

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to validate river directions and hub heights - are rivers flowing upwards?",
        exit_on_error=False)

    # Postgis settings
    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

    parser.add_argument('--overwrite', dest='overwrite', default=False, type=bool, help='overwrite existing slopes')

    # table settings
    parser.add_argument('-rt', '--river-table', dest='river_table', default='topology.recrivers', type=str, help='river table to check')
    parser.add_argument('-rc', '--river-id-column', dest='river_id_column', default='recroadid', type=str, help='river id column')
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom', type=str, help='river geometry column')
    parser.add_argument('-ra', '--river-huba-column', dest='river_a_column', default='hubaid', type=str, help='river hub a column')
    parser.add_argument('-rb', '--river-hubb-column', dest='river_b_column', default='hubbid', type=str, help='river hub b column')
    parser.add_argument('-rs', '--river-slope-column', dest='river_slope_column', default='slope', type=str, help='river slope column')

    parser.add_argument('-ht', '--hub-table', dest='hub_table', default='topology.rechubs', type=str, help='hub table to check')
    parser.add_argument('-hc', '--hub-id-column', dest='hub_id_column', default='rechubid', type=str, help='hub id column')
    parser.add_argument('-hg', '--hub-geo-column', dest='hub_geo_column', default='geom', type=str, help='hub geometry column')

    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')
    parser.add_argument('--crs-target', dest='crs_target', default=32633, type=int, help='projection target (has to support meters)')
    parser.add_argument('--segment-max', dest='max_segment', default=2000., type=float, help='segment size for sampling points along river paths (in meters)')

    # GeoTIFF settings
    parser.add_argument('-i', '--input-file', dest='file', type=str, help='input file (GeoTIFF)')
    parser.add_argument('-b', '--band', dest='band', default=1, type=int, help='band to use from GeoTIFF')

    # Google Geolocation API settings
    parser.add_argument('--google-api-key', dest='google_api_key', default='', type=str, help='Google API key for elevation data (if needed)')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # load GeoTIFF, if defined
    if args.file:
        rds: rasterio.io.DatasetReader = rasterio.open(abspath(args.file))
        rds_transformer = Transformer.from_crs(args.crs_source, rds.crs, always_xy=True)
        # get relevant band
        band = rds.read(args.band)

    # define projection
    project_forward = Transformer.from_crs(args.crs_source, args.crs_target, always_xy=True).transform
    project_back = Transformer.from_crs(args.crs_target, args.crs_source, always_xy=True).transform

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password, port=args.port)
    cur = conn.cursor()
    cur1 = conn.cursor() # second cursor
    cur_upd = conn.cursor() # update cursor

    # traverse river data
    stmt = f"select {args.river_id_column}, {args.river_a_column}, {args.river_b_column}, {args.river_geo_column} from {args.river_table}"
    if not args.overwrite:
        stmt += f" WHERE {args.river_slope_column} IS NULL OR {args.river_slope_column} < 0"

    cur.execute(stmt)
    for data in cur:
        # sample points
        path = wkb.loads(data[3])

        # path in m projection
        path_m = transform(project_forward, path)

        # see https://gis.stackexchange.com/questions/416284/splitting-multiline-or-linestring-into-equal-segments-of-particular-length-using
        # generate the equidistant points
        distances = np.arange(0, path_m.length, args.max_segment)
        points_m = MultiPoint([path_m.interpolate(distance) for distance in distances] + [path_m.boundary.geoms[1]])
        # transform back to lat/lon
        points = transform(project_back, points_m)

        # get heights from GeoTIFF or Google API
        heights = np.zeros(len(points.geoms))
        if args.file:
            for i, point in enumerate(points.geoms):
                xx, yy = rds_transformer.transform(point.x, point.y)
                x, y = rds.index(xx, yy)
                height = band[x, y]

                print(i, point.x, point.y, height)

        # TODO: complete
        # first test showed that both 1 or 2 km segments can still lead to negative slopes (uprunning rivers), so I
        # will not continue this approach.

        exit(0)

        # # TODO: sample segments and check this!
        #
        # # get heights from hub
        # cur1.execute(f"select {args.hub_geo_column} from {args.hub_table} WHERE {args.hub_id_column} = %s", (data[1],))
        # res = cur1.fetchone()
        # if res is None:
        #     print(f"Hub {data[1]} not found in hub table - hub a of {data[0]}!")
        #     continue  # skip if hub bid not found in rechubs table
        # point_a: Point = wkb.loads(res[0])
        #
        # cur1.execute(f"select {args.hub_geo_column} from {args.hub_table} WHERE {args.hub_id_column} = %s", (data[2],))
        # res = cur1.fetchone()
        # if res is None:
        #     print(f"Hub {data[2]} not found in hub table - hub b of {data[0]}!")
        #     continue  # skip if hub bid not found in rechubs table
        # point_b: Point = wkb.loads(res[0])
        #
        # # calculate length of river section by projecting and calculating distance
        # length: float = transform(project_forward, wkb.loads(data[3])).length
        #
        # slope = abs(point_a.z - point_b.z) / length
        # print(f"{data[0]} => Hub A: {point_a.z}, Hub B: {point_b.z}, length: {length}, slope: {slope}")
        # # cur1.execute(f"UPDATE {args.river_table} SET {args.river_slope_column} = %s WHERE {args.river_id_column} = %s", (slope, data[0],))
        # exit(0)

    conn.commit()