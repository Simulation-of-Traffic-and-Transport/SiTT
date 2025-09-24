# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""

"""
import argparse

import numpy as np
import pandas as pd
import psycopg2
from openpyxl.worksheet.dimensions import Dimension
from pyproj import Transformer
from shapely import wkb, Point, LineString
from shapely.errors import DimensionError
from shapely.ops import transform
import shapefile

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
    # table settings
    parser.add_argument('-rt', '--river-table', dest='river_table', default='topology.recrivers', type=str, help='river table to check')
    parser.add_argument('-rc', '--river-id-column', dest='river_id_column', default='recroadid', type=str, help='river id column')
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom_segments', type=str, help='river geometry column')
    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password,
                            port=args.port)
    cur = conn.cursor()
    cur2 = conn.cursor()

    # error file
    we = shapefile.Writer(target='river_widths_errors', shapeType=shapefile.POINT, autoBalance=True)
    we.field("reason", "C")

    # load all river paths
    cur.execute(f"select {args.river_id_column}, {args.river_geo_column} from {args.river_table}")
    for data in cur:
        recroadid: str = data[0]
        path: LineString = wkb.loads(data[1])

        # skip first and last points - they are our start and end points - they can touch a shore or be outside the river
        for i in range(1, len(path.coords) - 1):
            coord = path.coords[i]
            p = Point(coord[0], coord[1])
            wkb_point = wkb.dumps(p, srid=args.crs_source)

            # TODO: This can be done much faster in PostGIS using built-in functions
            # check if the point is within the river
            cur2.execute("SELECT ST_Contains(geom, %s) FROM water_wip.all_river_body", (wkb_point,))
            if not cur2.fetchone()[0]:
                print("Warning... point outside river", p)
                we.point(coord[0], coord[1])
                we.record("point outside river")
                continue

            # # find the closest point on the shoreline of any river
            # cur2.execute("SELECT ST_ClosestPoint(geom, %s) FROM water_wip.all_water_lines", (wkb_point,))
            # # we expect at exactly one result - and we assume that the point is within the river
            # closest_point: Point = wkb.loads(cur2.fetchone()[0])
            # if closest_point is None:
            #     print("Warning... closest_point", recroadid)
            #     # TODO: handle this
            #     continue

    we.close()