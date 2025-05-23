# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to calculate river slopes (inclines) using hub heights (z-heights). We use a simplified method to
calculate the slopes. We simply take the start and end points of each river path and calculate the slope for the whole
segment. Otherwise, we would need a much more accurate height model of the ancient topography.
"""
import argparse

import numpy as np
import pandas as pd
import psycopg2
from openpyxl.worksheet.dimensions import Dimension
from pyproj import Transformer
from shapely import wkb, Point
from shapely.errors import DimensionError
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
    parser.add_argument('-hh', '--hub-height-column', dest='hub_height_column', default='', type=str, help='hub height column (if applicable, takes height from geometry, if empty)')

    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')
    parser.add_argument('--crs-target', dest='crs_target', default=32633, type=int, help='projection target (has to support meters)')

    # export settings
    parser.add_argument('--export', dest='export', default=False, type=bool, help='Export statistics to a Excel file (openpyxl library required)')
    parser.add_argument('--export-name', dest='export_name', default="river_routes_slopes.xlsx", type=str, help='Export file name')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # define projection
    project_forward = Transformer.from_crs(args.crs_source, args.crs_target, always_xy=True).transform

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password, port=args.port)
    cur = conn.cursor()
    cur1 = conn.cursor() # second cursor
    cur_upd = conn.cursor() # update cursor

    # traverse river data
    stmt = f"select {args.river_id_column}, {args.river_a_column}, {args.river_b_column}, {args.river_geo_column} from {args.river_table}"
    if not args.overwrite:
        stmt += f" WHERE {args.river_slope_column} IS NULL OR {args.river_slope_column} < 0"

    # gather list of slopes
    slopes = []

    # prepare SQL query for hub heights
    if args.hub_height_column:
        query = f"SELECT {args.hub_geo_column}, {args.hub_height_column} FROM {args.hub_table} WHERE {args.hub_id_column} = %s"
    else:
        query = f"SELECT {args.hub_geo_column} FROM {args.hub_table} WHERE {args.hub_id_column} = %s"

    cur.execute(stmt)
    for data in cur:
        # get heights from hub
        cur1.execute(query, (data[1],))
        res = cur1.fetchone()
        if res is None:
            print(f"Hub {data[1]} not found in hub table - hub a of {data[0]}!")
            continue  # skip if hub bid not found in rechubs table
        point_a: Point = wkb.loads(res[0])
        if args.hub_height_column:
            height_a: float = float(res[1])
        else:
            height_a: float = point_a.z

        cur1.execute(query, (data[2],))
        res = cur1.fetchone()
        if res is None:
            print(f"Hub {data[2]} not found in hub table - hub b of {data[0]}!")
            continue  # skip if hub bid not found in rechubs table
        point_b: Point = wkb.loads(res[0])
        if args.hub_height_column:
            height_b: float = float(res[1])
        else:
            height_b: float = point_a.z

        # calculate length of river section by projecting and calculating distance
        length: float = transform(project_forward, wkb.loads(data[3])).length

        try:
            slope = abs(height_a - height_b) / length
            print(f"{data[0]} => Hub A: {height_a}, Hub B: {height_b}, length: {length}, slope: {slope}")
            cur1.execute(f"UPDATE {args.river_table} SET {args.river_slope_column} = %s WHERE {args.river_id_column} = %s", (slope, data[0],))
        except DimensionError as e:
            print(f"Invalid geometry for river {data[0]} {e}: Point A {point_a} ({data[1]}, height: {height_a}),  Point B {point_b} ({data[2]}, height: {height_b})!")
            exit(0)

        slopes.append(slope)

    conn.commit()

    # show statistics
    slopes = np.array(slopes)
    print("\nStatistics:")
    print("Average slope:", slopes.mean())
    print("Maximum slope:", slopes.max())
    print("Minimum slope:", slopes.min())
    print("Median slope:", np.median(slopes))

    if args.export:
        df = pd.DataFrame(slopes)
        df.to_excel(args.export_name)
        print("Statistics exported to", args.export_name)
