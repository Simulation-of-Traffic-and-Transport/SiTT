# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to calculate river inclinations (slopes) using hub heights (z-heights).
"""
import argparse

import psycopg2
from pyproj import Transformer
from shapely import wkb, Point
from shapely.ops import transform

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to validate river directions and hub heights - are rivers flowing upwards maybe?",
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
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom', type=str, help='river geometry column')
    parser.add_argument('-ra', '--river-huba-column', dest='river_a_column', default='hubaid', type=str, help='river hub a column')
    parser.add_argument('-rb', '--river-hubb-column', dest='river_b_column', default='hubbid', type=str, help='river hub b column')
    parser.add_argument('-rd', '--river-inclination-column', dest='river_inclination_column', default='inclination', type=str, help='river inclination column')

    parser.add_argument('-ht', '--hub-table', dest='hub_table', default='topology.rechubs', type=str, help='hub table to check')
    parser.add_argument('-hc', '--hub-id-column', dest='hub_id_column', default='rechubid', type=str, help='hub id column')
    parser.add_argument('-hg', '--hub-geo-column', dest='hub_geo_column', default='geom', type=str, help='hub geometry column')

    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')
    parser.add_argument('--crs-target', dest='crs_target', default=32633, type=int, help='projection target (has to support meters)')

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
    cur.execute(f"select {args.river_id_column}, {args.river_a_column}, {args.river_b_column}, {args.river_geo_column} from {args.river_table} WHERE {args.river_inclination_column} IS NULL OR {args.river_inclination_column} < 0")
    for data in cur:
        # get heights from hub
        cur1.execute(f"select {args.hub_geo_column} from {args.hub_table} WHERE {args.hub_id_column} = %s", (data[1],))
        res = cur1.fetchone()
        if res is None:
            print(f"Hub {data[1]} not found in hub table - hub a of {data[0]}!")
            continue  # skip if hub bid not found in rechubs table
        point_a: Point = wkb.loads(res[0])

        cur1.execute(f"select {args.hub_geo_column} from {args.hub_table} WHERE {args.hub_id_column} = %s", (data[2],))
        res = cur1.fetchone()
        if res is None:
            print(f"Hub {data[2]} not found in hub table - hub b of {data[0]}!")
            continue  # skip if hub bid not found in rechubs table
        point_b: Point = wkb.loads(res[0])

        # calculate length of river section by projecting and calculating distance
        length: float = transform(project_forward, wkb.loads(data[3])).length

        print(f"{data[0]} => Hub A: {point_a.z}, Hub B: {point_b.z}, length: {length}")
        slope = abs(point_a.z - point_b.z) / length
        cur1.execute(f"UPDATE {args.river_table} SET {args.river_inclination_column} = %s WHERE {args.river_id_column} = %s", (slope, data[0],))

    conn.commit()