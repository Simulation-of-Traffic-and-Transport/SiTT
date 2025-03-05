# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to create river segments in the database. Segment length can be set and is approximate. A length
of between 10 is a reasonable default, because we will combine these segments into larger chunks later. The algorithm
uses PostGIS and not Shapely du to being more accurate when projecting points.
"""

import argparse

import psycopg2

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="This is an example how to create river segments in the database.",
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

    parser.add_argument('-l', '--segment-length', dest='segment_length', default=10., type=float, help='length of each segment in meters')

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
    cur_upd = conn.cursor()  # update cursor

    # check if geom_segments exists, create column if not
    schema, table = args.river_table.split('.')
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table}' AND table_schema= '{schema}' AND column_name = '{args.river_geo_column}_segments')")
    if not cur.fetchone()[0]:
        cur_upd.execute(f"ALTER TABLE {args.river_table} ADD {args.river_geo_column}_segments geography(LineStringZ, 4326)")
        print("Adding column for river segments...")

    # update segments column
    cur.execute(f"UPDATE {args.river_table} SET {args.river_geo_column}_segments = ST_Segmentize(ST_Force3D({args.river_geo_column})::geography, {args.segment_length})")
    conn.commit()
    print("Done.")