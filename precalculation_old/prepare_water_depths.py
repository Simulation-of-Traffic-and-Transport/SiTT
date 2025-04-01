# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Prepare water depths table - populate with data from segments for manual entering depths.
"""

import argparse
from urllib import parse

from sqlalchemy import create_engine, text

if __name__ == "__main__":
    """Prepare water depths table - populate with data from segments for manual entering depths."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Prepare water depths table - populate with data from segments for manual entering depths.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # connect to database
    conn = create_engine('postgresql://' + args.user + ':' + parse.quote_plus(args.password) + '@' + args.server + ':' +
                         str(args.port) + '/' + args.database).connect()

    print("Preparing water depths...")

    # delete temp table
    conn.execute(text("DROP TABLE IF EXISTS sitt.temp_water_lines"))
    conn.commit()

    print("Creating temp_water_lines table...")

    conn.execute(text("SELECT geom, water_body_id INTO sitt.temp_water_lines FROM (SELECT st_intersection(a.geom, b.geom) as geom, a.water_body_id FROM sitt.water_parts as a, sitt.water_parts as b WHERE a.is_river = 'y' AND b.is_river = 'y' AND ST_Touches(a.geom, b.geom)) as results WHERE st_geometrytype(geom) = 'ST_LineString'"))
    conn.commit()

    print("Getting centroids from parts_line table...")

    # now get centroids for all parts
    c = 0
    for point in conn.execute(text("SELECT water_body_id, st_astext(st_centroid(geom)) FROM sitt.temp_water_lines")):
        stmt = text(f"INSERT INTO sitt.water_depths (geom, water_body_id) VALUES ('SRID={args.crs_no};{point[1]}', {point[0]})")
        conn.execute(stmt)
        c += 1
        if c % 10000 == 0:
            print(f"{c}... done")

    conn.commit()
    print(f"Added {c} water depths into table as first step")

    # now we take the minimum point and delete all points that are too close (with 500m)
    done = False
    min_id = -1

    while not done:
        stmt = text(f"SELECT id, geom, water_body_id FROM sitt.water_depths WHERE id > {min_id} ORDER BY id LIMIT 1")
        result = conn.execute(stmt).fetchone()
        if result:
            print(f"Deleting around point {result[0]} of water body {result[2]}...")
            # find points around this one
            conn.execute(text(f"DELETE FROM sitt.water_depths WHERE st_distancespheroid(geom, '{result[1]}') < 500 AND water_body_id = {result[2]} AND id!= {result[0]}"))
            conn.commit()

            # set min_id to the next id
            min_id = result[0]
        else:
            done = True

    # delete temp table
    conn.execute(text("DROP TABLE IF EXISTS sitt.temp_water_lines"))
    conn.commit()

    print("Done.")
