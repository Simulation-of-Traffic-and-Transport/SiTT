# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Segment rivers and water bodies - this uses Postgis and Geos >= 3.11.0 and is much faster than the Python only version.
"""

import argparse
from urllib import parse

from sqlalchemy import create_engine, text

if __name__ == "__main__":
    """Segment rivers and water bodies - this uses Postgis and Geos >= 3.11.0."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Segment rivers and water bodies - this uses Postgis and Geos >= 3.11.0.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

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

    print("Converting shapes...")

    # truncate
    conn.execute(text("TRUNCATE TABLE sitt.water_parts"))
    conn.commit()

    # ids of water bodies that are connected to harbor hubs
    water_body_geoms = {}  # keeps geometries
    water_body_rivers = {}  # keeps is_rivers

    # get all harbors and get nearest water bodies
    for harbor in conn.execute(text("SELECT geom FROM sitt.hubs WHERE harbor = true")):
        result = conn.execute(text(f"SELECT id, geom, is_river FROM sitt.water_bodies ORDER BY ST_DistanceSpheroid(geom, '{harbor[0]}') LIMIT 1")).fetchone()
        if result[0] not in water_body_geoms:
            water_body_geoms[result[0]] = result[1]
            water_body_rivers[result[0]] = result[2]

    print(f"Considering {len(water_body_geoms)} water bodies near harbors.")

    # read water body entries
    for body_id in water_body_geoms:
        # skip lakes for now
        if not water_body_rivers[body_id]:
            print("Skipping non-river water body", body_id)
            continue

        print("Segmenting water body", body_id)

        geom = water_body_geoms[body_id]
        is_river = water_body_rivers[body_id]

        # split the water body into triangles
        parts = conn.execute(text("SELECT (ST_dump(ST_TriangulatePolygon('" + geom + "'))).geom"))
        count = 0
        for part in parts:
            # We force 2D, because this will cause less troubles later on. We will recalculate the 3D model later on.
            conn.execute(text(f"INSERT INTO sitt.water_parts (geom, water_body_id, is_river) VALUES (ST_Force2D('{part[0]}'), {body_id}, {is_river})"))
            count += 1

        conn.commit()
        print("Wrote", count, "parts for body", body_id)

    print("Done.")
