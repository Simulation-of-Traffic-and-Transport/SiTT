# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Segment rivers and water bodies - this uses Postgis but no Geos and is very slow.
"""

import argparse
from urllib import parse

from sqlalchemy import create_engine, text
from shapely import wkb, get_parts, prepare, destroy_prepared, is_ccw, \
    delaunay_triangles, contains, overlaps, intersection, STRtree, LineString, Polygon, MultiPolygon, Point, \
    relate_pattern, centroid, shortest_line


if __name__ == "__main__":
    """Segment rivers and water bodies - this uses Postgis but no Geos and is very slow."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Segment rivers and water bodies - this uses Postgis but no Geos and is very slow.",
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
        print("Segmenting water body", body_id)

        geom = wkb.loads(water_body_geoms[body_id])
        is_river = water_body_rivers[body_id]
        prepare(geom)

        # split the water body into triangles
        parts = get_parts(delaunay_triangles(geom))
        total = len(parts)
        c = 0
        for part in parts:
            c += 1
            if contains(geom, part):
                conn.execute(text(
                    f"INSERT INTO sitt.water_parts (geom, water_body_id, is_river) VALUES (ST_Force2D('{part}'), {body_id}, {is_river})"))
                print(c / total)
                conn.commit()
            elif overlaps(geom, part):
                pass
                sub_parts = get_parts(intersection(geom, part))
                for p in sub_parts:
                    if p.geom_type == 'Polygon':
                        conn.execute(text(
                            f"INSERT INTO sitt.water_parts (geom, water_body_id, is_river) VALUES (ST_Force2D('{p}'), {body_id}, {is_river})"))
                        print(c / total)
                        conn.commit()

        destroy_prepared(geom)

        print("Wrote", total, "parts for body", body_id)

    print("Done.")
