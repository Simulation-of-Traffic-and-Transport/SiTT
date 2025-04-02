# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to create lake routes from hubs and water bodies.
"""
import argparse

import psycopg2
from extremitypathfinder import PolygonEnvironment
from shapely import wkb, Polygon, LineString, force_2d, is_ccw, contains
from shapely.ops import nearest_points
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

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password, port=args.port)
    cur = conn.cursor()
    cur1 = conn.cursor() # second cursor

    # find harbors along rivers
    water_bodies = {}
    harbors_for_water_bodies = {}
    # get all harbors and get nearest water bodies
    cur.execute("SELECT rechubid, geom FROM topology.rechubs WHERE harbor = 'y'")
    for harbor in cur:
        cur1.execute(f"SELECT id, geom, is_river FROM sitt.water_bodies ORDER BY ST_DistanceSpheroid(geom, '{harbor[1]}') LIMIT 1")
        result = cur1.fetchone()
        # river?
        if result[2]:
            continue

        if result[0] not in harbors_for_water_bodies:
            harbors_for_water_bodies[result[0]] = []
            water_bodies[result[0]] = wkb.loads(result[1])
        harbor_geom = wkb.loads(harbor[1])
        harbors_for_water_bodies[result[0]].append((harbor[0], force_2d(harbor_geom)))

    # create shapefile to check later
    w = shapefile.Writer(target='lake_routes', shapeType=shapefile.POLYLINE, autoBalance=True)
    w.field("id", "C")

    # now we have harbors and water bodies, let's find the routes
    for body_id in water_bodies:
        geom = water_bodies[body_id]
        harbors = harbors_for_water_bodies[body_id]

        print("Checking water body", body_id, "with", len(harbors), "harbors.")

        # we need at least two harbors
        if len(harbors_for_water_bodies[body_id]) < 2:
            continue

        if type(geom) is not Polygon:
            print("Geometry of water body", body_id, "is not a polygon. Skipping...")
            continue

        # create environment for shortest paths
        environment = PolygonEnvironment()
        shore_line = force_2d(geom.exterior)  # exterior hull
        if not is_ccw(shore_line):  # need to be counter-clockwise
            shore_line = shore_line.reverse()

        holes: list[tuple[float, float]] = []  # keeps holes
        for hole in force_2d(geom.interiors):
            if is_ccw(hole):  # need to be clockwise
                hole = hole.reverse()
            holes.append(list(hole.coords)[:-1])

        environment.store(list(shore_line.coords)[:-1], holes, validate=True)
        # TODO: If something does not right, look here: https://github.com/jannikmi/extremitypathfinder/issues/84

        # now get the shortest paths between all points
        for i in range(len(harbors)):
            for j in range(i + 1, len(harbors)):
                print("Adding edge from", harbors[i][0], "to", harbors[j][0])
                # get the points
                p1 = harbors[i][1]
                p2 = harbors[j][1]

                # ensure that the points are inside the polygon
                if not contains(geom, p1):
                    p1, _ = nearest_points(geom, p1)
                if not contains(geom, p2):
                    p2, _ = nearest_points(geom, p2)

                # get the shortest path
                path, _ = environment.find_shortest_path(list(p1.coords)[0], list(p2.coords)[0])

                # ensure connection to hubs
                if p1 != harbors[i][1]:
                    path.insert(0, (harbors[i][1].x, harbors[i][1].y, h1))
                if p2 != harbors[j][1]:
                    path.append((harbors[j][1].x, harbors[j][1].y, h2))
                shortest_path = LineString(path)

                edge_id = f"lake-{body_id}-{harbors[i][0]}-{harbors[j][0]}"

                print(edge_id, shortest_path)

                # now we could save this to database, or we persist it to a shapefile for manual processing, we choose the latter
                w.line([shortest_path.coords])
                w.record(edge_id)

    w.close()