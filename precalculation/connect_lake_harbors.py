# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Connect lake harbors via edges.
"""

import argparse
from urllib import parse

from extremitypathfinder import PolygonEnvironment
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from shapely import wkb, is_ccw, \
    contains, LineString, Polygon, force_2d
from shapely.ops import nearest_points, transform
from sqlalchemy import create_engine, Table, Column, MetaData, \
    String, Float, JSON, text, insert

if __name__ == "__main__":
    """Connect lake harbors via edges."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Connect lake harbors via edges.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('-f', '--crs-from', dest='crs_from', default=4326, type=int, help='projection source')
    parser.add_argument('-t', '--crs-to', dest='crs_to', default=32633, type=int,
                        help='projection target (should support meters)')
    parser.add_argument('--xy', dest='always_xy', default=True, type=bool, help='use the traditional GIS order')
    parser.add_argument('-c', '--cost', dest='cost_factor', default=1., type=float,
                        help='cost factor for edges (multiplied by length in meters)')

    parser.add_argument('--empty-edges', dest='empty_edges', default=False, type=bool,
                        help='empty edges database before import')
    parser.add_argument('--delete-roads', dest='delete_roads', default=True, type=bool,
                        help='delete roads edges from database before import')

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

    # create transformer
    transformer = Transformer.from_crs(args.crs_from, args.crs_to, always_xy=args.always_xy)

    # define edge table
    idCol = Column('id', String, primary_key=True)
    geom_col = Column('geom', Geometry)
    hub_id_a = Column('hub_id_a', String)
    hub_id_b = Column('hub_id_b', String)
    edge_type = Column('type', String)
    cost_a_b = Column('cost_a_b', Float)
    cost_b_a = Column('cost_b_a', Float)
    data_col = Column('data', JSON)
    edges_table = Table("edges", MetaData(), idCol, geom_col, hub_id_a, hub_id_b, edge_type, cost_a_b,
                        cost_b_a, data_col, schema=args.schema)

    print("Connecting lake harbors...")

    # Truncate edges?
    if args.empty_edges:
        print("Truncating edges database...")
        conn.execute(text("TRUNCATE TABLE " + args.schema + ".edges;"))
        conn.commit()
    elif args.delete_roads:  # delete roads from edges table?
        print("Deleting lake edges from database...")
        conn.execute(text("DELETE FROM " + args.schema + ".edges WHERE type = 'lake';"))
        conn.commit()

    # ids of water bodies that are connected to harbor hubs
    water_body_geoms = {}  # keeps geometries
    harbors_for_water_bodies = {}  # keeps is_rivers

    # get all harbors and get nearest water bodies
    for harbor in conn.execute(text("SELECT id, geom FROM sitt.hubs WHERE harbor = true")):
        result = conn.execute(text(f"SELECT id, geom, is_river FROM sitt.water_bodies ORDER BY ST_DistanceSpheroid(geom, '{harbor[1]}') LIMIT 1")).fetchone()
        # add geometry to list
        if result[0] not in water_body_geoms and result[2] is False:
            water_body_geoms[result[0]] = result[1]
        # add harbor to list
        if result[0] not in harbors_for_water_bodies:
            harbors_for_water_bodies[result[0]] = []
        harbor_geom = wkb.loads(harbor[1])
        harbors_for_water_bodies[result[0]].append((harbor[0], force_2d(harbor_geom), harbor_geom.z))

    print(f"Considering {len(water_body_geoms)} water bodies near harbors.")

    # read water body entries
    for body_id in water_body_geoms:
        print("Checking water body", body_id, "with", len(harbors_for_water_bodies[body_id]), "harbors.")

        # we need at least two harbors
        if len(harbors_for_water_bodies[body_id]) < 2:
            continue

        geom = wkb.loads(water_body_geoms[body_id])
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
        # TODO: Check, there is something to do, our path does not look right...
        # open issue on this matter: https://github.com/jannikmi/extremitypathfinder/issues/84

        # now get the shortest paths between all points
        harbors = harbors_for_water_bodies[body_id]
        for i in range(len(harbors)):
            for j in range(i + 1, len(harbors)):
                print("Adding edge from", harbors[i][0], "to", harbors[j][0])
                # get the points
                p1 = harbors[i][1]  # points 2d
                p2 = harbors[j][1]
                h1 = harbors[i][2]  # heights
                h2 = harbors[j][2]

                # ensure that the points are inside the polygon
                if not contains(geom, p1):
                    p1, _ = nearest_points(geom, p1)
                if not contains(geom, p2):
                    p2, _ = nearest_points(geom, p2)

                # get the shortest path
                path_2d, _ = environment.find_shortest_path(list(p1.coords)[0], list(p2.coords)[0])
                # create heights
                path = []
                for idx, point in enumerate(path_2d):
                    # first half of the lake gets same height as first point
                    if idx / len(path_2d) < 0.5:
                        path.append((point[0], point[1], h1))
                    else:  # second half gets same height as second point
                        path.append((point[0], point[1], h2))

                # ensure connection to hubs
                if p1 != harbors[i][1]:
                    path.insert(0, (harbors[i][1].x, harbors[i][1].y, h1))
                if p2 != harbors[j][1]:
                    path.append((harbors[j][1].x, harbors[j][1].y, h2))
                shortest_path = LineString(path)

                # calculate length in meters
                base_length = transform(transformer.transform, shortest_path).length
                cost = base_length * args.cost_factor

                geo_stmt = WKTElement(shortest_path.wkt, srid=args.crs_from)
                edge_id = f"lake-{body_id}-{harbors[i][0]}-{harbors[j][0]}"

                # now, enter into edges table
                stmt = insert(edges_table).values(id=edge_id, geom=geo_stmt, hub_id_a=harbors[i][0],
                                                  hub_id_b=harbors[j][0], type='lake', cost_a_b=cost, cost_b_a=cost,
                                                  data={"length_m": base_length, "water_body_id": body_id})
                conn.execute(stmt)

    conn.commit()
    print("Done.")
