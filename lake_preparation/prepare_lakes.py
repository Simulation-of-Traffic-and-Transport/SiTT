# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Take lake data and generate the paths on them. Additionally, some route calculations are done"""

import argparse
from urllib import parse
from typing import Tuple
from geoalchemy2 import Geometry
from shapely import wkb
from sqlalchemy import create_engine, Table, Column, schema, MetaData, Integer, text, Connection, Row
from math import sqrt


def run():
    """Runs the whole process"""

    print("Initializing database from",
          _create_connection_string(args.server, args.database, args.user, args.password, args.port, for_printing=True))
    conn = create_engine(
        _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()

    # ensure schemas
    if not conn.dialect.has_schema(conn, args.schema):
        conn.execute(schema.CreateSchema(args.schema))
        print("Created schema:", args.schema)
    else:
        print("Schema exists")

    # ensure tables
    lakes_table = _get_lakes_table()
    _get_lakes_outline_table()
    _get_routes_normal_mode_table()
    _get_routes_scale_mode_table()

    metadata_obj.create_all(bind=conn, checkfirst=True)
    conn.commit()

    # clean old routes data
    if args.mode == "normals":
        command = text(f"TRUNCATE {args.routes_table};")
        conn.execute(command)
        conn.commit()
    elif args.mode == "scale":
        command = text(f"TRUNCATE {args.routes_table_scale};")
        conn.execute(command)
        conn.commit()

    # make routes
    for lake in conn.execute(lakes_table.select()):
        if args.mode == "normals":
            _calculate_lakes_with_normals(conn, lake)
        elif args.mode == "scale":
            _calculate_routes_with_down_scale(conn, lake)


def _create_connection_string(server: str, db: str, user: str, password: str, port: int, for_printing=False):
    """
    Create DB connection string

    :param for_printing: hide password, so connection can be printed
    """
    if for_printing:
        return 'postgresql://' + user + ':***@' + server + ':' + str(
            port) + '/' + db
    else:
        return 'postgresql://' + user + ':' + parse.quote_plus(password) + '@' + server + ':' + str(port) + '/' + db


def _get_lakes_table() -> Table:
    return Table(args.lakes_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("st_asewkt", Geometry(srid=args.crs_no)),
                 schema=args.schema)


def _get_lakes_outline_table() -> Table:
    return Table(args.lakes_outline_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry(geometry_type="LINESTRING", srid=args.crs_no)),
                 schema=args.schema)


def _get_routes_normal_mode_table() -> Table:
    return Table(args.routes_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry(geometry_type="LINESTRING", srid=args.crs_no)),
                 schema=args.schema)


def _get_routes_scale_mode_table() -> Table:
    return Table(args.routes_table_scale, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry(geometry_type="LINESTRING", srid=args.crs_no)),
                 schema=args.schema)


# TODO: Just used tuples to test the calculations very quickly. In an improvement step the
# calculation maybe should be done with some more convenient data structure
def _calc_center(a: Tuple[float, float], b: Tuple[float, float]) -> Tuple[float, float]:
    """
    Calculates the center point of the vector from point a to b.

    :param a: point a
    :param b: point b
    :return: the center of the vector AB
    """
    x = (a[0] + b[0]) / 2
    y = (a[1] + b[1]) / 2
    return x, y


def _calculate_lakes_with_normals(conn: Connection, lake: Row):
    """

    Calculate a linestring showing the routes taken along the shore of the lake.
    This is done by calculating the normal of each edge representing the shore and translating
    the edge along the normal. This routine has some artifacts at the moment and should not
    be used. Prefer ``_calculate_routes_with_down_scale()`` instead

    :param conn: connection used to add the generated data to the database
    :param lake: lake data for which the routes will be generated
    """
    geom = wkb.loads(lake[1].desc)
    points_query = text(f"SELECT (ST_DumpPoints('{geom}')).geom;")
    points = conn.execute(points_query).fetchall()

    outline_points = ""
    routes_points = []

    for pointIndex in range(0, len(points) - 1):
        p0 = wkb.loads(points[pointIndex][0])
        p1 = wkb.loads(points[pointIndex + 1][0])

        # calculate the normal of P0P1
        dx = p1.x - p0.x
        dy = p1.y - p0.y
        length = sqrt(dx ** 2 + dy ** 2)
        # TODO: 0.0005 is just a factor to inset the edges uniformly. This should become
        # a parameter in a sophisticated implementation
        normal = ((dy / length) * 0.0005, (-dx / length) * 0.0005)

        outline_points += f"{p0.x} {p0.y}, {p1.x} {p1.y}, "
        p0_inset = (p0.x + normal[0], p0.y + normal[1])
        p1_inset = (p1.x + normal[0], p1.y + normal[1])

        routes_points.append(p0_inset)
        routes_points.append(p1_inset)

    """ 
    Since the points are a linestring we have each point two times. One time as the end of
    of one line segment and then again as the start of the next one. These points can get 
    out of sync by insetting the edges. Therefore, in this step we calculate the center of
    such pairs and use this center for both values.
    """
    for pointIndex in range(1, len(routes_points) - 1, 2):
        center = _calc_center(routes_points[pointIndex], routes_points[pointIndex + 1])
        routes_points[pointIndex] = center
        routes_points[pointIndex + 1] = center

    # building the insertion string for the database
    routes_points_str = ""
    for pointIndex in range(0, len(routes_points) - 1, 2):
        p0 = routes_points[pointIndex]
        p1 = routes_points[pointIndex + 1]
        routes_points_str += f"{p0[0]} {p0[1]}, {p1[0]} {p1[1]}, "

    # We need to cut the last ", " characters from the last iteration of the loop.
    outline_points = outline_points[:-2]
    outline_points = f"ST_LineFromMultiPoint('MULTIPOINT({outline_points})')"
    routes_points_str = routes_points_str[:-2]
    routes_points_str = f"ST_LineFromMultiPoint('MULTIPOINT({routes_points_str})')"

    outline_command = text(
        f"INSERT INTO {args.lakes_outline_table} (geom) VALUES (ST_SetSRID({outline_points}, {args.crs_no}));")
    conn.execute(outline_command)
    routes_command = text(
        f"INSERT INTO {args.routes_table} (geom) VALUES (ST_SetSRID({routes_points_str}, {args.crs_no}));")
    conn.execute(routes_command)
    conn.commit()


def _calculate_routes_with_down_scale(conn: Connection, lake: Row):
    """

    Calculate a linestring showing the routes taken along the shore of the lake.
    This is done by scaling down the lake geometry with ``ST_Buffer`` from PostGIS.

    Source: https://postgis.net/docs/ST_Buffer.html

    This routine is the preferred one for calculating routes on lakes.

    :param conn: connection used to add the generated data to the database
    :param lake: lake data for which the routes will be generated
    """

    geom = wkb.loads(lake[1].desc)
    # TODO: -30 should be an inset of 30 meters. Probably should get fact checked before
    # a serious release. Probably should become a parameter also.
    command_str = f"SELECT ST_Buffer(ST_GeogFromText('{geom}'), -30);"
    points_query = text(command_str)
    points = conn.execute(points_query).fetchall()

    points_geom = wkb.loads(points[0])
    points_geom = points_geom[0]
    points_query = text(f"SELECT (ST_DumpPoints('{points_geom}')).geom;")
    points = conn.execute(points_query).fetchall()

    outline_points = ""

    for i in range(0, len(points) - 1):
        p0 = wkb.loads(points[i][0])
        p1 = wkb.loads(points[i + 1][0])
        outline_points += f"{p0.x} {p0.y}, {p1.x} {p1.y}, "

    # We need to cut the last ", " characters from the last iteration of the loop.
    outline_points = outline_points[:-2]
    outline_points = f"ST_LineFromMultiPoint('MULTIPOINT({outline_points})')"

    outline_command = text(
        f"INSERT INTO {args.routes_table_scale} (geom) VALUES (ST_SetSRID({outline_points}, {args.crs_no}));")
    conn.execute(outline_command)
    conn.commit()


if __name__ == "__main__":
    """Take lake data and generate the paths on them. Additionally, some route calculations are done"""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Take lake data an generate the routes on those",
        exit_on_error=False)
    parser.add_argument('action', default='help', choices=['help', 'run'],
                        help='action to perform')

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

    parser.add_argument('--mode', dest='mode', default='normals', type=str,
                        help='route calculation method. modes are normals, scale')
    parser.add_argument('--schema', dest='schema', default='topology', type=str, help='schema name')
    parser.add_argument('--lakes_table', dest='lakes_table', default='my_table', type=str,
                        help='name of lakes table')
    parser.add_argument('--lakes_outline_table', dest='lakes_outline_table', default='outlines', type=str,
                        help='name of lakes outlines table')
    parser.add_argument('--routes-table', dest='routes_table', default='routes', type=str,
                        help='name of routes table used in normal mode')
    parser.add_argument('--routes-table-scale-mode', dest='routes_table_scale', default='routes_scale',
                        type=str,
                        help='name of routes table used in scale mode')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')
    parser.add_argument('--crs-to', dest='crs_to', default=32633, type=int,
                        help='target projection (for approximation of lengths)')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # init meta data structure
    metadata_obj: MetaData = MetaData()

    # select action
    if args.action == 'help':
        parser.print_help()
    elif args.action == 'run':
        run()
