# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import roads from recroads into edges of the simulation."""

import argparse
import math
from urllib import parse

import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from shapely import force_2d, force_3d, wkb, Point
from shapely.ops import transform
from sqlalchemy import create_engine, Table, Column, MetaData, \
    String, text, insert
from sqlalchemy.dialects.postgresql import JSONB

from precalculation.common import parse_yes_no_entry

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="Import roads from recroads into edges of the simulation.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('--source-schema', dest='source_schema', default='topology', type=str, help='source schema name (recroads table)')
    parser.add_argument('-d', '--data-fields', dest='fields', nargs='*', help='data fields to import from recroads')
    parser.add_argument('-b', '--boolean-fields', dest='boolean_fields', nargs='*', help='data fields that have boolean values like "y" or true')
    parser.add_argument('-dir', '--direction-fields', dest='directions', nargs='*', help='direction fields to import from recroads')
    parser.add_argument('--id-field', dest='id_field', default='recroadid', type=str, help='field containing unique identifier')
    parser.add_argument('--hub-a-field', dest='huba_field', default='hubaid', type=str, help='field containing from hub id')
    parser.add_argument('--hub-b-field', dest='hubb_field', default='hubbid', type=str, help='field containing to hub id')
    parser.add_argument('--geom-field', dest='geom_field', default='geom', type=str, help='field containing geometry (should be 3D)')

    parser.add_argument('--correct-directions', dest='correct', default=True, type=bool, help='correct directions if necessary (tests geography of linestring)')
    parser.add_argument('--delete', dest='delete', default=True, type=bool, help='delete before import')
    parser.add_argument('--drop', dest='drop', default=False, type=bool, help='drop table before import')

    parser.add_argument('-f', '--crs-from', dest='crs_from', default=4326, type=int, help='projection source')
    parser.add_argument('-t', '--crs-to', dest='crs_to', default=32633, type=int,
                        help='projection target (should support meters)')
    parser.add_argument('--xy', dest='always_xy', default=True, type=bool, help='use the traditional GIS order')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    transformer = Transformer.from_crs(args.crs_from, args.crs_to, always_xy=args.always_xy)

    # connect to database
    conn = create_engine('postgresql://' + args.user + ':' + parse.quote_plus(args.password) + '@' + args.server + ':' +
                         str(args.port) + '/' + args.database).connect()

    if args.drop:
        print("Dropping table...")
        conn.execute(text("DROP TABLE IF EXISTS " + args.schema + ".edges;"))
        conn.commit()

    metadata_obj: MetaData = MetaData(schema=args.schema)

    # define edge table
    idCol = Column('id', String, primary_key=True)
    geom_col = Column('geom', Geometry(geometry_type='LINESTRINGZ', srid=4326, spatial_index=True))
    hub_id_a = Column('hub_id_a', String, index=True)
    hub_id_b = Column('hub_id_b', String, index=True)
    edge_type_col = Column('type', String, index=True)
    data_col = Column('data', JSONB)
    directions_col = Column('directions', JSONB)
    edges_table = Table("edges", metadata_obj, idCol, geom_col, hub_id_a, hub_id_b, edge_type_col, data_col, directions_col, schema=args.schema)

    metadata_obj.create_all(conn)

    # create JSONB index for data column
    conn.execute(text("CREATE INDEX IF NOT EXISTS edges_data_type_index ON " + args.schema + ".edges USING gin (data)"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS edges_directions_type_index ON " + args.schema + ".edges USING gin (directions)"))
    conn.commit()

    print("Database connected - working...")

    # delete existing roads
    if args.delete:
        print("Deleting old records...")
        conn.execute(text("DELETE FROM " + args.schema + ".edges WHERE type = 'road';"))
        conn.commit()

    # read original table
    gdf = gpd.read_postgis(f"SELECT * FROM {args.source_schema}.recroads", conn, geom_col=args.geom_field)

    # transform hubs format
    for _, row in gdf.iterrows():
        myid = row[args.id_field]
        geom = force_3d(row[args.geom_field])
        if geom is None:
            print(f"Skipping row {myid} because geometry is None.")
            continue
        hub_id_a = row[args.huba_field]
        hub_id_b = row[args.hubb_field]
        # check hub existence and distance
        hub_a = conn.execute(text(f"SELECT geom FROM sitt.hubs WHERE id = '{hub_id_a}'")).first()
        hub_b = conn.execute(text(f"SELECT geom FROM sitt.hubs WHERE id = '{hub_id_b}'")).first()
        if hub_a is None or hub_b is None:
            print(f"Skipping row {myid} because hubs missing: {hub_id_a} = {hub_a is None}, {hub_id_b} = {hub_b is None}.")
            continue

        # fix geometry direction if necessary
        is_reversed = False
        if args.correct:
            # create 2D points
            point_a = force_2d(wkb.loads(hub_a[0]))
            point_b = force_2d(wkb.loads(hub_b[0]))

            # points of route
            line_a = Point(geom.coords[0][0:2])
            line_b = Point(geom.coords[-1][0:2])

            dist_matrix = [
                point_a.distance(line_a),
                point_a.distance(line_b),
                point_b.distance(line_a),
                point_b.distance(line_b)
            ]

            # flip geometry?
            if dist_matrix[0] > dist_matrix[1] and dist_matrix[2] < dist_matrix[3]:
                geom = geom.reverse()
                is_reversed = True

        # add edge type
        edge_type = "road"
        # add JSONB data
        data = {}
        if args.fields and len(args.fields) > 0:
            for field in args.fields:
                if field in row:
                    if args.boolean_fields and field in args.boolean_fields:
                        # convert certain values to boolean
                        data[field] = parse_yes_no_entry(row[field])
                    else:
                        # if geo is reversed, also reverse list data
                        if is_reversed and type(row[field]) == list:
                            row[field].reverse()
                        data[field] = row[field]
        # add directions
        directions = {}
        if args.directions and len(args.directions) > 0:
            for direction in args.directions:
                dir_key = direction.lower()
                if direction in row and not math.isnan(row[direction]):
                    directions[dir_key] = int(row[direction])
                else:
                    directions[dir_key] = 0

        # add length in m
        data['length_m'] = transform(transformer.transform, geom).length

        # insert data into edges table
        stmt = insert(edges_table).values(id=myid, geom=WKTElement(geom.wkt), hub_id_a=hub_id_a, hub_id_b=hub_id_b, type=edge_type, data=data, directions=directions)
        conn.execute(stmt)

    conn.commit()

    print("...finished.")
