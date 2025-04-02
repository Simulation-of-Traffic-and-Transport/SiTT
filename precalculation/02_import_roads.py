# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import roads from recroads into edges of the simulation."""

import argparse
import math
from urllib import parse

import geopandas as gpd
from geoalchemy2 import Geometry, WKTElement
from shapely import force_3d
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
        # check hub existence
        if hub_id_a and not conn.execute(text(f"SELECT COUNT(*) FROM sitt.hubs WHERE id = '{hub_id_a}'")).one()[0]:
            print(f"Skipping row {myid} because hub {hub_id_a} does not exist.")
            continue
        if hub_id_b and not conn.execute(text(f"SELECT COUNT(*) FROM sitt.hubs WHERE id = '{hub_id_b}'")).one()[0]:
            print(f"Skipping row {myid} because hub {hub_id_b} does not exist.")
            continue

        # add edge type
        edge_type = "road"
        # add JSONB data
        data = {}
        if args.fields and len(args.fields) > 0:
            for field in args.fields:
                if field in row:
                    if field in args.boolean_fields:
                        # convert certain values to boolean
                        data[field] = parse_yes_no_entry(row[field])
                    else:
                        data[field] = row[field]
        # add directions
        directions = {}
        if args.directions and len(args.directions) > 0:
            for direction in args.directions:
                if direction in row and not math.isnan(row[direction]):
                    directions[direction] = int(row[direction])
                else:
                    directions[direction] = 0

        # insert data into hubs table
        stmt = insert(edges_table).values(id=myid, geom=WKTElement(geom.wkt), hub_id_a=hub_id_a, hub_id_b=hub_id_b, type=edge_type, data=data, directions=directions)
        conn.execute(stmt)

    conn.commit()

    print("...finished.")
