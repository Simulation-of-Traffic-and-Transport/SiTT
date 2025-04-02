# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import hubs from rechubs into hubs of the simulation.."""

import argparse
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
        description="Import hubs from rechubs into hubs of the simulation.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('--source-schema', dest='source_schema', default='topology', type=str, help='source schema name (rechubs table)')
    parser.add_argument('-d', '--data-fields', dest='fields', nargs='*', help='data fields to import from rechubs')
    parser.add_argument('-b', '--boolean-fields', dest='boolean_fields', nargs='*', help='data fields that have boolean values like "y" or true')
    parser.add_argument('--id-field', dest='id_field', default='rechubid', type=str, help='field containing unique identifier')
    parser.add_argument('--geom-field', dest='geom_field', default='geom', type=str, help='field containing geometry')
    parser.add_argument('--height-field', dest='height_field', type=str, help='field containing height if any')

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
        conn.execute(text("DROP TABLE IF EXISTS " + args.schema + ".hubs;"))
        conn.commit()

    metadata_obj: MetaData = MetaData(schema=args.schema)

    # define hub table
    idCol = Column('id', String, primary_key=True)
    geom_col = Column('geom', Geometry(geometry_type='POINTZ', srid=4326, spatial_index=True))
    data_col = Column('data', JSONB)
    hubs_table = Table("hubs", metadata_obj, idCol, geom_col, data_col, schema=args.schema)

    metadata_obj.create_all(conn)

    # create JSONB index for data column
    conn.execute(text("CREATE INDEX IF NOT EXISTS hubs_data_type_index ON " + args.schema + ".hubs USING gin (data)"))
    conn.commit()

    print("Database connected - working...")

    # delete existing hubs
    if args.delete:
        print("Deleting old records...")
        conn.execute(text("DELETE FROM " + args.schema + ".hubs;"))
        conn.commit()

    # read original table
    gdf = gpd.read_postgis(f"SELECT * FROM {args.source_schema}.rechubs", conn, geom_col=args.geom_field)

    # transform hubs format
    for _, row in gdf.iterrows():
        myid = row[args.id_field]
        geom = force_3d(row[args.geom_field])
        if args.height_field and args.height_field in row:
            # add height conditionally
            geom.z = row[args.height_field]
        # add JSONB data
        data = {}
        if args.fields and len(args.fields) > 0:
            for field in args.fields:
                if field in row:
                    if args.boolean_fields and field in args.boolean_fields:
                        # convert certain values to boolean
                        data[field] = parse_yes_no_entry(row[field])
                    else:
                        data[field] = row[field]

        # insert data into hubs table
        stmt = insert(hubs_table).values(id=myid, geom=WKTElement(geom.wkt), data=data)
        conn.execute(stmt)

    conn.commit()

    print("...finished.")