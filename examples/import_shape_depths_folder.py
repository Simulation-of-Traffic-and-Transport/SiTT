# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to import a folder with shape files containing point layers of water depths into the PostGis
database.

Create a table in your PostGIS database with the following schema:

create table topology.river_depths
(
    id    serial
        constraint river_depths_pk
            primary key,
    shape TEXT not null,
    geom  geometry(Point, 4326),
    depth float not null
);

create index river_depths_geom_index
    on topology.river_depths using gist (geom);

create index river_depths_shape_index
    on topology.river_depths (shape);
"""

import argparse
import os
from pathlib import Path

import geopandas as gpd
import psycopg2
from pyproj import Transformer
from shapely import wkb, Point, force_3d
from shapely.ops import transform

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

    # depth table
    parser.add_argument('-dt', '--depths-table', dest='depths_table', default='topology.river_depths', type=str, help='river_depths table to populate')

    # path settings
    parser.add_argument('-f', '--folder', dest='folder', default='depths', type=str, help='folder containing shape files')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection target')

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

    # check if geom_segments exists, exit if not (you should run create_river_segments.py first)
    schema, table = args.river_table.split('.')
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table}' AND table_schema= '{schema}' AND column_name = '{args.river_geo_column}_segments')")
    if not cur.fetchone()[0]:
        print(f"Error: Geometry column {args.river_geo_column}_segments does not exist in table {args.river_table}. Please run create_river_segments.py first.")
        conn.close()
        exit(1)

    # read shapefile folder
    for child in Path(args.folder).iterdir():
        if child.is_file() and child.suffix == '.shp':
            basename = os.path.splitext(child.name)[0]
            print(basename)

            # Read the shapefile
            gdf = gpd.read_file(child)

            # define projection - kept for some missing points
            project_forward = Transformer.from_crs(gdf.crs, args.crs_no, always_xy=True).transform

            # project
            gdf = gdf.to_crs(epsg=args.crs_no)

            for row in gdf.itertuples():
                if row.geometry is None:
                    # if geopandas does not convert points properly, convert it here
                    #print(f"Warning: Skipping row {row.Index} with missing geometry.", row)
                    point = transform(project_forward, Point(row.field_1, row.field_2, row.field_3))
                elif row.geometry.has_z is False:
                    point = force_3d(Point(row.geometry.x, row.geometry.y, row.field_3))  # assume z is the third field in the shapefile
                else:
                    point = row.geometry
                try:
                    point2d = Point(point.x, point.y)
                    depth = point.z

                    cur.execute(f"INSERT INTO {args.depths_table} (shape, geom, depth) VALUES ('{basename}', ST_GeomFromText('{point2d.wkt}'), {depth});")
                except:
                    print(f"Error: Failed to insert row {row.Index}.", basename, row, point)

        conn.commit()
