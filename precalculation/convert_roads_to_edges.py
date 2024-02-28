# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Convert roads to edges."""

import argparse
import os
import sys
from urllib import parse

import geopandas as gpd
import rasterio
from pyproj import Transformer
from shapely import wkb, ops, LineString
from shapely.geometry import shape
from sqlalchemy import create_engine, Table, Column, MetaData, \
    String, Float, JSON, select, text, update, insert, literal_column
from geoalchemy2 import Geometry, WKTElement
import numpy as np

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(
        description="Convert roads to edges.",
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
    parser.add_argument('--consider-heights', dest='consider_heights', default=True, type=bool,
                        help='calculate heights/slopes into length of path')

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

    print("Database connected - working...")

    # Truncate edges?
    if args.empty_edges:
        print("Truncating edges database...")
        conn.execute(text("TRUNCATE TABLE " + args.schema + ".edges;"))
        conn.commit()
    elif args.delete_roads:  # delete roads from edges table?
        print("Deleting roads edges from database...")
        conn.execute(text("DELETE FROM " + args.schema + ".edges WHERE type = 'road';"))
        conn.commit()

    # read roads and convert to edges
    for result in conn.execute(text("SELECT id, geom, hub_id_a, hub_id_b, roughness FROM sitt.roads")):
        # get column data
        road_id = result[0]
        geom = wkb.loads(result[1])
        hub_id_a = result[2]
        hub_id_b = result[3]
        roughness = result[4]

        # Calculate single legs
        length = 0.
        legs = []  # in m
        slopes = []  # in percent
        base_length = 0.  # in m, for calculating costs
        up_abs = 0.  # in m, for calculating costs
        down_abs = 0.  # in m, for calculating costs

        last_coord = None
        for coord in geom.coords:
            if last_coord is not None:
                # distance calculation for each leg
                leg = ops.transform(transformer.transform, LineString([last_coord, coord]))
                leg_length = leg.length
                base_length += leg.length

                # asc/desc
                diff = last_coord[2] - coord[2]

                # add height to length calculation
                if args.consider_heights:
                    leg_length = np.sqrt([leg_length * leg_length + diff * diff])[0]

                # logger.info("%f, %f", diff, leg_length)
                if leg_length > 0:
                    slope = diff / leg_length  # slope is in percent (0.00-1.00)
                    # add to absolute change in m
                    if diff > 0:
                        up_abs += diff
                    else:
                        down_abs -= diff
                else:
                    slope = 0.0

                legs.append(leg_length)
                slopes.append(slope)
                length += leg_length

            last_coord = coord

        # we calculate cost according to the DAV (Deutscher Alpenverein, German Alpine Association) formula:
        # * 4 km of flat terrain = 1h
        # * 300 m ascending = 1h
        # * 400 m descending = 1h
        # All three formulas are totalled.
        # Roughness of the road is multiplied with the total.
        cost_a_b = (base_length / 4000 + up_abs / 300 + down_abs / 400) * roughness
        cost_b_a = (base_length / 4000 + up_abs / 400 + down_abs / 300) * roughness

        geo_stmt = WKTElement(geom.wkt, srid=args.crs_from)

        # now, enter into edges table
        stmt = insert(edges_table).values(id=road_id, geom=geo_stmt, hub_id_a=hub_id_a,
                                          hub_id_b=hub_id_b, type='road', cost_a_b=cost_a_b, cost_b_a=cost_b_a,
                                          data={"length_m": length, "legs": legs, "slopes": slopes,
                                                "flat_length_m": base_length, "up_m": up_abs, "down_m": down_abs})
        conn.execute(stmt)

    conn.commit()
    print("Done.")
