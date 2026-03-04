# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import roads from recroads into edges of the simulation."""

import argparse
import math
from urllib import parse
import os

import geopandas as gpd
import numpy as np
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from shapely import force_2d, force_3d, wkb, Point, LineString, MultiPoint
from shapely.ops import transform, split, snap, nearest_points
from sqlalchemy import create_engine, Table, Column, MetaData, \
    String, text, insert
from sqlalchemy.dialects.postgresql import JSONB
import rasterio

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
    parser.add_argument('-dir', '--direction-fields', dest='directions', nargs='*', help='direction fields to import from recroads (if empty, automatically detected)')
    parser.add_argument('--id-field', dest='id_field', default='recroadid', type=str, help='field containing unique identifier')
    parser.add_argument('--hub-a-field', dest='huba_field', default='hubaid', type=str, help='field containing from hub id')
    parser.add_argument('--hub-b-field', dest='hubb_field', default='hubbid', type=str, help='field containing to hub id')
    parser.add_argument('--geom-field', dest='geom_field', default='geom', type=str, help='field containing geometry (should be 3D)')

    parser.add_argument('--correct-directions', dest='correct', default=True, type=bool, help='correct directions if necessary (tests geography of linestring)')
    parser.add_argument('-s', '--segment-length', dest='segment_length', default=500., type=float, help='maximum length of segments to recalculate roads in (0. to disable segmentation, default 500 meters)')
    parser.add_argument('--consider-heights', dest='consider_heights', default=True, type=bool,
                        help='calculate heights/slopes into length of path (only without segments)')
    parser.add_argument('-i', '--input-file', dest='file', type=str, help='input file for heights (GeoTIFF) - only needed for segmentation')
    parser.add_argument('--band', dest='band', default=1, type=int, help='band to use from GeoTIFF')
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

    # load GeoTIFF, if defined
    if args.segment_length > 0:
        if args.file:
            # check file exists
            if not os.path.exists(args.file):
                print("File does not exist: " + args.file)
                parser.exit(1)

            rds: rasterio.io.DatasetReader = rasterio.open(args.file)
            # get relevant band
            band = rds.read(args.band)
            print("Heights loaded from GeoTIFF.")

            if rds.crs != 'EPSG:' + str(args.crs_to):
                print("Error: GeoTIFF CRS does not match target CRS. Failing...")
                exit(8)
        else:
            print("No GeoTIFF specified.")
            exit(7)

    transformer = Transformer.from_crs(args.crs_from, args.crs_to, always_xy=args.always_xy)
    transformer_back = Transformer.from_crs(args.crs_to, args.crs_from, always_xy=args.always_xy)

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

    # check if directions are given - detect if needed
    if not args.directions or len(args.directions) == 0:
        args.directions = []
        for col_name in list(gdf.columns):
            # column must be integer (not float) and not "id"
            if col_name != "id" and type(gdf.dtypes[col_name]) == np.dtypes.Int64DType:
                # get unique values and check if they are within reasonable range
                unique_values = gdf[col_name].unique()
                vmin = gdf[col_name].min()
                vmax = gdf[col_name].max()
                # if unique values are within reasonable range and less than or equal to 4, it's probably a direction field
                if len(unique_values) <= 4 and vmin >= -1 and vmax <= 2:
                    args.directions.append(col_name)

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

        # Calculate single legs
        length = 0.
        legs = []  # in m
        leg_points = None
        slopes = []  # in percent (maximum slope if more than one)
        legs_up = []  # in m
        legs_down = []  # in m
        base_length = 0.  # in m, for calculating costs
        up_abs = 0.  # in m, for calculating costs
        down_abs = 0.  # in m, for calculating costs

        if args.segment_length > 0.:
            # into metric space
            g_m = transform(transformer.transform, force_2d(geom))

            # how many points do we need to create?
            points_to_create = math.ceil(g_m.length / args.segment_length)
            distance_delta = g_m.length / points_to_create
            distances = np.arange(0, g_m.length, distance_delta)

            # create points along the road
            points_m = MultiPoint([g_m.interpolate(distance) for distance in distances] + [Point(g_m.coords[-1])])

            # cut to snap (+/-1m)
            # see: https://gis.stackexchange.com/questions/416284/splitting-multiline-or-linestring-into-equal-segments-of-particular-length-using
            result = split(snap(g_m, points_m, 1), points_m)

            # transform points back to lat/lon
            points_back = transform(transformer_back.transform, points_m)
            points_list = []
            # snap points to road if possible, to avoid numerical issues
            for p in points_back.geoms:
                nearest_point, _ = nearest_points(geom, p)
                points_list.append(nearest_point)
            # set first and last point to the same point to avoid issues with LineString
            points_list[0] = force_2d(Point(geom.coords[0]))
            points_list[-1] = force_2d(Point(geom.coords[-1]))
            points = MultiPoint(points_list)

            segmented_road = split(snap(geom, points, 1.0e-12), points)
            if len(result.geoms) != len(segmented_road.geoms):
                print(f"{myid}, {len(result.geoms)}!= {len(segmented_road.geoms)} is a problem.")
                exit(9)

            # iterate over segments and calculate deltas
            for idx, line in enumerate(result.geoms):
                # create point every meter
                points_to_create = math.ceil(line.length)
                distance_delta = line.length / points_to_create
                distances = np.arange(0, line.length, distance_delta)

                # get the coordinates
                coords: list[Point] = [line.interpolate(distance) for distance in distances] + [
                    Point(line.coords[-1])]

                # calculate height delts
                last_height = None
                delta_up = 0.
                delta_down = 0.

                for coord in coords:
                    x, y = rds.index(coord.x, coord.y)
                    height = band[x, y]
                    # first entry
                    if last_height is None:
                        last_height = height
                        continue
                    # calculate delta
                    delta = height - last_height
                    last_height = height
                    # categorize
                    if delta > 0:
                        delta_up += delta
                    else:
                        delta_down += abs(delta)

                percent_up = delta_up / line.length
                percent_down = delta_down / line.length

                legs.append(line.length)  # in m
                slopes.append(max(percent_up, percent_down))
                legs_up.append(delta_up)
                legs_down.append(delta_down)
                up_abs += delta_up
                down_abs += delta_down

            # here, we assume flat lengths only
            length = g_m.length
            base_length = g_m.length
            leg_points = points
        else:
            last_coord = None
            for coord in geom.coords:
                if last_coord is not None:
                    # distance calculation for each leg
                    leg = transform(transformer.transform, LineString([last_coord, coord]))
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
                            legs_up.append(up_abs)
                            legs_down.append(0.)
                        else:
                            down_abs -= diff
                            legs_up.append(0.)
                            legs_down.append(-diff)
                    else:
                        slope = 0.0

                    legs.append(leg_length)
                    slopes.append(slope)
                    length += leg_length

                last_coord = coord

        # add length in m
        data['length_m'] = length
        data['legs'] = legs
        data['legs_up'] = legs_up
        data['legs_down'] = legs_down
        data['slopes'] = slopes
        data['up_m'] = up_abs
        data['down_m'] = down_abs
        data['flat_length_m'] = base_length
        if leg_points is not None:
            data['leg_points'] = leg_points.wkt

        # insert data into edges table
        stmt = insert(edges_table).values(id=myid, geom=WKTElement(geom.wkt), hub_id_a=hub_id_a, hub_id_b=hub_id_b, type=edge_type, data=data, directions=directions)
        conn.execute(stmt)

    conn.commit()

    print("...finished.")
