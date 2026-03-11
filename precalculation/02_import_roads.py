# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Import roads from recroads into edges of the simulation."""

import argparse
import math
import os
from urllib import parse

import geopandas as gpd
import numpy as np
import rasterio
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from scipy.signal import savgol_filter
from shapely import force_2d, force_3d, wkb, Point, MultiPoint
from shapely.ops import transform, nearest_points
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
    parser.add_argument('-dir', '--direction-fields', dest='directions', nargs='*', help='direction fields to import from recroads (if empty, automatically detected)')
    parser.add_argument('--id-field', dest='id_field', default='recroadid', type=str, help='field containing unique identifier')
    parser.add_argument('--hub-a-field', dest='huba_field', default='hubaid', type=str, help='field containing from hub id')
    parser.add_argument('--hub-b-field', dest='hubb_field', default='hubbid', type=str, help='field containing to hub id')
    parser.add_argument('--geom-field', dest='geom_field', default='geom', type=str, help='field containing geometry (should be 3D)')

    parser.add_argument('--correct-directions', dest='correct', default=True, type=bool, help='correct directions if necessary (tests geography of linestring)')
    parser.add_argument('-sl', '--segment-length', dest='segment_length', default=20., type=float, help='maximum length of segments to recalculate roads in (default 20 meters)')
    parser.add_argument('-gs', '--group-segments', dest='group_segments', default=25, type=int, help='group segments to create sub points for slope calculation etc. (default 25, i.e. leading to 500m segments in the end)')
    parser.add_argument('--smoothen',  dest='smooth_elevation',  default=True, type=bool, help='smoothen heights using Savitzky-Golay filter (only for segments, because here we have roughly same distance points)')
    parser.add_argument('--savgol-window', dest='savgol_window', default=25, type=int, help='window size for Savitzky-Golay filter')
    parser.add_argument('--savgol-polynomial', dest='savgol_polynomial', default=6, type=int, help='polynomial order for Savitzky-Golay filter')
    parser.add_argument('--consider-heights', dest='consider_heights', default=True, type=bool, help='calculate heights/slopes into length of path')
    parser.add_argument('-i', '--input-file', required=True, dest='file', type=str, help='input file for heights (GeoTIFF)')
    parser.add_argument('--band', dest='band', default=1, type=int, help='GeoTIFF band to use')
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

    # load GeoTIFF
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

        # prepare coordinates - transform to metric units (m)
        geom_m = transform(transformer.transform, force_2d(geom))

        # segmentize, how many points do we need to create?
        points_to_create = math.ceil(geom_m.length / args.segment_length)
        distance_delta = geom_m.length / points_to_create
        distances = np.arange(0, geom_m.length, distance_delta)

        # create points along the road
        points_m = [geom_m.interpolate(distance) for distance in distances] + [Point(geom_m.coords[-1])]

        # calculate heights, etc.
        xyz = np.zeros((len(points_m), 5))
        progress_m = 0.
        for i, point in enumerate(points_m):
            # get coordinates in DTM's CRS
            x, y = point.x, point.y
            xx, yy = rds.index(x, y)
            # get height from DTM
            xyz[i, 0] = x
            xyz[i, 1] = y
            xyz[i, 2] = band[xx, yy]

            # length to the next point (in meters)
            if i > 0:
                # l = LineString([points_m[i - 1], point]).length
                # # if length is less than distance_delta, we use the base distance (might be corner)
                # if l < distance_delta - 0.4:
                #     l = distance_delta
                # this is not 100% accurate, but the error for 10km is about 10m, so 0.1%. This is acceptable.
                progress_m += distance_delta
                xyz[i, 3] = distance_delta
                xyz[i, 4] = progress_m

        # smooth elevation if needed
        if args.smooth_elevation:
            # Savitzky-Golay filter to smooth elevation (https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.savgol_filter.html)
            window = args.savgol_window if len(xyz) > args.savgol_window else len(xyz)
            polynomial = args.savgol_polynomial if args.savgol_polynomial < window else window - 1
            if window > 1:
                xyz[:, 2] = savgol_filter(xyz[:, 2], window, polynomial)

        # heights distances
        diffs = np.diff(xyz[:, 2])
        slopes = np.zeros(len(diffs))
        lengths = np.zeros(len(diffs)) # calculate length including slope
        legs_up = np.zeros(len(diffs))
        legs_down = np.zeros(len(diffs))

        for i in range(len(slopes)):
            l = xyz[i+1][3]
            slope = diffs[i] / l if l > 0. else 0. # start at the second index point (i+1) to get slope
            slopes[i] = slope
            lengths[i] = np.sqrt(l ** 2 + diffs[i] ** 2)
            legs_up[i] = diffs[i] if diffs[i] > 0 else 0.
            legs_down[i] = diffs[i] if diffs[i] < 0 else 0.
        max_slope = np.max(slopes)
        if max_slope < 0.:
            max_slope = 0.
        min_slope = np.min(slopes)
        if min_slope > 0.:
            min_slope = 0.
        else:
            min_slope = np.abs(min_slope)

        # now we will combine these values into larger blocks

        # create leg point indexes
        indexes_for_legs = np.round(np.linspace(0, points_to_create-1, num=1+int(np.ceil(points_to_create/args.group_segments)))).astype(int)
        number_of_legs = len(indexes_for_legs)-1

        # calculate leg lengths - we will use the total length of the road as an approximation, because single leg lengths tend to vary a bit
        leg_lengths = np.full(number_of_legs, fill_value=geom_m.length/number_of_legs, dtype=np.float64)
        leg_legs_up = np.zeros(number_of_legs)
        leg_legs_down = np.zeros(number_of_legs)

        for i in range(len(indexes_for_legs)-1):
            leg_legs_up[i] = np.sum(legs_up[indexes_for_legs[i]:indexes_for_legs[i+1]])
            leg_legs_down[i] = np.sum(legs_down[indexes_for_legs[i]:indexes_for_legs[i+1]])

        # finally, we create the leg points as coordinate pairs
        xy = np.zeros([number_of_legs+1, 2])
        for i in range(len(indexes_for_legs)):
            p = points_m[indexes_for_legs[i]]
            xy[i] = [p.x, p.y]

        # transform points back to lat/lon
        points_back = transform(transformer_back.transform, MultiPoint(xy))
        leg_points = np.zeros([number_of_legs+1, 2])
        # snap points to the road if possible, to avoid numerical issues
        for i, p in enumerate(points_back.geoms):
            nearest_point, _ = nearest_points(geom, p)
            leg_points[i] = [nearest_point.x, nearest_point.y]

        # set the first and last point to the same point to avoid issues with LineString
        p = force_2d(Point(geom.coords[0]))
        leg_points[0] = [p.x, p.y]
        p = force_2d(Point(geom.coords[-1]))
        leg_points[-1] = [p.x, p.y]

        data = {
            'length_m': float(np.sum(lengths) if args.consider_heights else geom_m.length),
            'legs': leg_lengths.tolist(),
            'legs_up': leg_legs_up.tolist(),
            'legs_down': leg_legs_down.tolist(),
            'max_slope_up': float(max_slope),
            'max_slope_down': float(min_slope),
            'up_m': float(np.sum(diffs, where=diffs > 0)),
            'down_m': float(np.abs(np.sum(diffs, where=diffs < 0))),
            'flat_length_m': geom_m.length,
            'leg_points': leg_points.tolist(),
        }

        # insert data into edges table
        stmt = insert(edges_table).values(id=myid, geom=WKTElement(geom.wkt), hub_id_a=hub_id_a, hub_id_b=hub_id_b, type=edge_type, data=data, directions=directions)
        conn.execute(stmt)

    conn.commit()

    print("...finished.")
