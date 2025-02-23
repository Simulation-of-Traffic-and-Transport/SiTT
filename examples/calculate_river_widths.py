# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This is an example how to calculate river widths. The width per point in the segment is calculated. It uses the river
lines (shores and islands) as base reference, takes the closest point and tries to find an opposite point within a
certain angle.
"""
import argparse

import numpy as np
import psycopg2
import shapefile
from pyproj import Transformer
from shapely import wkb, Point, Polygon, LineString
from shapely.ops import transform

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
    # table settings
    parser.add_argument('-rt', '--river-table', dest='river_table', default='topology.recrivers', type=str, help='river table to check')
    parser.add_argument('-rc', '--river-id-column', dest='river_id_column', default='recroadid', type=str, help='river id column')
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom', type=str, help='river geometry column')
    parser.add_argument('-ra', '--river-huba-column', dest='river_a_column', default='hubaid', type=str, help='river hub a column')
    parser.add_argument('-rb', '--river-hubb-column', dest='river_b_column', default='hubbid', type=str, help='river hub b column')
    parser.add_argument('-rd', '--river-slope-column', dest='river_slope_column', default='slope', type=str, help='river slope column')

    parser.add_argument('-ht', '--hub-table', dest='hub_table', default='topology.rechubs', type=str, help='hub table to check')
    parser.add_argument('-hc', '--hub-id-column', dest='hub_id_column', default='rechubid', type=str, help='hub id column')
    parser.add_argument('-hg', '--hub-geo-column', dest='hub_geo_column', default='geom', type=str, help='hub geometry column')

    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')
    parser.add_argument('--crs-target', dest='crs_target', default=32633, type=int, help='projection target (has to support meters)')
    parser.add_argument('--degrees', dest='degrees', default=35., type=float, help='degrees for checking opposite shore')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    project_forward = Transformer.from_crs(args.crs_source, args.crs_target, always_xy=True).transform
    project_back = Transformer.from_crs(args.crs_target, args.crs_source, always_xy=True).transform

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password,
                            port=args.port)
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur_upd = conn.cursor()  # update cursor

    # create shapefile to check lines
    w = shapefile.Writer(target='river_widths', shapeType=shapefile.POLYLINE, autoBalance=True)
    w.field("width", "N", decimal=10)


    def create_rotation_matrix(degrees: float) -> np.ndarray:
        """ Create a rotation matrix for given degrees.

        :param degrees: The degrees of rotation.
        :return: The rotation matrix.
        """
        theta = np.radians(degrees)
        c, s = np.cos(theta), np.sin(theta)
        return np.array(((c, -s), (s, c)))


    def rotate_vector(vector: np.ndarray, rotation_matrix: np.ndarray) -> np.ndarray:
        """
        Rotate a vector by given rotation matrix.
        see https://scipython.com/book/chapter-6-numpy/examples/creating-a-rotation-matrix-in-numpy/

        :param vector: The vector to be rotated.
        :param rotation_matrix: The rotation matrix.
        :return: The rotated vector.
        """
        # rotate vector by given rotation matrix
        rot = np.dot(rotation_matrix, vector)
        # normalize vector (this will give a vector of length 1 unit - m in our case)
        return rot / np.linalg.norm(rot)

    # create rotation matrices left and right of shore vector (to look for opposite shore)
    R_f = create_rotation_matrix(args.degrees)
    R_b = create_rotation_matrix(360-args.degrees)


    def rotate_opposite_point(a: Point, b: Point, rotation_matrix: np.ndarray) -> Point | None:
        # project forward - this is important, because we need to "flatten" the projection in order to get correct
        # angles
        proj_a = transform(project_forward, a)
        proj_b = transform(project_forward, b)

        # line as opposite vector (because we deduct b from a and not the other way around)
        vec = np.array([proj_a.x - proj_b.x, proj_a.y - proj_b.y])
        if vec[0] == 0. and vec[1] == 0.:
            return None

        # rotate vector by given rotation matrix - multiply by 3000 to 3 km
        rot_vec = rotate_vector(vec, rotation_matrix) * 3000
        # project the rotated vector back to original coordinate system - add to original coordinates
        return transform(project_back, Point(proj_a.x + rot_vec[0], proj_a.y + rot_vec[1]))

    # counter
    c = 0

    # load all river paths
    cur.execute("select recroadid, geom_segments from topology.recrivers WHERE recroadid = 'STR-REC-WW-GLAN-002_down-2024_11_07'")
    for data in cur:
        path: LineString = wkb.loads(data[1])
        # skip first and last points - they are our start and end points
        # TODO - check these points
        for i in range(1, len(path.coords) - 1):
            c += 1 # increase counter

            coord = path.coords[i]
            p = Point(coord[0], coord[1])
            wkb_point = wkb.dumps(p, srid=args.crs_source)

            # find the closest point on the shoreline of any river
            cur2.execute("SELECT ST_ClosestPoint(geom, %s) FROM water_wip.all_water_lines", (wkb_point,))
            # we expect at exactly one result - and we assume that the point is within the river
            closest_point: Point = wkb.loads(cur2.fetchone()[0])
            if closest_point is None:
                print("Warning... closest_point", data[0])
                # TODO: handle this
                continue

            # rotate using rotation matrix
            rot_l = rotate_opposite_point(p, closest_point, R_f)
            rot_r = rotate_opposite_point(p, closest_point, R_b)

            # create triangular polygon to look for opposite shore
            triangle = Polygon([p, rot_l, rot_r])
            triangle_wkb = wkb.dumps(triangle, srid=args.crs_source)
            # find the closest opposite point on the shoreline of any river
            cur2.execute("SELECT ST_ClosestPoint(ST_Intersection(geom, %s), %s) FROM water_wip.all_water_lines", (triangle_wkb, wkb_point,))
            # we expect at exactly one result - and we assume that the point is within the river
            closest_opposite_point: Point = wkb.loads(cur2.fetchone()[0])
            if closest_opposite_point is None:
                print("Warning... closest_opposite_point", data[0])
                # TODO: handle this
                continue

            # create line string
            length_line = LineString([(closest_point.x, closest_point.y), (coord[0], coord[1]), (closest_opposite_point.x, closest_opposite_point.y)])

            # calculate width in m
            width = transform(project_forward, length_line).length

            # original line
            w.line([length_line.coords])
            w.record(width)

            # TODO update river width in database

    w.close()

    print(c)
