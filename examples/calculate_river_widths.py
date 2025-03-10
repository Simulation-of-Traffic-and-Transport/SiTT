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
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom_segments', type=str, help='river geometry column')
    parser.add_argument('-ra', '--river-huba-column', dest='river_a_column', default='hubaid', type=str, help='river hub a column')
    parser.add_argument('-rb', '--river-hubb-column', dest='river_b_column', default='hubbid', type=str, help='river hub b column')
    parser.add_argument('-rw', '--river-width-column', dest='river_width_column', default='width', type=str, help='river width column')

    parser.add_argument('-ht', '--hub-table', dest='hub_table', default='topology.rechubs', type=str, help='hub table to check')
    parser.add_argument('-hc', '--hub-id-column', dest='hub_id_column', default='rechubid', type=str, help='hub id column')
    parser.add_argument('-hg', '--hub-geo-column', dest='hub_geo_column', default='geom', type=str, help='hub geometry column')

    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')
    parser.add_argument('--crs-target', dest='crs_target', default=32633, type=int, help='projection target (has to support meters)')
    parser.add_argument('--degrees', dest='degrees', default=35., type=float, help='degrees for checking opposite shore')
    parser.add_argument('--wedge#length', dest='wedge_length', default=3000., type=float, help='length of legs to search for opposite shore in m')

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

    # add column, if needed
    # check if depth exists, create column otherwise
    schema, table = args.river_table.split('.')
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table}' AND table_schema= '{schema}' AND column_name = '{args.river_width_column}')")
    if not cur.fetchone()[0]:
        cur_upd.execute(f"ALTER TABLE {args.river_table} ADD {args.river_width_column} double precision[]")
        conn.commit()
        print("Adding column for river widths...")

    # create shapefile to check lines
    w = shapefile.Writer(target='river_widths', shapeType=shapefile.POLYLINE, autoBalance=True)
    w.field("width", "N", decimal=10)

    # error file
    we = shapefile.Writer(target='river_widths_errors', shapeType=shapefile.POINT, autoBalance=True)
    we.field("reason", "C")


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

        # rotate vector by given rotation matrix - multiply by wedge_length (default = 3000m)
        rot_vec = rotate_vector(vec, rotation_matrix) * args.wedge_length
        # project the rotated vector back to original coordinate system - add to original coordinates
        return transform(project_back, Point(proj_a.x + rot_vec[0], proj_a.y + rot_vec[1]))

    def unit_vector(vector: np.array):
        """ Returns the unit vector of the vector"""
        return vector / np.linalg.norm(vector)

    def angle(vector1: np.array, vector2: np.array) -> float:
        """ Returns the angle in degrees between given vectors"""
        v1_u = unit_vector(vector1)
        v2_u = unit_vector(vector2)
        minor = np.linalg.det(
            np.stack((v1_u[-2:], v2_u[-2:]))
        )
        if minor == 0:
            return 0. # return 0 degrees if vectors are parallel
        return np.degrees(np.sign(minor) * np.arccos(np.clip(np.dot(v1_u, v2_u), -1.0, 1.0)))

    def interpolate_line_point(line: LineString) -> Point | None:
        """
        Interpolates a line point by given length.

        :param line: The line to interpolate.
        :return: The interpolated point on the line.
        """
        proj_line = transform(project_forward, line)
        # to vector
        vec = np.array([proj_line.coords[1][0] - proj_line.coords[0][0], proj_line.coords[1][1] - proj_line.coords[0][1]])
        if vec[0] == 0. and vec[1] == 0.:
            return None
        interpolated_vec = unit_vector(vec) * args.wedge_length
        return transform(project_back, Point(proj_line.coords[0][0] + interpolated_vec[0], proj_line.coords[0][1] + interpolated_vec[1]))

    # counter
    c = 0

    # load all river paths
    cur.execute(f"select {args.river_id_column}, {args.river_geo_column} from {args.river_table}")
    for data in cur:
        recroadid: str = data[0]
        path: LineString = wkb.loads(data[1])

        # this will hold the depths for each point
        widths = np.zeros(len(path.coords) - 2)

        # skip first and last points - they are our start and end points - they will be interpolated later
        for i in range(1, len(path.coords) - 1):
            c += 1 # increase counter

            coord = path.coords[i]
            p = Point(coord[0], coord[1])
            wkb_point = wkb.dumps(p, srid=args.crs_source)

            # check if the point is within the river
            cur2.execute("SELECT ST_Contains(geom, %s) FROM water_wip.all_river_body", (wkb_point,))
            if not cur2.fetchone()[0]:
                print("Warning... point outside river", p)
                we.point(coord[0], coord[1])
                we.record("point outside river")
                continue

            # find the closest point on the shoreline of any river
            cur2.execute("SELECT ST_ClosestPoint(geom, %s) FROM water_wip.all_water_lines", (wkb_point,))
            # we expect at exactly one result - and we assume that the point is within the river
            closest_point: Point = wkb.loads(cur2.fetchone()[0])
            if closest_point is None:
                print("Warning... closest_point", recroadid)
                # TODO: handle this
                continue

            # rotate using rotation matrix
            rot_l = rotate_opposite_point(p, closest_point, R_f)
            rot_r = rotate_opposite_point(p, closest_point, R_b)
            if rot_l is None or rot_r is None:
                we.point(coord[0], coord[1])
                we.record("invalid rotation of point")
                print("Warning... invalid rotation of point", rot_l, rot_r)
                # TODO: handle this
                continue

            # check line angle - rot_l and rot_r must not be greater than the respective angles
            vec = np.array([p.x - closest_point.x, p.y - closest_point.y])  # this is the opposite vector of our current line
            # get maximum angles
            max_l = angle(vec, np.array([rot_l.x - p.x, rot_l.y - p.y]))
            max_r = angle(vec, np.array([rot_r.x - p.x, rot_r.y - p.y]))
            # swap variables
            if max_l < max_r:
                rot_r, rot_l = rot_l, rot_r
                max_r, max_l = max_l, max_r
            # get angles to line before and after
            coords_before = path.coords[i-1]
            coords_after = path.coords[i+1]
            vec_before = np.array([coords_before[0] - p.x, coords_before[1] - p.y])
            vec_after = np.array([ coords_after[0] - p.x, coords_after[1] - p.y])
            before = angle(vec, vec_before)
            after = angle(vec, vec_after)
            if before == 0 and after == 0:
                # quite unlikely, but will handle this anyway
                print("Warning... angles too odd - both 0", recroadid)
                continue
            # special case when line is the same as the shortest path to the shore - angle is 0 in this case
            # we will set the angle to 180 degrees and let the code below handle this
            if before == 0:
                if after > 0:
                    before = -180
                else:
                    before = 180
            if after == 0:
                if before > 0:
                    after = -180
                else:
                    after = 180
            # swap variables
            if before < after:
                coords_after, coords_before = coords_before, coords_after
                before, after = after, before

            # compare both angles
            if max_l > before:
                # adjust angle to point before
                rot_l = interpolate_line_point(LineString([p, Point(coords_before[0], coords_before[1])]))
            if max_r < after:
                # adjust to after
                rot_r = interpolate_line_point(LineString([p, Point(coords_after[0], coords_after[1])]))

            # create triangular polygon to look for opposite shore
            triangle = Polygon([p, rot_l, rot_r])
            triangle_wkb = wkb.dumps(triangle, srid=args.crs_source)
            # find the closest opposite point on the shoreline of any river
            cur2.execute("SELECT ST_ClosestPoint(ST_Intersection(geom, %s), %s) FROM water_wip.all_water_lines", (triangle_wkb, wkb_point,))
            # we expect at exactly one result - and we assume that the point is within the river
            closest_opposite_point: Point = wkb.loads(cur2.fetchone()[0])
            if closest_opposite_point is None:
                we.point(coord[0], coord[1])
                we.record("closest_opposite_point not found, possibly outside river")
                print("Warning... closest_opposite_point not found, possibly outside river", recroadid, i, f'POINT({coord[0]} {coord[1]})')
                # TODO: handle this -> note to correct the path here
                continue

            # create line string
            length_line = LineString([(closest_point.x, closest_point.y), (coord[0], coord[1]), (closest_opposite_point.x, closest_opposite_point.y)])

            # calculate width in m
            width = transform(project_forward, length_line).length

            # original line
            w.line([length_line.coords])
            w.record(width)

            # add to river width
            widths[i-1] = np.float64(width)

        # update river width in database
        widths_str = "{" + list(widths).__str__()[1:-1] + "}"
        cur_upd.execute(f"UPDATE {args.river_table} SET {args.river_width_column} = '{widths_str}' WHERE {args.river_id_column} = '{recroadid}'")
        conn.commit()

    w.close()
    we.close()

    print(c, "widths calculated")
