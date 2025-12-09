# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Example of how to combine the data aggregated in calculate_river_depths.py, calculate_river_slopes.py, and
calculate_river_widths.py into flow data using the Gauckler-Manning-Strickler flow formula.
"""
import argparse

import numpy as np
import psycopg2
import shapefile
from pyproj import Transformer
from shapely import wkb, Point, LineString, force_3d
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
    parser.add_argument('-rg', '--river-geo-column', dest='river_geo_column', default='geom', type=str, help='river geometry column (default is the segmented geometry column created by create_river_segments.py)')
    parser.add_argument('-rgs', '--river-geo-segments-column', dest='river_geo_segments_column', default='geom_segments', type=str, help='river geometry column (default is the segmented geometry column created by create_river_segments.py)')
    parser.add_argument('-rd', '--river-depths-column', dest='river_depths_column', default='depths', type=str, help='river depths column (will be created, if not existing)')
    parser.add_argument('-rs', '--river-slope-column', dest='river_slope_column', default='slope', type=str, help='river slope column')
    parser.add_argument('-rw', '--river-width-column', dest='river_width_column', default='width', type=str, help='river width column')
    parser.add_argument('-rf', '--river-flow-column', dest='river_flow_column', default='flow', type=str, help='river flow column')
    parser.add_argument('-rfc', '--river-flow-geometry-column', dest='river_flow_geometry_column', default='geom_flow', type=str, help='river flow geometry column (linestring)')

    # chunk settings
    parser.add_argument('--chunk-size', dest='chunk_size', default=10, type=float, help='Size of chunks to put back together')

    # flow settings
    # see: https://www.bauformeln.de/wasserbau/gerinnehydraulik/rauheitsbeiwerte-nach-strickler/ for more examples
    parser.add_argument('-k', '--kst', dest='kst', default=30, type=float, help='Gauckler–Manning-Strickler coefficient')
    parser.add_argument('-t', '--trapezoid', dest='is_trapezoid', default=False, type=bool, help='Assume trapezoid river bed, rectangular otherwise.')

    # projection settings
    parser.add_argument('--crs-source', dest='crs_source', default=4326, type=int, help='projection source')
    parser.add_argument('--crs-target', dest='crs_target', default=32633, type=int, help='projection target (has to support meters)')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    project_forward = Transformer.from_crs(args.crs_source, args.crs_target, always_xy=True).transform

    # Connect to the database
    conn = psycopg2.connect(host=args.server, dbname=args.database, user=args.user, password=args.password,
                            port=args.port)
    cur = conn.cursor()
    cur_upd = conn.cursor()

    def create_split_indexes(length: float) -> np.array:
        """
        Creates an array of segment lengths for splitting a river segment into smaller segments.
        The segments are distributed evenly, as far as possible.

        :param length: Length of segments.
        :return: Array of segment index lengths.
        """
        # special cases of very short segments
        if length < args.chunk_size:
            return np.array([int(np.floor(length/2)), int(np.ceil(length/2))])

        # we deduct 5 from start and end points, because these belong to those
        l = length - args.chunk_size
        # get rest
        rest = l % args.chunk_size
        # get number of segments based on the rest and the length, but always round down to a whole number
        n_segments = int((l-rest) / args.chunk_size)

        # create segment array, fill with 10s, respectively 5 for the first and last point
        segments = np.full(n_segments + 2, args.chunk_size)
        segments[0] = args.chunk_size / 2
        segments[-1] = args.chunk_size / 2

        # special case: if the rest is more than n_segments
        if rest > n_segments:
            if rest < args.chunk_size / 2:
                i = 0
                while rest > 0:
                    segments[i] += 1
                    rest -= 1
                    i += 1
                    if i >= len(segments):
                        i = 0
                return segments
            # very short, length == 15-19
            if n_segments == 0:
                return np.array([args.chunk_size / 2, rest, args.chunk_size / 2])
            # add short segment into middle
            pos = int(np.floor(n_segments / 2)) + 1
            # calculate segments in the middle to be 10 + rest
            segments[pos] = int(np.floor((args.chunk_size + rest)/2))
            return np.insert(segments, pos, int(np.ceil((args.chunk_size + rest)/2)))
        # less than 5, create some 11 elements segments
        if rest < args.chunk_size / 2:
            pos = int(np.floor(n_segments/2)) + 1
            for i in range(int(np.ceil(pos - rest/2)), int(np.ceil(pos + rest/2))):
                segments[i] = args.chunk_size + 1
            return segments

        # more than 4, add one segment and insert segments of 9 in the middle
        segments = np.insert(segments, 1, args.chunk_size) # add segment
        n_segments += 1
        rest = args.chunk_size - rest
        pos = int(np.floor(n_segments / 2)) + 1
        for i in range(int(np.ceil(pos - rest / 2)), int(np.ceil(pos + rest / 2))):
            segments[i] = args.chunk_size - 1
        return segments

    def create_segmentable_list(length: float) -> np.array:
        """
        Creates an array for splitting river data into smaller segments (usable by numpy).

        :param length: Length of segments.
        :return: array of index positions for splitting the river data.
        """
        segment_lengths = create_split_indexes(length)
        return np.cumsum(segment_lengths)[:-1] # remove last position

    # add flow column, if needed
    schema, table = args.river_table.split('.')
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table}' AND table_schema= '{schema}' AND column_name = '{args.river_flow_column}')")
    if not cur.fetchone()[0]:
        cur_upd.execute(f"ALTER TABLE {args.river_table} ADD {args.river_flow_column} double precision[]")
        conn.commit()
        print("Adding column for river flows...")

    # add flow geometry column, if needed
    cur.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '{table}' AND table_schema= '{schema}' AND column_name = '{args.river_flow_geometry_column}')")
    if not cur.fetchone()[0]:
        cur_upd.execute(f"ALTER TABLE {args.river_table} ADD {args.river_flow_geometry_column} geometry(LineStringZ,4326)")
        conn.commit()
        print("Adding geometry column for river flows...")

    # create shapefile to check lines
    w = shapefile.Writer(target='river_flows', shapeType=shapefile.POINT, autoBalance=True)
    w.field("width", "N", decimal=10)
    w.field("depth", "N", decimal=10)
    w.field("slope", "N", decimal=10)
    w.field("flow", "N", decimal=10)

    # error file
    we = shapefile.Writer(target='river_flows_errors', shapeType=shapefile.POINT, autoBalance=True)
    we.field("reason", "C")

    c = 0

    # load all river paths
    cur.execute(f"select {args.river_id_column}, {args.river_geo_column}, {args.river_geo_segments_column}, {args.river_depths_column}, {args.river_slope_column}, {args.river_width_column} from {args.river_table}")
    for data in cur:
        if data[2] is None or data[3] is None or data[4] is None or data[5] is None:
            print(f"Skipping {data[0]} (missing data)")
            continue

        recroadid: str = data[0]
        path: LineString = wkb.loads(data[1])
        segments: LineString = wkb.loads(data[2])
        depths: np.ndarray = np.array(data[3])
        slope: np.float64 = np.float64(data[4])
        widths: np.ndarray = np.array(data[5])
        # append first and last point to widths
        widths = np.insert(np.append(widths, 0.), 0, 0.)

        # check sanity of data - lengths should be equal
        if len(segments.coords) != len(depths) or len(segments.coords) != len(widths):
            print(f"Skipping {recroadid} (sanity check failed)")
            print(len(depths))
            print(len(widths))
            print(len(segments.coords))
            continue

        # create split indexes for calculation
        indexes = create_segmentable_list(len(segments.coords))
        depth_sections: list[np.array] = np.split(depths, indexes)
        coord_sections: list[np.array] = np.split(np.array(segments.coords), indexes)
        width_sections: list[np.array] = np.split(widths, indexes)
        size = len(depth_sections)

        # this will hold the flows for each point
        flows = np.zeros(size)
        flow_coordinates = np.zeros((size, 2))

        for i in range(size):
            # get river segment lengths for setting weights
            section_lengths = np.zeros(len(coord_sections[i]) - 1)
            for j in range(len(coord_sections[i]) - 1):
                line = LineString([(coord_sections[i][j][0], coord_sections[i][j][1]), (coord_sections[i][j+1][0], coord_sections[i][j+1][1])])
                section_lengths[j] = transform(project_forward, line).length

            section_weights = np.zeros(len(coord_sections[i]))
            for j in range(len(coord_sections[i])):
                if j == 0:
                    section_weights[j] = section_lengths[j] / 2
                elif j == len(coord_sections[i]) - 1:
                    section_weights[j] = section_lengths[j-1] / 2
                else:
                    section_weights[j] = section_lengths[j-1] / 2 + section_lengths[j] / 2

            # create weights per section
            section_weights = section_weights / np.sum(section_weights)

            # calculate average depth and width for this river segment - use weights from sections
            average_depth = -np.average(depth_sections[i], weights=section_weights)
            if average_depth <= 0.:
                average_depth = 0.2 # set depth to 0.2 m if depth is negative
            average_width = np.average(width_sections[i], weights=section_weights)
            if i == 0:
                # take first point of starting point
                coords = coord_sections[i][0]
            elif i == size - 1:
                # take last point of ending point
                coords = coord_sections[i][-1]
            else:
                # take middle point
                coords = coord_sections[i][len(coord_sections[i]) // 2]
            # create point without Z
            p = Point(coords[0], coords[1])

            # calculate flow velocity using Gauckler-Manning-Strickler (Kst)
            # taken from https://www.gabrielstrommer.com/rechner/fliessgeschwindigkeit-durchfluss/
            if args.is_trapezoid:
                # cross-sectional area for trapezoid river bed
                # we assume bottom of river is 50% of river width
                w1 = average_width
                w2 = average_width / 2.
                a = (w1 + w2) / 2 * average_depth
                # sides can be calculated as right triangles
                c = (average_depth ** 2 + ((w1 - w2) / 2) ** 2) ** 0.5
                u = w2 + 2 * c
            else:
                # cross-sectional area for rectangular river bed
                a = average_width * average_depth
                u = average_width + 2 * average_depth
            # hydraulic radius
            if u == 0 or a == 0:
                r = 0.
            else:
                r = a / u
            # Gauckler-Manning-Strickler flow formula
            vm = args.kst * r ** (2 / 3) * slope ** (1 / 2) # flow rate is in m/s
            if np.isnan(vm):
                we.point(coords[0], coords[1])
                if np.isnan(slope):
                    we.record("slope is NaN")
                elif np.isnan(average_width):
                    we.record("width is NaN")
                elif average_depth < 0:
                    we.record("depth is negative")
                else:
                    we.record("vm is NaN")
                # fix for import
                vm = 0.

            # write to shapefile
            w.point(coords[0], coords[1])
            w.record(average_width, average_depth, slope, vm)

            # add to list of flows
            flows[i] = np.float64(vm)
            flow_coordinates[i] = np.array((coords[0], coords[1]))

        c += len(flows)
        new_coords = force_3d(LineString(flow_coordinates)).wkt

        # update river width in database
        flows_str = "{" + ','.join([str(flow) for flow in flows]) + "}"
        cur_upd.execute(f"UPDATE {args.river_table} SET {args.river_flow_column} = '{flows_str}', {args.river_flow_geometry_column} = st_geomfromewkt('SRID={args.crs_source};{new_coords}') WHERE {args.river_id_column} = '{recroadid}'")
        conn.commit()

    w.close()
    we.close()

    print(f"Created {c} river flow points.")
