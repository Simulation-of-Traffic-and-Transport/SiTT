# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Create basic river networks and persist them to disk. Preparation step for actual river network creation.
This script can take quite a while to finish.
"""

import argparse
import pickle
import sys
from urllib import parse

import igraph as ig
from shapely import wkb, STRtree, relate_pattern, centroid, shortest_line, union_all, line_merge, simplify, LineString
from shapely.ops import transform
from sqlalchemy import create_engine, text
from pyproj import Transformer


def _add_vertex(g: ig.Graph, water_body_id: int, idx: int, geom: object) -> str:
    """Add a vertex to the graph. if it does not exist yet. Returns the index of the vertex."""
    str_idx = 'river-' + str(water_body_id) + '-' + str(idx)
    try:
        g.vs.find(name=str_idx)
    except:
        # get center of geometry
        center = centroid(geom)

        geom_data = f"'SRID={args.crs_from};{geom.wkt}'"

        # calculate the depth of the water body in m, take from the closest measure point
        distance_rel = conn.execute(text(f"SELECT depth_m, st_distancespheroid(geom, {geom_data}) AS distance FROM sitt.water_depths WHERE water_body_id = {water_body_id} ORDER BY distance LIMIT 1")).fetchone()
        depth_m = float(distance_rel[0])

        # get shore lines
        shores = []
        for shore_entries in conn.execute(text(f"SELECT DISTINCT (ST_Dump(ST_Intersection(st_force2d(geom), {geom_data}))).geom FROM sitt.water_lines WHERE st_touches(st_force2d(geom), {geom_data})")):
            shores.append(wkb.loads(shore_entries[0]))

        # calculate width
        min_width = sys.float_info.max
        shore_length = len(shores)
        is_bump = False
        for i in range(shore_length):
            for j in range(i + 1, shore_length):
                m_w = transform(transformer.transform, shortest_line(shores[i], shores[j])).length
                if m_w < min_width:
                    min_width = m_w

        # if we have a single shore line or a length of 0, we probably have a "bump" in our river - calculate width a
        # bit differently
        if min_width < 0.1:
            # combine shores into a single line
            shore = simplify(line_merge(union_all(shores)), 0.000001)
            # max width is points of farthest lines in this "bump"
            max_width = transform(transformer.transform, LineString([shore.coords[0], shore.coords[-1]])).length
            is_bump = True

        g.add_vertex(str_idx, geom=geom, center=center, shores=shores, depth_m=depth_m, is_bump=is_bump)

    return str_idx


if __name__ == "__main__":
    """Create basic river networks and persist them to disk. Preparation step for actual river network creation."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Create basic river networks and persist them to disk. Preparation step for actual river network creation.",
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
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

    # connect to database
    conn = create_engine('postgresql://' + args.user + ':' + parse.quote_plus(args.password) + '@' + args.server + ':' +
                         str(args.port) + '/' + args.database).connect()

    # create transformer
    transformer = Transformer.from_crs(args.crs_from, args.crs_to, always_xy=args.always_xy)

    print("Creating networks from water body triangles...")

    # consider every water body in water_parts
    for water_body_id in conn.execute(text("SELECT DISTINCT water_body_id FROM sitt.water_parts WHERE is_river = true")):
        water_body_id = water_body_id[0]
        print("Networking water body", water_body_id)

        # get all data
        tree = []
        for row in conn.execute(text(f"SELECT geom FROM sitt.water_parts WHERE water_body_id = {water_body_id}")):
            tree.append(wkb.loads(row[0]))

        print("Got", len(tree), "parts to consider")

        # keeps entities already considered and still to consider
        already_considered = set()
        to_consider = set()
        to_consider.add(0)  # we will always consider the first entry

        # efficient tree tester for fast geometry functions
        tree: STRtree = STRtree(tree)

        # graph
        g = ig.Graph()
        counter = 0

        while len(to_consider) > 0:
            idx = to_consider.pop()
            already_considered.add(idx)
            entity = tree.geometries.take(idx)

            str_idx = _add_vertex(g, water_body_id, idx, entity)

            # get neighbors
            for id in tree.query(entity, 'touches'):
                if id in already_considered or not relate_pattern(entity, tree.geometries.take(id), '****1****'):
                    continue
                to_consider.add(id)

                # add vertex
                str_id = _add_vertex(g, water_body_id, id, tree.geometries.take(id))

                g.add_edge(str_idx, str_id)

            counter += 1
            if counter % 1000 == 0:
                print(counter, "shapes processed, water body", water_body_id)

        # pickle graph
        with open('graph_dump_' + str(water_body_id) + '.pickle', 'wb') as f:
            pickle.dump(g, f)

        print("Graph saved to 'graph_dump_" + str(water_body_id) + ".pickle'")
