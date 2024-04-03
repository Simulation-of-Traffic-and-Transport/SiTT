# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
TODO
"""

import argparse
import pickle
import sys
from os.path import exists, abspath
from urllib import parse
from zlib import crc32

import igraph as ig
import rasterio
from geoalchemy2 import Geometry, WKTElement
from pyproj import Transformer
from shapely import Polygon, Point, LineString, shortest_line, intersection, centroid, union, force_2d, force_3d, wkb
from shapely.ops import transform
from sqlalchemy import create_engine, Table, Column, MetaData, \
    String, Float, Boolean, JSON, text, insert

from path_weeder import PathWeeder


def _add_vertex(g: ig.Graph, attributes):
    try:
        g.vs.find(name=attributes['name'])
    except:
        g.add_vertices(1, attributes=attributes)

def _get_outer_neighbor(g: ig.Graph, name: str, excluded_names: list[str]) -> ig.Vertex | None:
    """
    Get the outer neighbor of a vertex in the graph. Neighbors must not be in the list of excluded_names.
    :param g: graph
    :param name: name to look for
    :param excluded_names: list of vertex names to exclude
    :return:
    """
    neighbors = [vertex for vertex in g.vs.find(name=name).neighbors() if
                 vertex['name'] not in excluded_names]
    if len(neighbors) == 1:
        return neighbors[0]
    if len(neighbors) > 1:
        print("fatal error: too many neighbors", name, neighbors)
        sys.exit(-1)
    return None


def _expand_point_list_with_outer_neighbors(og: ig.Graph, tg: ig.Graph, name: str, excluded_names: list[str]) -> str:
    """
    Expand the target graph with the outer neighbors of a vertex in the original graph, if applicable
    :param og: original graph
    :param tg: target graph
    :param name: name of vertex to find neighbors for
    :param excluded_names: list of vertex names to exclude
    :return:
    """
    neighbor = _get_outer_neighbor(og, name, excluded_names)
    if neighbor is not None:
        # add new vertex to the target graph
        tg.add_vertices(1, attributes=neighbor.attributes())
        # connect
        tg.add_edge(name, neighbor['name'])
        return neighbor['name']
    return name


def _get_segments(g: ig.Graph, source: str, target: str) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    last_vertex = None

    # get list of vertices along the shortest path
    path = g.get_shortest_path(source, target)
    for v_id in path:
        v = g.vs[v_id]
        if last_vertex is None:
            last_vertex = v
            continue
        # compare this vertex with the last one
        if v['depth_m'] != last_vertex['depth_m']:
            # add segment to list
            segments.append((last_vertex['name'], v['name']))
            last_vertex = v

    # close
    last_node_name = g.vs[path[-1]]['name']
    if last_node_name != last_vertex['name']:
        segments.append((last_vertex['name'], last_node_name))

    return segments


def _get_minimum_distance_in_vertex(vertex: ig.Vertex, transformer: Transformer) -> float:
    """Get the minimum distance between a point and a polygon boundary."""
    return transform(transformer.transform, shortest_line(vertex['geom'].boundary, vertex['center'])).length


def _update_edge_attributes_of_direct_neighbors(tg: ig.Graph, transformer: Transformer):
    """
    Update existing edge attributes of direct neighbors
    :param tg: target graph to update
    :param transformer: transformer
    :return:
    """
    for e in tg.es:
        # direct connection
        source = tg.vs[e.source]
        target = tg.vs[e.target]
        e['name'] = source['name'] + '=' + target['name']
        e['geom'] = LineString([source['center'], target['center']])
        # minimum width is width of common line
        common_line = intersection(source['geom'], target['geom'])
        e['min_width'] = transform(transformer.transform, common_line).length
        e['length'] = transform(transformer.transform, e['geom']).length
        e['depth_m'] = (source['depth_m'] + target['depth_m']) / 2
        e['shape'] = union(source['geom'], target['geom'])


def _create_compacted_line_data(og: ig.Graph, tg: ig.Graph, source: str, target: str, transformer: Transformer):
    """merge a path from source to target in the graph into a single shape and edge"""

    points: list = []
    last_shape: Polygon | None = None

    shortest_path = og.get_shortest_path(source, target)

    # add center of first shape
    vertex = og.vs[shortest_path[0]]
    depth_m = vertex['depth_m']  # depth is same for all vertices along this line
    points.append(vertex['center'])
    min_width = sys.float_info.max
    complete_shape: Polygon = vertex['geom']

    # TODO: check
    # min width of first and last end points added...
    # min_width = min(_get_minimum_distance_in_vertex(vertex, transformer) * 2,
    #                 _get_minimum_distance_in_vertex(g.vs[shortest_path[-1]], transformer)) * 2
    # # *2, because we need minimum to both sides

    for id in shortest_path:
        vertex = og.vs[id]

        # find common line of both shapes and take the center of it to get the new point
        if last_shape is not None:
            common_line = intersection(last_shape, vertex['geom'])
            # calculate the length of the common line - this is the width of river
            length = transform(transformer.transform, common_line).length
            if length < min_width:
                min_width = length
            center: Point = centroid(common_line)
            if center.is_empty:
                points.append(vertex['center'])
            else:
                points.append(center)

            complete_shape = union(complete_shape, vertex['geom'])

        last_shape = vertex['geom']

    # add center of first and last shapes
    vertex = og.vs[shortest_path[-1]]
    points.append(vertex['center'])
    geom = LineString(points)

    # create edge
    tg.add_edge(source, target, name=source + '=' + target + '-' + hex(crc32(geom.wkb)), geom=geom, min_width=min_width,
                length=transform(transformer.transform, geom).length, depth_m=depth_m, shape=complete_shape)


def _get_harbor_groups() -> dict[str, list[tuple[str, Point, float]]]:
    harbors_for_water_bodies = {}  # keeps is_rivers
    # get all harbors and get nearest water bodies
    for harbor in conn.execute(text("SELECT id, geom FROM sitt.hubs WHERE harbor = true")):
        result = conn.execute(text(
            f"SELECT id, geom, is_river FROM sitt.water_bodies ORDER BY ST_DistanceSpheroid(geom, '{harbor[1]}') LIMIT 1")).fetchone()
        # add harbor to list
        if result[0] not in harbors_for_water_bodies:
            harbors_for_water_bodies[result[0]] = []
        harbor_geom = wkb.loads(harbor[1])
        harbors_for_water_bodies[result[0]].append((harbor[0], force_2d(harbor_geom), harbor_geom.z))

    return harbors_for_water_bodies


def _get_edges_table() -> Table:
    # define edge table
    idCol = Column('id', String, primary_key=True)
    geom_col = Column('geom', Geometry)
    hub_id_a = Column('hub_id_a', String)
    hub_id_b = Column('hub_id_b', String)
    edge_type = Column('type', String)
    cost_a_b = Column('cost_a_b', Float)
    cost_b_a = Column('cost_b_a', Float)
    data_col = Column('data', JSON)
    return Table("edges", MetaData(), idCol, geom_col, hub_id_a, hub_id_b, edge_type, cost_a_b,
                 cost_b_a, data_col, schema=args.schema)


def _get_hubs_table() -> Table:
    # define hubs table
    idCol = Column('id', String, primary_key=True)
    geom_col = Column('geom', Geometry)
    overnight = Column('overnight', Boolean)
    harbor = Column('harbor', Boolean)
    market = Column('market', Boolean)
    data_col = Column('data', JSON)
    return Table("hubs", MetaData(), idCol, geom_col, overnight, harbor, market, data_col,
                 schema=args.schema)


if __name__ == "__main__":
    """TODO"""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="TODO",  # TODO
        exit_on_error=False)

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')
    parser.add_argument('--schema', dest='schema', default='sitt', type=str, help='schema name')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')
    parser.add_argument('--crs-to', dest='crs_to', default=32633, type=int,
                        help='target projection (for approximation of lengths)')
    parser.add_argument('--xy', dest='always_xy', default=True, type=bool, help='use the traditional GIS order')
    parser.add_argument('-i', '--input-file', dest='file', required=True, type=str, help='input file (GeoTIFF)')
    parser.add_argument('-b', '--band', dest='band', default=1, type=int, help='band to use from GeoTIFF')
    parser.add_argument('-k', '--kst', dest='kst', default=25, type=float,
                        help='Gauckler–Manning-Strickler coefficient')
    parser.add_argument('-t', '--trapezoid', dest='is_trapezoid', default=True, type=bool,
                        help='Assume trapezoid river bed, rectangular otherwise.')

    parser.add_argument('--empty-edges', dest='empty_edges', default=False, type=bool,
                        help='empty edges database before import')
    parser.add_argument('--delete-rivers', dest='delete_rivers', default=True, type=bool,
                        help='delete river edges and hubs from database before import')

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

    # define tables
    edges_table = _get_edges_table()
    hubs_table = _get_hubs_table()

    transformer = Transformer.from_crs(args.crs_no, args.crs_to, always_xy=args.always_xy)

    # load geo
    rds: rasterio.io.DatasetReader = rasterio.open(abspath(args.file))
    rds_transformer = Transformer.from_crs(args.crs_no, rds.crs, always_xy=args.always_xy)
    # get relevant band
    band = rds.read(args.band)

    print("Loading pickled graphs and working on them...")

    # load harbors
    harbor_groups = _get_harbor_groups()

    # consider every water body in water_parts
    for water_body_id in conn.execute(text("SELECT DISTINCT water_body_id FROM sitt.water_parts WHERE is_river = true")):
        water_body_id = water_body_id[0]
        print("************** Loading network for water body", water_body_id, "**************")

        if not exists('graph_dump_' + str(water_body_id) + '.pickle'):
            print("Graph file graph_dump_" + str(water_body_id) + ".pickle does not exist, skipping...")
            continue

        # check harbors existence for this water body
        if water_body_id not in harbor_groups:
            print("No harbors found for water body", water_body_id, " - skipping...")
            continue

        print("Loading graph from pickle file graph_dump_" + str(water_body_id) + ".pickle")

        g: ig.Graph = pickle.load(open('graph_dump_' + str(water_body_id) + '.pickle', 'rb'))

        # Add harbors to graph
        print("Adding harbors to graph")

        closest_vertices = {}

        for v in g.vs:
            for harbor in harbor_groups[water_body_id]:
                dist = v['center'].distance(harbor[1])
                if harbor[0] not in closest_vertices or dist < closest_vertices[harbor[0]][0]:
                    closest_vertices[harbor[0]] = (dist, v)

        # add harbor vertices
        for harbor in harbor_groups[water_body_id]:
            v1 = g.add_vertex(name=harbor[0], geom=harbor[1], center=harbor[1], is_harbor=True)
            v2 = closest_vertices[harbor[0]][1]

            g.add_edge(v1, v2, name=harbor[0] + '-' + v2['name'])

        print("Compacting graph - creating river segments")

        # compact graph
        # in a way, we do something similar to http://szhorvat.net/mathematica/IGDocumentation/#igsmoothen - but we
        # need to preserve the geometry
        # inspired by https://stackoverflow.com/questions/68499507/reduce-number-of-nodes-edges-of-a-graph-in-nedworkx

        # create a copy of our graph - add connectors first => connectors are all nodes with more than 2 degrees
        # tg is our target graph
        tg: ig.Graph = g.subgraph([vertex['name'] for vertex in g.vs if vertex.degree() > 2])
        # add data to edges in subgraph
        _update_edge_attributes_of_direct_neighbors(tg, transformer)

        # walk chain components - these all have degree <= 2
        # if 1 => end points
        # if 2 => simple connectors between two shapes
        # walk chains and get endpoints for each component cluster
        for component in g.subgraph([vertex for vertex in g.vs if 0 < vertex.degree() <= 2]).connected_components().subgraphs():
            # Case 1: single endpoint without multiple neighbors
            if component.vcount() == 1:
                # find vertex in original graph and look for neighbors
                source = component.vs[0]
                neighbors = g.vs.find(name=source['name']).neighbors()
                # in the original graph, this point might be a single endpoint - or between two connectors
                if len(neighbors) == 1:
                    # endpoint
                    target = neighbors[0]
                elif len(neighbors) == 2:
                    # connector
                    source = neighbors[0]
                    target = neighbors[1]
                else:
                    print("fatal error: too many neighbors", source, neighbors)
                    sys.exit(-1)

                # add vertices
                _add_vertex(tg, source.attributes())
                _add_vertex(tg, target.attributes())

                # construct edge
                _create_compacted_line_data(g, tg, source['name'], target['name'], transformer)

            # Case 2: line of points - two endpoints
            else:
                names_to_exclude = [vertex['name'] for vertex in component.vs]
                endpoints = [vertex['name'] for vertex in component.vs if vertex.degree() == 1]
                if len(endpoints) != 2:
                    print("fatal error: too many endpoints", endpoints)
                    sys.exit(-1)
                # source and target are the endpoints of the line
                source = component.vs.find(name=endpoints[0])['name']
                target = component.vs.find(name=endpoints[1])['name']

                # expand to connector points, so we can connect to points on the target graph
                source = _expand_point_list_with_outer_neighbors(g, component, source, excluded_names=names_to_exclude)
                target = _expand_point_list_with_outer_neighbors(g, component, target, excluded_names=names_to_exclude)

                # get segments of equal depth
                for segment in _get_segments(component, source, target):
                    # add vertices
                    _add_vertex(tg, component.vs.find(name=segment[0]).attributes())
                    _add_vertex(tg, component.vs.find(name=segment[1]).attributes())

                    # construct edge
                    _create_compacted_line_data(component, tg, segment[0], segment[1], transformer)

        print("Compacting finished. Adding heights of vertices in order to calculate slopes.")

        for v in tg.vs:
            # add height
            xx, yy = rds_transformer.transform(v['center'].x, v['center'].y)
            x, y = rds.index(xx, yy)
            height = band[x, y]
            v['center'] = Point(v['center'].x, v['center'].y, height)

        for e in tg.es:
            # add slopes
            source = tg.vs[e.source]
            target = tg.vs[e.target]

            # harbor?
            if source['is_harbor'] or target['is_harbor']:
                # make shippable
                e['slope'] = 0.
                e['flow_rate'] = 0.
                e['flow_from'] = 'none'
                e['min_width'] = 1000.
                e['depth_m'] = 1000.
                continue

            h1 = source['center'].z
            h2 = target['center'].z
            diff = h1 - h2
            # we calculate the length from average of the two endpoints + line length, should be relatively accurate
            # TODO: find a better solution, like taking the shape and creating a center line between the shore lines
            l1 = transform(transformer.transform, force_2d(LineString([source['center'], target['center']]))).length
            l2 = e['length']
            length = l1 + l2 / 2
            slope = abs(diff) / length
            # taken from https://www.gabrielstrommer.com/rechner/fliessgeschwindigkeit-durchfluss/
            if args.is_trapezoid:
                # cross-sectional area for trapezoid river bed
                # we assume bottom of river is 50% of river width
                w1 = e['min_width']
                w2 = e['min_width'] / 2
                a = (w1+w2)/2 * e['depth_m']
                # sides can be calculated as right triangles
                c = (e['depth_m']**2 + ((w1-w2)/2)**2)**0.5
                u = w2 + 2*c
            else:
                # cross-sectional area for rectangular river bed
                a = e['min_width'] * e['depth_m']
                u = e['min_width'] + 2*e['depth_m']
            # hydraulic radius
            r = a / u
            # Gauckler-Manning-Strickler flow formula
            vm = args.kst * r**(2/3) * slope**(1/2)

            # add everything to edge
            e['slope'] = slope
            e['flow_rate'] = vm
            if diff > 0:
                e['flow_from'] = source['name']
            elif diff < 0:
                e['flow_from'] = target['name']
            else:
                e['flow_from'] = 'none'

        ##############################################################################################
        print("Weeding out graph to edges necessary to travel each harbor.")
        # Compact the graph by only storing the edges necessary to travel from each to each harbor
        # on the 5 best routes to do so
        path_weeder: PathWeeder = PathWeeder(tg)
        path_weeder.init(args.crs_no, args.crs_to)

        harbors = tg.vs.select(is_harbor=True)
        graphs = []
        for start_harbor_index in range(0, len(harbors)):
            for end_harbor_index in range(start_harbor_index + 1, len(harbors)):
                start_name = harbors[start_harbor_index]["name"]
                end_name = harbors[end_harbor_index]["name"]
                weeded_paths = path_weeder.get_k_paths(start_name, end_name, 5)
                for path in weeded_paths.paths:
                    subgraph: ig.Graph = weeded_paths.graph.subgraph_edges(path[1])
                    graphs.append(subgraph)

        base_graph = graphs[0]
        tg = base_graph.union(graphs[1:], byname=True)

        # create riversTruncate edges?
        if args.empty_edges:
            print("Truncating edges database...")
            conn.execute(text("TRUNCATE TABLE " + args.schema + ".edges;"))
            conn.commit()
        elif args.delete_rivers:  # delete rivers from edges table?
            print("Deleting river edges and hubs from database...")
            conn.execute(text("DELETE FROM " + args.schema + ".edges WHERE type = 'river';"))
            conn.execute(text("DELETE FROM " + args.schema + ".hubs WHERE data @> '{\"type\": \"river\"}';"))
            conn.commit()

        # create hubs
        print("Creating hubs...")
        for v in tg.vs:
            # do not insert existing hubs
            result = conn.execute(text("SELECT id FROM " + args.schema + ".hubs WHERE id = '{}'".format(v['name']))).fetchone()
            if result is None:
                geo_stmt = WKTElement(v['center'].wkt, srid=args.crs_no)
                is_harbor = ('is_harbor' in v.attributes() and v['is_harbor']) or False

                # enter edge into hubs table
                stmt = insert(hubs_table).values(id=v['name'], geom=geo_stmt, overnight=False, harbor=is_harbor,
                                                 market=False,
                                                 data={"type": "river", "depth_m": v['depth_m'],
                                                       "min_width": v['min_width'], "max_width": v['max_width'],
                                                       "is_bump": v['is_bump'], "water_body_id": water_body_id})
                conn.execute(stmt)

        conn.commit()

        # create edges
        print("Creating edges...")
        for e in tg.es:
            geo_stmt = WKTElement(force_3d(e['geom']).wkt, srid=args.crs_no)
            from_id = tg.vs[e.source]['name']
            to_id = tg.vs[e.target]['name']

            # calculate cost of edges
            # kph = e['flow_rate'] * 3.6  # km/h from m/s
            # TODO: create a cost formula that makes sense
            cost_a_b = 1.
            cost_b_a = 1.

            stmt = insert(edges_table).values(id=e['name'], geom=geo_stmt, hub_id_a=from_id,
                                              hub_id_b=to_id, type='river', cost_a_b=cost_a_b, cost_b_a=cost_b_a,
                                              data={"length_m": e['length'], "shape": e['shape'].wkt, "slope": e['slope'],
                                                    "flow_rate": e['flow_rate'], "min_width": e['min_width'],
                                                    "depth_m": e['depth_m'], "flow_from": e['flow_from'],
                                                    "water_body_id": water_body_id})
            conn.execute(stmt)

        conn.commit()

print("Done!")
