# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Segment rivers and water bodies - this is stuff that may take a very long time to complete, so it should be
done in advance."""

import argparse
import pickle
import sys
import zlib
from os.path import exists
from urllib import parse

import igraph as ig
import pyproj
import shapely.ops as sp_ops
from extremitypathfinder import PolygonEnvironment
from geoalchemy2 import Geometry
from pyproj import Transformer
from shapely import wkb, is_ccw, \
    intersection, STRtree, LineString, Polygon, MultiPolygon, Point, \
    relate_pattern, centroid, shortest_line
from sqlalchemy import Connection, create_engine, Table, Column, literal_column, insert, schema, MetaData, \
    Integer, Boolean, String, Float, select, text, func

from precalculation.path_weeder import PathWeeder


def init():
    """Initialize database."""
    print("Initializing database from",
          _create_connection_string(args.server, args.database, args.user, args.password, args.port, for_printing=True))

    conn = create_engine(
        _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()

    # ensure schemas
    if not conn.dialect.has_schema(conn, args.topology_schema):
        conn.execute(schema.CreateSchema(args.topology_schema))
        print("Created schema:", args.topology_schema)
    if not conn.dialect.has_schema(conn, args.wip_schema):
        conn.execute(schema.CreateSchema(args.wip_schema))
        print("Created schema:", args.wip_schema)

    # ensure tables
    _get_water_body_table()
    _get_parts_table()
    _get_water_depths()
    metadata_obj.create_all(bind=conn, checkfirst=True)
    conn.commit()


def networks():
    """Create networks from water body triangles. This is the most complex of the process."""
    print("Create networks from water body triangles. This is the most complex part of the process.")

    # database stuff
    conn = create_engine(
        _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()
    water_body_table = _get_water_body_table()
    parts_table = _get_parts_table()
    nodes_table = _get_water_nodes_table()
    edges_table = _get_water_edges_table()

    metadata_obj.create_all(bind=conn, checkfirst=True)

    conn.execute(text("TRUNCATE TABLE " + args.wip_schema + ".water_nodes"))
    conn.execute(text("TRUNCATE TABLE " + args.wip_schema + ".water_edges"))
    conn.commit()

    transformer = Transformer.from_crs(args.crs_no, args.crs_to, always_xy=True)

    # get ids of water bodies that are connected to hubs
    water_bodies = _get_water_bodies_to_consider(conn, water_body_table)

    # read water body entries
    for body in conn.execute(water_body_table.select().where(water_body_table.c.id.in_(water_bodies.keys()))):
        print("Networking water body", body[0], "- river:", body[2])

        # river?
        if body[2]:  # col 2 is bool true or false, true is river
            if exists('graph_dump_' + str(body[0]) + '.pickle'):
                print("Loading graph from pickle file graph_dump_" + str(body[0]) + ".pickle")

                with open('graph_dump_' + str(body[0]) + '.pickle', 'rb') as f:
                    g = pickle.load(f)
            else:
                # get all data
                tree = []
                stmt = select(parts_table.c.geom).select_from(parts_table).where(parts_table.c.water_body_id == body[0])
                for row in conn.execute(stmt):
                    tree.append(wkb.loads(row[0].desc))

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

                # connect harbors to the closest parts - these are the starting points of our graph
                for harbor in water_bodies[body[0]]:
                    # normalize, remove z
                    p = Point(harbor[1].x, harbor[1].y)

                    # create harbor vertex
                    g.add_vertex(harbor[0], geom=p, center=p, harbor=True)

                    # get nearest neighbor and add vertex to the graph
                    id = tree.nearest(p)
                    str_id = _add_vertex(g, body[0], id, tree.geometries.take(id))

                    # add edge
                    g.add_edge(harbor[0], str_id)

                print("Added", len(water_bodies[body[0]]), "harbors to graph")

                while len(to_consider) > 0:
                    idx = to_consider.pop()
                    already_considered.add(idx)
                    entity = tree.geometries.take(idx)

                    str_idx = _add_vertex(g, body[0], idx, entity)

                    # get neighbors
                    for id in tree.query(entity, 'touches'):
                        if id in already_considered or not relate_pattern(entity, tree.geometries.take(id), '****1****'):
                            continue
                        to_consider.add(id)

                        # add vertex
                        str_id = _add_vertex(g, body[0], id, tree.geometries.take(id))

                        g.add_edge(str_idx, str_id)

                    counter += 1
                    if counter % 1000 == 0:
                        print(counter, "shapes processed, water body", body[0])

                # pickle graph
                with open('graph_dump_' + str(body[0]) + '.pickle', 'wb') as f:
                    pickle.dump(g, f)

                print("Graph saved to 'graph_dump_" + str(body[0]) + ".pickle'")

            print("Neighbors created/loaded, compacting graph.")

            # compact graph
            # in a way, we do something similar to http://szhorvat.net/mathematica/IGDocumentation/#igsmoothen - but we
            # need to preserve the geometry
            # inspired by https://stackoverflow.com/questions/68499507/reduce-number-of-nodes-edges-of-a-graph-in-nedworkx

            # get all nodes that are simple connectors or end points (degree <= 2)
            is_chain = [vertex for vertex in g.vs if 0 < vertex.degree() <= 2]
            # vertices that are not simple connectors or end points (degree > 2) - will be added to
            connectors = [vertex['name'] for vertex in g.vs if vertex.degree() > 2]

            # create a copy of our graph - add connectors and dangling endpoints first
            tg: ig.Graph = g.subgraph(connectors)
            for e in tg.es:
                # direct connection
                source = tg.vs[e.source]
                target = tg.vs[e.target]
                e['name'] = source['name'] + '=' + target['name']
                e['geom'] = LineString([source['center'], target['center']])
                e['min_width'] = min(_get_minimum_distance_in_polygon(source['geom'], target['center'], transformer),
                                     _get_minimum_distance_in_polygon(target['geom'], target['center'], transformer))
                e['length'] = sp_ops.transform(transformer.transform, e['geom']).length

            # find chain components - this is a list of single edge lists
            # walk chains and get endpoints for each component cluster
            for component in g.subgraph(is_chain).connected_components().subgraphs():
                # Case 1: single endpoint without neighbors
                if component.vcount() == 1:
                    # find vertex in original graph and look for neighbors
                    source = component.vs[0]['name']
                    neighbors = g.vs.find(name=source).neighbors()
                    # in the original graph, this point might be a single endpoint - or between two connectors
                    if len(neighbors) == 1:
                        # endpoint
                        target = neighbors[0]['name']
                    elif len(neighbors) == 2:
                        # connector
                        source = neighbors[0]['name']
                        target = neighbors[1]['name']
                    else:
                        print("fatal error: too many neighbors", source, neighbors)
                        sys.exit(-1)
                # Case 2: line of points - two endpoints
                else:
                    names_to_exclude = [vertex['name'] for vertex in component.vs]
                    endpoints = [vertex['name'] for vertex in component.vs if vertex.degree() == 1]
                    if len(endpoints) != 2:
                        print("fatal error: too many endpoints", endpoints)
                        sys.exit(-1)
                    source = component.vs.find(name=endpoints[0])['name']
                    target = component.vs.find(name=endpoints[1])['name']
                    # in the original graph, find neighbors that are not on the current line
                    possible_neighbor = _get_outer_neighbor(g, source, names_to_exclude)
                    if possible_neighbor is not None:
                        source = possible_neighbor

                    possible_neighbor = _get_outer_neighbor(g, target, names_to_exclude)
                    if possible_neighbor is not None:
                        target = possible_neighbor

                # add vertices to new graph
                try:
                    tg.vs.find(name=source)
                except:
                    source_node = g.vs.find(name=source)
                    tg.add_vertices(1, attributes=source_node.attributes())
                try:
                    tg.vs.find(name=target)
                except:
                    target_node = g.vs.find(name=target)
                    tg.add_vertices(1, attributes=target_node.attributes())

                # construct new edge from path
                [line, min_width] = _merge_path(g, source, target, transformer)
                length = sp_ops.transform(transformer.transform, line).length
                edge_name = source + '=' + target
                try:
                    tg.es.find(name=edge_name)
                except:
                    tg.add_edge(source, target, geom=line, name=edge_name, length=length, min_width=min_width)

            # Compact the graph by only storing the edges necessary to travel from each to each harbor
            # on the 5 best routes to do so
            path_weeder: PathWeeder = PathWeeder(tg)
            path_weeder.init(args.crs_no, args.crs_to)

            harbors = tg.vs.select(harbor=True)
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

            for v in tg.vs:
                stmt = insert(nodes_table).values(
                    id=v['name'],
                    geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(v['center']) + "'"), water_body_id=body[0],
                    is_river=body[2])
                conn.execute(stmt)

            for e in tg.es:
                # just to make sure...
                if e['min_width'] is None:
                    e['min_width'] = 0.
                    # TODO: should be set!
                stmt = insert(edges_table).values(
                    id=e['name'],
                    geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(e['geom']) + "'"),
                    water_body_id=body[0],
                    is_river=body[2],
                    min_width=e['min_width'],
                    length=e['length'])
                conn.execute(stmt)

            conn.commit()

            # # pickle graph
            # # TODO: remove from debug
            # with open('graph_dump.pickle', 'wb') as f:
            #     pickle.dump(tg, f)
            #
            # print("Graph saved to 'graph_dump.pickle'")
        else:  # false, not a river =>
            # consider lake
            geom = wkb.loads(body[1].desc)

            # get smaller ring shape from geometry of lake - this is where the boats can navigate within
            # This is easier than doing the same in shapely (would need to transform into meters bases projection, then
            # transform back).
            result_shape: Polygon|MultiPolygon = wkb.loads(conn.execute(text(f"SELECT ST_Buffer(ST_GeogFromText('SRID={args.crs_no};{geom}'), -{args.lake_shore_distance})")).fetchone()[0])
            # buffer can split our shape into multiple sub polygons, so we need to take case of this
            shapes_to_check: list[Polygon] = []

            if type(result_shape) is MultiPolygon:
                for sub_shape in result_shape.geoms:
                    shapes_to_check.append(sub_shape)
            else:
                shapes_to_check.append(result_shape)

            for ring_shape in shapes_to_check:
                # now get the points where the harbors connect to the ring shape of the lake
                harbor_lines: list[tuple[str, Point]] = []
                points_for_navigation: list[list[Point]] = []

                for hub_geom in water_bodies[body[0]]:
                    nearest_points = sp_ops.nearest_points(hub_geom[1], ring_shape)
                    harbor_lines.append((hub_geom[0], hub_geom[1],))
                    points_for_navigation.append(nearest_points[1])

                # create environment for shortest paths
                environment = PolygonEnvironment()
                shore_line = ring_shape.exterior  # exterior hull
                if not is_ccw(shore_line):  # need to be counter-clockwise
                    shore_line = shore_line.reverse()

                holes: list[tuple[float, float]] = []  # keeps holes
                for hole in ring_shape.interiors:
                    if is_ccw(hole):  # need to be clockwise
                        hole = hole.reverse()
                    holes.append(list(hole.coords)[:-1])

                environment.store(list(shore_line.coords)[:-1], holes, validate=True)
                # TODO: Check, there is something to do, our path does not look right...
                # open issue on this matter: https://github.com/jannikmi/extremitypathfinder/issues/84

                # now get the shortest paths between all points
                for i in range(len(points_for_navigation)):
                    for j in range(i + 1, len(points_for_navigation)):
                        path, _ = environment.find_shortest_path(list(points_for_navigation[i].coords)[0], list(points_for_navigation[j].coords)[0])
                        path.insert(0, (harbor_lines[i][1].coords.xy[0][0], harbor_lines[i][1].coords.xy[1][0]))
                        path.append((harbor_lines[j][1].coords.xy[0][0], harbor_lines[j][1].coords.xy[1][0]))
                        shortest_path = LineString(path)
                        length = sp_ops.transform(transformer.transform, shortest_path).length

                        stmt = insert(edges_table).values(
                            id=harbor_lines[i][0] + '=' + harbor_lines[j][0] + ';' + str(zlib.crc32(wkb.dumps(shortest_path))),
                            geom=literal_column("'SRID=" + str(args.crs_no) + ";" + str(shortest_path) + "'"), water_body_id=body[0],
                            is_river=False, min_width=0., length=length)
                        conn.execute(stmt)
                        # TODO: any measurement for width?

                conn.commit()


def _get_water_bodies_to_consider(conn: Connection, water_body_table: Table) -> dict[int, list[tuple[str, Point]]]:
    """
    Narrow down water bodies to list of ids that are connected to harbor hubs + their hubs.
    :return: dict[int, list[str]]
    """
    hubs_table = _get_hubs_table()

    # dict of water body ids and connected hubs
    water_body_list: dict[int, list[tuple[str, Point]]] = {}

    cur = conn.execute(select(hubs_table.c.geom, hubs_table.c.rechubid, select(water_body_table.c.id).order_by(
        func.ST_DistanceSpheroid(hubs_table.c.geom, water_body_table.c.geom)).limit(
        1).scalar_subquery()).where(hubs_table.c.harbor == 'y').compile(
        compile_kwargs={'literal_binds': True}))
    for wb in cur:
        if wb[2] not in water_body_list:
            water_body_list[wb[2]] = []
        shp = wkb.loads(wb[0].desc)
        water_body_list[wb[2]].append((wb[1], shp,))

    return water_body_list


def _get_outer_neighbor(g: ig.Graph, name: str, excluded_names: list[str]) -> str | None:
    """
    Get the outer neighbor of a vertex in the graph. Neighbors must not be in the list of my_ids.
    :param g: graph
    :param name: name to look for
    :param excluded_names: list of vertex names to exclude
    :return:
    """
    neighbors = [vertex['name'] for vertex in g.vs.find(name=name).neighbors() if
                 vertex['name'] not in excluded_names]
    if len(neighbors) == 1:
        return neighbors[0]
    if len(neighbors) > 1:
        print("fatal error: too many neighbors", name, neighbors)
        sys.exit(-1)
    return None


def _get_minimum_distance_in_polygon(polygon: Polygon, center: Point, transformer: pyproj.Transformer) -> float:
    """Get the minimum distance between a point and a polygon boundary."""
    pts = sp_ops.nearest_points(polygon.boundary, center)
    return sp_ops.transform(transformer.transform, shortest_line(pts[0], pts[1])).length

def _add_vertex(g: ig.Graph, water_body_id: int, idx: int, geom: object) -> str:
    """Add a vertex to the graph. if it does not exist yet. Returns the index of the vertex."""
    str_idx = str(water_body_id) + '-' + str(idx)
    try:
        g.vs.find(name=str_idx)
    except:
        center = centroid(geom)

        g.add_vertex(str_idx, geom=geom, center=center)

    return str_idx


def _merge_path(g: ig.Graph, source: str, target: str, transformer: pyproj.Transformer) -> [LineString, float]:
    """merge a path from source to target in the graph into a single shape and edge"""
    points: list = []
    last_shape: Polygon | None = None

    shortest_path = g.get_shortest_paths(source, target)[0]

    # add center of first shape
    vertex = g.vs[shortest_path[0]]
    points.append(vertex['center'])

    # min width of first and last end points added...
    v2 = g.vs[shortest_path[-1]]
    if type(vertex['geom']) is Point or type(v2['geom']) is Point:
        # do not consider harbors
        min_width = 0
    else:
        min_width = min(_get_minimum_distance_in_polygon(vertex['geom'], vertex['center'], transformer),
                        _get_minimum_distance_in_polygon(v2['geom'], v2['center'], transformer)) * 2
        # *2, because we need minimum to both sides

    for id in shortest_path:
        vertex = g.vs[id]

        # find common line of both shapes and take the center of it to get the new point
        if last_shape is not None:
            common_line = intersection(last_shape, vertex['geom'])
            # calculate the length of the common line - this is the width of river
            length = sp_ops.transform(transformer.transform, common_line).length
            if length < min_width:
                min_width = length
            center: Point = centroid(common_line)
            if center.is_empty:
                points.append(vertex['center'])
            else:
                points.append(center)

        last_shape = vertex['geom']

    # add center of first and last shapes
    vertex = g.vs[shortest_path[-1]]
    points.append(vertex['center'])

    return LineString(points), min_width


def _create_connection_string(server: str, db: str, user: str, password: str, port: int, for_printing=False):
    """
    Create DB connection string

    :param for_printing: hide password, so connection can be printed
    """
    if for_printing:
        return 'postgresql://' + user + ':***@' + server + ':' + str(
            port) + '/' + db
    else:
        return 'postgresql://' + user + ':' + parse.quote_plus(password) + '@' + server + ':' + str(port) + '/' + db


def _get_water_body_table() -> Table:
    return Table(args.water_body_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry),
                 Column("is_river", Boolean),
                 schema=args.topology_schema)

def _get_water_nodes_table() -> Table:
    """
    Returns table containing calculated water nodes
    :return: table
    """
    return Table(args.water_nodes_table, metadata_obj,
                        Column("id", String(length=16), primary_key=True),
                        Column("geom", Geometry('POINT')),
                        Column("water_body_id", Integer, index=True),
                        Column("is_river", Boolean),
                        schema=args.wip_schema)

def _get_water_edges_table() -> Table:
    """
    Returns table containing calculated water edges
    :return: table
    """
    return Table(args.water_edges_table, metadata_obj,
                 Column("id", String(length=32), primary_key=True),
                 Column("geom", Geometry('LINESTRING')),
                 Column("water_body_id", Integer, index=True),
                 Column("is_river", Boolean),
                 Column("length", Float, default=0.),
                 Column("min_width", Float, default=0.),  # minimum width of water
                 Column("incline", Float, default=0.),  # incline angle of water
                 Column("medium_velocity", Float, default=0.),  # base velocity of this edge path
                 schema=args.wip_schema)

def _get_parts_table() -> Table:
    return Table(args.parts_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry('POLYGON')),
                 Column("water_body_id", Integer, index=True),
                 Column("is_river", Boolean),
                 schema=args.wip_schema)


def _get_hubs_table() -> Table:
    return Table(args.hubs_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry('POLYGON')),
                 Column("rechubid", String, index=True, unique=True),
                 Column("overnight", String),  # contains y/n
                 Column("harbor", String),  # contains y/n
                 schema=args.topology_schema)

def _get_water_depths() -> Table:
    return Table(args.water_depths_table, metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("geom", Geometry('POINT')),
                 Column("water_body_id", Integer, index=True),
                 Column("depth_m", Float),
                 schema=args.topology_schema)


if __name__ == "__main__":
    """Segment rivers and water bodies - this is stuff that may take a (very) long time."""

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Segment rivers and water bodies - this is stuff that will take a (very) long time to complete, so it should be done in advance.",
        exit_on_error=False)
    parser.add_argument('action', default='help', choices=['help', 'init', 'networks'],
                        help='action to perform')

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

    parser.add_argument('--topology-schema', dest='topology_schema', default='topology', type=str,
                        help='topology schema name')
    parser.add_argument('--wip-schema', dest='wip_schema', default='water_wip', type=str, help='water_wip schema name')
    parser.add_argument('--parts-table', dest='parts_table', default='parts', type=str, help='name of parts table')
    parser.add_argument('--water-depths-table', dest='water_depths_table', default='water_depths', type=str, help='name of water depths table')
    parser.add_argument('--water-nodes-table', dest='water_nodes_table', default='water_nodes', type=str, help='name of water nodes table')
    parser.add_argument('--water-edges-table', dest='water_edges_table', default='water_edges', type=str, help='name of water edges table')
    parser.add_argument('--water-body-table', dest='water_body_table', default='water_body', type=str,
                        help='name of water_body table')
    parser.add_argument('--original-hubs-table', dest='hubs_table', default='rechubs', type=str,
                        help='table containing original hubs (and harbors)')
    parser.add_argument('--lake-shore-distance', dest='lake_shore_distance', default=15, type=float,
                        help='Distance in meters between lake and shore for circular path along shore in lakes')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')
    parser.add_argument('--crs-to', dest='crs_to', default=32633, type=int,
                        help='target projection (for approximation of lengths)')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    # init meta data structure
    metadata_obj: MetaData = MetaData()

    # select action
    if args.action == 'help':
        parser.print_help()
        print("\nActions:")
        print("\ninit - initialize the database (create schemas/tables)")
        print("\nnetworks - create networks for water bodies from the triangles created in the segmentation")
    elif args.action == 'init':
        init()
    elif args.action == 'networks':
        networks()
