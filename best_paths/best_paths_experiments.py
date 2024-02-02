# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Find the best paths for a given route through the graph"""

import time
import pickle
import random
import typing
import heapq
from urllib import parse
from pyproj import Transformer
import shapely.ops as sp_ops
import igraph as ig
import argparse
from sqlalchemy import create_engine, schema, Table, MetaData, Column, Integer, String, insert, delete
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import LineString
from typing import List, Tuple, NewType

"""
Good routes for testing:
    - start_id: 161, end_id: 369 (has diverse paths for the route and fast)
    - start_id: 18723, end_id: 15062 (for performance)
"""

EdgeList = NewType('EdgeList', List[int])
distance_cache = {}


class BestPathsResult:
    """
    Contains the results of the search for the k-best paths through
    a graph. The paths can be found with different algorithms. For
    easier operation on the paths the underlying graph is incorporated
    into the class. Paths are stored as EdgeLists.

    You can access edge data for any path with:

    # accesses the 4th edge of the first path in the paths list
    edge = self.graph.es[self.paths[0][1][3]]

    """

    def __init__(self, graph: ig.Graph, start_id: int, end_id: int):
        self.graph = graph
        self.start = start_id
        self.end = end_id
        self.paths: List[Tuple[str, EdgeList]] = []

    def summary(self) -> str:
        summary = f"graph:\n{self.graph.summary()}\n"
        summary += f"start vertex={self.start}, end vertex={self.end}\n"
        summary += f"paths count: {len(self.paths)}\n"

        for path_index in range(0, len(self.paths)):
            p = self.paths[path_index]
            d = 0
            for e in p[1]:
                d += self.graph.es[e]["length"]
            summary += f"\t[{path_index}]: type={p[0]}, length={d}\n"

        return summary


class Node:
    """
    Node class for custom A* implementation.
    """

    def __init__(self, vertex_id: int, cost_so_far: float, parent):
        self.vertex_id = vertex_id
        self.cost_so_far = cost_so_far
        self.parent = parent


def get_k_shortest_paths_a_star(start_index: int, end_index: int, k: int, graph: ig.Graph) -> typing.List[EdgeList]:
    paths = []

    for path in _a_star(start_index, end_index, graph):
        paths.append(path)
        if len(paths) == k:
            break

    return paths


def _a_star(start_index: int, end_index: int, graph: ig.Graph) -> EdgeList:

    first: Node = Node(start_index, 0, None)
    first_entry = (0, first)
    open_list: typing.List[typing.Tuple[float, Node]] = []
    open_list_set = {first.vertex_id: first_entry}
    heapq.heappush(open_list, first_entry)

    while len(open_list) > 0:

        current = heapq.heappop(open_list)[1]
        del open_list_set[current.vertex_id]

        # found a path
        if current.vertex_id == end_index:

            # build up the path as a list of vertices
            path_vertices = []
            node_entry = current
            while node_entry is not None:
                path_vertices.append(node_entry.vertex_id)
                node_entry = node_entry.parent
            path_vertices.reverse()

            # build EdgeList from vertices
            edge_list: EdgeList = EdgeList([])
            for vertex_index in range(1, len(path_vertices)):
                start_vertex = path_vertices[vertex_index - 1]
                end_vertex = path_vertices[vertex_index]
                matching_edge = graph.es.select(_within=[start_vertex, end_vertex])[0]
                edge_list.append(matching_edge.index)

            yield edge_list

        neighbors = g.vs[current.vertex_id].all_edges()
        for edge in neighbors:

            # if the edge leads to the parent we don't consider it. This is a speed-up
            # and prevents the algorithm from finding paths with round trips. This can
            # happen because we look for paths with more than one path.
            if (current.parent is not None and
                    (edge.target == current.parent.vertex_id or edge.source == current.parent.vertex_id)):
                continue

            new_cost = current.cost_so_far + edge["length"]
            neighbor_id = edge.target_vertex.index if edge.source_vertex.index == current.vertex_id \
                else edge.source_vertex.index

            if neighbor_id in open_list_set:
                old_node_entry = open_list_set[neighbor_id]
                old_node: Node = old_node_entry[1]
                if new_cost < old_node.cost_so_far:
                    open_list.remove(old_node_entry)
                    old_node.cost_so_far = new_cost
                    old_node.parent = current
                    h = new_cost + _heuristic(graph, old_node.vertex_id, end_index)
                    new_entry = (h, old_node)
                    heapq.heappush(open_list, new_entry)
                    open_list_set[old_node.vertex_id] = new_entry

            else:
                new_node = Node(neighbor_id, new_cost, current)
                distance_to_target = new_cost + _heuristic(graph, neighbor_id, end_index)
                entry = (distance_to_target, new_node)
                heapq.heappush(open_list, entry)
                open_list_set[new_node.vertex_id] = entry


def _heuristic(graph: ig.Graph, a: int, b: int):

    global distance_cache
    if (a, b) in distance_cache:
        return distance_cache[(a, b)]
    else:
        start_position = graph.vs[a]["world_position"]
        end_position = graph.vs[b]["world_position"]
        distance = start_position.distance(end_position)
        distance_cache[(a, b)] = distance
        return distance


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


def _get_best_paths_table() -> Table:
    return Table("best_paths", metadata_obj,
                 Column("id", Integer, primary_key=True, autoincrement=True),
                 Column("start", String),
                 Column("end", String),
                 Column("method", String),
                 Column("geom", Geometry(geometry_type="LINESTRING", srid=args.crs_no)),
                 schema=args.schema)


def _build_linestring_for_path(graph: ig.Graph, edge_list: EdgeList) -> LineString:

    vertices = []
    first_edge = graph.es[edge_list[0]]
    edge_geo = first_edge["geom"]
    points = list(edge_geo.coords)
    vertices.append(points)

    # We skip the first element because we already add it before we go into
    # the loop.
    iterator = iter(edge_list)
    next(iterator)
    for edge_index in iterator:

        edge_geo = graph.es[edge_index]["geom"]
        points = list(edge_geo.coords)

        first_point_in_sequence = points[0]
        last_point_in_sequence = points[-1]
        first_point_in_prior_sequence = vertices[-1][0]
        last_point_in_prior_sequence = vertices[-1][-1]

        # Not in every case the edge vertices are aligned in the same direction.
        # This means the end vertex of the prior edge is not always the start vertex
        # of the next edge. Therefore, in some cases we have to swap in which order we
        # add the vertices.
        if last_point_in_prior_sequence == first_point_in_sequence:
            vertices.append(points)

        elif last_point_in_prior_sequence == last_point_in_sequence:
            points.reverse()
            vertices.append(points)

        # In some cases it even happens that the beginning of the prior sequence is matched.
        # I think this only occurs directly on the beginning of graphs?
        # In that case also the already added vertices have to be swapped in the list.
        elif (first_point_in_prior_sequence == first_point_in_sequence or
              first_point_in_prior_sequence == last_point_in_sequence):

            last_segment = vertices[-1]
            last_segment.reverse()
            vertices[-1] = last_segment

            if first_point_in_prior_sequence == first_point_in_sequence:
                vertices.append(points)
            else:
                points.reverse()
                vertices.append(points)

        else:
            assert False, "Unreachable, there must be a edge case in this path"

    # As a last step we unroll all the segments into one continuous list of vertices
    vertices = [point for segment in vertices for point in segment]
    return LineString(vertices)


if __name__ == "__main__":
    """Read pickled graph."""

    # good graphs for testing:

    # parse arguments
    parser = argparse.ArgumentParser(
        description="Take to nodes in a graph and find the n-best paths",
        exit_on_error=False)
    parser.add_argument('action', default='help', choices=['help', 'input', 'run'],
                        help='action to perform')

    parser.add_argument('--start', dest='start', type=str, help='name of the start node')
    parser.add_argument('--end', dest='end', type=str, help='name of the end node')

    parser.add_argument('--crs', dest='crs_no', default=4326, type=int, help='projection')

    parser.add_argument('-H', '--server', dest='server', default='localhost', type=str, help='database server')
    parser.add_argument('-D', '--database', dest='database', default='sitt', type=str, help='path to database')
    parser.add_argument('-U', '--user', dest='user', default='postgres', type=str, help='postgres user')
    parser.add_argument('-P', '--password', dest='password', default='postgres', type=str, help='postgres password')
    parser.add_argument('-p', '--port', dest='port', default=5432, type=int, help='postgres port')

    parser.add_argument('--schema', dest='schema', default='wip', type=str, help='schema name')

    # parse or help
    args: argparse.Namespace | None = None

    try:
        args = parser.parse_args()
    except:
        parser.print_help()
        parser.exit(1)

    with open('graph_dump.pickle', 'rb') as f:
        g: ig.Graph = pickle.load(f)
        print(g.summary())

        # precompute all world positions to speed up A* heuristic
        transformer = Transformer.from_crs(4326, 32633, always_xy=True)
        world_positions = []
        for vertex in g.vs:
            position = sp_ops.transform(transformer.transform, vertex["center"])
            world_positions.append(position)
        g.vs["world_position"] = world_positions
        print(g.summary())

        vertex_count = len(g.vs)
        start_node = 0
        end_node = 0

        # select action
        if args.action == 'help':
            parser.print_help()
        elif args.action == 'input':
            start_node = input(f"Start node (index between 0-{vertex_count - 1} or random for a random point:")
            if start_node == "random":
                start_node = random.randint(0, vertex_count - 1)
            else:
                start_node = int(start_node)

            end_node = input(f"End node (index between 0-{vertex_count - 1} or random for a random point:")
            if end_node == "random":
                end_node = random.randint(0, vertex_count - 1)
            else:
                end_node = int(end_node)
        elif args.action == 'run':
            start_node = g.vs.select(name_eq=args.start)
            start_node = start_node[0].index
            end_node = g.vs.select(name_eq=args.end)
            end_node = end_node[0].index

        print(f"start node: {start_node}")
        print(f"end node: {end_node}")

        # setting up database access
        metadata_obj: MetaData = MetaData()
        conn = create_engine(
            _create_connection_string(args.server, args.database, args.user, args.password, args.port)).connect()

        if not conn.dialect.has_schema(conn, args.schema):
            conn.execute(schema.CreateSchema(args.schema))
            conn.commit()
            print("Created schema:", args.schema)
        else:
            print("Schema exists")

        best_paths_table = _get_best_paths_table()
        metadata_obj.create_all(bind=conn, checkfirst=True)
        conn.commit()

        stmt = delete(best_paths_table)
        conn.execute(stmt)

        # finding the best paths
        k_best_paths = BestPathsResult(g, start_node, end_node)

        start = time.time()
        builtin_paths = g.get_k_shortest_paths(start_node, end_node, k=5, weights=g.es["length"], output="epath")
        end = time.time()
        print(f"Method: igraph builtin")
        print(f"Found {len(builtin_paths)} paths")
        print(f"The operation took {(end - start):.2f} seconds.")

        for found_path in builtin_paths:
            k_best_paths.paths.append(("builtin", found_path))

        start = time.time()
        a_star_paths = get_k_shortest_paths_a_star(start_node, end_node, 5, g)
        end = time.time()
        for found_path in a_star_paths:
            k_best_paths.paths.append(("a*", found_path))

        print(f"Method: A*")
        print(f"Found {len(a_star_paths)} paths")
        print(f"The operation took {(end - start):.2f} seconds.")

        start = time.time()
        visited_edges = {}
        for i in range(0, 5):
            found_path = g.get_shortest_path_astar(start_node, end_node, _heuristic,
                                                   weights=g.es["length"],
                                                   output="epath")
            k_best_paths.paths.append(("a*-igraph", found_path))
            for edge in found_path:
                g.es[edge]["length"] *= 1.1
        end = time.time()
        print(f"Method: IGraph A*")
        print(f"The operation took {(end - start):.2f} seconds.")

        # saving and loading with pickle
        with open(f'graph.pickle', 'wb') as file:
            pickle.dump(k_best_paths, file)

        with open(f'graph.pickle', 'rb') as file:
            results: BestPathsResult = pickle.load(file)
            print(results.summary())

            print("Write paths to database")
            for path_info in results.paths:
                line = _build_linestring_for_path(g, path_info[1])
                geom = WKTElement(line.wkt, srid=args.crs_no)
                stmt = insert(best_paths_table).values(start=args.start, end=args.end,
                                                       method=path_info[0], geom=geom)
                conn.execute(stmt)

        conn.commit()
