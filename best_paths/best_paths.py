# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Find the best paths for a given route through the graph"""

import igraph as ig
import pickle
from typing import List, Tuple, NewType
from pyproj import Transformer
import shapely.ops as sp_ops

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
        # TODO: change start_id: int, end_id: int to str and search vertices using self.graph.vs.find(name=start_id)
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


with open('graph_dump.pickle', 'rb') as f:
    g: ig.Graph = pickle.load(f)
    print(g.summary())

    # the world position is needed for the A* heuristic
    # doing it upfront saved a lot of time
    transformer = Transformer.from_crs(4326, 32633, always_xy=True)
    world_positions = []
    for vertex in g.vs:
        position = sp_ops.transform(transformer.transform, vertex["center"])
        world_positions.append(position)
    g.vs["world_position"] = world_positions

    # any vertex id of the graph can be used
    # should be a parameter off course
    start_node = 18723
    end_node = 15062

    k_best_paths = BestPathsResult(g, start_node, end_node)

    for i in range(0, 5):
        found_path = g.get_shortest_path_astar(start_node, end_node, _heuristic,
                                               weights=g.es["length"],
                                               output="epath")
        k_best_paths.paths.append(("a*-igraph", found_path))

        # this makes a visited edge more expensive to take in the next search
        for edge in found_path:
            g.es[edge]["length"] *= 1.1

    print(k_best_paths.summary())
