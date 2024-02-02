# SPDX-FileCopyrightText: 2023-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Find the best paths for a given route through the graph"""

import igraph as ig
from typing import List, Tuple, NewType
from pyproj import Transformer
import shapely.ops as sp_ops

EdgeList = NewType('EdgeList', List[int])


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

    def __init__(self, graph: ig.Graph, start: str, end: str):
        self.graph = graph
        self.start: str = start
        self.end: str = end
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


class PathWeeder:

    def __init__(self, graph: ig.Graph):
        self.graph: ig.Graph = graph.copy()
        self.base_graph = graph
        self.distance_cache = {}

    def init(self, crs_from: int, crs_to: int):
        transformer = Transformer.from_crs(crs_from, crs_to, always_xy=True)
        world_positions = []
        for vertex in self.graph.vs:
            position = sp_ops.transform(transformer.transform, vertex["center"])
            world_positions.append(position)
        self.graph.vs["world_position"] = world_positions

    def get_k_paths(self, start: str, end: str, k: int) -> BestPathsResult:

        def _heuristic(graph: ig.Graph, a: int, b: int):
            if (a, b) in self.distance_cache:
                return self.distance_cache[(a, b)]
            else:
                start_position = graph.vs[a]["world_position"]
                end_position = graph.vs[b]["world_position"]
                distance = start_position.distance(end_position)
                self.distance_cache[(a, b)] = distance
                return distance

        result = BestPathsResult(self.base_graph, start, end)

        for i in range(0, k):

            start_id = self.graph.vs.find(name=start)
            end_id = self.graph.vs.find(name=end)

            path = self.graph.get_shortest_path_astar(start_id, end_id, _heuristic,
                                                      weights=self.graph.es["length"],
                                                      output="epath")

            result.paths.append(("a*-igraph", path))

            # this makes a visited edge more expensive to take in the next search
            for edge in path:
                self.graph.es[edge]["length"] *= 1.1

        self.graph.es["length"] = self.base_graph.es["length"]
        self.distance_cache = {}
        return result
