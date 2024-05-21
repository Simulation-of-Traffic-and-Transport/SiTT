# SPDX-FileCopyrightText: 2023-present Fabian Behrens, Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Find the best paths for a given route through the graph"""

import igraph as ig
import numpy as np
from typing import List, Tuple, NewType
from pyproj import Transformer
from shapely import ops

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

    def __init__(self, graph: ig.Graph, start: str, end: str, weight: str = "length"):
        self.graph = graph
        self.start: str = start
        self.end: str = end
        self.paths: List[Tuple[str, EdgeList]] = []
        self.weight = weight

    def summary(self) -> str:
        summary = f"graph:\n{self.graph.summary()}\n"
        summary += f"start vertex={self.start}, end vertex={self.end}\n"
        summary += f"paths count: {len(self.paths)}\n"

        for path_index in range(0, len(self.paths)):
            p = self.paths[path_index]
            d = 0
            for e in p[1]:
                d += self.graph.es[e][self.weight]
            summary += f"\t[{path_index}]: type={p[0]}, length={d}\n"

        return summary


class PathWeeder:
    """
    This class can be used to compact the graph by only storing the edges necessary to travel from each to each harbor
    based on an A* heuristic.
    """
    def __init__(self, graph: ig.Graph, weight: str = "length", center: str = "center"):
        self.graph: ig.Graph = graph.copy()  # make a copy of the graph, because we will modify it
        self.base_graph = graph
        self.distance_cache = {}
        self.weight = weight
        self.center = center

    def init(self, crs_from: int, crs_to: int) -> None:
        transformer = Transformer.from_crs(crs_from, crs_to, always_xy=True)
        world_positions = []
        for vertex in self.graph.vs:
            position = ops.transform(transformer.transform, vertex[self.center])
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
                # if distance > 0:
                #     # calculate slope
                #     diff_h = (end_position.z - start_position.z) / distance
                #     if diff_h > 0:
                #         # use exponential function to increase the cost of higher slopes
                #         distance = ((diff_h + 1) ** 5) * distance
                #     if diff_h < 0:
                #         distance = 1.01 * distance
                #         # use exponential function to reduce the cost of higher slopes - not as much as increasing
                #         # slopes
                #         distance = ((diff_h + 1) ** 3) * distance
                self.distance_cache[(a, b)] = distance
                return distance

        result = BestPathsResult(self.base_graph, start, end, self.weight)

        for i in range(0, k):

            start_id = self.graph.vs.find(name=start)
            end_id = self.graph.vs.find(name=end)

            path = self.graph.get_shortest_path_astar(start_id, end_id, _heuristic,
                                                      weights=self.graph.es[self.weight],
                                                      output="epath", mode="all")

            result.paths.append(("a*-igraph", path))

            # this makes a visited edge more expensive to take in the next search
            for edge in path:
                self.graph.es[edge][self.weight] *= 1.1

        self.graph.es[self.weight] = self.base_graph.es[self.weight]
        self.distance_cache = {}
        return result
