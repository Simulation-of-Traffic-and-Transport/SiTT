# SPDX-FileCopyrightText: 2023-present Fabian Behrens, Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Find the best paths for a given route through the graph"""

import igraph as ig
from typing import List, Tuple, NewType
import shapefile
import shapely
import os.path as path

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
        self.paths: List[Tuple[str, list[int]]] = []
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
        self.crs_from = 0
        self.crs_to = 0

    def init(self, crs_from: int, crs_to: int) -> None:
        self.crs_from = crs_from
        self.crs_to = crs_to

    def get_k_paths(self, start: str, end: str, k: int) -> BestPathsResult:
        # # create articulation_points
        # articulation_points = []
        # for idx in self.graph.articulation_points():
        #     articulation_points.append(self.graph.vs[idx]['name'])
        #
        # # now test hubs around articulation_points if they can reach start and end points if graph is cut at articulation_point
        # cuttable_hubs = []
        #
        # for articulation_point in articulation_points:
        #     # do not cut at start or end points
        #     if articulation_point == start or articulation_point == end:
        #         continue
        #
        #     tg: ig.Graph = self.graph.copy()
        #     v = tg.vs.find(name=articulation_point)
        #     neighbors = v.neighbors()
        #
        #     tg.delete_vertices(v)
        #
        #     cut_count = 0
        #     for neighbor in neighbors:
        #         nname = neighbor['name']
        #         if nname in cuttable_hubs or nname == start or nname == end:
        #             continue
        #
        #         reached_start = False
        #         reached_end = False
        #         for check_vertex in tg.bfsiter(neighbor.index):
        #             if check_vertex['name'] == start:
        #                 reached_start = True
        #             elif check_vertex['name'] == end:
        #                 reached_end = True
        #             if reached_start and reached_end:
        #                 break
        #
        #         if not reached_start and not reached_end:
        #             cuttable_hubs.append(nname)
        #             cut_count += 1
        #
        #     # # also add articulation_point to cuttable_hubs if all neighbors are cuttable hubs or there is only one neighbor left
        #     # if cut_count >= len(neighbors) - 1:
        #     #     cuttable_hubs.append(articulation_point)
        #
        # # copy graph and delete cuttable_hubs
        # tg: ig.Graph = self.graph.copy()
        # tg.delete_vertices(cuttable_hubs)
        start_v = self.graph.vs.find(name=start)
        end_v = self.graph.vs.find(name=end)
        start_id = start_v.index
        end_id = end_v.index
        #
        # found = None
        # for g in tg.connected_components():
        #     if not (start_id in g and end_id in g):
        #         tg.delete_vertices(g)
        #
        # # ok, this is our final graph
        # convert_graph_to_shapefile(tg, ".", "target_graph.shp")
        # self.graph = tg
        # # TODO: continue
        # exit(0)

        result = BestPathsResult(self.base_graph, start, end, self.weight)

        for i in range(0, k):
            # path = get_shortest_path(self.graph, start_id, end_id)
            path = self.graph.get_shortest_path(v=start_id, to=end_id, weights=self.weight, output="epath")

            result.paths.append(("graph", path))

            # this makes a visited edge more expensive to take in the next search
            for edge in path:
                self.graph.es[edge][self.weight] *= 1.1

        self.graph.es[self.weight] = self.base_graph.es[self.weight]
        self.distance_cache = {}
        return result


def convert_graph_to_shapefile(graph: ig.Graph, output_path: str, shapefile_name: str) -> None:
    w = shapefile.Writer(target=path.join(output_path, shapefile_name), shapeType=shapefile.POLYLINE, autoBalance=True)
    w.field("name", "C")

    for e in graph.es:
        geom: shapely.LineString = e['geom']
        if shapely.is_ccw(geom):  # need to be clockwise
            geom = geom.reverse()

        coords = list([c[0], c[1]] for c in list(coord for coord in geom.coords))
        w.line([coords])
        w.record(e["name"])

    w.close()


# TODO:
# google-drive://ronix75@googlemail.com/GVfsSharedWithMe/1lT4TAgWlKEnnDHOmJ9FRQVDb2al5GouV/1GWd14DWct9sE441Si6GTr_MgmPjNSsml/1lyFQ5XBcCy9WYkyl5ybetIRG-vxN6V5p
# Flusstiefen
