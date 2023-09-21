# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create routes to be traversed by the simulation."""
import logging
from bisect import insort

import igraph as ig
import yaml

from sitt import BaseClass, Configuration, Context, PreparationInterface

logger = logging.getLogger()


class SortableRoute:
    legs: []
    length: float

    def __lt__(self, other):
        return self.length < other.length


class CreateRoutes(BaseClass, PreparationInterface):
    """
    Create routes to be traversed by the simulation. This is used right before the simulation starts.
    You need precalculated routes for this, so run CalculatePathsAndHubs before calling this class.
    """

    def __init__(self, maximum_routes: int = 0, maximum_difference_from_shortest: float = 0.):
        super().__init__()
        self.maximum_routes: int = maximum_routes
        """Maximum number of routes to retain (if greater than 0, x shortest routes will be retained)."""
        self.maximum_difference_from_shortest: float = maximum_difference_from_shortest
        """Maximum difference from shortest route (if greater than 0)"""

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info("PreparationInterface CreateRoutes: creating routes and checking lengths")

        """prepare simulation"""
        # Checking start and stop hubs
        if not config.simulation_start:
            logger.error("simulation_start is empty - simulation failed!")
        if not config.simulation_end:
            logger.error("simulation_end is empty - simulation failed!")

        # We first create the set of simple edge paths and then construct a directed graph from this. The directed graph
        # will contain all possible paths from source to target, so we can efficiently traverse it.
        context.routes = ig.Graph(directed=True)

        # create sorted routes in order of increasing lengths
        sorted_routes = []

        # iGraph
        g = ig.Graph()

        for node in context.graph.nodes(data=True):
            g.add_vertex(node[0], **node[1])

        for edge in context.graph.edges(data=True):
            g.add_edge(edge[0], edge[1], **edge[2])

        out = g.get_k_shortest_paths(config.simulation_start, config.simulation_end, k=100, weights=g.es['length_m'], mode="all")

        print(len(out))

        # TODO: https://github.com/guilhermemm/k-shortest-path
        # Besser ist Eppstein?
        # Siehe auch igraph-Packet
        # Networkx slow: https://www.timlrx.com/blog/benchmark-of-popular-graph-network-packages-v2
        # k-shortest paths using Yen's or Eppstein's algorithm
        # Yen for networkx/igraph:
        # https://stackoverflow.com/questions/15878204/k-shortest-paths-implementation-in-igraph-networkx-yens-algorithm
        print("TODO")
        exit()
        for p in nx.all_simple_edge_paths(context.graph, config.simulation_start, config.simulation_end):
            r = SortableRoute()
            r.length = 0.
            r.legs = p

            # get total length
            for leg in p:
                r.length += context.graph[leg[0]][leg[1]][leg[2]]['length_m']

            insort(sorted_routes, r)

        # remove routes longer than a certain length, if set
        if self.maximum_difference_from_shortest > 0 and len(sorted_routes):
            maximum_length = sorted_routes[0].length * self.maximum_difference_from_shortest

            found = -1
            for idx in range(len(sorted_routes) - 1):
                if sorted_routes[idx + 1].length > maximum_length:
                    found = idx
                    break

            # special case: cutoff on first entry
            if found == 0:
                sorted_routes = []
                logger.info("PreparationInterface CreateRoutes: cutoff longer routes to length 0")
            if found > 0:
                sorted_routes = sorted_routes[:found]
                if logger.level <= logging.INFO:
                    logger.info(
                        "PreparationInterface CreateRoutes: cutoff longer routes to length " + str(len(sorted_routes)))

        all_routes = list(map(lambda a: a.legs, sorted_routes))

        # prune routes by maximum_routes
        if 0 < self.maximum_routes < len(all_routes):
            all_routes = all_routes[:self.maximum_routes]
            if logger.level <= logging.INFO:
                logger.info(
                    "PreparationInterface CreateRoutes: cutoff maximum number of routes to length " + str(
                        len(all_routes)))

        for p in all_routes:
            for leg in p:
                if not context.routes.has_edge(leg[0], leg[1], leg[2]):
                    context.routes.add_edge(leg[0], leg[1], leg[2])

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CreateRoutes"
