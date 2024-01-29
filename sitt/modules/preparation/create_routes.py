# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create routes to be traversed by the simulation."""
import logging
import sys

import igraph as ig
import numpy as np
from shapely import reverse
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
        """Maximum difference from shortest route (factor, if greater than 1)"""

    def run(self, config: Configuration, context: Context) -> Context:
        if logger.level <= logging.INFO:
            logger.info("PreparationInterface CreateRoutes: creating routes and checking lengths")

        """prepare simulation"""
        # Checking start and stop hubs
        if not config.simulation_start:
            logger.error("simulation_start is empty - simulation failed!")
        if not config.simulation_end:
            logger.error("simulation_end is empty - simulation failed!")

        # get raw routes
        all_paths, min_length, max_length = self._get_raw_routes(config.simulation_start, config.simulation_end,
                                                                 context.graph)

        # prune routes
        all_paths = self._prune_routes_maximum_difference_from_shortest(all_paths, min_length, max_length)
        all_paths = self._prune_routes_maximum_routes(all_paths)

        # Now create the set of simple edge paths and then construct a directed graph from this. The directed graph will
        # contain all possible paths from source to target, so we can efficiently traverse it.
        edges_considered: set[int] = set()
        g = ig.Graph(directed=True)

        for p in all_paths:
            g = self._add_directed_graph(p[1], config.simulation_start, context.graph, g)

        context.routes = g

        # # TODO: https://github.com/guilhermemm/k-shortest-path
        # # Besser ist Eppstein?
        # # Siehe auch igraph-Packet
        # # Networkx slow: https://www.timlrx.com/blog/benchmark-of-popular-graph-network-packages-v2
        # # k-shortest paths using Yen's or Eppstein's algorithm
        # # Yen for networkx/igraph:
        # # https://stackoverflow.com/questions/15878204/k-shortest-paths-implementation-in-igraph-networkx-yens-algorithm
        # print("TODO")
        # exit()
        # for p in nx.all_simple_edge_paths(context.graph, config.simulation_start, config.simulation_end):
        #     r = SortableRoute()
        #     r.length = 0.
        #     r.legs = p
        #
        #     # get total length
        #     for leg in p:
        #         r.length += context.graph[leg[0]][leg[1]][leg[2]]['length_m']
        #
        #     insort(sorted_routes, r)
        #
        # for p in all_routes:
        #     for leg in p:
        #         if not context.routes.has_edge(leg[0], leg[1], leg[2]):
        #             context.routes.add_edge(leg[0], leg[1], leg[2])

        if logger.level <= logging.INFO:
            logger.info("PreparationInterface CreateRoutes: finished creating routes and checking lengths - "
                        f"considered {len(all_paths)} routes. Created directed graph with {len(g.vs)} "
                        f"vertices and {len(g.es)} edges.")

        return context

    def _get_raw_routes(self, simulation_start: str, simulation_end: str, g: ig.Graph)\
            -> tuple[list[tuple[float, list[int]]], float, float]:
        """
        Get raw shortest paths from start to end - this will return a list of tuples (distance, path), containing the
        path (edge numbers in igraph) and the distance from start to end (in m). The method also returns the shortest
        and longest distance as float.

        :param simulation_start:
        :param simulation_end:
        :param g:
        :return: tuples of paths and their length, shortest and longest distance
        """

        # We create a number of shortest paths from the start to the end - this is a set of paths we can choose from.
        # We will reduce the numbers below.
        raw_shortest_paths = g.get_k_shortest_paths(simulation_start, to=simulation_end, k=100,
                                                    weights='length_m', mode='all', output='epath')  # edge path!

        longest_path_distance: float = sys.float_info.min
        shortest_path_distance: float = sys.float_info.max
        shortest_paths: list[tuple[float, list[int]]] = []

        # determine length of each shortest path
        for path in raw_shortest_paths:
            if len(path) > 0:
                distance = 0.0
                for e in path:
                    distance += g.es[e]["length_m"]

                shortest_paths.append((distance, path))

                if distance > longest_path_distance:
                    longest_path_distance = distance
                if distance < shortest_path_distance:
                    shortest_path_distance = distance

        return shortest_paths, shortest_path_distance, longest_path_distance

    def _prune_routes_maximum_difference_from_shortest(self, all_paths: list[tuple[float, list[int]]],
                                                       min_length: float, max_length: float)\
            -> list[tuple[float, list[int]]]:
        """
        Remove routes longer than a certain length, if set in config.

        :param all_paths:
        :param min_length:
        :param max_length:
        :return: Updated list of routes.
        """
        if self.maximum_difference_from_shortest > 1.0 and len(all_paths):
            maximum_length_allowed = min_length * self.maximum_difference_from_shortest
            # only continue, if maximum_length_allowed is lower than max_length
            if maximum_length_allowed <= max_length:
                path_copy: list[tuple[float, list[int]]] = []

                for path in all_paths:
                    if path[0] < maximum_length_allowed:
                        path_copy.append(path)

                if logger.level <= logging.INFO:
                    logger.info(
                        "PreparationInterface CreateRoutes: Maximum difference from shortest route allowed: Weeded out "
                        + str(len(all_paths) - len(path_copy)) + " routes, maximum length allowed: "
                        + str(maximum_length_allowed))

                return path_copy

        return all_paths

    def _prune_routes_maximum_routes(self, all_paths: list[tuple[float, list[int]]]) -> list[tuple[float, list[int]]]:
        """
        Prune routes, so that total number of routes is less than maximum_routes. This method will sort the routes by
        length, so shorter routes are preferred.

        :param all_paths:
        :return:
        """
        # prune routes by maximum_routes
        if 0 < self.maximum_routes < len(all_paths):
            # ok, we take the n shortest routes, so we need to sort them first
            all_paths = sorted(all_paths, key=lambda x: x[0])

            all_paths = all_paths[:self.maximum_routes]
            if logger.level <= logging.INFO:
                logger.info(
                    "PreparationInterface CreateRoutes: cutoff maximum number of routes to length "
                    + str(len(all_paths)))

        return all_paths

    def _add_directed_graph(self, edges: list[int], start: str, g: ig.Graph, tg: ig.Graph) -> ig.Graph:
        """
        Will add a directed graph to target graph from data of the source graph.

        :param edges: edge list to add from source graph to target graph
        :param start: start none name
        :param g: source graph
        :param tg: target graph
        :return: None
        """

        # add start vertex, last vertex will keep last vertex to start from, so we know how to direct the edges
        last_vertex = g.vs.find(name=start)
        self._add_vertex_to_graph(last_vertex.attributes(), tg)

        # traverse edges and check their direction
        for e in edges:
            edge = g.es[e]  # current edge to consider
            target: ig.Vertex | None = None  # keeps next target vertex
            flip: bool = False  # flip direction?

            if last_vertex.index == edge.source:  # edge is in correct order, add it to the target graph as is
                # set target vertex
                target = g.vs[edge.target]
            elif last_vertex.index != edge.source and last_vertex.index == edge.target:  # reverse edge
                # set source vertex
                target = g.vs[edge.source]
                flip = True
            else:
                # this case should not happen, but just to be safe...
                logger.fatal("PreparationInterface CreateRoutes: graph error - not consecutive vertices!")

            # add vertex
            self._add_vertex_to_graph(target.attributes(), tg)

            # add edge
            self._add_edge_to_graph(last_vertex['name'], target['name'], edge, flip, tg)

            last_vertex = target

        return tg

    def _add_vertex_to_graph(self, attributes: dict, g: ig.Graph) -> None:
        """
        Will add a vertex to a graph, if it does not exist yet (name = unique id)

        :param attributes: vertex attributes
        :param g: graph
        :return: None
        """
        try:
            g.vs.find(name=attributes['name'])
        except:
            g.add_vertex(**attributes)

    def _add_edge_to_graph(self, from_name: str, to_name: str, edge: ig.Edge, flip: bool, g: ig.Graph) -> None:
        """
        Will add an edge to a graph, if it does not exist yet (name = unique id)

        :param edge: edge to copy
        :param flip: flip direction?
        :param g: graph
        :return: None
        """
        try:
            g.es.find(name=edge['name'])
        except:
            # add new edge
            attr = edge.attributes().copy()
            attr['legs'] = np.copy(attr['legs'])
            attr['slopes'] = np.copy(attr['slopes'])

            if flip:
                # flip data
                attr['legs'] = np.flip(attr['legs'])
                attr['slopes'] = np.flip(attr['slopes']) * -1  # reverse slop degrees, too
                attr['geom'] = reverse(attr['geom'])
                attr['name'] = attr['name'] + '_rev'  # add new name - should be very unlikely, but just in case

            g.add_edge(from_name, to_name, **attr)

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CreateRoutes"
