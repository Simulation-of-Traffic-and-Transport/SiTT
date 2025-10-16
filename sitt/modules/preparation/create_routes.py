# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create routes to be traversed by the simulation."""
import logging

import igraph as ig
import numpy as np
import yaml

from sitt import BaseClass, Configuration, Context, PreparationInterface
from sitt.convert_graph_to_directed import convert_graph_to_directed

logger = logging.getLogger()

class CreateRoutes(BaseClass, PreparationInterface):
    """
    Create routes to be traversed by the simulation. This is used right before the simulation starts.
    You need precalculated routes for this, so run CalculatePathsAndHubs before calling this class.
    """

    def __init__(self, check_graph: bool = False):
        super().__init__()
        self.check_graph: bool = check_graph
        """Maximum difference from shortest route (factor, if greater than 1)"""


    def run(self, config: Configuration, context: Context) -> Context:
        """prepare simulation"""
        starts = config.simulation_starts
        ends = config.simulation_ends
        autodetect_starts = False
        autodetect_ends = False
        if starts is None or len(starts) == 0:
            autodetect_starts = True
            starts = "[autodetect]"
        if ends is None or len(ends) == 0:
            autodetect_ends = True
            ends = "[autodetect]"
        if config.simulation_route is None:
            logger.error("simulation_route is empty - simulation failed!")

        if logger.level <= logging.INFO:
            logger.info(
                f"PreparationInterface CreateRoutes: using route {config.simulation_route} (reversed: {config.simulation_route_reverse}) from {starts} to {ends}")

        # convert to directed graph
        context.routes = convert_graph_to_directed(context.graph, config.simulation_route, config.simulation_route_reverse)

        if autodetect_starts:
            config.simulation_starts = self.detect_starts_end(context.routes, 'in')
            if logger.level <= logging.INFO:
                logger.info(f"Autodetected simulation_starts: {config.simulation_starts}")

        if autodetect_ends:
            config.simulation_ends = self.detect_starts_end(context.routes, 'out')
            if logger.level <= logging.INFO:
                logger.info(f"Autodetected simulation_ends: {config.simulation_ends}")

        if self.check_graph:
            self.run_check_graph(config, context.routes)

        if logger.level <= logging.INFO:
            logger.info(f"PreparationInterface CreateRoutes: Created directed graph with {len(context.routes.vs)} "
                        f"vertices and {len(context.routes.es)} edges.")

        return context

    def run_check_graph(self, config: Configuration, g: ig.Graph):
        # do we have disjunct graphs?
        components = len(g.connected_components(mode='weak'))
        if components > 1:
            logger.error(f"WARNING: Graph has {components} disjunct components. Please make sure this is correct!")

        # test paths from starts to end
        for hub in config.simulation_starts:
            try:
                paths = g.get_shortest_paths(v=hub, to=config.simulation_ends)
                for idx, path in enumerate(paths):
                    steps = len(path)
                    if steps == 0:
                        logger.warning("Pathfinding: No path from hub {hub} to {config.simulation_ends[idx]}")
                    else:
                        logger.info(f"Pathfinding: Shortest path from hub {hub} to {config.simulation_ends[idx]} has {steps} steps.")
            except Exception as e:
                logger.error(f"Error while checking graph: {str(e)}")
                raise

    def detect_starts_end(self, g: ig.Graph, mode: str) -> list[str]:
        """
        Detect start or end vertices in a graph based on their degree.

        This method identifies vertices that have no incoming or outgoing edges
        depending on the specified mode. These vertices typically represent
        starting points (no incoming edges) or ending points (no outgoing edges)
        in a directed graph.

        Args:
            g (ig.Graph): The igraph Graph object to analyze for start/end vertices.
            mode (str): The degree mode to check. Should be 'in' to find vertices
                       with no incoming edges (potential starts) or 'out' to find
                       vertices with no outgoing edges (potential ends).

        Returns:
            list[str]: A list of vertex names that have zero degree in the
                      specified mode. Returns the 'name' attribute of vertices
                      that match the criteria.
        """
        # get indexes of vertices with no out- or in-degree
        indexes = np.where(np.array(g.degree(mode=mode)) == 0)[0]
        # return names of these vertices
        return g.vs[indexes]['name']

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CreateRoutes"
