# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create routes to be traversed by the simulation."""
import logging

import igraph as ig
import yaml

from sitt import BaseClass, Configuration, Context, PreparationInterface
from sitt.convert_graph_to_directed import convert_graph_to_directed

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

    def __init__(self, check_graph: bool = False):
        super().__init__()
        self.check_graph: bool = check_graph
        """Maximum difference from shortest route (factor, if greater than 1)"""


    def run(self, config: Configuration, context: Context) -> Context:
        """prepare simulation"""
        # Checking start and stop hubs
        if config.simulation_start is None:
            logger.error("simulation_start is empty - simulation failed!")
        if config.simulation_end is None:
            logger.error("simulation_end is empty - simulation failed!")
        if config.simulation_route is None:
            logger.error("simulation_route is empty - simulation failed!")

        if logger.level <= logging.INFO:
            logger.info(
                f"PreparationInterface CreateRoutes: using route {config.simulation_route} (reversed: {config.simulation_route_reverse}) from {config.simulation_start} to {config.simulation_end}")

        # convert to directed graph
        context.routes = convert_graph_to_directed(context.graph, config.simulation_route, config.simulation_route_reverse)

        if self.check_graph:
            self.run_check_graph(config, context.routes)

        if logger.level <= logging.INFO:
            logger.info(f"PreparationInterface CreateRoutes: Created directed graph with {len(context.routes.vs)} "
                        f"vertices and {len(context.routes.es)} edges.")

        return context

    def run_check_graph(self, config: Configuration, g: ig.Graph):
        # test route
        try:
            steps = g.get_shortest_path(config.simulation_start, config.simulation_end)
            logger.info(f"Shortest route has {len(steps)} steps.")
        except Exception as e:
            logger.error(f"Error while checking graph: {str(e)}")
            raise

        # TODO: more tests, like clusters or so?

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CreateRoutes"
