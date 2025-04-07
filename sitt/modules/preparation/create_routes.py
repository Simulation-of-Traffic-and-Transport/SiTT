# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create routes to be traversed by the simulation."""
import logging

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

    def __init__(self, maximum_routes: int = 0, maximum_difference_from_shortest: float = 0., k_shortest: int = 100):
        super().__init__()
        self.maximum_difference_from_shortest: float = maximum_difference_from_shortest
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

        if logger.level <= logging.INFO:
            logger.info(f"PreparationInterface CreateRoutes: Created directed graph with {len(context.routes.vs)} "
                        f"vertices and {len(context.routes.es)} edges.")

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CreateRoutes"
