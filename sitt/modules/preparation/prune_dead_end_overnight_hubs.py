# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy module for testing"""
import logging

import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class PruneDeadEndOvernightHubs(PreparationInterface):
    """
    PruneDeadEndOvernightHubs identifies and marks "dead-end" overnight hubs as no-go.

    This class is responsible for analyzing a given context and marking certain
    overnight hubs as "no-go" if they are considered dead ends. An overnight hub
    is deemed a dead end if it only has a single neighboring node in the graph,
    and that single neighbor is also an overnight hub. The purpose of this class
    is to prepare the routing network for efficient operations by identifying these
    irrelevant hubs and removing their usability for routing.

    :ivar skip: Determines whether the pruning should be skipped or executed.
    :type skip: bool
    """

    def run(self, config: Configuration, context: Context) -> Context:
        """
        Evaluates the context for overnight hubs with dead ends and marks them as "no-go".
        A hub is considered a dead end if it has only one neighbor, which itself is an
        overnight hub. This function updates the context accordingly.

        :param config: A configuration object containing operational parameters like
                       simulation start and end points.
        :type config: Configuration
        :param context: A context object containing information about the current state,
                        including routes and graphs.
        :type context: Context
        :return: The updated context after processing the overnight hubs.
        :rtype: Context
        """
        if self.skip:
            return context

        counter = 0

        for overnight_hub in context.routes.vs.select(overnight=True):
            # ignore simulation starts and ends
            if overnight_hub['name'] in config.simulation_starts or overnight_hub['name'] in config.simulation_ends:
                continue

            # check if we have only one neighbor in the undirected graph (the directed one might have two vertices, one in and one out)
            hub = context.graph.vs.find(name=overnight_hub['name'])
            neighbors = hub.neighbors()
            # if we have only one neighbor and if it's an overnight hub, we mark this hub as a no-go
            if len(neighbors) == 1 and neighbors[0]['overnight_hub'] is not None:
                overnight_hub['no_go'] = True
                counter += 1

        if logger.level <= logging.INFO:
            logger.info(f"PreparationInterface PruneDeadEndOvernightHubs marked {counter} overnight hubs as no-go (dead ends).")

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "PruneDeadEndOvernightHubs"
