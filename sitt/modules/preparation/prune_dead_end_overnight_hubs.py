# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy module for testing"""
import logging

import yaml

from sitt import Configuration, Context, PreparationInterface

logger = logging.getLogger()


class PruneDeadEndOvernightHubs(PreparationInterface):
    """Dummy class for testing - this is an empty class that can be taken as template for custom modules."""

    def run(self, config: Configuration, context: Context) -> Context:
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
            logger.info(f"PreparationInterface PruneDeadEndOvernightHubs marked: {counter} overnight hubs as no-go (dead ends).")

        return context

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "PruneDeadEndOvernightHubs"
