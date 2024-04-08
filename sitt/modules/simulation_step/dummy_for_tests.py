# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy stepper that runs at a fixed speed. Useful for testing."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class DummyForTests(SimulationStepInterface):
    def __init__(self, time_taken_per_node: float = 1., force_stop_at_node: None | str = None):
        super().__init__()
        self.time_taken_per_node: float = time_taken_per_node
        self.force_stop_at_node: float = force_stop_at_node

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        # Signal to stop at this stop
        if self.force_stop_at_node and agent.this_hub == self.force_stop_at_node:
            agent.state.signal_stop_here = True
        else:
            # fixed speed in kph
            agent.state.time_taken = self.time_taken_per_node

        agent.state.time_for_legs = [agent.state.time_taken]

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface DummyForTests run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "DummyForTests"
