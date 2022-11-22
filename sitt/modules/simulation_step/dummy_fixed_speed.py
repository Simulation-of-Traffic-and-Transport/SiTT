# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy stepper that runs at a fixed speed. Useful for testing."""
import logging

import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class DummyFixedSpeed(SimulationStepInterface):
    def __init__(self, speed: float = 5.):
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        # precalculate next hub
        leg = context.graph.get_edge_data(agent.this_hub, agent.next_hub, agent.route_key)

        # fixed speed in kph
        agent.state.time_taken = leg['length_m'] / (self.speed * 1000)

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface DummyFixedSpeed run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "DummyFixedSpeed"
