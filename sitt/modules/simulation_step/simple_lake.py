# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simple lake stepper that assumes a fixed speed on lakes (like being rowed)."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class SimpleLake(SimulationStepInterface):
    def __init__(self, speed: float = 3.):
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge,
                     is_reversed: bool) -> State:
        # skipped?
        if self.skip:
            return agent.state

        # not a lake?
        if next_leg['type'] != 'lake':
            logger.error(f"SimulationInterface SimpleLake error, path {agent.route_key} is not a lake")
            agent.state.signal_stop_here = True
            return agent.state

        # fixed speed in kph
        agent.state.time_taken = next_leg['length_m'] / (self.speed * 1000)
        agent.state.time_for_legs = [agent.state.time_taken]

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleLake run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleLake"
