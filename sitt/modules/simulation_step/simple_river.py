# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simple river simulation stepper. Can be used in river systems."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class SimpleRiver(SimulationStepInterface):
    def __init__(self, speed: float = 3.):
        """
        :param speed: average/minimum row speed in kph (if river is slower than this, take this as minimum speed). If
        rowing against the current, the current's pull will be deducted from the speed.
        """
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge,
                     is_reversed: bool) -> State:
        # skipped?
        if self.skip:
            return agent.state

        # not a river?
        if next_leg['type'] != 'river':
            logger.error(f"SimulationInterface SimpleRiver error, path {agent.route_key} is not a river")
            agent.state.signal_stop_here = True
            return agent.state

        # determine speed
        current_speed = self.speed
        kph = next_leg['flow_rate'] * 3.6
        if next_leg['flow_to'] == agent.next_hub:
            # originating from this hub - check speed of current
            if kph > current_speed:
                current_speed = kph
        elif next_leg['flow_to'] == agent.this_hub:
            # traversing against the current
            current_speed -= kph
            if current_speed < 0:
                agent.state.signal_stop_here = True
                if logger.level <= logging.DEBUG:
                    logger.debug(
                        f"SimpleRiver against current failed: {agent.this_hub} to {agent.next_hub} via {agent.route_key}, current speed = {kph} k/h")
                return agent.state

        # fixed speed in kph
        agent.state.time_taken = next_leg['length_m'] / (current_speed * 1000)
        agent.state.time_for_legs = [agent.state.time_taken]

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleRiver run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleRiver"
