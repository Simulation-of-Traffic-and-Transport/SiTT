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
    def __init__(self, speed: float = 3., tow_speed: float = 0.):
        """
        :param speed: average/minimum row speed in kph (if river is slower than this, take this as minimum speed).
        :param tow_speed: speed in kph when rowing against the current. If not provided, defaults to speed. Half the
        current's pull will be deducted from the speed, although this is not accurate.
        """
        super().__init__()
        self.speed: float = speed
        self.tow_speed: float = tow_speed if tow_speed >= 0. else self.speed

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

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        if is_reversed:
            r = reversed(r)

        for i in r:
            length = next_leg['legs'][i]  # length is in meters

            # river speed
            kph = next_leg['flow_rate'] * 3.6

            # determine speed
            if 'is_tow' in next_leg.attribute_names() and next_leg['is_tow']:
                # rowing against the current, aka towing
                current_speed = self.tow_speed
                # traversing against the current
                current_speed -= kph/2. # half the current's pull will be deducted from the speed, although this is not accurate
                if current_speed < 0:
                    agent.state.signal_stop_here = True
                    if logger.level <= logging.DEBUG:
                        logger.debug(
                            f"SimpleRiver against current failed: {agent.this_hub} to {agent.next_hub} via {agent.route_key}, current speed = {kph} k/h")
                    return agent.state
            else:
                current_speed = self.speed
                # if the pull is greater than the river speed, use the river speed
                if kph > current_speed:
                    current_speed = kph

            # calculate time taken in units (hours) for this part
            calculated_time = length / (current_speed * 1000)

            time_for_legs.append(calculated_time)
            time_taken += calculated_time

        # save things in state
        agent.state.time_taken = time_taken
        agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleRiver run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleRiver"
