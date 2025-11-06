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
    def __init__(self, min_speed_down: float = 3.35, min_speed_up: float = 3.35):
        """
        :param min_speed_down: minimum row speed in kph (if river is slower than this, take this as minimum speed).
        :param min_speed_up: speed in kph when towing upriver. If not provided, defaults to speed.
        """
        super().__init__()
        self.min_speed_down: float = min_speed_down
        self.min_speed_up: float = min_speed_up if min_speed_up >= 0. else self.speed

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
        flows = next_leg['flow'].copy()

        if is_reversed:
            r = reversed(r)
            flows.reverse() # also reverse flow

        for i in r:
            coords = next_leg['geom'].coords[i]
            # run hooks
            if not self.run_hooks(config, context, agent, next_leg, coords, time_taken):
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, {agent} cancelled")
                return agent.state

            length = next_leg['legs'][i]  # length is in meters

            # river speed - we take this and the next point's flow rate to calculate the speed
            kph = (flows[i] + flows[i+1])/2  * 3.6

            # determine speed
            if 'direction' in next_leg.attribute_names() and next_leg['direction'] == 'upstream':
                min_speed = self.min_speed_up
            else:
                min_speed = self.min_speed_down

            current_speed = max(min_speed, kph)

            # calculate time taken in units (hours) for this part
            calculated_time = length / (current_speed * 1000)

            time_for_legs.append(calculated_time)
            time_taken += calculated_time

            # check if time taken exceeds max_time - should finish today
            if agent.current_time + time_taken > agent.max_time:
                agent.state.last_coordinate_after_stop = coords
                agent.state.signal_stop_here = True
                break

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
