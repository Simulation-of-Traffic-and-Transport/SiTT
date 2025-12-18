# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simple river simulation stepper. Can be used in river systems."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface
from sitt.modules.simulation_step import SimpleDAV

logger = logging.getLogger()


class SimpleRiver(SimulationStepInterface):
    def __init__(self, min_speed_down: float = 3.35, min_speed_up: float = 3.35, upstream_is_dav: bool = True,
                 ascend_per_hour: float = 300, descend_per_hour: float = 400):
        """
        :param min_speed_down: minimum row speed in kph (if river is slower than this, take this as minimum speed).
        :param min_speed_up: speed in kph when towing upriver. If not provided, defaults to speed.
        """
        super().__init__()
        self.min_speed_down: float = min_speed_down
        self.min_speed_up: float = min_speed_up if min_speed_up >= 0. else self.speed
        self.upstream_is_dav: bool = upstream_is_dav
        self.ascend_per_hour: float = ascend_per_hour
        """m of height per hour while ascending - upstream"""
        self.descend_per_hour: float = descend_per_hour
        """m of height per hour while descending - upstream"""

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        # skipped?
        if self.skip:
            return agent.state

        # not a river?
        if next_leg['type'] != 'river':
            logger.error(f"SimulationInterface SimpleRiver error, path {agent.route_key} is not a river")
            agent.state.signal_stop_here = True
            return agent.state

        # check upstream river -> this behaves like Simple DAV
        if self.upstream_is_dav and 'direction' in next_leg.attribute_names() and next_leg['direction'] == 'upwards':
            dav = SimpleDAV(speed=self.min_speed_up, ascend_per_hour=self.ascend_per_hour, descend_per_hour=self.descend_per_hour)
            return dav.update_state(config, context, agent, next_leg)

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        flows = next_leg['flow'].copy()

        if agent.state.is_reversed:
            r = reversed(r)
            flows.reverse() # also reverse flow

        for i in r:
            coords = next_leg['geom'].coords[i]
            # run hooks
            (time_taken, cancelled) = self.run_hooks(config, context, agent, next_leg, coords, time_taken)
            if cancelled:
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, cancelled state")
                return agent.state

            length = next_leg['legs'][i]  # length is in meters

            # determine speed
            if 'direction' in next_leg.attribute_names() and next_leg['direction'] == 'upwards':
                # this is only used, if self.upstream_is_dav is false
                current_speed = self.min_speed_up
            else:
                # river speed - we take this and the next point's flow rate to calculate the speed
                kph = (flows[i] + flows[i + 1]) / 2 * 3.6
                current_speed = max(self.min_speed_down, kph)

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

        if config.keep_leg_times:
            agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleRiver run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleRiver"
