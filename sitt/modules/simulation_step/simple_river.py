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
    """
    Represents a simple river simulation step handler.

    The `SimpleRiver` class is designed to model the handling of a river during
    a simulation. It incorporates factors such as minimum row speeds, towing
    speeds, and the rate of ascent and descent on an upstream path. It is part
    of a larger simulation system and integrates with other components to update
    the state of agents traversing the river path.

    :ivar min_speed_down: The minimum row speed in kilometers per hour (kph) when going downstream.
    :type min_speed_down: float
    :ivar min_speed_up: The speed in kph when towing upstream, defaults to downstream speed if not specified.
    :type min_speed_up: float
    :ivar upstream_is_dav: Determines if the upstream river behaves like a Simple DAV (determined by the direction).
    :type upstream_is_dav: bool
    :ivar ascend_per_hour: The ascent in meters per hour while going upstream.
    :type ascend_per_hour: float
    :ivar descend_per_hour: The descent in meters per hour while going downstream.
    :type descend_per_hour: float
    """
    def __init__(self, min_speed_down: float = 3.35, min_speed_up: float = 3.35, upstream_is_dav: bool = True,
                 ascend_per_hour: float = 300, descend_per_hour: float = 400):
        """
        Constructor for initializing an object with parameters to control and define
        speed limits and height gain/loss per hour for virtual upstream and downstream
        simulations. These parameters are used to specify movement characteristics such
        as minimal speeds, height increase per hour when ascending, and height
        decrease per hour when descending.

        :param min_speed_down: Float value representing the minimum speed in meters
            per hour while moving downstream.
        :param min_speed_up: Float value representing the minimum speed in meters
            per hour while moving upstream. Default is 3.35. If the value provided
            is less than 0, it defaults to self.speed.
        :param upstream_is_dav: Boolean value indicating whether the upstream
            motion is considered as "dav" (true) or operated normally (false).
        :param ascend_per_hour: Float value representing the height gain in meters
            per hour while simulating upstream ascent.
        :param descend_per_hour: Float value representing the height loss in meters
            per hour while simulating downstream descent.
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
        """
        Updates the state of the agent while traversing a river segment.

        The method validates if the next segment is a river and, if so, calculates
        the traversal time for each leg of the segment. It considers external
        conditions like upstream flow, hooks execution, and maximum time restrictions.
        For upstream segments, it determines if specific rules for ascending waters are
        applicable. The agent's state is updated with the results, including traversal
        time, stop signals, and other necessary information.

        :param config: Configuration object containing simulation settings
                       and runtime parameters.
        :type config: Configuration
        :param context: Contextual information used to execute hooks and other
                        dynamic behaviors.
        :type context: Context
        :param agent: Agent currently traversing the river segment. The agent's
                      state and pertinent attributes are updated in place.
        :type agent: Agent
        :param next_leg: River segment to be traversed. Contains data about the
                         geometry, flow, and other relevant attributes of the segment.
        :type next_leg: ig.Edge
        :return: Updated state of the agent after processing the river segment.
        :rtype: State
        """
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
            (time_taken, cancelled) = self.run_hooks(config, context, agent, next_leg, i, coords, time_taken)
            if cancelled:
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, cancelled state")
                break

            length = next_leg['legs'][i]  # length is in meters

            # determine speed
            if 'direction' in next_leg.attribute_names() and next_leg['direction'] == 'upwards':
                # this is only used, if self.upstream_is_dav is false
                current_speed = self.min_speed_up
            else:
                # river speed - we take this point's flow rate to calculate the speed
                kph = flows[i] * 3.6
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
