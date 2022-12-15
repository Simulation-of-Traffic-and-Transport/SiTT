# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
Other than that, it does not take into account weather or other factors.
"""
import logging

import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent

logger = logging.getLogger()


class SimpleWithEnvironment(SimulationStepInterface):
    """
    Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
    Other than that, it does not take into account weather or other factors.
    """

    def __init__(self, speed: float = 5.0, ascend_slowdown_factor: float = 0.05,
                 descend_slowdown_factor: float = 0.025):
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_slowdown_factor: float = ascend_slowdown_factor
        """time taken is modified by slope in degrees multiplied by this number when ascending"""
        self.descend_slowdown_factor: float = descend_slowdown_factor
        """time taken is modified by slope in degrees multiplied by this number when descending"""

    def update_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        state = agent.state

        # precalculate next hub
        path_id = (agent.this_hub, agent.next_hub, agent.route_key)
        leg = context.get_directed_path_by_id(path_id, agent.this_hub)
        if not leg:
            logger.error("SimulationInterface SimpleRunner error, path not found ", str(path_id))
            # state.status = Status.CANCELLED
            return state

        # create range to traverse
        if leg['is_reversed']:
            r = range(len(leg['legs']) - 1, -1, -1)
            p_offset_start = 1  # offset in legs for start point
        else:
            r = range(len(leg['legs']))
            p_offset_start = 0

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []
        space_time_data_legs: list[dict[str, any]] = []

        for i in r:
            length = leg['legs'][i]
            slope = leg['slopes'][i]
            if leg['is_reversed']:
                slope *= -1

            if slope < 0:
                slope_factor = slope * self.descend_slowdown_factor * -1
            else:
                slope_factor = slope * self.ascend_slowdown_factor

            # apply environment
            coords = leg['geom'].coords[i + p_offset_start]
            space_time_data: dict[str, any] = {}

            if len(context.space_time_data):
                for key in context.space_time_data:
                    values = context.space_time_data[key].get(coords[1], coords[0], agent.current_day,
                                                              agent.current_time + time_taken, config)
                    for value in values:
                        space_time_data[value] = values[value]

            # calculate time taken in units (hours) for this part
            calculated_time = length / self.speed / 1000 * (1 + slope_factor)

            # consider environment
            if 'temperature' in space_time_data:
                # TODO: implement it!
                pass
            if 'rainfall' in space_time_data:
                # TODO: implement it!
                pass
            if 'snowfall' in space_time_data:
                # TODO: implement it!
                pass
            if 'snow_depth' in space_time_data and space_time_data['snow_depth'] > 0:
                # print(space_time_data['snow_depth'])
                # TODO: implement it!
                pass

            time_for_legs.append(calculated_time)
            space_time_data_legs.append(space_time_data)
            time_taken += calculated_time

        # save things in state
        state.time_taken = time_taken
        state.time_for_legs = time_for_legs
        state.data_for_legs = space_time_data_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface Simple run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}")

        return state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "Simple"
