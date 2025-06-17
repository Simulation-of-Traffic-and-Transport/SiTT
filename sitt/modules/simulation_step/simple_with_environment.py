# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
It will also take weather and environmental hazards into account, using a linear factor for slowdown in certain cases.
"""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent

logger = logging.getLogger()


class SimpleWithEnvironment(SimulationStepInterface):
    """
    Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
    Other than that, it does not take into account weather or other factors.
    """

    def __init__(self, speed: float = 5.0, ascend_slowdown_factor: float = 0.05,
                 descend_slowdown_factor: float = 0.025, rainfall_slowdown_factor: float = 1000.0,
                 snowfall_slowdown_factor: float = 10000.0, snow_depth_slowdown_factor: float = 1.0,
                 temperature_slowdown_factors: dict[float, float] = {-1000.0: 0.08, -20.0: 0.05,
                                                                     -10.0: 0.0,
                                                                     25.0: 0.01,
                                                                     30.0: 0.02,
                                                                     35.0: 0.04,
                                                                     40.0: 0.08}):
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_slowdown_factor: float = ascend_slowdown_factor
        """time taken is modified by slope in percents multiplied by this number when ascending"""
        self.descend_slowdown_factor: float = descend_slowdown_factor
        """time taken is modified by slope in percents multiplied by this number when descending"""
        self.rainfall_slowdown_factor: float = rainfall_slowdown_factor
        """slowdown factor for rainfall"""
        self.snowfall_slowdown_factor: float = snowfall_slowdown_factor
        """slowdown factor for snowfall"""
        self.snow_depth_slowdown_factor: float = snow_depth_slowdown_factor
        """slowdown factor for snow depth"""
        self.temperature_slowdown_factors: dict[float, float] = temperature_slowdown_factors
        """slowdown factors for temperature.
        
        This is a list of minimum temperature keys to use this slowdown. It must be defined in ascending order."""

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge,
                     is_reversed: bool) -> State:
        state = agent.state

        # precalculate next hub
        path_id = agent.route_key
        if not next_leg:
            logger.error("SimulationInterface SimpleRunner error, path not found ", str(path_id))
            # state.status = Status.CANCELLED
            return state

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []
        space_time_data_legs: list[dict[str, any]] = []

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        if is_reversed:
            r = reversed(r)

        for i in r:
            length = next_leg['legs'][i]
            slope = next_leg['slopes'][i]

            if slope < 0:
                slope_factor = slope * self.descend_slowdown_factor * -1
            else:
                slope_factor = slope * self.ascend_slowdown_factor

            # apply environment
            coords = next_leg['geom'].coords[i]
            space_time_data: dict[str, any] = {}

            if len(context.space_time_data):
                for key in context.space_time_data:
                    values = context.space_time_data[key].get(coords[1], coords[0], config.get_agent_date(agent, time_taken))
                    for value in values:
                        space_time_data[value] = values[value]

            # calculate time taken in units (hours) for this part
            calculated_time = length / self.speed / 1000 * (1 + slope_factor)

            # consider environment
            if 'temperature' in space_time_data:
                calculated_time = (1 + self.__get_temperature_slowdown_for(
                    space_time_data['temperature'])) * calculated_time

            if 'rainfall' in space_time_data and space_time_data['rainfall'] > 0:
                calculated_time = ((space_time_data['rainfall'] * self.rainfall_slowdown_factor) + 1) * calculated_time

                if logger.level <= logging.DEBUG:
                    logger.debug(" * Rainfall", str(space_time_data['rainfall']))

            if 'snowfall' in space_time_data and space_time_data['snowfall'] > 0:
                calculated_time = ((space_time_data['snowfall'] * self.snowfall_slowdown_factor) + 1) * calculated_time

                if logger.level <= logging.DEBUG:
                    logger.debug(" * Snowfall", str(space_time_data['snowfall']))

            if 'snow_depth' in space_time_data and space_time_data['snow_depth'] > 0:
                calculated_time = ((space_time_data[
                                        'snow_depth'] * self.snow_depth_slowdown_factor) + 1) * calculated_time

                if logger.level <= logging.DEBUG:
                    logger.debug(" * Snow height", str(space_time_data['snow_depth']))

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

    def __get_temperature_slowdown_for(self, temperature) -> float:
        """Retrieve the slowdown for the given temperature"""
        if len(self.temperature_slowdown_factors) > 0:
            factor = 0.0

            for temp in self.temperature_slowdown_factors:
                if temperature >= temp:
                    # go on as long as we are above the given temperature
                    factor = self.temperature_slowdown_factors[temp]
                else:
                    return factor

        return 0.0

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "Simple"
