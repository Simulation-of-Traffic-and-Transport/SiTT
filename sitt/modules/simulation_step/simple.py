# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending slopes.
Moreover, this stepper will not care for the type of path (river, etc.).
Other than that, it does not take into account weather or other factors.
"""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, SimulationStepInterface, State, Agent

logger = logging.getLogger()


class Simple(SimulationStepInterface):
    """
    Simple stepper will have a constant speed and will have a certain slowdown factor for ascending and descending
    slopes. Moreover, this stepper will not care for the type of path (river, etc.).
    Other than that, it does not take into account weather or other factors.
    """

    def __init__(self, speed: float = 5.0, ascend_slowdown_factor: float = 0.05,
                 descend_slowdown_factor: float = 0.025):
        """
        Initialize the Simple simulation stepper with movement parameters.

        This constructor sets up a simple movement model that uses constant speed with
        slope-based adjustments for ascending and descending terrain.

        Args:
            speed (float, optional): The base movement speed of the agent in kilometers per hour (kph).
                Defaults to 5.0.
            ascend_slowdown_factor (float, optional): The factor by which travel time is increased
                when ascending slopes. The actual slowdown is calculated as slope percentage multiplied
                by this factor. Defaults to 0.05.
            descend_slowdown_factor (float, optional): The factor by which travel time is increased
                when descending slopes. The actual slowdown is calculated as slope percentage multiplied
                by this factor. Defaults to 0.025.

        Returns:
            None
        """
        super().__init__()
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_slowdown_factor: float = ascend_slowdown_factor
        """time taken is modified by slope in percents multiplied by this number when ascending"""
        self.descend_slowdown_factor: float = descend_slowdown_factor
        """time taken is modified by slope in percents multiplied by this number when descending"""

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """
        Update the agent's state by calculating travel time for the next leg of the journey.

        This method calculates the time required to traverse the next leg of the agent's route,
        taking into account the base speed, terrain slopes, and direction of travel. The calculation
        applies different slowdown factors for ascending and descending slopes. If configured, it
        also stores individual leg times for detailed analysis.

        Args:
            config (Configuration): The simulation configuration object containing settings such as
                whether to keep individual leg times (keep_leg_times flag).
            context (Context): The simulation context providing environmental and global state
                information (currently unused in this implementation).
            agent (Agent): The agent whose state is being updated. Contains the current state,
                route information, and hub identifiers.
            next_leg (ig.Edge): An igraph Edge object representing the next segment of the route.
                Expected to contain 'legs' (list of segment lengths) and 'slopes' (list of slope
                percentages) attributes. If None, an error is logged and the current state is returned.

        Returns:
            State: The updated state object with calculated time_taken and optionally time_for_legs.
                If next_leg is None, returns the unmodified current state.
        """
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

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        if state.is_reversed:
            r = reversed(r)

        for i in r:
            length = next_leg['legs'][i]
            slope = next_leg['slopes'][i]

            if slope < 0:
                slope_factor = slope * self.descend_slowdown_factor * -1
            else:
                slope_factor = slope * self.ascend_slowdown_factor

            # calculate time taken in units (hours) for this part
            calculated_time = length / self.speed / 1000 * (1 + slope_factor)

            time_for_legs.append(calculated_time)
            time_taken += calculated_time

        # save things in state
        state.time_taken = time_taken
        if config.keep_leg_times:
            state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface Simple run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}")

        return state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "Simple"
