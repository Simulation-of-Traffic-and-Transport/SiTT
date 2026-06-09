# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation hook for snow heights."""
import logging
import datetime as dt

import igraph as ig

from sitt import Configuration, Context, Agent
from sitt.base import SimulationStepHookInterface

logger = logging.getLogger()


class PauseOnSnow(SimulationStepHookInterface):
    """
    Pauses agent movement based on snow conditions and slope thresholds.

    This hook checks whether the agent's current transport type and conditions,
    such as snow height and maximum slope, exceed defined thresholds. If so,
    it pauses the agent's movement until conditions improve. Snow thresholds
    are configurable for each transport type, allowing for flexibility in
    determining pause conditions.

    :ivar pause_thresholds: Snow height and slope thresholds for different transport
        types. The format is a dictionary where keys are transport types and values
        are lists of conditions. Each condition is a dictionary specifying the
        `min_slope` and `max_snow`.
    :type pause_thresholds: dict
    """
    def __init__(self, pause_thresholds: dict = {}):
        """
        Initializes the instance with specified snow height thresholds for different
        transport types.

        :param pause_thresholds: A dictionary containing snow height thresholds for
            various transport types. The dictionary must follow the format
            {'transport_type': [{'min_slope': float, 'max_snow': float}, ...]}
            where `min_slope` is the minimum slope value and `max_snow` is the maximum
            snow height threshold allowed for that transport type.
        :type pause_thresholds: dict
        """
        super().__init__()
        self.pause_thresholds = pause_thresholds
        """Snow height thresholds for different transport types.  Format: {'transport_type': [{'min_slope': 0.3, 'max_snow': 0.5},...]}"""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
    time_offset: float) -> tuple[float, bool, bool]:
        """
        Executes a hook function to determine if an agent's journey should be delayed due to snow
        and slope conditions.

        :param config: Instance of Configuration to provide necessary data for computations and
                       agent-specific configurations.
        :type config: Configuration
        :param context: Instance of Context to retrieve spatial and temporal data related to the
                        environment.
        :type context: Context
        :param agent: Instance of Agent representing the moving entity whose behavior is being
                      controlled.
        :type agent: Agent
        :param next_leg: Representation of the next route segment the agent will traverse,
                         including attributes like slope.
        :type next_leg: ig.Edge
        :param i: Index of the current segment in the agent's journey, used for route traversal
                  calculations.
        :type i: int
        :param coords: Tuple representing geographic coordinates in the format (longitude, latitude).
        :type coords: tuple
        :param time_offset: Current offset in simulation time, used to compute the future state of the
                            agent's journey.
        :type time_offset: float
        :return: A tuple containing updated time offset, a boolean indicating whether the agent was
                 delayed, and a boolean indicating whether the journey should terminate.
        :rtype: tuple[float, bool, bool]
        """
        # check skip conditions
        if self.do_skip(agent, next_leg):
            return time_offset, False, False

        # do we have a condition for our transport type?
        if agent.transport_type not in self.pause_thresholds:
            return time_offset, False, False

        current_day = config.get_agent_date(agent, time_offset)

        # get snow from context data
        snow_height = context.find_space_time_data(coords[1], coords[0], current_day, 'snow')
        if snow_height is None:
            """Skip if no snow_height data found."""
            return time_offset, False, False

        # get slope
        max_slope = max(next_leg['max_slope_up'], next_leg['max_slope_down'])
        snow_height = float(snow_height)

        for condition in self.pause_thresholds[agent.transport_type]:
            if max_slope >= condition['min_slope'] and snow_height > condition['max_snow']:
                # update time offset to the next full hour
                next_hour = current_day + dt.timedelta(hours=1, minutes=-current_day.minute,
                                                       seconds=-current_day.second,
                                                       microseconds=-current_day.microsecond)
                wait_time = (next_hour - current_day).total_seconds() / 3600
                time_offset += wait_time

                agent.add_rest(wait_time, reason=f"snow")
                return time_offset, True, False

        return time_offset, False, False
