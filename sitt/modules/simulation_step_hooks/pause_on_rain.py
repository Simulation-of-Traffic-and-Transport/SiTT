# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation Hook that pauses the agent when it is X °C or higher (e.g. 25 °C)."""
import logging
import datetime as dt

import igraph as ig

from sitt import Configuration, Context, Agent
from sitt.base import SimulationStepHookInterface

logger = logging.getLogger()


class PauseOnRain(SimulationStepHookInterface):
    """
    Implements a simulation step hook that pauses agent movement in case of rainy
    weather conditions exceeding certain thresholds.

    This class is used to adjust agent behavior in simulations based on
    environmental conditions, specifically rain intensity. It checks rain-related
    data at the configured time and location and determines if the agent should
    pause based on predefined thresholds.

    :ivar pause_thresholds: Pause thresholds for different edge types in rainy
        weather conditions.
    :type pause_thresholds: dict
    :ivar light_rain_max_slopes: Maximum allowable slope for movement under light
        rain conditions for various transport types.
    :type light_rain_max_slopes: dict
    """
    def __init__(self, pause_thresholds: dict = {}, light_rain_max_slopes: dict = {}):
        """
        Represents a configuration class for specifying weather-related thresholds for
        pausing tasks and defining maximum slopes under light rain for different conditions.

        This class is typically used to configure or initialize parameters relevant to
        rainy weather scenarios, such as determining pause thresholds or defining the
        limits for light rain slope based on various edge or transport types.

        :param pause_thresholds: Pause thresholds for different edge types in rainy
            weather conditions.
        :type pause_thresholds: dict
        :param light_rain_max_slopes: Maximum allowable light rain slopes for different
            transport types.
        :type light_rain_max_slopes: dict
        """
        self.pause_thresholds = pause_thresholds
        """Pause thresholds for different edge types in rainy weather conditions."""
        self.light_rain_max_slopes = light_rain_max_slopes
        """Max light rain slope for different transport types."""
        super().__init__()

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
                 time_offset: float) -> tuple[float, bool, bool]:
        """
        Evaluates weather conditions and adjusts the agent's activity by introducing pauses based on rainfall thresholds
        and terrain slope conditions. This method ensures agents adapt to unforeseen weather impacts, updating their
        time offset accordingly.

        :param config: Configuration instance that provides global and agent-specific parameters and methods.
        :type config: Configuration

        :param context: Context containing methods to access space-time specific data.
        :type context: Context

        :param agent: Agent whose activity is being evaluated and potentially paused.
        :type agent: Agent

        :param next_leg: The subsequent leg the agent is supposed to traverse. Contains leg attributes like `max_slope_up`
            and `max_slope_down` that influence decision-making under light rain conditions.
        :type next_leg: ig.Edge

        :param i: Index or identifier of the specific iteration in the agent's travel sequence.
        :type i: int

        :param coords: A tuple containing the geographic coordinates (latitude, longitude) for weather evaluation.
        :type coords: tuple

        :param time_offset: The current time offset in fractional hours from the initial simulation time.
        :type time_offset: float

        :return: A tuple containing the updated time offset in fractional hours and two boolean values indicating
            state changes caused by the hook.
        :rtype: tuple[float, bool, bool]
        """
        # check skip conditions
        if self.do_skip(agent, next_leg):
            return time_offset, False, False

        current_day = config.get_agent_date(agent, time_offset)
        data = context.find_multiple_space_time_data(coords[1], coords[0], current_day, 'light_rain', 'heavy_rain_2', 'heavy_rain_3', 'persistent_rain_2', 'persistent_rain_3')
        for key, value in data.items():
            data[key] = bool(value)

        do_pause = False

        # pause threshold for this leg type
        if next_leg['type'] in self.pause_thresholds:
            min_rain = str(self.pause_thresholds[next_leg['type']])
            if data['persistent_rain_' + min_rain] or data['heavy_rain_' + min_rain]:
                do_pause = True

        # light rain?
        if data['light_rain'] and agent.transport_type in self.light_rain_max_slopes and next_leg['max_slope_up'] is not None and next_leg['max_slope_down'] is not None:
            slope = max(next_leg['max_slope_up'], next_leg['max_slope_down'])
            max_slope = self.light_rain_max_slopes[agent.transport_type]
            if slope >= max_slope:
                do_pause = True

        if do_pause:
            # update time offset to the next full hour
            next_hour = current_day + dt.timedelta(hours=1, minutes=-current_day.minute,
                                                   seconds=-current_day.second,
                                                   microseconds=-current_day.microsecond)
            wait_time = (next_hour - current_day).total_seconds() / 3600
            time_offset += wait_time

            agent.add_rest(wait_time, reason=f"rain")
            if logger.level <= logging.INFO:
                logger.info(f"PauseOnRain: Agent {agent.uid} in hub {agent.this_hub} is resting due to rain.")

        return time_offset, False, False