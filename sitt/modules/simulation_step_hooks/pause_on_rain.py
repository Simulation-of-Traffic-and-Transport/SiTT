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
    def __init__(self, pause_thresholds: dict = {}, light_rain_max_slopes: dict = {}):
        self.pause_thresholds = pause_thresholds
        """Pause thresholds for different edge types in rainy weather conditions."""
        self.light_rain_max_slopes = light_rain_max_slopes
        """Max light rain slope for different transport types."""
        super().__init__()

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
                 time_offset: float) -> tuple[float, bool, bool]:
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