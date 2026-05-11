# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation hooks for wind-related conditions - wind gusts for now."""
import logging
import datetime as dt

import igraph as ig

from sitt import Configuration, Context, Agent
from sitt.base import SimulationStepHookInterface

logger = logging.getLogger()


class PauseOnWind(SimulationStepHookInterface):
    def __init__(self, pause_thresholds: dict = {}, additional_thresholds: dict[str, float] = {}):
        super().__init__()
        self.pause_thresholds = pause_thresholds
        """Pause thresholds for different weather conditions."""
        self.additional_thresholds: dict[str, float] = additional_thresholds
        """Additional pause thresholds for different data types (if set)."""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
                 time_offset: float) -> tuple[float, bool, bool]:
        # check skip conditions
        if self.do_skip(agent, next_leg):
            return time_offset, False, False

        # do we have a condition for our transport type?
        if agent.transport_type not in self.pause_thresholds:
            return time_offset, False, False


        current_day = config.get_agent_date(agent, time_offset)

        wind_gust = context.find_space_time_data(coords[1], coords[0], current_day, 'wind')
        if wind_gust is None:
            """Skip if no wind_gust data found."""
            return time_offset, False, False

        wind_gust = float(wind_gust)

        do_pause = False

        # first, we check additional thresholds
        if len(self.additional_thresholds):
            for data_type, threshold in self.additional_thresholds.items():
                if data_type in agent.additional_data and wind_gust >= threshold:
                    do_pause = True
                    break

        if not do_pause and wind_gust >= self.pause_thresholds[agent.transport_type]:
            do_pause = True

        if do_pause:
            # update time offset to the next full hour
            next_hour = current_day + dt.timedelta(hours=1, minutes=-current_day.minute,
                                                   seconds=-current_day.second,
                                                   microseconds=-current_day.microsecond)
            wait_time = (next_hour - current_day).total_seconds() / 3600
            time_offset += wait_time

            agent.add_rest(wait_time, reason=f"wind")
            if logger.level <= logging.INFO:
                logger.info(f"PauseOnWind: Agent {agent.uid} in hub {agent.this_hub} is resting due to wind gust.")

        return time_offset, do_pause, False