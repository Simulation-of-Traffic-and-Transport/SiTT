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
    def __init__(self, pause_thresholds: dict = {}):
        super().__init__()
        self.pause_thresholds = pause_thresholds
        """Snow height thresholds for different transport types.  Format: {'transport_type': [{'min_slope': 0.3, 'max_snow': 0.5},...]}"""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
    time_offset: float) -> tuple[float, bool, bool]:
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
