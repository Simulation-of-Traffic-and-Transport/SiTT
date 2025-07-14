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


class PauseOnXDegrees(SimulationStepHookInterface):
    def __init__(self, pause_threshold=25., adjust_temp_with_height=True, adjust_temp_step = 0.65):
        super().__init__()
        self.pause_threshold: float = pause_threshold
        """Temperature threshold at which the agent should pause."""
        self.adjust_temp_with_height: bool = adjust_temp_with_height
        """Adjust temperature to the mean height - adjust_temp_step degrees per 100 meters."""
        self.adjust_temp_step: float = adjust_temp_step
        """Step size for adjusting temperature."""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, coords: tuple,
                 time_offset: float) -> tuple[float, bool]:

        current_day = config.get_agent_date(agent, time_offset)

        count_data = 0
        temperature: float | None = None
        mean_height: float | None = None
        if len(context.space_time_data):
            for key in context.space_time_data:
                values = context.space_time_data[key].get(coords[1], coords[0], current_day, fields=['temperature', 'mean_height'])

                if 'temperature' in values:
                    temperature = values['temperature']
                    count_data += 1
                if self.adjust_temp_with_height and 'mean_height' in values:
                    mean_height = values['mean_height']
                    count_data += 1

                if count_data >= 2:
                    break

        # adjust temperature to the mean height - self.adjust_temp_step degrees per 100 meters (default is 1)
        if mean_height is not None and mean_height > 0. and len(coords) > 2 and coords[2] > 0.:
            temperature += round((mean_height - coords[2]) / 100) * self.adjust_temp_step

        # too hot, add a pause and wait for the next full hour
        if temperature is not None and temperature >= self.pause_threshold:
            # update time offset to the next full hour
            next_hour = current_day + dt.timedelta(hours=1, minutes=-current_day.minute, seconds=-current_day.second,
                                                   microseconds=-current_day.microsecond)
            time_offset += (next_hour - current_day).total_seconds() / 3600

        return time_offset, False
