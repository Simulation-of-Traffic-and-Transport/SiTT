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
    def __init__(self, pause_threshold=None, pause_thresholds: dict[str, float] = {}, additional_thresholds: dict[str, float] = {},
                 adjust_temp_with_height=True, adjust_temp_step = 0.65, temperature_field='t'):
        super().__init__()
        self.pause_threshold: float | None = pause_threshold
        """General temperature threshold at which the agent should pause (default None)."""
        self.pause_thresholds: dict[str, float] = pause_thresholds
        """Temperature threshold at which the agent should pause."""
        self.additional_thresholds: dict[str, float] = additional_thresholds
        """Additional pause thresholds for different data types (if set)."""

        self.adjust_temp_with_height: bool = adjust_temp_with_height
        """Adjust temperature to the mean height - adjust_temp_step degrees per 100 meters."""
        self.adjust_temp_step: float = adjust_temp_step
        """Step size for adjusting temperature."""
        self.temperature_field: str = temperature_field
        """Key for temperature."""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
                 time_offset: float) -> tuple[float, bool, bool]:
        # check skip conditions
        if self.do_skip(agent, next_leg):
            return time_offset, False, False

        current_day = config.get_agent_date(agent, time_offset)

        # get temperature from context data
        temperature = context.find_space_time_data(coords[1], coords[0], current_day, 't')
        if temperature is None:
            """Skip if no temperature data found."""
            return time_offset, False, False

        if self.adjust_temp_with_height:
            # adjust temperature per 100 meters
            temperature = float(temperature) + round(next_leg['height_deviation'][i] / 100) * self.adjust_temp_step

        do_pause = False

        # first, we check additional thresholds
        if len(self.additional_thresholds):
            for data_type, threshold in self.additional_thresholds.items():
                if data_type in agent.additional_data and temperature >= threshold:
                    do_pause = True
                    break

        if not do_pause and len(self.pause_thresholds) and agent.transport_type in self.pause_thresholds and temperature >= self.pause_thresholds[agent.transport_type]:
            do_pause = True

        if not do_pause and self.pause_threshold is not None and temperature >= self.pause_threshold:
            do_pause = True

        # too hot, add a pause and wait for the next full hour
        if do_pause:
            # update time offset to the next full hour
            next_hour = current_day + dt.timedelta(hours=1, minutes=-current_day.minute, seconds=-current_day.second,
                                                   microseconds=-current_day.microsecond)
            time_offset += (next_hour - current_day).total_seconds() / 3600

        return time_offset, True, False
