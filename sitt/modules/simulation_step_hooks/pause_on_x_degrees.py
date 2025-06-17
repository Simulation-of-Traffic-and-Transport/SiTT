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
    def __init__(self, pause_threshold=25.):
        super().__init__()
        self.pause_threshold: float = pause_threshold
        """Temperature threshold at which the agent should pause."""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, coords: tuple,
                 time_offset: float) -> tuple[float, bool]:

        current_day = config.get_agent_date(agent, time_offset)

        temperature: float | None = None
        if len(context.space_time_data):
            for key in context.space_time_data:
                values = context.space_time_data[key].get(coords[1], coords[0], current_day, fields=['temperature'])

                if 'temperature' in values:
                    temperature = values['temperature']
                    break

        # too hot, add a pause and wait for the next full hour
        if temperature >= self.pause_threshold:
            # update time offset to the next full hour
            next_hour = current_day + dt.timedelta(hours=1, minutes=-current_day.minute, seconds=-current_day.second,
                                                   microseconds=-current_day.microsecond)
            time_offset += (next_hour - current_day).total_seconds() / 3600

        return time_offset, False
