# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simulation Hook that uses resting rules to simulate agents pausing and recovering."""
import logging
import datetime as dt

import igraph as ig

from sitt import Configuration, Context, Agent
from sitt.base import SimulationStepHookInterface

logger = logging.getLogger()

class Resting(SimulationStepHookInterface):
    def __init__(self, rest_times = [{'after_minutes': 180, 'pause_minutes': 20}, {'after_minutes': 60, 'pause_minutes': 5}], noon: bool = True, noon_start: float = 11., noon_end: float = 14., noon_pause_minutes: int = 60, noon_gap_to_last_rest: int = 60, noon_gap_max_pause: int = 15, noon_gap_min_gap: int = 30):
        super().__init__()
        self.rest_times: list[dict] = rest_times
        """Resting rules for different time periods."""
        self.noon: bool = noon
        """Whether to consider noon as a resting time."""
        self.noon_start: float = noon_start
        """Start of noon (in hours)."""
        self.noon_end: float = noon_end
        """End of noon (in hours)."""
        self.noon_pause_minutes: int = noon_pause_minutes
        """Pause time during noon (in minutes)."""
        self.noon_gap_to_last_rest: int = noon_gap_to_last_rest
        """Maximum gap to the last rest during noon (in minutes)."""
        self.noon_gap_max_pause: int = noon_gap_max_pause
        """Maximum rest time (in minutes) that can occur in the gap."""
        self.noon_gap_min_gap: int = noon_gap_min_gap
        """Minimum gap to the last rest during noon (in minutes)."""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, coords: tuple,
                 time_offset: float) -> tuple[float, bool]:

        # Get current day time (in hours)
        now = agent.current_time + time_offset

        # Check if it's noon
        if self.is_noon(now):
            min_gap = self.noon_gap_min_gap / 60.0
            most_recent_rest = agent.get_most_recent_rest_time()
            if most_recent_rest is None or most_recent_rest <= now - min_gap:
                after = self.noon_gap_to_last_rest / 60.0
                max_pause = self.noon_gap_max_pause / 60.0
                rest_length = agent.get_longest_rest_time_within(now, after)
                if rest_length is None or rest_length <= max_pause:
                    pause = self.noon_pause_minutes / 60.
                    agent.add_rest(pause, time=now)
                    time_offset += pause
                    return time_offset, False

        # Check if it's a resting time
        for rest_time in self.rest_times:
            after = rest_time['after_minutes']/60.0

            # skip times that are too early
            if now - after <= agent.start_time:
                continue

            pause = rest_time['pause_minutes'] / 60.
            # check longest rest time within the given time period
            rest_length = agent.get_longest_rest_time_within(now, after)
            if rest_length is None or rest_length < pause:
                # no rest found, add one
                agent.add_rest(pause, time=now)
                time_offset += pause

        return time_offset, False

    def is_noon(self, now: float) -> bool:
        return self.noon and self.noon_start <= now <= self.noon_end