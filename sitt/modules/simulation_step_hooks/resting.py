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
    def __init__(self, rest_times = [{'after_minutes': 160, 'pause_minutes': 20}, {'after_minutes': 55, 'pause_minutes': 5}],
                 noon: bool = True, noon_start: float = 11., noon_end: float = 14., noon_pause_minutes: int = 60,
                 noon_gap_to_last_rest: int = 60, noon_gap_max_pause: int = 20, noon_gap_min_gap: int = 30, skip: dict = None):
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
        self.skip: dict = skip
        """Dictionary to skip resting times for specific agents."""

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, coords: tuple,
                 time_offset: float) -> tuple[float, bool]:
        # check skip conditions
        if self.do_skip(agent, next_leg):
            return time_offset, False

        # Get current day time (in hours)
        now = agent.current_time + time_offset
        time_of_day = now % 24.

        # reset at time_offset
        if agent.current_time == agent.start_time:
            agent.additional_data['noon_rest'] = False
            return time_offset, False

        # Check if it's noon
        if self.is_noon(time_of_day) and not agent.additional_data.get('noon_rest', False):
            min_gap = self.noon_gap_min_gap / 60.0
            most_recent_rest = agent.get_most_recent_rest_time()
            if most_recent_rest is None or most_recent_rest <= now - min_gap:
                after = self.noon_gap_to_last_rest / 60.0
                max_pause = self.noon_gap_max_pause / 60.0
                rest_length = agent.get_longest_rest_time_within(now, after)
                if rest_length is None or rest_length <= max_pause:
                    # do noon rest
                    pause = self.noon_pause_minutes / 60.
                    agent.add_rest(pause, time=now, reason='noon')
                    time_offset += pause
                    # set flag
                    agent.additional_data['noon_rest'] = True
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
                agent.add_rest(pause, time=now, reason=f"{rest_time['pause_minutes']}mins")
                time_offset += pause

        return time_offset, False

    def is_noon(self, time_of_day: float) -> bool:
        return self.noon and self.noon_start <= time_of_day <= self.noon_end

    def do_skip(self, agent: Agent, next_leg: ig.Edge):
        # check skip conditions
        if self.skip and len(self.skip) > 0:

            # additional data check - e.g. agent has a specific additional data type set
            if 'additional_data' in self.skip and len(self.skip['additional_data']) > 0:
                for key, values in self.skip['additional_data'].items():
                    if key not in agent.additional_data or agent.additional_data[key] in values:
                        return True
        return False