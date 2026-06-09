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
    """
    Handles the logic for scheduling rest periods for simulation agents during their tasks.

    This class implements rules for rest scheduling using predefined rest times and specific
    criteria for special rest periods during noon hours. The purpose is to simulate rest
    behaviors in agents, ensuring periodic breaks according to the defined configurations and
    conditions.

    :ivar rest_times: Resting rules specifying conditions such as the time elapsed since the
        last rest and the pause duration. Each rule is a dictionary containing the keys
        'after_minutes' and 'pause_minutes'.
    :type rest_times: list[dict]
    :ivar noon: Flag indicating whether the simulation should consider noon as a special
        resting period.
    :type noon: bool
    :ivar noon_start: The hour of the day (in 24-hour format) when noon rest starts.
    :type noon_start: float
    :ivar noon_end: The hour of the day (in 24-hour format) when noon rest ends.
    :type noon_end: float
    :ivar noon_pause_minutes: The duration of the noon rest period in minutes.
    :type noon_pause_minutes: int
    :ivar noon_gap_to_last_rest: The maximum time gap allowed (in minutes) since the last
        rest for noon rest eligibility.
    :type noon_gap_to_last_rest: int
    :ivar noon_gap_max_pause: The maximum rest duration (in minutes) allowed in the gap
        between the last rest and noon rest.
    :type noon_gap_max_pause: int
    :ivar noon_gap_min_gap: The minimum time gap allowed (in minutes) since the last rest
        for noon rest eligibility.
    :type noon_gap_min_gap: int
    """
    def __init__(self, rest_times = [{'after_minutes': 160, 'pause_minutes': 20}, {'after_minutes': 55, 'pause_minutes': 5}],
                 noon: bool = True, noon_start: float = 11., noon_end: float = 14., noon_pause_minutes: int = 60,
                 noon_gap_to_last_rest: int = 60, noon_gap_max_pause: int = 20, noon_gap_min_gap: int = 30, skip: dict = None):
        """
        Initializes the configuration for defining resting periods, including custom schedules and specific rules
        for the noon time frame. This constructor allows flexibility in defining rest times at different intervals
        and settings for noon-related pauses.

        :param rest_times: List of dictionaries where each dictionary specifies 'after_minutes' indicating the time in
            minutes after which rest is needed, and 'pause_minutes' specifying the duration of rest in minutes.
        :type rest_times: list[dict]
        :param noon: Determines whether or not to incorporate a noon pause in the resting rules.
        :type noon: bool
        :param noon_start: The starting hour (in decimal format) at which noon begins.
        :type noon_start: float
        :param noon_end: The ending hour (in decimal format) at which noon ends.
        :type noon_end: float
        :param noon_pause_minutes: The duration of the pause during noon in minutes.
        :type noon_pause_minutes: int
        :param noon_gap_to_last_rest: The maximum allowed time gap between the last rest and the noon pause in minutes.
        :type noon_gap_to_last_rest: int
        :param noon_gap_max_pause: The maximum rest time (in minutes) allowed in the gap to noon.
        :type noon_gap_max_pause: int
        :param noon_gap_min_gap: The minimum gap (in minutes) required before noon from the last rest.
        :type noon_gap_min_gap: int
        :param skip: Optional parameter to specify additional conditions or formats to skip certain rest calculations.
        :type skip: dict or None
        """
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

    def run_hook(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge, i: int, coords: tuple,
                 time_offset: float) -> tuple[float, bool, bool]:
        """
        Executes the hook process to manage and potentially adjust the agent's time offset,
        ensuring periodic rest intervals and special noon rest logic are applied based on
        the agent's schedule and activity.

        This function evaluates multiple conditions, including skip conditions, the current
        time of day, and the agent’s resting history. It updates the time offset, rest flags,
        and agent's data attributes accordingly. This ensures the simulation adheres to required
        rest patterns and constraints.

        :param config: Configuration settings relevant to the simulation environment.
        :type config: Configuration
        :param context: Context object providing shared or dynamic information for simulation.
        :type context: Context
        :param agent: The agent instance whose state and behavior are being managed.
        :type agent: Agent
        :param next_leg: The next segment (or leg) of the agent's planned journey.
        :type next_leg: ig.Edge
        :param i: The index or identifier for the current operation or leg in sequence.
        :type i: int
        :param coords: Coordinates relevant to the current agent's position or operation.
        :type coords: tuple
        :param time_offset: Current time offset for agent actions, in hours.
        :type time_offset: float
        :return: A tuple containing the updated time_offset, a boolean indicating if rest was
                 taken, and another boolean for additional conditions (currently unused).
        :rtype: tuple[float, bool, bool]
        """
        # check skip conditions
        if self.do_skip(agent, next_leg):
            return time_offset, False, False

        # Get current time data
        now = agent.current_time + time_offset
        time_of_day = now % 24.

        # reset at start of day
        if now == agent.start_time:
            agent.additional_data['noon_rest'] = False
            return time_offset, False, False

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
                    return time_offset, True, False

        # Check if it's a resting time
        had_rest = False
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
                had_rest = True
                break

        return time_offset, had_rest, False

    def is_noon(self, time_of_day: float) -> bool:
        """
        Determines if the given time of day falls within the defined noon period.

        This method evaluates whether the provided time of day is within the
        start and end time for noon, given that the noon period is active.

        :param time_of_day: The time of day to be checked.
        :type time_of_day: float
        :return: True if the time of day is within the noon period and the noon
                 period is active, otherwise False.
        :rtype: bool
        """
        return self.noon and self.noon_start <= time_of_day <= self.noon_end
