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
    """
    Logic to pause the agent's progression when specific temperature thresholds are reached.

    This class evaluates whether an agent should pause its movement based on temperature
    conditions. The thresholds for pausing can be defined globally, per transport type, or for
    specific data types. Additionally, temperature can be adjusted based on height deviations
    to reflect realistic conditions.

    :ivar pause_threshold: General temperature threshold at which the agent should pause
        (default None).
    :type pause_threshold: float | None
    :ivar pause_thresholds: Dictionary specifying temperature thresholds for various transport
        types.
    :type pause_thresholds: dict[str, float]
    :ivar additional_thresholds: Additional pause thresholds for different data types
        (if set).
    :type additional_thresholds: dict[str, float]
    :ivar adjust_temp_with_height: Flag indicating whether to adjust the temperature based
        on height deviations.
    :type adjust_temp_with_height: bool
    :ivar adjust_temp_step: The step size for temperature adjustment per 100-meter height
        deviation.
    :type adjust_temp_step: float
    :ivar temperature_field: The key for accessing temperature data from the context.
    :type temperature_field: str
    """
    def __init__(self, pause_threshold=None, pause_thresholds: dict[str, float] = {}, additional_thresholds: dict[str, float] = {},
                 adjust_temp_with_height=True, adjust_temp_step = 0.65, temperature_field='t'):
        """
        Initializes an instance of the class that controls temperature thresholds and adjustments.

        The class is used to manage various temperature configurations, including a general
        pause threshold, specific thresholds for particular types, and additional thresholds.
        It also supports adjustments to temperature with respect to height variations.

        :param pause_threshold: General temperature threshold at which the agent should pause.
        :param pause_thresholds: Dictionary of specific temperature thresholds for particular types.
        :param additional_thresholds: Dictionary of additional pause thresholds for different data types.
        :param adjust_temp_with_height: Whether to adjust temperature thresholds based on height variations.
        :param adjust_temp_step: Step size for adjusting temperature due to height changes,
            measured in degrees per 100 meters.
        :param temperature_field: Key representing the field used for temperature data.

        :ivar pause_threshold: General temperature threshold at which the agent should pause.
        :ivar pause_thresholds: Dictionary representing specific temperature thresholds for particular types.
        :ivar additional_thresholds: Dictionary representing additional pause thresholds for different data types.
        :ivar adjust_temp_with_height: Boolean value determining if temperature adjustments should occur
            with height variations.
        :ivar adjust_temp_step: Step size for adjusting temperature caused by height changes.
        :ivar temperature_field: String key identifying the temperature field.
        """
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
        """
        Executes a hook for the agent's simulation process. This function performs conditions and threshold checks based
        on environmental and agent-specific parameters, updating the agent's state if necessary.

        :param config: Configuration object providing simulation configuration and utility methods.
        :type config: Configuration
        :param context: Context object containing spatial and temporal simulation data.
        :type context: Context
        :param agent: Agent object representing the simulated entity.
        :type agent: Agent
        :param next_leg: Graph edge representing the next leg of the agent's journey.
        :type next_leg: ig.Edge
        :param i: Index of the current sub-leg or segment in the leg.
        :type i: int
        :param coords: Tuple containing latitude and longitude coordinates of the current position.
        :type coords: tuple
        :param time_offset: The offset in hours from the start of the simulation for this agent.
        :type time_offset: float
        :return: Updated time offset, a flag indicating whether a pause occurred, and a boolean reserved for other state flags.
        :rtype: tuple[float, bool, bool]
        """
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
            wait_time = (next_hour - current_day).total_seconds() / 3600
            time_offset += wait_time

            agent.add_rest(wait_time, reason=f"heat")

        return time_offset, do_pause, False
