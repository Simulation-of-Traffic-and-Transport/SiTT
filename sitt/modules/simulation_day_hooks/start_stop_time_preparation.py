# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This preparation will add a certain padding to the agent's start and stop time.
"""
import logging
import datetime as dt
import math
from shapely import Point
from dateutil import tz
from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults
from timezonefinder import TimezoneFinder
from suntime import Sun

logger = logging.getLogger()

# CONSTANT
TO_RAD = math.pi/180.0

class StartStopTimePreparation(SimulationDayHookInterface):
    """
    This preparation will add a certain padding to the agent's start and stop time.
    It uses the timezonefinder library to get the timezone of the agent's hub.
    """

    def __init__(self, day_start_padding: float = -0.5, day_end_padding: float = 0.5):
        """
        Initialize the StartStopTimePreparation hook.

        This constructor sets up the padding values for adjusting agent start and stop times
        relative to sunrise and sunset, and initializes the timezone finder utility.

        Args:
            day_start_padding (float, optional): The number of hours to add to the sunrise time
                to determine the agent's start time. Negative values result in starting before
                sunrise. Defaults to -0.5 (30 minutes before sunrise).
            day_end_padding (float, optional): The number of hours to add to the sunset time
                to determine the agent's end time. Positive values result in ending after sunset.
                Defaults to 0.5 (30 minutes after sunset).

        Returns:
            None
        """
        super().__init__()
        self.day_start_padding: float = day_start_padding
        """add this number of hours to sunrise time"""
        self.day_end_padding: float = day_end_padding
        """add this number of hours to sunset time"""
        self.tf = TimezoneFinder()

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        """
        Execute the simulation day hook to prepare agent start and stop times based on sunrise and sunset.

        This method calculates the start and stop times for all active agents based on the sunrise and sunset
        times at their respective hub locations. It applies the configured padding to these times and updates
        the agents' time-related attributes. Additionally, it updates the 'sleep_until' data for agents that
        have finished their routes to ensure continuity with the next day's start times.

        Args:
            config (Configuration): The simulation configuration containing settings such as the start date.
            context (Context): The simulation context providing access to hub data and geographical information.
            agents (list[Agent]): The list of active agents to be prepared for the current simulation day.
            agents_finished_for_today (list[Agent]): The list of agents that have completed their activities
                for the current day (currently unused in this implementation).
            results (SetOfResults): The collection of simulation results, including agents that have completed
                their routes. Used to update sleep times for continuity.
            current_day (int): The current day number of the simulation (1-indexed).

        Returns:
            list[Agent]: The list of agents with updated start and stop times for the current day.
        """
        # calculate current day
        current_dt: dt.date = config.start_date + dt.timedelta(days=current_day - 1)

        # date times by hubs
        hubs: dict[str, tuple[float, float, float, float]] = {}

        for agent in agents:
            self.prepare_for_new_day(context, hubs, agent, current_day, current_dt)

        # update result agents, so that the end times correlate with the start times of the next day
        if len(results.agents):
            for agent in results.agents:
                # if len(agent.route) > 0:
                last_hub = agent.route[-1]
                if 'sleep_until' not in agent.additional_data and last_hub in hubs:
                    agent.additional_data['sleep_until'] = hubs[last_hub][0]

        return agents

    def prepare_for_new_day(self, context: Context, hubs: dict[str, tuple[float, float, float, float]], agent: Agent, current_day: int, current_dt: dt.date):
        """
        Prepare an agent for a new simulation day by calculating and setting time-related attributes.

        This method calculates or retrieves the sunrise, sunset, start time, and end time for the agent's
        current hub location. It updates the agent's time attributes (start_time, current_time, max_time)
        and stores sunrise and sunset information in the agent's additional data. Hub data is cached in
        the provided dictionary to avoid redundant calculations for agents at the same hub.

        Args:
            context (Context): The simulation context providing access to hub data and geographical information.
            hubs (dict[str, tuple[float, float, float, float]]): A dictionary mapping hub identifiers to tuples
                containing (sunrise, sunset, start_time, end_time) in hours since simulation start. Used as a
                cache to avoid recalculating times for the same hub.
            agent (Agent): The agent to be prepared for the new day. Its time attributes and additional data
                will be updated.
            current_day (int): The current day number of the simulation (1-indexed).
            current_dt (dt.date): The current date for which to calculate the times.

        Returns:
            None: This method modifies the agent and hubs dictionary in place and does not return a value.
        """
        try:
            # aggregate hub data
            if agent.this_hub not in hubs:
                sunrise, sunset, start_time, end_time = self.get_start_end_time_for_hub(context, current_dt, current_day, agent.this_hub)
                hubs[agent.this_hub] = (sunrise, sunset, start_time, end_time)
            else:
                sunrise, sunset, start_time, end_time = hubs[agent.this_hub]

            agent.start_time = start_time
            agent.current_time = start_time
            agent.max_time = end_time
            agent.additional_data['sunrise'] = sunrise
            agent.additional_data['sunset'] = sunset
        except Exception as ex:
            print(ex)
            # ignore exceptions completely
            pass

    def get_start_end_time_for_hub(self, context: Context, current_dt: dt.date, current_day: int, hub: str) -> tuple[float, float, float, float]:
        """
        Get the start and end time for a specific hub on a given day.

        This method calculates the start and end times based on the sunrise and sunset at the hub's geographical
        location. It applies the configured start and end padding to these times. The times are returned as hours
        since the beginning of the simulation.

        Args:
            context (Context): The simulation context, used to retrieve hub data.
            current_dt (dt.date): The current date for which to calculate the times.
            current_day (int): The current day number of the simulation.
            hub (str): The identifier of the hub.

        Returns:
            tuple[float, float]: A tuple containing the calculated start time and end time in hours since the
            simulation began.
        """
        current_position: Point = context.get_hub_by_id(hub)['geom']

        # get timezone of current position
        time_zone: dt.tzinfo = tz.gettz(self.tf.timezone_at(lng=current_position.x, lat=current_position.y))

        # create Sun entry for coordinates
        sun = Sun(current_position.y, current_position.x)

        # Create a date in your machine's local time zone
        start_dt = dt.datetime(current_dt.year, current_dt.month, current_dt.day, 12, 0, 0, 0, time_zone)
        end_dt = start_dt + dt.timedelta(hours=24) # add a day; for some reason, sunset has to be defined like this...

        sunrise = sun.get_sunrise_time(start_dt, time_zone)
        sunset = sun.get_sunset_time(end_dt, time_zone)

        # remove dst offset to make statistics a bit more straightforward
        sunrise -= dt.timedelta(seconds=start_dt.dst().seconds)
        sunset -= dt.timedelta(seconds=end_dt.dst().seconds)

        # adjust with deltas for sunrise and sunset
        sunrise_adjusted = sunrise + dt.timedelta(hours=self.day_start_padding)
        sunset_adjusted = sunset + dt.timedelta(hours=self.day_end_padding)
        # technically, sunset will be different at the destination - on the other hand, this will hardly make a
        # difference in a real-world scenario (a few minutes at most).

        return (self._calculate_hour(current_day, sunrise),
                self._calculate_hour(current_day, sunset),
                self._calculate_hour(current_day, sunrise_adjusted),
                self._calculate_hour(current_day, sunset_adjusted))

    @staticmethod
    def _calculate_hour(current_day: int, date_time: dt.datetime) -> float:
        """
        Calculate the absolute hour since the start of the simulation.

        This method converts a datetime object to a floating-point hour value representing
        the time elapsed since the beginning of the simulation (day 1, hour 0). The calculation
        accounts for the current simulation day and the specific time within that day.

        Args:
            current_day (int): The current day number of the simulation (1-indexed, where 1 is
                the first day of the simulation).
            date_time (dt.datetime): The datetime object to convert, containing the hour and
                minute components to be converted to a fractional hour value.

        Returns:
            float: The total number of hours since the start of the simulation, including
                fractional hours derived from minutes. For example, day 1 at 12:30 would
                return 12.5, while day 2 at 12:30 would return 36.5.
        """
        # calculate hour since start of day
        return date_time.hour + date_time.minute / 60 + (current_day - 1) * 24

    @staticmethod
    def _force_range(v, max):
        """
        Force a value to be within the range [0, max).

        This method normalizes a value to ensure it falls within the valid range from 0 (inclusive)
        to max (exclusive). If the value is negative, it wraps around by adding max. If the value
        is greater than or equal to max, it wraps around by subtracting max. This is useful for
        circular or periodic values such as angles or time values.

        Args:
            v (float): The value to be normalized into the valid range.
            max (float): The maximum value (exclusive) of the range. The valid range is [0, max).

        Returns:
            float: The normalized value that falls within the range [0, max). If v is already
                within this range, it is returned unchanged. Otherwise, it is adjusted by adding
                or subtracting max as needed.
        """
        # force v to be >= 0 and < max
        if v < 0:
            return v + max
        elif v >= max:
            return v - max
        return v

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        pass

