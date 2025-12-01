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

    def __init__(self, day_start_padding: float = 0.5, day_end_padding: float = 1.):
        super().__init__()
        self.day_start_padding: float = day_start_padding
        """add this amount of hours after sunrise"""
        self.day_end_padding: float = day_end_padding
        """add this amount of hours before sunset"""
        self.tf = TimezoneFinder()

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        # calculate current day
        current_dt: dt.date = config.start_date + dt.timedelta(days=current_day - 1)

        # date times by hubs
        hubs: dict[str, tuple[float, float]] = {}

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

    def prepare_for_new_day(self, context: Context, hubs: dict[str, tuple[float, float]], agent: Agent, current_day: int, current_dt: dt.date):
        try:
            # aggregate hub data
            if agent.this_hub not in hubs:
                start_time, end_time = self.get_start_end_time_for_hub(context, current_dt, current_day, agent.this_hub)
                hubs[agent.this_hub] = (start_time, end_time)
            else:
                start_time, end_time = hubs[agent.this_hub]

            agent.start_time = start_time
            agent.current_time = start_time
            agent.max_time = end_time
        except Exception as ex:
            print(ex)
            # ignore exceptions completely
            pass

    def get_start_end_time_for_hub(self, context: Context, current_dt: dt.date, current_day: int, hub: str) -> tuple[float, float]:
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
        current_dt = dt.datetime(current_dt.year, current_dt.month, current_dt.day, 0, 0, 0, 0, time_zone)
        sunrise = sun.get_sunrise_time(current_dt, time_zone)
        sunset = sun.get_sunset_time(current_dt, time_zone)

        # remove dst offset to make statistics a bit more straightforward
        sunrise -= dt.timedelta(seconds=sunrise.dst().seconds)
        sunset -= dt.timedelta(seconds=sunset.dst().seconds)

        # adjust with deltas for sunrise and sunset
        sunrise += dt.timedelta(hours=self.day_start_padding)
        sunset -= dt.timedelta(hours=self.day_end_padding)
        # technically, sunset will be different at the destination - on the other hand, this will hardly make a
        # difference in a real-world scenario (a few minutes at most).

        return sunrise.hour + sunrise.minute / 60 + (current_day - 1) * 24, sunset.hour + sunset.minute/60 + (current_day - 1) * 24

    @staticmethod
    def _force_range(v, max):
        # force v to be >= 0 and < max
        if v < 0:
            return v + max
        elif v >= max:
            return v - max
        return v

    def finish_simulation(self, config: Configuration, context: Context, current_day: int) -> None:
        pass

