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
from sitt import SimulationPrepareDayInterface, Configuration, Context, Agent
from timezonefinder import TimezoneFinder
from suntime import Sun

logger = logging.getLogger()

# CONSTANT
TO_RAD = math.pi/180.0

class StartStopTimePreparation(SimulationPrepareDayInterface):
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

    def prepare_for_new_day(self, config: Configuration, context: Context, agent: Agent):
        try:
            # calculate current day
            current_day: dt.date = config.start_date + dt.timedelta(days=agent.current_day - 1)
            current_position: Point = context.get_hub_by_id(agent.this_hub)['geom']

            # get timezone of current position
            time_zone: dt.tzinfo = tz.gettz(self.tf.timezone_at(lng=current_position.x, lat=current_position.y))

            # create Sun entry for coordinates
            sun = Sun(current_position.y, current_position.x)

            # On a special date in your machine's local time zone
            current_dt = dt.datetime(current_day.year, current_day.month, current_day.day, 0, 0, 0, 0, time_zone)
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

            agent.current_time = sunrise.hour + sunrise.minute/60
            agent.max_time = sunset.hour + sunset.minute/60
        except Exception as ex:
            print(ex)
            # ignore exceptions completely
            pass

    @staticmethod
    def _force_range(v, max):
        # force v to be >= 0 and < max
        if v < 0:
            return v + max
        elif v >= max:
            return v - max
        return v
