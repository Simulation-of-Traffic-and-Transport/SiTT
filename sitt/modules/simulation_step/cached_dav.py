# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Cached DAV agent which enhances the DAV agent by storing leg data in the database in order to speed up calculations
a bit.
"""
import json
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, State, Agent
from .cached_interface import CachedInterface

logger = logging.getLogger()


class CachedDAV(CachedInterface):
    """
    Cached DAV agent which enhances the DAV agent by storing leg data in the database in order to speed up calculations
    a bit.
    """

    def __init__(self, speed: float = 4.0, ascend_per_hour: float = 300, descend_per_hour: float = 400,
                 server: str = 'localhost', port: int = 5432, db: str = 'sitt', user: str = 'postgres',
                 password: str = 'postgres', schema: str = 'sitt', connection: str | None = None):
        super().__init__(server, port, db, user, password, schema, connection)
        self.speed: float = speed
        """kph of this agent"""
        self.ascend_per_hour: float = ascend_per_hour
        """m of height per hour while ascending"""
        self.descend_per_hour: float = descend_per_hour
        """m of height per hour while descending"""

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        # precalculate next hub
        if not next_leg:
            logger.error("SimulationInterface CachedDAV error, path not found ", str(agent.route_key))
            # state.status = Status.CANCELLED
            return agent.state

        # init connection and tables, if necessary
        self._initialize()

        # create cache key - reversed is ok, because name will be different
        cache_key = next_leg['name'] + '_' + str(config.get_agent_date(agent))

        # check cache
        exists, time_taken, time_for_legs = self._load_from_cache(cache_key, config.config_key)
        if exists:
            if config.keep_leg_times:
                # convert to json
                time_for_legs = json.loads(time_for_legs)
        else:
            # traverse and calculate time taken for this leg of the journey
            time_taken = 0.
            time_for_legs: list[float] = []

            # create range to traverse - might be reversed
            r = range(len(next_leg['legs']))
            if agent.state.is_reversed:
                r = reversed(r)

            for i in r:
                coords = next_leg['geom'].coords[i]
                # run hooks
                (time_taken, cancelled) = self.run_hooks(config, context, agent, next_leg, coords, time_taken)
                if cancelled:
                    if logger.level <= logging.DEBUG:
                        logger.debug(f"SimulationInterface hooks run, cancelled state")
                    return agent.state

                length = next_leg['legs'][i]  # length is in meters
                m_asc_desc = next_leg['slopes'][i] * length  # m asc/desc over this length
                if agent.state.is_reversed:
                    m_asc_desc = -m_asc_desc  # reverse m_asc_desc for descending part

                if m_asc_desc < 0:
                    up_down_time = (m_asc_desc * -1) / self.descend_per_hour if self.descend_per_hour > 0 else 0.
                else:
                    up_down_time = m_asc_desc / self.ascend_per_hour if self.ascend_per_hour > 0 else 0.

                # calculate time taken in units (hours) for this part
                calculated_time = length / self.speed / 1000 + up_down_time

                time_for_legs.append(calculated_time)
                time_taken += calculated_time

                # check if time taken exceeds max_time - should finish today
                if agent.current_time + time_taken > agent.max_time:
                    agent.state.last_coordinate_after_stop = coords
                    agent.state.signal_stop_here = True
                    break

            # save to cache
            self._save_to_cache(cache_key, config.config_key, "CachedDAV", time_taken, time_for_legs)

        # save things in state
        agent.state.time_taken = time_taken

        if config.keep_leg_times:
            agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface CachedDAV run, from {agent.this_hub} to {agent.next_hub} "
                "via {agent.route_key}, time taken = {state.time_taken:.2f}")

        return agent.state

    def _get_base_times(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge):
        # check cache
        exists, time_taken, time_for_legs = self._load_from_cache(next_leg['name'], config.config_key)
        if exists:
            if config.keep_leg_times:
                # convert to json
                time_for_legs = json.loads(time_for_legs)

        exit(0)

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CachedDAV"
