# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Cached Lake stepper that assumes a constant speed on lakes (like being rowed). It enhances the SimpleLake stepper
with a caching mechanism.
"""
import json
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State
from .cached_interface import CachedInterface

logger = logging.getLogger()


class CachedLake(CachedInterface):
    """
    Cached Lake stepper that assumes a constant speed on lakes (like being rowed). It enhances the SimpleLake stepper
    with a caching mechanism.
    """

    def __init__(self, speed: float = 3.):
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        # skipped?
        if self.skip:
            return agent.state

        # not a lake?
        if next_leg['type'] != 'lake':
            logger.error(f"SimulationInterface CachedLake error, path {agent.route_key} is not a lake")
            agent.state.signal_stop_here = True
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
                # determine speed
                current_speed = self.speed

                # calculate time taken in units (hours) for this part
                calculated_time = length / (current_speed * 1000)

                time_for_legs.append(calculated_time)
                time_taken += calculated_time

                # check if time taken exceeds max_time - should finish today
                if agent.current_time + time_taken > agent.max_time:
                    agent.state.last_coordinate_after_stop = coords
                    agent.state.signal_stop_here = True
                    break

            # save to cache
            self._save_to_cache(cache_key, config.config_key, "CachedLake", time_taken, time_for_legs)

        # save things in state
        agent.state.time_taken = time_taken

        if config.keep_leg_times:
            agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface CachedLake run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "CachedLake"
