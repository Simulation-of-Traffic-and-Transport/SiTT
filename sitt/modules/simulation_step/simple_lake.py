# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Simple lake stepper that assumes a fixed speed on lakes (like being rowed)."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class SimpleLake(SimulationStepInterface):
    def __init__(self, speed: float = 3.):
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge,
                     is_reversed: bool) -> State:
        # skipped?
        if self.skip:
            return agent.state

        # not a lake?
        if next_leg['type'] != 'lake':
            logger.error(f"SimulationInterface SimpleLake error, path {agent.route_key} is not a lake")
            agent.state.signal_stop_here = True
            return agent.state

        # traverse and calculate time taken for this leg of the journey
        time_taken = 0.
        time_for_legs: list[float] = []

        # create range to traverse - might be reversed
        r = range(len(next_leg['legs']))
        if is_reversed:
            r = reversed(r)

        for i in r:
            coords = next_leg['geom'].coords[i]
            # run hooks
            if not self.run_hooks(config, context, agent, next_leg, coords, time_taken):
                if logger.level <= logging.DEBUG:
                    logger.debug(f"SimulationInterface hooks run, {agent} cancelled")
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

        # save things in state
        agent.state.time_taken = time_taken
        agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleLake run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleLake"
