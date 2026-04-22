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
    """Simple lake stepper that assumes a fixed speed on lakes (like being rowed)."""

    def __init__(self, speed: float = 3.):
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """Update the agent's state for traversing a lake segment at a fixed rowing speed.

        This method simulates lake travel by calculating the time required to traverse
        a lake segment at a constant speed (representing rowing). It processes each leg
        of the journey, runs hooks at each coordinate, and handles time constraints.
        The method temporarily sets the agent's propulsion to 'sailing' during calculation.

        Args:
            config (Configuration): The simulation configuration containing settings such as
                whether to keep individual leg times.
            context (Context): The current simulation context providing environmental and
                state information.
            agent (Agent): The agent traversing the lake, containing current position, time,
                route information, and state data.
            next_leg (ig.Edge): The graph edge representing the lake segment to traverse,
                must have type 'lake' and contain geometry and leg length information.

        Returns:
            State: The updated state of the agent after processing the lake traversal,
                including time taken, coordinates, and any stop signals. Returns the
                unmodified state if the step is skipped or if the leg type is invalid.
        """
        # skipped?
        if self.skip:
            return agent.state

        # not a lake?
        if next_leg['type'] != 'lake':
            logger.error(f"SimulationInterface SimpleLake error, path {agent.route_key} is not a lake")
            agent.state.signal_stop_here = True
            return agent.state

        # temporarily add propulsion to the agent's state
        agent.additional_data['propulsion'] = 'sailing'

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

        # save things in state
        agent.state.time_taken = time_taken

        if config.keep_leg_times:
            agent.state.time_for_legs = time_for_legs

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface SimpleLake run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        # delete temporary propulsion data
        del agent.additional_data['propulsion']

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "SimpleLake"
