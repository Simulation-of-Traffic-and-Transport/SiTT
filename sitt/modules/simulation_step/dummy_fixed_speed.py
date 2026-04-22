# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy stepper that runs at a fixed speed. Useful for testing."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class DummyFixedSpeed(SimulationStepInterface):
    def __init__(self, speed: float = 5.):
        """
        Initialize a DummyFixedSpeed simulation stepper with a constant travel speed.

        This constructor creates a simulation step module that calculates travel times
        based on a fixed speed value, useful for testing and baseline simulations.

        Args:
            speed (float, optional): The constant travel speed in kilometers per hour (km/h).
                Defaults to 5.0 km/h.
        """
        super().__init__()
        self.speed: float = speed

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """
        Updates the agent's state by calculating travel time based on a fixed speed.

        This method calculates the time taken to traverse the next leg of the journey
        using a constant speed (in km/h). It updates the agent's state with the calculated
        time and optionally stores individual leg times if configured to do so.

        Args:
            config (Configuration): The simulation configuration object containing settings
                such as whether to keep individual leg times.
            context (Context): The current simulation context (not used in this implementation).
            agent (Agent): The agent whose state is being updated. The agent's state will be
                modified with the calculated travel time.
            next_leg (ig.Edge): An igraph Edge object representing the next leg of the journey.
                Must contain a 'length_m' attribute specifying the leg length in meters.

        Returns:
            State: The updated state object of the agent, containing the calculated time_taken
                and optionally the time_for_legs list.
        """
        # fixed speed in kph
        agent.state.time_taken = next_leg['length_m'] / (self.speed * 1000)
        if config.keep_leg_times:
            agent.state.time_for_legs = [agent.state.time_taken]

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface DummyFixedSpeed run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "DummyFixedSpeed"
