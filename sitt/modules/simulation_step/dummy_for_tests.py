# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Dummy stepper that runs at a fixed speed. Useful for testing."""
import logging

import igraph as ig
import yaml

from sitt import Configuration, Context, Agent, State, SimulationStepInterface

logger = logging.getLogger()


class DummyForTests(SimulationStepInterface):
    def __init__(self, time_taken_per_node: float = 1., force_stop_at_node: None | str = None):
        """
        Initialize the DummyForTests simulation stepper.

        This constructor sets up a dummy stepper that simulates agent movement at a fixed speed,
        primarily used for testing purposes. It allows configuration of a constant time per node
        and an optional forced stop at a specific node.

        Args:
            time_taken_per_node (float, optional): The fixed time in time units that an agent
                takes to traverse from one node to another. Defaults to 1.0.
            force_stop_at_node (None | str, optional): The identifier of a specific node where
                the agent should be forced to stop. If None, no forced stop is applied.
                Defaults to None.
        """
        super().__init__()
        self.time_taken_per_node: float = time_taken_per_node
        self.force_stop_at_node: float = force_stop_at_node

    def update_state(self, config: Configuration, context: Context, agent: Agent, next_leg: ig.Edge) -> State:
        """
        Update the agent's state for the current simulation step.

        This method simulates agent movement at a fixed speed by updating the agent's state
        with a constant time per node. If a forced stop node is configured and matches the
        agent's current hub, the agent is signaled to stop at that location. Otherwise, the
        agent's travel time is set to the configured fixed time per node.

        Args:
            config (Configuration): The simulation configuration object containing global
                settings and parameters.
            context (Context): The current simulation context providing environmental and
                runtime information.
            agent (Agent): The agent whose state is being updated. The agent's current hub,
                next hub, route key, and state are accessed and modified.
            next_leg (ig.Edge): The next edge (leg) in the agent's route that will be
                traversed. This parameter is not used in the current implementation but
                is part of the interface.

        Returns:
            State: The updated state object of the agent, containing the modified time_taken,
                time_for_legs, and potentially signal_stop_here flag.
        """
        # Signal to stop at this stop
        if self.force_stop_at_node and agent.this_hub == self.force_stop_at_node:
            agent.state.signal_stop_here = True
        else:
            # fixed speed in kph
            agent.state.time_taken = self.time_taken_per_node

        agent.state.time_for_legs = [agent.state.time_taken]

        if not self.skip and logger.level <= logging.DEBUG:
            logger.debug(
                f"SimulationInterface DummyForTests run, from {agent.this_hub} to {agent.next_hub} via {agent.route_key}, time taken = {agent.state.time_taken:.2f}")

        return agent.state

    def __repr__(self):
        return yaml.dump(self)

    def __str__(self):
        return "DummyForTests"
