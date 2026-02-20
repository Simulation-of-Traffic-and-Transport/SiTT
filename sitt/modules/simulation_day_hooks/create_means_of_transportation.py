# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This preparation will add a certain padding to the agent's start and stop time.
"""
import copy
import logging

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults
from sitt.base import generate_id

logger = logging.getLogger()

class CreateMeansOfTransportation(SimulationDayHookInterface):
    def __init__(self, types: list[str] = None):
        super().__init__()
        self.types: list[str] = types
        """Types of means of transportation to be created."""

    def run(self, config: Configuration, context: Context, agents: list[Agent], agents_finished_for_today: list[Agent],
            results: SetOfResults, current_day: int) -> list[Agent]:
        # Skip with warning if no types specified
        if self.types is None or len(self.types) == 0:
            logger.warning("No types of means of transportation specified.")
            return agents

        # multiply agents by means of transportation
        typed_agents = []

        for agent in agents:
            for new_type in self.types:
                new_agent = copy.deepcopy(agent)
                new_agent.uid = generate_id()
                new_agent.additional_data['ttype'] = new_type

                typed_agents.append(new_agent)

        return typed_agents

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        pass
