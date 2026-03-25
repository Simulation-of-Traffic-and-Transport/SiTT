# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
This preparation will add a certain padding to the agent's start and stop time.
"""
import copy
import logging
import math

from sitt import SimulationDayHookInterface, Configuration, Context, Agent, SetOfResults, \
    SimulationDefineStateInterface, State
from sitt.base import generate_id

logger = logging.getLogger()

class CreateMeansOfTransportation(SimulationDayHookInterface, SimulationDefineStateInterface):
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

        # multiply agents by the means of transportation
        typed_agents = []

        # memorize hubs
        visited_hubs: dict[str, set[str]] = {}
        parents: dict[str, list[str]] = {}
        # memorize which routes are defined to be traversed by which type of agents today
        route_signatures_taken: dict[str, set[str]] = {}

        for agent in agents:
            if agent.this_hub not in visited_hubs:
                visited_hubs[agent.this_hub] = set()
                parents[agent.this_hub] = []
            if agent.route_key not in route_signatures_taken:
                route_signatures_taken[agent.route_key] = set()

            # update visited hubs and route signatures taken for the current day
            visited_hubs[agent.this_hub].update(agent.visited_hubs)
            parents[agent.this_hub].append(agent.uid)
            route_signatures_taken[agent.route_key].add(agent.type_signature)

            if agent.type_signature is None:
                # pretty much only on the first day
                for new_type in self.types:
                    new_agent = copy.deepcopy(agent)
                    new_agent.uid = generate_id()
                    #new_agent.additional_data['ttype'] = new_type
                    new_agent.type_signature = new_type

                    typed_agents.append(new_agent)
            else:
                typed_agents.append(agent)

        # ignore the first day because we do not have forced routes yet
        if current_day > 1:
            # for each hub, we test if there are ways to proceed
            for hub, visited_hubs_set in visited_hubs.items():
                for edge in context.routes.incident(hub, mode='out'):
                    e = context.routes.es[edge]

                    # skip if no types were defined (e.g., not in config)
                    if 'types_per_day' not in e.attributes():
                        continue

                    # the target hub must not be in the visited hubs
                    target = e.target_vertex['name']
                    if target == hub:
                        target = e.source_vertex['name']
                    if target in visited_hubs[hub]:
                        continue

                    # get types of yesterday
                    types_per_day = e['types_per_day']
                    if types_per_day is None:
                        types_per_day = set()
                    else:
                        types_per_day = types_per_day[current_day-1] if current_day-1 in types_per_day else set()

                    # which types of signatures are still available for this route?
                    name = e['name']
                    possible_types: set[str] = set()
                    for check_type in self.types:
                        # weed out types that were taken yesterday, or will be taken today
                        if check_type not in types_per_day and name in route_signatures_taken and check_type not in route_signatures_taken[name]:
                            possible_types.add(check_type)

                    # any possible types left?
                    if len(possible_types) > 0:
                        new_agent = Agent(hub, target, name)
                        new_agent.visited_hubs = visited_hubs_set
                        new_agent.parents = parents[hub]

                        for i, new_type in enumerate(possible_types):
                            if i == 0:
                                new_agent.type_signature = new_type
                                typed_agents.append(new_agent)
                            else:
                                new_agent_clone = copy.deepcopy(new_agent)
                                new_agent_clone.uid = generate_id()
                                new_agent_clone.type_signature = new_type
                                typed_agents.append(new_agent_clone)

        return typed_agents

    def finish_simulation(self, results: SetOfResults, config: Configuration, context: Context, current_day: int) -> None:
        pass

    def define_state(self, config: Configuration, context: Context, agent: Agent) -> State:
        state = agent.state

        # ignore, if there is no last route
        if agent.last_route is None or agent.last_route == '':
            return state

        # get edge and mark that this signature has been used for a certain day
        edge = context.get_path_by_id(agent.last_route)
        if edge is not None:
            if 'types_per_day' not in edge.attributes() or edge['types_per_day'] is None:
                edge['types_per_day'] = {}
            current_day = math.floor(agent.current_time / 24) + 1
            if current_day not in edge['types_per_day']:
                edge['types_per_day'][current_day] = set()
                edge['types_per_day'][current_day].add(agent.type_signature)

        return state
