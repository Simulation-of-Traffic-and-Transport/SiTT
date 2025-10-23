# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""
Prune the agent list to reduce the number of agents to only unique ones, merging route data for duplicates.
"""
from sitt import SimulationPrePostPrepareDayInterface, Configuration, Context, Agent, SetOfResults


class PruneAgentList(SimulationPrePostPrepareDayInterface):
    """
    Prune the agent list to reduce the number of agents to only unique ones, merging route data for duplicates.
    """

    def prepare_for_new_day(self, config: Configuration, context: Context, agents: list[Agent],
                            results: SetOfResults) -> list[Agent]:
        """
        Prune the agent list to reduce the number of agents to only unique ones, merging route data for duplicates.

        This method iterates through a list of agents, identifies duplicates based on their hash,
        and merges the graph data (routes) of these duplicates into a single agent entry.
        This is useful for consolidating agent information before the start of a new simulation day.

        :param config: The simulation configuration. Not used in this method.
        :param context: The simulation context. Not used in this method.
        :param agents: The list of agents to be pruned.
        :param results: The set of results from previous simulation steps. Not used in this method.
        :return: A new list of agents containing only unique agents with their route data merged.
        """

        hashed_agents: dict[str, Agent] = {}

        for agent in agents:
            hash_id = agent.hash()
            if hash_id not in hashed_agents:
                hashed_agents[hash_id] = agent
            else:
                # merge graphs - we want to have all possible graphs at the end

                # we start with copying/merging hub data
                for hub in agent.route_data.vs:
                    if 'agents' in hub.attribute_names():
                        try:
                            data = hashed_agents[hash_id].route_data.vs.find(name=hub['name'])
                            if 'agents' not in data.attribute_names():
                                data['agents'] = {}
                            for uid in hub['agents']:
                                if uid not in data['agents']:
                                    data['agents'][uid] = hub['agents'][uid]
                        except:
                            hashed_agents[hash_id].route_data.add_vertices(1, attributes=hub.attributes())

                # now connect edges
                for edge in agent.route_data.es:
                    try:
                        data = hashed_agents[hash_id].route_data.es.find(key=edge['key'])
                        if 'agents' not in data.attribute_names():
                            data['agents'] = {}
                        for uid in edge['agents']:
                            if uid not in data['agents']:
                                data['agents'][uid] = edge['agents'][uid]
                    except:
                        hashed_agents[hash_id].route_data.add_edge(edge.source_vertex['name'],
                                                                   edge.target_vertex['name'], agents=edge['agents'],
                                                                   key=edge['key'])

        return list(hashed_agents.values())
