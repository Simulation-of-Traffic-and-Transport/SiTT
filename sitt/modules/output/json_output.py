# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create basic json output"""
import json
import logging
from typing import Dict, List, Tuple

from shapely.geometry import mapping

from sitt import Agent, Configuration, Context, OutputInterface, SetOfResults, is_truthy

logger = logging.getLogger()


class JSONOutput(OutputInterface):
    """Create basic json output"""

    def __init__(self, to_string: bool = True, show_output: bool = False, save_output: bool = False,
                 filename: str = 'simulation_output.json', indent: int = 0):
        super().__init__()
        self.to_string: bool = to_string
        """Convert data to string"""
        self.show_output: bool = show_output
        """Display output in logging"""
        self.save_output: bool = save_output
        """Save output to file?"""
        self.filename: str = filename
        """Filename for output file?"""
        self.indent: int | None = indent
        """Display JSON nicely (if > 0, indent by this number of spaces)?"""

        self.config: Configuration | None = None
        self.context: Context | None = None

    def run(self, config: Configuration, context: Context, set_of_results: SetOfResults) -> str:
        if self.skip:
            return ''

        logger.info("OutputInterface JSONOutput run")

        self.config = config
        self.context = context

        # indent 0 is treated as no indent
        if self.indent == 0:
            self.indent = None

        # create dictionary from data using methods below
        result = self.create_dict_from_data(set_of_results)
        if self.to_string:
            result = json.dumps(result, indent=self.indent)
        if self.show_output:
            # always log at log level to show output
            logger.log(logger.level, result)

        if self.save_output:
            file = open(self.filename, 'w')

            # already converted to string?
            if self.to_string:
                file.write(result)
            else:
                file.write(json.dumps(result, indent=self.indent))

            file.close()

        return result

    def create_dict_from_data(self, set_of_results: SetOfResults) -> Dict[str, any]:
        """create a dict from passed data"""

        agents_finished, history = self._agent_list_to_data(set_of_results.agents_finished)
        agents_cancelled, merge_history = self._agent_list_to_data(set_of_results.agents_cancelled)

        # merge full list
        history = self._merge_history_lists(history, merge_history)

        nodes, paths = self._graph_to_data(set_of_results.agents_finished + set_of_results.agents_cancelled)

        # TODO add more data from configuration and context
        return {
            "simulation_start": self.config.simulation_start,
            "simulation_end": self.config.simulation_end,
            "agents_finished": agents_finished,
            "agents_cancelled": agents_cancelled,
            "history": list(history.values()),
            "nodes": nodes,
            "paths": paths,
        }

    def _agent_list_to_data(self, agents: List[Agent]) -> Tuple[List[dict], Dict[str, Dict[str, any]]]:
        """converts a list of agents to raw data"""
        main_agent_list: List[dict] = []
        agent_list: Dict[str, Dict[str, any]] = {}

        for agent in agents:
            # get data, is a dict of agent data and list of agents
            agent_data, added_list = self._agent_to_data(agent)

            # aggregate agent data
            agent_list = self._merge_history_lists(agent_list, added_list)

            main_agent_list.append(agent_data)

        return main_agent_list, agent_list

    def _agent_to_data(self, agent: Agent) -> Tuple[dict, Dict[str, Dict[str, any]]]:
        """converts a single agent to raw data, it is a dict of agent data and the agent list with leg data"""

        status: str = 'undefined'
        day: int = 0
        if agent.day_cancelled >= 0:
            status = 'cancelled'
            day = agent.day_cancelled
        if agent.day_finished >= 0:
            status = 'finished'
            day = agent.day_finished

        history: Dict[str, Dict[str, any]] = {}
        # keeps unique list of agent ids
        uids: set = {agent.uid}

        # add edges to history
        for edge in agent.route_data.es:
            if 'agents' in edge.attribute_names():
                edge_key = edge['key']
                from_key = agent.route_data.vs[edge.source]['name']
                from_to = agent.route_data.vs[edge.target]['name']
                # remove _rev from edge key
                if edge_key.endswith('_rev'):
                    edge_key = edge_key[:-4]
                    from_key = from_to
                    from_to = agent.route_data.vs[edge.source]['name']
                history[edge_key] = {
                    "type": "edge",
                    "id": edge_key,
                    "from": from_key,
                    "to": from_to,
                    "agents": edge['agents'],
                }

                # add to list of agent ids
                for ag in edge['agents']:
                    uids.add(ag)

        # add hubs to history
        for hub in agent.route_data.vs:
            if 'agents' in hub.attribute_names():
                history[hub['name']] = {
                    "type": "node",
                    "id": hub['name'],
                    "agents": hub['agents'],
                }

        agent = {
            "uid": agent.uid,
            "uids": list(uids),
            "status": status,
            "day": day,
            "hour": agent.current_time,
        }

        return agent, history

    def _merge_history_lists(self, list1: Dict[str, Dict[str, any]], list2: Dict[str, Dict[str, any]]) -> Dict[str, Dict[str, any]]:
        """Helper to merge agent lists"""

        for key in list2:
            if key not in list1:
                list1[key] = list2[key]
            else:
                if 'agents' not in list1[key]:
                    list1[key]['agents'] = {}
                merged = dict()
                merged.update(list1[key]['agents'])
                merged.update(list2[key]['agents'])
                list1[key]['agents'] = merged

        return list1

    def _graph_to_data(self, agents: list[Agent]) -> Tuple[List[dict], List[dict]]:
        # only the paths taken by agents
        nodes_to_add: set[str] = set()
        paths_to_add: set[str] = set()

        for agent in agents:
            for edge in agent.route_data.es:
                edge_key = edge['key']
                if edge_key.endswith('_rev'):
                    edge_key = edge_key[:-4]
                paths_to_add.add(edge_key)

            for hub in agent.route_data.vs:
                nodes_to_add.add(hub['name'])

        nodes: List[dict] = []
        paths: List[dict] = []

        # aggregate node data
        for node in self.context.graph.vs:
            if node['name'] not in nodes_to_add:
                continue

            data = {'id': node['name']}

            for key in node.attribute_names():
                if key == 'geom':
                    data['geom'] = mapping(node['geom'])
                elif key == 'overnight':
                    data['overnight'] = is_truthy(node['overnight'])
                else:
                    data[key] = node[key]

            nodes.append(data)

        # aggregate path data - from routes, because these are directed
        for path in self.context.graph.es:
            if path['name'] not in paths_to_add:
                continue

            paths.append({
                'id': path['name'],
                "from": self.context.graph.vs[path.source]['name'],
                "to": self.context.graph.vs[path.target]['name'],
                'type': path["type"],
                'length_m': path['length_m'],
                'geom': mapping(path['geom']),
            })

        return nodes, paths

    def __repr__(self):
        return json.dumps(self)

    def __str__(self):
        return "JSONOutput"
