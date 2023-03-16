# SPDX-FileCopyrightText: 2022-present Maximilian Kalus <info@auxnet.de>
#
# SPDX-License-Identifier: MIT
"""Create basic json output"""
import json
import logging
from typing import Dict, List, Tuple

import networkx as nx
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

        nodes, paths = self._graph_to_data()

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

        # add legs to history
        for leg in agent.route_data.edges(data=True, keys=True):
            if 'agents' in leg[3]:
                history[leg[2]] = {
                    "type": "edge",
                    "id": leg[2],
                    "from": leg[0],
                    "to": leg[1],
                    "agents": leg[3]['agents'],
                }

                # add to list of agent ids
                for ag in leg[3]['agents']:
                    uids.add(ag)

        # add hubs to history
        for hub in agent.route_data.nodes(data=True):
            if 'agents' in hub[1]:
                history[hub[0]] = {
                    "type": "node",
                    "id": hub[0],
                    "agents": hub[1]['agents'],
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
            elif len(list1[key]['agents']) != len(list2[key]['agents']):
                logger.warning('############# Different lengths of lists:', list1[key]['agents'], list2[key]['agents'])
                # TODO: fix this if we have cases

        return list1

    def _graph_to_data(self) -> Tuple[List[dict], List[dict]]:
        nodes: List[dict] = []
        paths: List[dict] = []

        # aggregate node data
        for node in self.context.graph.nodes(data=True):
            data = {'id': node[0]}

            for key in node[1]:
                if key == 'geom':
                    data['geom'] = mapping(node[1]['geom'])
                elif key == 'overnight':
                    data['overnight'] = is_truthy(node[1]['overnight'])
                else:
                    data[key] = node[1][key]

            nodes.append(data)

        # aggregate path data - from routes, because these are directed
        for path in self.context.graph.edges(data=True, keys=True):
            paths.append({
                'id': path[2],
                'from': path[3]["hubaid"],
                'to': path[3]["hubbid"],
                'length_m': path[3]['length_m'],
                'geom': mapping(path[3]['geom']),
            })

        return nodes, paths

    def __repr__(self):
        return json.dumps(self)

    def __str__(self):
        return "JSONOutput"
